package reflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
)

// Run starts a reflow node from cfg and returns a Host. The Host owns
// goroutines and TCP listeners; call Host.Close (or cancel ctx) to shut down.
//
// Run is the only public entrypoint user binaries need. Typical usage:
//
//	cfg := reflow.Config{
//	    Node:    reflow.NodeConfig{ID: 1, RaftAddr: "127.0.0.1:5410"},
//	    Storage: reflow.StorageConfig{DataDir: "/var/lib/reflow"},
//	}
//	cfg.Handlers.Registry = sdk.NewRegistry()
//	host, err := reflow.Run(ctx, cfg)
func Run(ctx context.Context, cfg Config) (*Host, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	cfg = withDefaults(cfg)

	logger := buildLogger(cfg.Logging)
	slog.SetDefault(logger)

	var metricsRegisterer prometheus.Registerer
	var metrics *observability.Metrics
	if !cfg.Metrics.Disabled {
		if cfg.Metrics.Registry != nil {
			metricsRegisterer = cfg.Metrics.Registry
		} else {
			metricsRegisterer = prometheus.DefaultRegisterer
		}
		metrics = observability.NewMetrics(metricsRegisterer)
	}

	var metricsCloser func() error
	if !cfg.Metrics.Disabled && cfg.Metrics.Addr != "" {
		metricsCloser = startMetricsServer(cfg.Metrics, logger)
	}

	// NumPartitionShards is the routing modulus — independent of peer
	// count. Multi-node deployments may host every shard on every peer
	// so the two happen to coincide, but the engine must not bake that
	// assumption in.
	numShards := uint64(len(cfg.Cluster.Shards))
	if numShards == 0 {
		numShards = 1
	}
	shards := cfg.Cluster.Shards
	if len(shards) == 0 {
		shards = []uint64{1}
	}
	snapshotTriggers := make(map[uint64]chan struct{}, len(shards))
	for _, sh := range shards {
		snapshotTriggers[sh] = make(chan struct{}, 1)
	}
	hcfg := engine.HostConfig{
		NodeID:             cfg.Node.ID,
		RaftAddr:           cfg.Node.RaftAddr,
		DataDir:            cfg.Storage.DataDir,
		Log:                logger,
		EnableMetrics:      !cfg.Metrics.Disabled,
		GossipBindAddr:     cfg.Node.GossipBindAddr,
		GossipAdvAddr:      cfg.Node.GossipAdvAddr,
		GrpcEndpoint:       cfg.Node.DeliveryAddr,
		Peers:              toEnginePeers(cfg.Cluster.Peers),
		JoinExisting:       cfg.Cluster.JoinExisting,
		NumPartitionShards: numShards,
		Metrics:            metrics,
		OnSnapshotPersisted: func(shardID uint64) {
			ch, ok := snapshotTriggers[shardID]
			if !ok {
				return
			}
			select {
			case ch <- struct{}{}:
			default:
			}
		},
	}
	eh, err := engine.NewHost(hcfg)
	if err != nil {
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, fmt.Errorf("reflow: NewHost: %w", err)
	}

	multiNode := len(hcfg.Peers) > 0

	// All transport-security and authn/z material is built upfront so a
	// configuration error halts startup before any listener opens. Each
	// listener owns its own creds.ListenerCreds (Close threaded through
	// Host.Close); the auth interceptors are shared.
	var (
		deliveryCreds *creds.ListenerCreds
		adminCreds    *creds.ListenerCreds
		authCloser    func() error
	)
	bail := func(err error) (*Host, error) {
		if authCloser != nil {
			_ = authCloser()
		}
		_ = creds.CloseAll(deliveryCreds, adminCreds)
		_ = eh.Close()
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, err
	}

	if multiNode {
		dc, derr := creds.Build(cfg.Delivery.Creds, logger)
		if derr != nil {
			return bail(fmt.Errorf("reflow: delivery creds: %w", derr))
		}
		deliveryCreds = dc
		recordListenerSecurity(metrics, "delivery", dc)
		if dc.SecurityLevel == credentials.NoSecurity {
			logger.Warn("reflow: multi-node delivery using insecure transport — " +
				"node-to-node traffic is unauthenticated and unencrypted")
		}
	}
	if multiNode && !cfg.Admin.Disabled && cfg.Admin.Addr != "" {
		ac, aerr := creds.Build(cfg.Admin.Creds, logger)
		if aerr != nil {
			return bail(fmt.Errorf("reflow: admin creds: %w", aerr))
		}
		adminCreds = ac
		recordListenerSecurity(metrics, "admin", ac)
	}
	if multiNode {
		uIc, sIc, closer, ierr := auth.NewServerInterceptors(auth.Config{
			Extractor:  &auth.SPIFFEExtractor{TrustDomain: cfg.Auth.trustDomainOrDefault()},
			PolicyFile: cfg.Auth.PolicyFile,
			Log:        logger,
		})
		if ierr != nil {
			return bail(fmt.Errorf("reflow: auth interceptors: %w", ierr))
		}
		authCloser = closer
		_ = uIc // captured by buildServerOpts
		_ = sIc
		// stash for later — Go's flow control means we just call
		// buildServerOpts inside the listener-startup sections below.
		// Bundle them into a small struct to avoid free-variable noise.
		interceptors := &serverInterceptors{unary: uIc, stream: sIc}

		ds, dln, dc, derr := startDeliveryListener(eh, cfg, deliveryCreds, interceptors, logger)
		if derr != nil {
			return bail(derr)
		}

		// Wire the delivery host with both the inbound server and the
		// pooled outbound client. Both share the same creds spec so the
		// cluster forms a closed trust loop.
		_ = ds
		_ = dln
		eh.SetCrossShardSender(dc)

		host, herr := finishStartup(ctx, cfg, eh, multiNode, shards, snapshotTriggers,
			ds, dln, dc, deliveryCreds, adminCreds, interceptors, authCloser,
			metricsCloser, logger)
		if herr != nil {
			ds.Stop()
			_ = dln.Close()
			_ = dc.Close()
			return bail(herr)
		}
		return host, nil
	}

	// Single-node path: no Delivery listener, no Admin listener, no auth
	// interceptors. Shard 0 still runs (1-replica Raft group) so the
	// deployment registry is available; an in-memory admin.Server is
	// constructed only when Handlers.Endpoints is non-empty so
	// autoSeedEndpoints can register deployments without an external
	// admin listener.
	runner, err := eh.StartMetadataShard()
	if err != nil {
		return bail(fmt.Errorf("reflow: StartMetadataShard: %w", err))
	}
	logger.Info("reflow: metadata shard started", "shard", 0)

	for _, sh := range shards {
		if _, err := eh.StartPartition(sh); err != nil {
			return bail(fmt.Errorf("reflow: StartPartition(%d): %w", sh, err))
		}
		logger.Info("reflow: partition started", "shard", sh)
	}

	if len(cfg.Handlers.Endpoints) > 0 {
		srv, sErr := admin.NewServer(admin.Config{
			Host:   eh,
			Runner: runner,
			Log:    logger,
		})
		if sErr != nil {
			return bail(fmt.Errorf("reflow: admin.NewServer: %w", sErr))
		}
		go autoSeedEndpoints(ctx, srv, runner, cfg.Handlers.Endpoints, logger)
	}

	return &Host{
		engine:        eh,
		metricsCloser: metricsCloser,
	}, nil
}

// serverInterceptors bundles the unary+stream pair so it can be passed
// without three positional args.
type serverInterceptors struct {
	unary  grpc.UnaryServerInterceptor
	stream grpc.StreamServerInterceptor
}

// startDeliveryListener builds the Delivery client (so partitions get a
// Sender on startup) and the Delivery gRPC server. The two share one
// creds.Spec (mTLS or insecure) so the cluster forms a closed trust loop.
func startDeliveryListener(
	eh *engine.Host,
	cfg Config,
	lc *creds.ListenerCreds,
	ic *serverInterceptors,
	logger *slog.Logger,
) (*grpc.Server, net.Listener, *delivery.Client, error) {
	dc, err := delivery.NewClient(delivery.ClientConfig{
		Resolver:    eh,
		Log:         logger,
		DialOptions: lc.ClientDial,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reflow: delivery client: %w", err)
	}

	ln, err := net.Listen("tcp", cfg.Node.DeliveryAddr)
	if err != nil {
		_ = dc.Close()
		return nil, nil, nil, fmt.Errorf("reflow: listen delivery %s: %w", cfg.Node.DeliveryAddr, err)
	}

	opts := []grpc.ServerOption{
		grpc.Creds(lc.Server),
		grpc.ChainUnaryInterceptor(ic.unary),
		grpc.ChainStreamInterceptor(ic.stream),
	}
	gs := grpc.NewServer(opts...)
	deliveryv1.RegisterDeliveryServer(gs, delivery.NewServer(eh, logger))
	go func() {
		if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("reflow: delivery gRPC Serve exited", "err", err)
		}
	}()
	logger.Info("reflow: delivery listening", "addr", ln.Addr().String(),
		"driver", string(lc.Driver))
	return gs, ln, dc, nil
}

// finishStartup wires shard 0 + partition shards + optional snapshot
// producer + admin server, then packages everything into a Host. Errors
// here are surfaced by the caller which runs the bail cleanup.
func finishStartup(
	ctx context.Context,
	cfg Config,
	eh *engine.Host,
	multiNode bool,
	shards []uint64,
	snapshotTriggers map[uint64]chan struct{},
	deliverySrv *grpc.Server,
	deliveryLn net.Listener,
	deliveryClient *delivery.Client,
	deliveryCreds *creds.ListenerCreds,
	adminCreds *creds.ListenerCreds,
	ic *serverInterceptors,
	authCloser func() error,
	metricsCloser func() error,
	logger *slog.Logger,
) (*Host, error) {
	// Start shard 0 before partition shards so the partition table is
	// established as partitions come up.
	if _, err := eh.StartMetadataShard(); err != nil {
		return nil, fmt.Errorf("reflow: StartMetadataShard: %w", err)
	}
	logger.Info("reflow: metadata shard started", "shard", 0)

	for _, sh := range shards {
		if _, err := eh.StartPartition(sh); err != nil {
			return nil, fmt.Errorf("reflow: StartPartition(%d): %w", sh, err)
		}
		logger.Info("reflow: partition started", "shard", sh)
	}

	var (
		adminSrv     *grpc.Server
		adminLn      net.Listener
		snapshotCxl  context.CancelFunc
		snapshotRepo *snapshot.BlobRepository
	)

	var snapshotRepoIface snapshot.Repository
	if multiNode && cfg.Snapshot.URL != "" {
		bucket, err := snapshot.OpenBucket(context.Background(), cfg.Snapshot.URL)
		if err != nil {
			return nil, fmt.Errorf("reflow: open snapshot bucket: %w", err)
		}
		snapshotRepo = &snapshot.BlobRepository{
			Bucket: bucket,
			Retain: cfg.Snapshot.Retain,
		}
		snapshotRepoIface = snapshotRepo
		snapCtx, cancel := context.WithCancel(context.Background())
		snapshotCxl = cancel
		if cfg.Snapshot.Interval > 0 {
			source := &engine.HostSnapshotSource{Host: eh}
			for _, sh := range shards {
				go snapshot.RunProducer(snapCtx, snapshot.ProducerConfig{
					ShardID:    sh,
					Interval:   cfg.Snapshot.Interval,
					Source:     source,
					Repo:       snapshotRepoIface,
					ScratchDir: cfg.Snapshot.ScratchDir,
					Trigger:    snapshotTriggers[sh],
					Log:        logger,
				})
			}
			logger.Info("reflow: snapshot producer started",
				"interval", cfg.Snapshot.Interval, "shards", shards)
		}
		hasTiered := cfg.Snapshot.TieredDaily > 0 || cfg.Snapshot.TieredWeekly > 0 || cfg.Snapshot.TieredMonthly > 0
		if cfg.Snapshot.Retain > 0 || cfg.Snapshot.RetentionAge > 0 || hasTiered {
			for _, sh := range shards {
				go snapshot.RunReaper(snapCtx, snapshot.ReaperConfig{
					ShardID:       sh,
					Interval:      time.Hour,
					Repo:          snapshotRepoIface,
					Retain:        cfg.Snapshot.Retain,
					RetentionAge:  cfg.Snapshot.RetentionAge,
					TieredDaily:   cfg.Snapshot.TieredDaily,
					TieredWeekly:  cfg.Snapshot.TieredWeekly,
					TieredMonthly: cfg.Snapshot.TieredMonthly,
					Log:           logger,
				})
			}
			logger.Info("reflow: snapshot reaper started",
				"retain", cfg.Snapshot.Retain,
				"retention_age", cfg.Snapshot.RetentionAge,
				"tiered_daily", cfg.Snapshot.TieredDaily,
				"tiered_weekly", cfg.Snapshot.TieredWeekly,
				"tiered_monthly", cfg.Snapshot.TieredMonthly,
				"shards", shards)
		}
	}

	if multiNode && !cfg.Admin.Disabled && cfg.Admin.Addr != "" {
		runner := eh.MetadataRunner()
		if runner == nil {
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			return nil, errors.New("reflow: metadata runner not initialized; cannot start admin")
		}
		ln, lErr := net.Listen("tcp", cfg.Admin.Addr)
		if lErr != nil {
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			return nil, fmt.Errorf("reflow: listen admin %s: %w", cfg.Admin.Addr, lErr)
		}
		adminLn = ln
		srv, sErr := admin.NewServer(admin.Config{
			Host:       eh,
			Runner:     runner,
			Repo:       snapshotRepoIface,
			Source:     &engine.HostSnapshotSource{Host: eh},
			Log:        logger,
			ScratchDir: cfg.Snapshot.ScratchDir,
		})
		if sErr != nil {
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			_ = adminLn.Close()
			return nil, fmt.Errorf("reflow: admin server: %w", sErr)
		}
		adminSrv = grpc.NewServer(
			grpc.Creds(adminCreds.Server),
			grpc.ChainUnaryInterceptor(ic.unary),
			grpc.ChainStreamInterceptor(ic.stream),
		)
		srv.Register(adminSrv)
		go func() {
			if err := adminSrv.Serve(adminLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error("reflow: admin gRPC Serve exited", "err", err)
			}
		}()
		logger.Info("reflow: admin listening", "addr", adminLn.Addr().String(),
			"driver", string(adminCreds.Driver))

		// Auto-seed remote-handler deployments from config. Runs as a
		// background goroutine so a slow handler endpoint doesn't block
		// Run; each failure is logged and the next endpoint is tried.
		// ctx is the Run caller's context — cancelling it cancels the
		// seed loop.
		if len(cfg.Handlers.Endpoints) > 0 {
			go autoSeedEndpoints(ctx, srv, runner, cfg.Handlers.Endpoints, logger)
		}
	} else if len(cfg.Handlers.Endpoints) > 0 {
		logger.Warn("reflow: Handlers.Endpoints requires admin server (multi-node + Admin.Addr); skipping",
			"endpoints", len(cfg.Handlers.Endpoints))
	}

	return &Host{
		engine:         eh,
		metricsCloser:  metricsCloser,
		deliverySrv:    deliverySrv,
		deliveryLn:     deliveryLn,
		deliveryClient: deliveryClient,
		deliveryCreds:  deliveryCreds,
		adminSrv:       adminSrv,
		adminLn:        adminLn,
		adminCreds:     adminCreds,
		authCloser:     authCloser,
		snapshotCxl:    snapshotCxl,
		snapshotRepo:   snapshotRepo,
	}, nil
}

// autoSeedEndpoints waits for shard 0 leadership, then issues
// RegisterDeployment once per endpoint via the admin server's internal
// path. Already-registered endpoints aren't deduplicated — each call
// produces a fresh deployment_id because RegisterDeployment mints a new
// UUID per invocation. Operators who don't want duplicate registrations
// on every restart should configure endpoints via koanf only for one-shot
// seeding and unset the field for subsequent boots.
//
// Runs as a fire-and-forget goroutine; logs each outcome at INFO / WARN.
func autoSeedEndpoints(ctx context.Context, srv *admin.Server, runner *engine.MetadataRunner, endpoints []HandlerEndpoint, log *slog.Logger) {
	// Wait for shard 0 leadership before registering. The poll cadence
	// is 200ms — fast enough to feel snappy in tests, slow enough that
	// a non-leader node doesn't spin a CPU. Bound the wait so a stuck
	// startup doesn't keep the goroutine alive forever.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if runner != nil && runner.IsLeader() {
			break
		}
		if time.Now().After(deadline) {
			log.Warn("reflow: auto-seed endpoints: shard 0 leadership not reached within 2m; skipping",
				"endpoints", len(endpoints))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}

	for _, ep := range endpoints {
		if ctx.Err() != nil {
			return
		}
		regCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := srv.RegisterDeployment(regCtx, &adminv1.RegisterDeploymentRequest{Url: ep.URL})
		cancel()
		if err != nil {
			log.Warn("reflow: auto-seed endpoint failed", "url", ep.URL, "err", err)
			continue
		}
		log.Info("reflow: auto-seed endpoint registered",
			"url", ep.URL, "deployment_id", resp.GetDeploymentId())
	}
}

// toEnginePeers maps the public Peer type to the internal engine.Peer.
func toEnginePeers(in []Peer) []engine.Peer {
	if len(in) == 0 {
		return nil
	}
	out := make([]engine.Peer, len(in))
	for i, p := range in {
		out[i] = engine.Peer{
			NodeID:     p.NodeID,
			RaftAddr:   p.RaftAddr,
			GossipAddr: p.GossipAddr,
			NodeHostID: p.NodeHostID,
		}
	}
	return out
}

func validate(cfg Config) error {
	if cfg.Node.ID == 0 {
		return errors.New("reflow: Node.ID must be > 0")
	}
	if cfg.Node.RaftAddr == "" {
		return errors.New("reflow: Node.RaftAddr is required")
	}
	if cfg.Storage.DataDir == "" {
		return errors.New("reflow: Storage.DataDir is required")
	}
	return nil
}

func withDefaults(cfg Config) Config {
	if !cfg.Metrics.Disabled && cfg.Metrics.Addr == "" {
		cfg.Metrics.Addr = ":9090"
	}
	return cfg
}

func buildLogger(cfg LoggingConfig) *slog.Logger {
	if cfg.Handler != nil {
		return slog.New(cfg.Handler).With("service", "reflow")
	}
	return observability.NewLogger(cfg.Level)
}

// recordListenerSecurity stamps the per-listener SecurityLevel gauge so
// dashboards can flag NoSecurity (=0) listeners. Metrics may be nil when
// the operator disabled collection — in that case this is a no-op.
func recordListenerSecurity(m *observability.Metrics, listener string, lc *creds.ListenerCreds) {
	if m == nil || lc == nil {
		return
	}
	m.ListenerSecurityLevel.
		WithLabelValues(listener, string(lc.Driver)).
		Set(float64(lc.SecurityLevel))
}

func startMetricsServer(cfg MetricsConfig, log *slog.Logger) func() error {
	mux := http.NewServeMux()
	if cfg.Registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(cfg.Registry, promhttp.HandlerOpts{}))
	} else {
		mux.Handle("/metrics", promhttp.Handler())
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: cfg.Addr, Handler: mux}
	go func() {
		log.Info("reflow: metrics listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("reflow: metrics server exited", "err", err)
		}
	}()
	return srv.Close
}
