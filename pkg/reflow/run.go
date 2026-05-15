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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/observability"
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
//	cfg.Handlers = sdk.NewRegistry()
//	// cfg.Handlers.Register("Greeter", "hello", greetHandler)  // Step 9
//	host, err := reflow.Run(ctx, cfg)
func Run(ctx context.Context, cfg Config) (*Host, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	cfg = withDefaults(cfg)

	// Install the configured logger as the process default so internal
	// components that fall back to slog.Default() inherit it.
	logger := buildLogger(cfg.Logging)
	slog.SetDefault(logger)

	// Register Prometheus collectors against the chosen registry. Done
	// once at startup; the metrics handler reuses whichever registry was
	// passed.
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

	// Optionally start the /metrics HTTP server.
	var metricsCloser func() error
	if !cfg.Metrics.Disabled && cfg.Metrics.Addr != "" {
		metricsCloser = startMetricsServer(cfg.Metrics, logger)
	}

	// Bring up the internal engine Host.
	//
	// NumPartitionShards is the routing modulus — independent of peer
	// count. Phase 4.1 deployments host every shard on every peer so the
	// two happen to coincide, but the engine must not bake that
	// assumption in. We pass len(Cluster.Shards) when the caller
	// specified an explicit shard list, otherwise 1 (single-shard
	// default that matches the [1] default applied to cfg.Cluster.Shards
	// below).
	numShards := uint64(len(cfg.Cluster.Shards))
	if numShards == 0 {
		numShards = 1
	}
	// Allocate one buffered-1 trigger channel per partition shard
	// upfront so the OnSnapshotPersisted hook installed on the Host
	// has somewhere to fan signals into. The snapshot producer
	// (started later) consumes from these channels; consumer-less
	// signals queue up to one and drop after — bounded and benign.
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
		Handlers:           cfg.Handlers,
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
				// A trigger is already pending; drop. Producer will
				// pick it up on its next iteration.
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

	var deliverySrv *grpc.Server
	var deliveryLn net.Listener
	var deliveryClient *delivery.Client

	if multiNode {
		// Phase 4.2: build the Delivery TLS material upfront so both
		// the outbound client and the inbound server share one node
		// cert. When TLS files are absent, fall back to insecure for
		// dev/test ergonomics — Step 8 (reflowd CLI) enforces
		// "multi-node requires TLS" at the binary level.
		clientDialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		var serverCreds credentials.TransportCredentials
		if !cfg.TLS.IsZero() {
			td := cfg.TLS.TrustDomainOrDefault()
			clientTLS, tlsErr := BuildDeliveryClientTLS(cfg.TLS.files(), td)
			if tlsErr != nil {
				_ = eh.Close()
				if metricsCloser != nil {
					_ = metricsCloser()
				}
				return nil, fmt.Errorf("reflow: build delivery client TLS: %w", tlsErr)
			}
			clientDialOpts = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(clientTLS))}

			serverTLS, sErr := BuildDeliveryServerTLS(cfg.TLS.files(), td)
			if sErr != nil {
				_ = eh.Close()
				if metricsCloser != nil {
					_ = metricsCloser()
				}
				return nil, fmt.Errorf("reflow: build delivery server TLS: %w", sErr)
			}
			serverCreds = credentials.NewTLS(serverTLS)
		}

		// Build the delivery client first so partitions get a Sender on
		// startup. Resolver is the engine.Host itself (PartitionLeaderHint
		// + NodeEndpoint).
		dc, dcErr := delivery.NewClient(delivery.ClientConfig{
			Resolver:    eh,
			Log:         logger,
			DialOptions: clientDialOpts,
		})
		if dcErr != nil {
			_ = eh.Close()
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: delivery client: %w", dcErr)
		}
		deliveryClient = dc
		eh.SetCrossShardSender(dc)

		// Start the Delivery gRPC listener on Node.DeliveryAddr — the
		// same endpoint published via gossip NodeHostMeta. Phase 4.2 may
		// co-host this with ingress; Phase 4.1 keeps the listener
		// dedicated to keep startup ordering simple.
		ln, lnErr := net.Listen("tcp", cfg.Node.DeliveryAddr)
		if lnErr != nil {
			_ = dc.Close()
			_ = eh.Close()
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: listen delivery %s: %w", cfg.Node.DeliveryAddr, lnErr)
		}
		deliveryPolicy, dpErr := auth.BuildMethodPolicy(
			deliveryv1.File_deliveryv1_delivery_proto.Services().ByName("Delivery"))
		if dpErr != nil {
			_ = dc.Close()
			_ = ln.Close()
			_ = eh.Close()
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: build delivery authz policy: %w", dpErr)
		}
		deliveryAuthz := auth.NewProtoPolicyAuthorizer(deliveryPolicy)
		deliveryMapper := &auth.CertClaimMapper{TrustDomain: cfg.TLS.TrustDomainOrDefault()}

		gsOpts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(deliveryMapper, deliveryAuthz, logger)),
			grpc.ChainStreamInterceptor(auth.StreamInterceptor(deliveryMapper, deliveryAuthz, logger)),
		}
		if serverCreds != nil {
			gsOpts = append(gsOpts, grpc.Creds(serverCreds))
		}
		gs := grpc.NewServer(gsOpts...)
		deliveryv1.RegisterDeliveryServer(gs, delivery.NewServer(eh, logger))
		deliverySrv = gs
		deliveryLn = ln
		go func() {
			if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error("reflow: delivery gRPC Serve exited", "err", err)
			}
		}()
		logger.Info("reflow: delivery listening", "addr", ln.Addr().String())

		// Start shard 0 (metadata Raft group) before partition shards so
		// the partition table is being established as partitions come up.
		// We don't strictly need to await its leader before starting
		// partition shards in Phase 4.1 — every node hosts every
		// partition and the static peer list makes the partition table
		// redundant on the wire — but starting it first matches the
		// Phase 4.2 sequence and avoids race-prone test orderings.
		if _, mErr := eh.StartMetadataShard(); mErr != nil {
			deliverySrv.Stop()
			_ = ln.Close()
			_ = dc.Close()
			_ = eh.Close()
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: StartMetadataShard: %w", mErr)
		}
		logger.Info("reflow: metadata shard started", "shard", 0)
	}

	for _, sh := range shards {
		if _, err := eh.StartPartition(sh); err != nil {
			if deliverySrv != nil {
				deliverySrv.Stop()
				_ = deliveryLn.Close()
			}
			if deliveryClient != nil {
				_ = deliveryClient.Close()
			}
			_ = eh.Close()
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: StartPartition(%d): %w", sh, err)
		}
		logger.Info("reflow: partition started", "shard", sh)
	}

	// Phase 4.2 surfaces below this point. All optional + multi-node-only.
	var (
		adminSrv     *grpc.Server
		adminLn      net.Listener
		snapshotCxl  context.CancelFunc
		snapshotRepo *snapshot.BlobRepository
	)
	cleanup := func() {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		if adminSrv != nil {
			adminSrv.Stop()
		}
		if adminLn != nil {
			_ = adminLn.Close()
		}
		if deliverySrv != nil {
			deliverySrv.Stop()
			_ = deliveryLn.Close()
		}
		if deliveryClient != nil {
			_ = deliveryClient.Close()
		}
		_ = eh.Close()
		if metricsCloser != nil {
			_ = metricsCloser()
		}
	}

	var snapshotRepoIface snapshot.Repository
	if multiNode && cfg.Snapshot.URL != "" {
		bucket, err := snapshot.OpenBucket(context.Background(), cfg.Snapshot.URL)
		if err != nil {
			cleanup()
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
			cleanup()
			return nil, errors.New("reflow: metadata runner not initialized; cannot start admin")
		}
		if cfg.TLS.IsZero() {
			cleanup()
			return nil, errors.New("reflow: admin server requires TLS configuration")
		}
		adminTLS, atErr := BuildAdminServerTLS(cfg.TLS.files(), cfg.TLS.TrustDomainOrDefault())
		if atErr != nil {
			cleanup()
			return nil, fmt.Errorf("reflow: build admin TLS: %w", atErr)
		}
		ln, lErr := net.Listen("tcp", cfg.Admin.Addr)
		if lErr != nil {
			cleanup()
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
			cleanup()
			return nil, fmt.Errorf("reflow: admin server: %w", sErr)
		}
		adminPolicy, perr := auth.BuildMethodPolicy(
			adminv1.File_adminv1_admin_proto.Services().ByName("Admin"))
		if perr != nil {
			cleanup()
			return nil, fmt.Errorf("reflow: build admin authz policy: %w", perr)
		}
		adminAuthz := auth.NewProtoPolicyAuthorizer(adminPolicy)
		adminMapper := &auth.CertClaimMapper{TrustDomain: cfg.TLS.TrustDomainOrDefault()}
		adminSrv = grpc.NewServer(
			grpc.Creds(credentials.NewTLS(adminTLS)),
			grpc.ChainUnaryInterceptor(
				auth.UnaryInterceptor(adminMapper, adminAuthz, logger),
			),
			grpc.ChainStreamInterceptor(
				auth.StreamInterceptor(adminMapper, adminAuthz, logger),
			),
		)
		srv.Register(adminSrv)
		go func() {
			if err := adminSrv.Serve(adminLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error("reflow: admin gRPC Serve exited", "err", err)
			}
		}()
		logger.Info("reflow: admin listening", "addr", adminLn.Addr().String())
	}

	return &Host{
		engine:         eh,
		metricsCloser:  metricsCloser,
		deliverySrv:    deliverySrv,
		deliveryLn:     deliveryLn,
		deliveryClient: deliveryClient,
		adminSrv:       adminSrv,
		adminLn:        adminLn,
		snapshotCxl:    snapshotCxl,
		snapshotRepo:   snapshotRepo,
	}, nil
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
