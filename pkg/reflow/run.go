package reflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	connect "connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	httpingress "github.com/twinfer/reflow/internal/ingress/http"
	internalwebhook "github.com/twinfer/reflow/internal/ingress/webhook"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/pkg/adminclient"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
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
//	cfg.Handlers.Registry = handler.NewRegistry()
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

	// Build the engine→handler JWT signer up front when delivery creds
	// use the cert-provider driver, so it can be wired into the host's
	// handler registry at construction. Other drivers leave it nil —
	// connectclient then skips the Authorization header.
	var handlerSigner *creds.Signer
	if cfg.Delivery.Creds.Driver == creds.DriverCertProvider {
		hs, sErr := creds.BuildSigner(cfg.Delivery.Creds.CertProvider, logger)
		if sErr != nil {
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflow: handler signer: %w", sErr)
		}
		handlerSigner = hs
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
		AdminEndpoint:      cfg.Admin.Addr,
		Peers:              toEnginePeers(cfg.Cluster.Peers),
		JoinExisting:       cfg.Cluster.JoinExisting,
		NumPartitionShards: numShards,
		Metrics:            metrics,
		HandlerSigner:      handlerSigner,
		EagerStateMaxBytes: cfg.Handlers.EagerStateMaxBytes,
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
	eh, err := engine.NewHost(ctx, hcfg)
	if err != nil {
		if handlerSigner != nil {
			handlerSigner.Close()
		}
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, fmt.Errorf("reflow: NewHost: %w", err)
	}

	// Feature predicates. Each listener and ancillary service is gated on
	// its own config — peer count drives only cross-shard delivery and
	// the multi-node-insecure warning.
	crossShard := len(hcfg.Peers) > 1
	adminEnabled := !cfg.Admin.Disabled && cfg.Admin.Addr != ""

	// All transport-security and authn/z material is built upfront so a
	// configuration error halts startup before any listener opens. The
	// HTTP auth middleware is built unconditionally — admin, delivery,
	// and ingress all wrap their Connect handlers with it. The policy
	// file (or embedded starter policy when unset) is the single knob
	// for who is allowed through; "ingress is open to anonymous" is
	// expressed in the policy's ingress_open allow rule, not by
	// skipping the middleware.
	var (
		deliveryCreds  *creds.ListenerCreds
		adminCreds     *creds.ListenerCreds
		httpAuthCloser func() error
		httpAuthMW     func(http.Handler) http.Handler
	)
	bail := func(err error) (*Host, error) {
		if httpAuthCloser != nil {
			_ = httpAuthCloser()
		}
		_ = creds.CloseAll(deliveryCreds, adminCreds)
		if handlerSigner != nil {
			handlerSigner.Close()
		}
		_ = eh.Close()
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, err
	}

	mw, mwCloser, mwErr := auth.HTTPMiddleware(buildAuthConfig(cfg.Auth), logger)
	if mwErr != nil {
		return bail(fmt.Errorf("reflow: auth middleware: %w", mwErr))
	}
	httpAuthMW = mw
	httpAuthCloser = mwCloser

	if crossShard {
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
	if adminEnabled {
		ac, aerr := creds.Build(cfg.Admin.Creds, logger)
		if aerr != nil {
			return bail(fmt.Errorf("reflow: admin creds: %w", aerr))
		}
		adminCreds = ac
		recordListenerSecurity(metrics, "admin", ac)
	}

	var (
		deliverySrv    *connectserver.Server
		deliveryClient *delivery.Client
	)
	if crossShard {
		ds, dc, derr := startDeliveryListener(ctx, eh, cfg, deliveryCreds, httpAuthMW, logger)
		if derr != nil {
			return bail(derr)
		}
		deliverySrv, deliveryClient = ds, dc
		eh.SetCrossShardSender(dc)
	}

	host, herr := finishStartup(ctx, cfg, eh, adminEnabled, shards, snapshotTriggers,
		deliverySrv, deliveryClient, deliveryCreds, adminCreds, handlerSigner,
		httpAuthMW, httpAuthCloser, metricsCloser, metricsRegisterer, metrics, logger)
	if herr != nil {
		if deliverySrv != nil {
			_ = deliverySrv.Close()
		}
		if deliveryClient != nil {
			_ = deliveryClient.Close()
		}
		return bail(herr)
	}
	return host, nil
}

// startDeliveryListener builds the Delivery client (so partitions get a
// Sender on startup) and the Delivery Connect server. The two share one
// creds.Spec (mTLS or insecure) so the cluster forms a closed trust loop.
// The auth middleware is the same instance admin and ingress use; the
// starter policy gates /reflow.delivery.v1.Delivery/* to node/* principals.
func startDeliveryListener(
	ctx context.Context,
	eh *engine.Host,
	cfg Config,
	lc *creds.ListenerCreds,
	mw func(http.Handler) http.Handler,
	logger *slog.Logger,
) (*connectserver.Server, *delivery.Client, error) {
	dc, err := delivery.NewClient(delivery.ClientConfig{
		Resolver:        eh,
		Log:             logger,
		ClientTLSConfig: lc.ClientTLSConfig,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("reflow: delivery client: %w", err)
	}

	srv := delivery.NewServer(eh, logger)
	path, handler := srv.NewHandler()
	cs, cerr := connectserver.New(ctx, connectserver.Config{
		Addr: cfg.Node.DeliveryAddr,
		TLS:  lc.ServerTLSConfig,
		Log:  logger,
	}, connectserver.Route{Path: path, Handler: mw(handler)})
	if cerr != nil {
		_ = dc.Close()
		return nil, nil, fmt.Errorf("reflow: listen delivery %s: %w", cfg.Node.DeliveryAddr, cerr)
	}
	logger.Info("reflow: delivery listening", "addr", cs.Addr(),
		"driver", string(lc.Driver))
	return cs, dc, nil
}

// startIngressListener builds cfg.Ingress.Creds and starts the ingress
// runtime against eh. Always called by Run — ingress is the user-facing
// API. Returns (nil, nil, nil) when cfg.Ingress.Disabled is set so
// operators can run engine nodes behind a separate ingress fleet.
// Multi-node insecure deployments emit a WARN at startup; single-node
// insecure is silent because that's the dev default.
//
// The auth middleware is required and is the same instance admin and
// delivery share. The starter policy's ingress_open allow rule lets
// anonymous traffic through; operators tighten this in the policy
// file, never by skipping the middleware here.
//
// Returns (runtime, listenerCreds, error). On error the caller is
// responsible for releasing any other resources it has accumulated;
// this helper closes only what it created itself.
func startIngressListener(
	ctx context.Context,
	eh *engine.Host,
	cfg Config,
	multiNode bool,
	mw func(http.Handler) http.Handler,
	metrics *observability.Metrics,
	logger *slog.Logger,
) (*ingress.Runtime, *creds.ListenerCreds, error) {
	if cfg.Ingress.Disabled {
		logger.Info("reflow: ingress disabled (cfg.ingress.disabled=true); clients must use a separate ingress fleet")
		return nil, nil, nil
	}
	lc, err := creds.Build(cfg.Ingress.Creds, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("reflow: ingress creds: %w", err)
	}
	if multiNode && lc.SecurityLevel == credentials.NoSecurity {
		logger.Warn("reflow: ingress is running on an insecure listener — multi-node deployments should configure cfg.Ingress.Creds")
	}
	// Pre-validate webhook config against the registry so a typo'd
	// verifier name or missing field aborts startup cleanly before
	// the listener binds (the in-closure NewManager below cannot
	// surface errors back to the caller).
	webhookSources := buildWebhookSources(cfg.Webhooks)
	if err := internalwebhook.ValidateSources(webhookSources); err != nil {
		_ = creds.CloseAll(lc)
		return nil, nil, fmt.Errorf("reflow: webhook config: %w", err)
	}
	icfg := ingress.Config{
		Addr:       cfg.Ingress.Addr,
		TLS:        lc.ServerTLSConfig,
		Log:        logger,
		Middleware: mw,
	}
	icfg.ExtraRoutes = func(srv *ingress.Server) []connectserver.Route {
		var routes []connectserver.Route
		if !cfg.Ingress.HTTP.Disabled {
			httpCfg := httpingress.Config{
				MaxBodyBytes: cfg.Ingress.HTTP.MaxBodyBytes,
				MaxPollMs:    cfg.Ingress.HTTP.MaxPollMs,
			}
			routes = append(routes, connectserver.Route{
				Path:    "/v1/",
				Handler: mw(httpingress.NewRouter(srv, httpCfg, metrics)),
			})
		}
		// Sources are validated above; this construction is
		// guaranteed to succeed.
		wm, _ := internalwebhook.NewManager(webhookSources, srv, logger)
		for _, r := range wm.Routes() {
			routes = append(routes, connectserver.Route{
				Path:    r.Path,
				Handler: mw(r.Handler),
			})
		}
		return routes
	}
	rt, err := ingress.Start(ctx, eh, icfg)
	if err != nil {
		_ = creds.CloseAll(lc)
		return nil, nil, fmt.Errorf("reflow: ingress start: %w", err)
	}
	logger.Info("reflow: ingress listening",
		"addr", rt.Addr(),
		"driver", string(lc.Driver))
	return rt, lc, nil
}

// finishStartup wires shard 0 + partition shards + optional snapshot
// producer + admin server, then packages everything into a Host. Errors
// here are surfaced by the caller which runs the bail cleanup.
func finishStartup(
	ctx context.Context,
	cfg Config,
	eh *engine.Host,
	adminEnabled bool,
	shards []uint64,
	snapshotTriggers map[uint64]chan struct{},
	deliverySrv *connectserver.Server,
	deliveryClient *delivery.Client,
	deliveryCreds *creds.ListenerCreds,
	adminCreds *creds.ListenerCreds,
	handlerSigner *creds.Signer,
	httpAuthMW func(http.Handler) http.Handler,
	authCloser func() error,
	metricsCloser func() error,
	metricsRegisterer prometheus.Registerer,
	metrics *observability.Metrics,
	logger *slog.Logger,
) (*Host, error) {
	// Joiners register themselves with shard 0 BEFORE starting any
	// local shards: dragonboat's StartOnDiskReplica(nil, join=true,...)
	// will block forever if the joining ReplicaID isn't already part of
	// each shard's configuration. SelfJoin dials the metadata leader
	// (resolved via gossip NodeHostMeta) and proposes RegisterNode +
	// BeginRebalanceStep; the rebalancer drives SyncRequestAddReplica
	// from the leader side. See plans/humble-chasing-quokka.md.
	if cfg.Cluster.JoinExisting {
		if err := callSelfJoin(ctx, cfg, eh, logger); err != nil {
			return nil, fmt.Errorf("reflow: SelfJoin: %w", err)
		}
	}

	// Start shard 0 before partition shards so the partition table is
	// established as partitions come up.
	runner, err := eh.StartMetadataShard()
	if err != nil {
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
		adminSrv     *connectserver.Server
		snapshotCxl  context.CancelFunc
		snapshotRepo *snapshot.BlobRepository
	)

	var snapshotRepoIface snapshot.Repository
	if cfg.Snapshot.URL != "" {
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

	// Build the in-process admin.Server unconditionally — it's the engine
	// proposer + snapshot/deployment glue that autoSeedEndpoints needs,
	// even when no external listener is configured. The gRPC listener
	// only goes up when adminEnabled.
	adminCfg := admin.Config{
		Host:       eh,
		Runner:     runner,
		Repo:       snapshotRepoIface,
		Source:     &engine.HostSnapshotSource{Host: eh},
		Log:        logger,
		ScratchDir: cfg.Snapshot.ScratchDir,
	}
	// Avoid the typed-nil interface trap: only assign the Signer field
	// when the underlying *creds.Signer is non-nil.
	if handlerSigner != nil {
		adminCfg.Signer = handlerSigner
	}
	srv, sErr := admin.NewServer(adminCfg)
	if sErr != nil {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		return nil, fmt.Errorf("reflow: admin server: %w", sErr)
	}

	if adminEnabled {
		path, h := srv.NewHandler()
		cs, lErr := connectserver.New(ctx, connectserver.Config{
			Addr: cfg.Admin.Addr,
			TLS:  adminCreds.ServerTLSConfig,
			Log:  logger,
		}, connectserver.Route{Path: path, Handler: httpAuthMW(h)})
		if lErr != nil {
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			return nil, fmt.Errorf("reflow: admin listener: %w", lErr)
		}
		adminSrv = cs
		logger.Info("reflow: admin listening", "addr", cs.Addr(),
			"driver", string(adminCreds.Driver))
	}

	multiNode := len(cfg.Cluster.Peers) > 1
	ingressRT, ingressCreds, err := startIngressListener(ctx, eh, cfg, multiNode, httpAuthMW, metrics, logger)
	if err != nil {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		if adminSrv != nil {
			_ = adminSrv.Close()
		}
		return nil, err
	}

	var esManager *eventsource.Manager
	if ingressRT != nil {
		esManager, err = eventsource.NewManager(cfg.EventSources, ingressRT.Server(), metricsRegisterer, logger)
		if err != nil {
			_ = ingressRT.Close()
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			if adminSrv != nil {
				_ = adminSrv.Close()
			}
			return nil, err
		}
	} else if len(cfg.EventSources.Sources) > 0 {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		if adminSrv != nil {
			_ = adminSrv.Close()
		}
		return nil, fmt.Errorf("reflow: event_sources requires an enabled ingress listener")
	}
	if esManager != nil {
		go esManager.Run(ctx)
	}

	// Auto-seed remote-handler deployments from config. Spawned AFTER
	// the last error-returning step so a failed Run doesn't leave this
	// goroutine running with no Host to attach to. Each failure inside
	// the seed loop is logged and the next endpoint is tried; ctx is the
	// Run caller's context — cancelling it cancels the seed loop.
	if len(cfg.Handlers.Endpoints) > 0 {
		go autoSeedEndpoints(ctx, srv, runner, cfg.Handlers.Endpoints, logger)
	}

	return &Host{
		engine:         eh,
		metricsCloser:  metricsCloser,
		ingressRT:      ingressRT,
		ingressCreds:   ingressCreds,
		deliverySrv:    deliverySrv,
		deliveryClient: deliveryClient,
		deliveryCreds:  deliveryCreds,
		adminSrv:       adminSrv,
		adminCreds:     adminCreds,
		authCloser:     authCloser,
		snapshotCxl:    snapshotCxl,
		snapshotRepo:   snapshotRepo,
		handlerSigner:  handlerSigner,
		eventSources:   esManager,
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
		depID, err := srv.AutoSeed(regCtx, ep.URL)
		cancel()
		if err != nil {
			log.Warn("reflow: auto-seed endpoint failed", "url", ep.URL, "err", err)
			continue
		}
		log.Info("reflow: auto-seed endpoint registered",
			"url", ep.URL, "deployment_id", depID)
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
	// Ingress is started by Run unless cfg.Ingress.Disabled. If the
	// operator left Addr empty, fall back to the well-known reflow port
	// so `reflow.Run` works out of the box.
	if !cfg.Ingress.Disabled && cfg.Ingress.Addr == "" {
		cfg.Ingress.Addr = ":8080"
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

// callSelfJoin discovers the metadata leader via gossip
// (NodeHostMeta.admin_endpoint) and dials its Admin/SelfJoin RPC to
// register this node with shard 0 before any local shards are started.
// Discovery is gossip-only — no fallback to peer-list dialing. If the
// leader hint hasn't propagated within the bounded poll window, the
// outer 3-attempt retry tries again from scratch; ultimately a timeout
// surfaces as an error so the process supervisor restarts reflowd.
//
// CallWithLeaderRedirect handles per-attempt LeaderHint chasing (the
// gossip view was stale by one heartbeat); the outer retry here handles
// transient cluster-wide Unavailable conditions during cold start.
func callSelfJoin(ctx context.Context, cfg Config, host *engine.Host, log *slog.Logger) error {
	req := &adminv1.AddNodeRequest{
		NodeId:       cfg.Node.ID,
		RaftAddr:     cfg.Node.RaftAddr,
		GossipAddr:   cfg.Node.GossipAdvAddr,
		GrpcEndpoint: cfg.Node.DeliveryAddr,
	}
	if req.GossipAddr == "" {
		req.GossipAddr = cfg.Node.GossipBindAddr
	}
	backoff := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	var lastErr error
	for attempt := range 3 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff[attempt-1]):
			}
		}
		leaderAddr, err := waitForLeaderAdmin(ctx, host, 10*time.Second, 200*time.Millisecond)
		if err != nil {
			lastErr = err
			log.Warn("SelfJoin: gossip leader-hint timeout, retrying",
				"attempt", attempt+1, "err", err)
			continue
		}
		log.Info("SelfJoin: dialing metadata leader", "addr", leaderAddr,
			"node_id", req.NodeId, "attempt", attempt+1)
		err = adminclient.CallWithLeaderRedirect(ctx, adminclient.DialOptions{
			Addr:  leaderAddr,
			Creds: cfg.Admin.Creds,
		}, 3, func(rctx context.Context, cli adminv1connect.AdminClient) error {
			_, e := cli.SelfJoin(rctx, connect.NewRequest(req))
			return e
		})
		if err == nil {
			log.Info("SelfJoin: registered with shard 0", "node_id", req.NodeId)
			return nil
		}
		lastErr = err
		// Retry only on transient Unavailable; terminal errors
		// (PermissionDenied, InvalidArgument, ...) short-circuit.
		if connect.CodeOf(err) != connect.CodeUnavailable {
			return err
		}
		log.Warn("SelfJoin: transient Unavailable, retrying",
			"attempt", attempt+1, "err", err)
	}
	return fmt.Errorf("exhausted retries: %w", lastErr)
}

// waitForLeaderAdmin polls Host.PartitionLeaderHint(0) +
// NodeAdminEndpoint(id) on a bounded ticker. Gossip-only — no fallback
// to peer-list dialing. Timeout → error so the caller can decide to
// retry or bail.
func waitForLeaderAdmin(ctx context.Context, host *engine.Host, timeout, tick time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		if id, ok := host.PartitionLeaderHint(0); ok {
			if addr, ok := host.NodeAdminEndpoint(id); ok {
				return addr, nil
			}
		}
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("no metadata leader via gossip within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
		}
	}
}
