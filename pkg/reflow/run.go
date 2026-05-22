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

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/clusterctl"
	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/rebalance"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	httpingress "github.com/twinfer/reflow/internal/ingress/http"
	internalwebhook "github.com/twinfer/reflow/internal/ingress/webhook"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/secretstore"
	hcvaultkms "github.com/twinfer/reflow/pkg/kms/hcvault"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/pkg/reflowclient"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"

	// KMS providers always-linked, config-gated. Each subpackage's
	// init() calls registry.RegisterKMSClient under a sync.Once.
	// AWS / GCP self-register and read the standard credential chain;
	// Vault registers via the explicit hcvaultkms.Register call below
	// (it needs a token file). BlobKMS is the no-managed-KMS fallback.
	_ "github.com/twinfer/reflow/pkg/kms/awskms"
	_ "github.com/twinfer/reflow/pkg/kms/blob"
	_ "github.com/twinfer/reflow/pkg/kms/gcpkms"
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
//	cfg.Handlers.Endpoints = []reflow.HandlerEndpoint{{URL: "http://localhost:9000"}}
//	host, err := reflow.Run(ctx, cfg)
func Run(ctx context.Context, cfg Config) (*Host, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	cfg = withDefaults(cfg)

	logger := buildLogger(cfg.Logging)
	slog.SetDefault(logger)

	// Vault is the one KMS provider that needs explicit config (token
	// file + optional URI prefix narrowing). Other providers (BlobKMS,
	// AWS, GCP) self-register from their package init(). Address is a
	// host:port; the scheme is prepended here so the registered Tink
	// prefix matches actual hcvault:// URIs passed to GetAEAD.
	if cfg.KMS.Vault.TokenFile != "" {
		uriPrefix := ""
		if cfg.KMS.Vault.Address != "" {
			uriPrefix = hcvaultkms.DefaultURIPrefix + cfg.KMS.Vault.Address
		}
		if err := hcvaultkms.Register(uriPrefix, cfg.KMS.Vault.TokenFile, nil); err != nil {
			return nil, fmt.Errorf("reflow: hcvault register: %w", err)
		}
	}

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

	// Shard-0 TableNotifiers fan out apply-path commits to local
	// subsystems (eventsource Reconciler, webhook Reconciler).
	// Constructed before engine.NewHost so the FSM picks them up at
	// start; their Subscribe() ends are handed to subsystem
	// goroutines later.
	eventSourceNotifier := cluster.NewTableNotifier()
	webhookSourceNotifier := cluster.NewTableNotifier()
	secretNotifier := cluster.NewTableNotifier()
	lpOwnersNotifier := cluster.NewTableNotifier()
	rebalanceDrainNotifier := cluster.NewTableNotifier()

	hcfg := engine.HostConfig{
		NodeID:             cfg.Node.ID,
		RaftAddr:           cfg.Node.RaftAddr,
		RaftAdvertisedAddr: cfg.Node.RaftAdvertisedAddr,
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
		EagerStateMaxBytes: cfg.Handlers.EagerStateMaxBytes,
		ClusterNotifiers: cluster.Notifiers{
			EventSourceTable:    eventSourceNotifier,
			WebhookSourceTable:  webhookSourceNotifier,
			SecretTable:         secretNotifier,
			LPOwnersTable:       lpOwnersNotifier,
			RebalanceDrainTable: rebalanceDrainNotifier,
		},
		Rebalance: rebalance.Config{
			Mode:                       cfg.Rebalance.Mode,
			MaxConcurrentTransfers:     cfg.Rebalance.MaxConcurrentTransfers,
			MinSecondsBetweenTransfers: *cfg.Rebalance.MinSecondsBetweenTransfers,
			SkewEngagePct:              cfg.Rebalance.SkewEngagePct,
			SkewDisengagePct:           cfg.Rebalance.SkewDisengagePct,
		},
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
	// Assign HandlerSigner only when non-nil so a nil *creds.Signer
	// (insecure delivery) leaves the interface field nil. Otherwise
	// connectclient's `if c.signer != nil` would see a non-nil interface
	// wrapping a nil concrete pointer and call Sign on a nil receiver
	// — surfaces as "reflow/creds: nil Signer" on the first
	// engine→handler dispatch.
	if handlerSigner != nil {
		hcfg.HandlerSigner = handlerSigner
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

	host, herr := finishStartup(ctx, startupDeps{
		cfg:                   cfg,
		eh:                    eh,
		adminEnabled:          adminEnabled,
		shards:                shards,
		snapshotTriggers:      snapshotTriggers,
		deliverySrv:           deliverySrv,
		deliveryClient:        deliveryClient,
		deliveryCreds:         deliveryCreds,
		adminCreds:            adminCreds,
		handlerSigner:         handlerSigner,
		httpAuthMW:            httpAuthMW,
		authCloser:            httpAuthCloser,
		metricsCloser:         metricsCloser,
		metricsRegisterer:     metricsRegisterer,
		metrics:               metrics,
		eventSourceNotifier:   eventSourceNotifier,
		webhookSourceNotifier: webhookSourceNotifier,
		secretNotifier:        secretNotifier,
		lpOwnersNotifier:      lpOwnersNotifier,
		logger:                logger,
	})
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
	secrets *secretstore.Resolver,
	metrics *observability.Metrics,
	metricsRegisterer prometheus.Registerer,
	logger *slog.Logger,
) (*ingress.Runtime, *creds.ListenerCreds, *internalwebhook.Manager, error) {
	if cfg.Ingress.Disabled {
		logger.Info("reflow: ingress disabled (cfg.ingress.disabled=true); clients must use a separate ingress fleet")
		return nil, nil, nil, nil
	}
	lc, err := creds.Build(cfg.Ingress.Creds, logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reflow: ingress creds: %w", err)
	}
	recordListenerSecurity(metrics, "ingress", lc)
	if multiNode && lc.SecurityLevel == credentials.NoSecurity {
		logger.Warn("reflow: ingress is running on an insecure listener — multi-node deployments should configure cfg.Ingress.Creds")
	}
	// Manager is constructed inside ExtraRoutes (needs srv as the
	// Submitter) and captured here so finishStartup can spawn the
	// reconciler against it.
	var webhookMgr *internalwebhook.Manager
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
		// Always mount the /webhooks/ catch-all even when no sources
		// are configured — an empty snapshot 404s every request, so
		// the route is harmless and gives operators a stable mount
		// point to add sources at runtime via `cluster apply`.
		m, merr := internalwebhook.New(srv, secrets, metricsRegisterer, logger)
		if merr != nil {
			// New only fails on nil submitter; srv is non-nil here.
			logger.Error("reflow: webhook Manager init failed", "err", merr)
			return routes
		}
		webhookMgr = m
		for _, r := range m.Routes() {
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
		return nil, nil, nil, fmt.Errorf("reflow: ingress start: %w", err)
	}
	logger.Info("reflow: ingress listening",
		"addr", rt.Addr(),
		"driver", string(lc.Driver))
	return rt, lc, webhookMgr, nil
}

// startupDeps bundles the resources Run hands to finishStartup. The
// struct exists only to keep the call site readable; it's not exported
// and has no behavior of its own.
type startupDeps struct {
	cfg                   Config
	eh                    *engine.Host
	adminEnabled          bool
	shards                []uint64
	snapshotTriggers      map[uint64]chan struct{}
	deliverySrv           *connectserver.Server
	deliveryClient        *delivery.Client
	deliveryCreds         *creds.ListenerCreds
	adminCreds            *creds.ListenerCreds
	handlerSigner         *creds.Signer
	httpAuthMW            func(http.Handler) http.Handler
	authCloser            func() error
	metricsCloser         func() error
	metricsRegisterer     prometheus.Registerer
	metrics               *observability.Metrics
	eventSourceNotifier   *cluster.TableNotifier
	webhookSourceNotifier *cluster.TableNotifier
	secretNotifier        *cluster.TableNotifier
	lpOwnersNotifier      *cluster.TableNotifier
	logger                *slog.Logger
}

// finishStartup wires shard 0 + partition shards + optional snapshot
// producer + admin server, then packages everything into a Host. Errors
// here are surfaced by the caller which runs the bail cleanup.
func finishStartup(ctx context.Context, d startupDeps) (*Host, error) {
	cfg := d.cfg
	eh := d.eh
	adminEnabled := d.adminEnabled
	shards := d.shards
	snapshotTriggers := d.snapshotTriggers
	deliverySrv := d.deliverySrv
	deliveryClient := d.deliveryClient
	deliveryCreds := d.deliveryCreds
	adminCreds := d.adminCreds
	handlerSigner := d.handlerSigner
	httpAuthMW := d.httpAuthMW
	authCloser := d.authCloser
	metricsCloser := d.metricsCloser
	metricsRegisterer := d.metricsRegisterer
	metrics := d.metrics
	eventSourceNotifier := d.eventSourceNotifier
	webhookSourceNotifier := d.webhookSourceNotifier
	secretNotifier := d.secretNotifier
	lpOwnersNotifier := d.lpOwnersNotifier
	logger := d.logger
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

	// Build the in-process ClusterCtl + Config servers unconditionally —
	// the Config server is the engine proposer + deployment glue that
	// autoSeedEndpoints needs, even when no external listener is
	// configured. The Connect listeners only go up when adminEnabled.
	clusterSrv, cErr := clusterctl.NewServer(clusterctl.Config{
		Host:       eh,
		Runner:     runner,
		Repo:       snapshotRepoIface,
		Source:     &engine.HostSnapshotSource{Host: eh},
		Log:        logger,
		ScratchDir: cfg.Snapshot.ScratchDir,
		Rebalance: rebalance.Config{
			Mode:                       cfg.Rebalance.Mode,
			MaxConcurrentTransfers:     cfg.Rebalance.MaxConcurrentTransfers,
			MinSecondsBetweenTransfers: *cfg.Rebalance.MinSecondsBetweenTransfers,
			SkewEngagePct:              cfg.Rebalance.SkewEngagePct,
			SkewDisengagePct:           cfg.Rebalance.SkewDisengagePct,
		},
	})
	if cErr != nil {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		return nil, fmt.Errorf("reflow: clusterctl server: %w", cErr)
	}
	configCfg := config.Config{
		Host:   eh,
		Runner: runner,
		Log:    logger,
	}
	// Avoid the typed-nil interface trap: only assign the Signer field
	// when the underlying *creds.Signer is non-nil.
	if handlerSigner != nil {
		configCfg.Signer = handlerSigner
	}
	configSrv, cfErr := config.NewServer(configCfg)
	if cfErr != nil {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		return nil, fmt.Errorf("reflow: config server: %w", cfErr)
	}

	if adminEnabled {
		clusterPath, clusterH := clusterSrv.NewHandler()
		configPath, configH := configSrv.NewHandler()
		cs, lErr := connectserver.New(ctx, connectserver.Config{
			Addr: cfg.Admin.Addr,
			TLS:  adminCreds.ServerTLSConfig,
			Log:  logger,
		},
			connectserver.Route{Path: clusterPath, Handler: httpAuthMW(clusterH)},
			connectserver.Route{Path: configPath, Handler: httpAuthMW(configH)},
		)
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

	// Routing Reconciler refreshes the per-node Partitioner's atomic
	// LPOwners snapshot. Started as soon as shard 0 is up; the first
	// SyncRead returns empty until the metadata-leader bootstrap seed
	// commits, at which point the Partitioner switches from modulo
	// fallback to table-driven routing.
	go func() {
		reader := lpOwnersReader{host: eh}
		if rerr := routing.RunReconciler(ctx, lpOwnersNotifier.Subscribe(), reader, eh.PartitionerRef(), logger); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: routing reconcile loop exited", "err", rerr)
		}
	}()

	// SecretStore Resolver runs alongside the ingress listener so the
	// webhook Manager can Lookup(name) on each reconcile pass. The
	// Resolver itself is hot-path safe — no per-call work in Lookup.
	secrets := secretstore.New(metricsRegisterer, logger)
	go func() {
		reader := secretReader{host: eh}
		if rerr := secrets.RunReconciler(ctx, secretNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: secretstore reconcile loop exited", "err", rerr)
		}
	}()

	multiNode := len(cfg.Cluster.Peers) > 1
	ingressRT, ingressCreds, webhookMgr, err := startIngressListener(ctx, eh, cfg, multiNode, httpAuthMW, secrets, metrics, metricsRegisterer, logger)
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
		esManager, err = eventsource.New(ingressRT.Server(), metricsRegisterer, logger)
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
		reader := eventSourceReader{host: eh}
		go func() {
			if rerr := esManager.RunReconciler(ctx, eventSourceNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
				logger.Warn("reflow: eventsource reconcile loop exited", "err", rerr)
			}
		}()
	}
	if webhookMgr != nil {
		reader := webhookSourceReader{host: eh}
		go func() {
			if rerr := webhookMgr.RunReconciler(ctx, webhookSourceNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
				logger.Warn("reflow: webhook reconcile loop exited", "err", rerr)
			}
		}()
	}

	// Auto-seed remote-handler deployments from config. Spawned AFTER
	// the last error-returning step so a failed Run doesn't leave this
	// goroutine running with no Host to attach to. Each failure inside
	// the seed loop is logged and the next endpoint is tried; ctx is the
	// Run caller's context — cancelling it cancels the seed loop.
	if len(cfg.Handlers.Endpoints) > 0 {
		go autoSeedEndpoints(ctx, configSrv, runner, cfg.Handlers.Endpoints, logger)
	}
	// Bootstrap-seed event sources from the koanf config. Runs only on
	// the shard-0 leader and only when the EventSourceTable is empty —
	// otherwise the file is ignored (operators manage via CLI after
	// first run). See autoSeedEventSources.
	//
	// Webhook sources have no koanf bootstrap path as of PR4 — secrets
	// are operator-supplied (encrypted blobs + KEK URIs); operators
	// register webhooks post-start via `reflowd config create-secret`
	// + `reflowd config upsert-webhook ...`, or `reflowd config apply
	// -f <file>`.
	if len(cfg.EventSources.Sources) > 0 {
		go autoSeedEventSources(ctx, configSrv, runner, eh, cfg.EventSources.Sources, logger)
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
		webhookSources: webhookMgr,
	}, nil
}

// eventSourceReader is the Reader adapter the Reconcile loop uses to
// pull desired state. Converts the proto-shaped EventSourceRecord rows
// fetched from shard 0 into the Go-shaped SourceConfig the Manager
// already understands.
type eventSourceReader struct {
	host *engine.Host
}

func (r eventSourceReader) ListEventSources(ctx context.Context) ([]eventsource.SourceConfig, uint64, error) {
	// SyncRead rejects deadlineless contexts; pin a short timeout.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.EventSources(readCtx)
	if err != nil {
		return nil, 0, err
	}
	out := make([]eventsource.SourceConfig, 0, len(list.Sources))
	for _, rec := range list.Sources {
		out = append(out, eventSourceConfigFromProto(rec))
	}
	return out, list.TableRevision, nil
}

// webhookSourceReader is the Reader adapter the webhook reconcile loop
// uses to pull desired state. Returns the raw proto records — secret
// bytes are looked up via the SecretStore Resolver inside the loop.
type webhookSourceReader struct {
	host *engine.Host
}

func (r webhookSourceReader) ListWebhookSources(ctx context.Context) ([]*enginev1.WebhookSourceRecord, uint64, error) {
	// dragonboat's SyncRead rejects deadlineless contexts; pin a short
	// timeout so the reconcile loop never blocks indefinitely.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.WebhookSources(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Sources, list.TableRevision, nil
}

// secretReader is the Reader adapter the SecretStore reconciler uses
// to pull desired state. Mirrors webhookSourceReader — same SyncRead
// timeout, same proto pass-through.
type secretReader struct {
	host *engine.Host
}

func (r secretReader) ListSecrets(ctx context.Context) ([]*enginev1.SecretRecord, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.Secrets(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Records, list.TableRevision, nil
}

// lpOwnersReader is the Reader adapter the routing reconciler uses to
// pull the current lp → shard_id snapshot. SyncReads shard 0 via
// host.LPOwners and lifts the result into the map the Partitioner stores
// atomically.
type lpOwnersReader struct {
	host *engine.Host
}

func (r lpOwnersReader) SnapshotLPOwners(ctx context.Context) (map[uint32]uint64, uint64, error) {
	// dragonboat's SyncRead rejects deadlineless contexts; pin a short
	// timeout so the reconcile loop never blocks indefinitely.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.LPOwners(readCtx)
	if err != nil {
		return nil, 0, err
	}
	out := make(map[uint32]uint64, len(list.Records))
	for _, rec := range list.Records {
		out[rec.GetLp()] = rec.GetShardId()
	}
	return out, list.TableRevision, nil
}

// autoSeedEventSources mirrors autoSeedEndpoints for the EventSourceTable.
// On a fresh cluster (table empty) it proposes one UpsertEventSource per
// configured source with CAS-on-zero so a racing operator-Apply at the
// same time gets the conflict instead of silently overwriting the
// operator's intent. Skips the seed entirely once any row exists.
//
// Runs as a fire-and-forget goroutine; logs warnings on per-source
// failures and continues.
func autoSeedEventSources(ctx context.Context, srv *config.Server, runner *engine.MetadataRunner, host *engine.Host, sources []eventsource.SourceConfig, log *slog.Logger) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if runner != nil && runner.IsLeader() {
			break
		}
		if time.Now().After(deadline) {
			log.Warn("reflow: auto-seed event sources: shard 0 leadership not reached within 2m; skipping",
				"sources", len(sources))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Skip when an operator-managed table already exists. A non-zero
	// revision is the durable marker that someone (a prior seed or an
	// operator) wrote.
	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	list, err := host.EventSources(listCtx)
	cancel()
	if err != nil {
		log.Warn("reflow: auto-seed event sources: read table failed; skipping", "err", err)
		return
	}
	if list.TableRevision != 0 || len(list.Sources) != 0 {
		log.Info("reflow: auto-seed event sources: table non-empty; skipping seed",
			"revision", list.TableRevision, "rows", len(list.Sources))
		return
	}

	for _, sc := range sources {
		if ctx.Err() != nil {
			return
		}
		rec := eventSourceProtoFromConfig(sc)
		seedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := srv.AutoSeedEventSource(seedCtx, rec)
		cancel()
		if err != nil {
			log.Warn("reflow: auto-seed event source failed", "name", sc.Name, "err", err)
			continue
		}
		log.Info("reflow: auto-seed event source registered", "name", sc.Name)
	}
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
func autoSeedEndpoints(ctx context.Context, srv *config.Server, runner *engine.MetadataRunner, endpoints []HandlerEndpoint, log *slog.Logger) {
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
	if cfg.Rebalance.Mode == "" {
		cfg.Rebalance.Mode = RebalanceModeOff
	}
	if cfg.Rebalance.MaxConcurrentTransfers == 0 {
		cfg.Rebalance.MaxConcurrentTransfers = 1
	}
	if cfg.Rebalance.MinSecondsBetweenTransfers == nil {
		// Production default. Operators who want no cooldown set the
		// key to an explicit 0 in YAML / env, which decodes into a
		// non-nil pointer to 0 and skips this branch.
		def := uint32(60)
		cfg.Rebalance.MinSecondsBetweenTransfers = &def
	}
	if cfg.Rebalance.SkewEngagePct == 0 {
		cfg.Rebalance.SkewEngagePct = 15
	}
	if cfg.Rebalance.SkewDisengagePct == 0 {
		cfg.Rebalance.SkewDisengagePct = 8
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
	req := &clusterctlv1.AddNodeRequest{
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
		err = reflowclient.CallWithLeaderRedirect(ctx, reflowclient.DialOptions{
			Addr:  leaderAddr,
			Creds: cfg.Admin.Creds,
		}, 3, func(rctx context.Context, cli *reflowclient.Client) error {
			_, e := cli.Cluster.SelfJoin(rctx, connect.NewRequest(req))
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
