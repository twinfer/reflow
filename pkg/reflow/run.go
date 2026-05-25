package reflow

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	connect "connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/authz"
	"github.com/twinfer/reflow/internal/bootstrap"
	"github.com/twinfer/reflow/internal/certmgr"
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
	"github.com/twinfer/reflow/internal/ingress/quota"
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

	// NumPartitionShards is the routing modulus and the bootstrap shard
	// count — independent of peer count and replication factor (every
	// shard is replicated on every peer, RF=N). 0 means auto: the peer
	// count, or 1 for solo. Physical shard ids are contiguous 1..S.
	numShards := cfg.Cluster.NumPartitionShards
	if numShards == 0 {
		numShards = uint64(len(cfg.Cluster.Peers))
		if numShards == 0 {
			numShards = 1
		}
	}
	shards := make([]uint64, 0, numShards)
	for sh := uint64(1); sh <= numShards; sh++ {
		shards = append(shards, sh)
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
	partitionTableNotifier := cluster.NewTableNotifier()
	eventSourceNotifier := cluster.NewTableNotifier()
	webhookSourceNotifier := cluster.NewTableNotifier()
	secretNotifier := cluster.NewTableNotifier()
	caRootNotifier := cluster.NewTableNotifier()
	joinTokenNotifier := cluster.NewTableNotifier()
	lpOwnersNotifier := cluster.NewTableNotifier()
	rebalanceDrainNotifier := cluster.NewTableNotifier()
	tenantNotifier := cluster.NewTableNotifier()
	tenantDEKNotifier := cluster.NewTableNotifier()
	platformConfigNotifier := cluster.NewTableNotifier()

	// TenantDEKResolver constructed before HostConfig so the per-shard
	// StoreFactory closures pick it up. defaultAEAD is nil today —
	// tenant_id=0 (anonymous / single-tenant traffic) passes through
	// the encstore wrapper as plaintext. Encryption-at-rest for tenant
	// 0 would need a bootstrap KMS pointer; deferred until operators
	// have a use case. RunReconciler is started further down, after
	// engine.NewHost so the Reader adapter has a live *engine.Host.
	tenantDEKs := secretstore.NewTenantDEKResolver(metricsRegisterer, logger, nil)

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
			PartitionTable:      partitionTableNotifier,
			EventSourceTable:    eventSourceNotifier,
			WebhookSourceTable:  webhookSourceNotifier,
			SecretTable:         secretNotifier,
			CARootTable:         caRootNotifier,
			JoinTokenTable:      joinTokenNotifier,
			LPOwnersTable:       lpOwnersNotifier,
			RebalanceDrainTable: rebalanceDrainNotifier,
			TenantTable:         tenantNotifier,
			TenantDEKTable:      tenantDEKNotifier,
			PlatformConfigTable: platformConfigNotifier,
		},
		TenantDEKResolver: tenantDEKs,
		Rebalance: rebalance.Config{
			Mode:                       cfg.Rebalance.Mode,
			MaxConcurrentTransfers:     cfg.Rebalance.MaxConcurrentTransfers,
			MinSecondsBetweenTransfers: *cfg.Rebalance.MinSecondsBetweenTransfers,
			SkewEngagePct:              cfg.Rebalance.SkewEngagePct,
			SkewDisengagePct:           cfg.Rebalance.SkewDisengagePct,
		},
		Audit: engine.AuditConfig{
			Logger:            cfg.Audit.Logger,
			RetentionDuration: *cfg.Audit.RetentionDuration,
			GcInterval:        cfg.Audit.GcInterval,
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

	mw, mwCloser, jwtVerifier, mwErr := auth.HTTPMiddleware(buildAuthConfig(cfg.Auth), logger)
	if mwErr != nil {
		return bail(fmt.Errorf("reflow: auth middleware: %w", mwErr))
	}
	httpAuthMW = mw
	httpAuthCloser = mwCloser
	tenantOIDC := auth.NewTenantOIDCReconciler(jwtVerifier, buildAuthConfig(cfg.Auth).OIDC, logger)

	// Cedar authorization engine, shared by every Connect service via the
	// authz interceptor. Seeded with the in-binary foundational policies
	// until PR3 moves policy text onto shard 0.
	authzEngine, azErr := authz.NewEngine([]byte(authz.FoundationalClusterPolicies))
	if azErr != nil {
		return bail(fmt.Errorf("reflow: authz engine: %w", azErr))
	}
	authzInterceptor := authz.NewInterceptor(authzEngine, logger, len(cfg.Auth.OIDC) > 0)

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
		ds, dc, derr := startDeliveryListener(ctx, eh, cfg, deliveryCreds, httpAuthMW, authzInterceptor, logger)
		if derr != nil {
			return bail(derr)
		}
		deliverySrv, deliveryClient = ds, dc
		eh.SetCrossShardSender(dc)
		eh.SetLPSSTUploader(delivery.NewLPSSTUploader(eh, dc, logger))
	}

	host, herr := finishStartup(ctx, startupDeps{
		cfg:                    cfg,
		eh:                     eh,
		adminEnabled:           adminEnabled,
		shards:                 shards,
		snapshotTriggers:       snapshotTriggers,
		deliverySrv:            deliverySrv,
		deliveryClient:         deliveryClient,
		deliveryCreds:          deliveryCreds,
		adminCreds:             adminCreds,
		handlerSigner:          handlerSigner,
		httpAuthMW:             httpAuthMW,
		authzInterceptor:       authzInterceptor,
		authzEngine:            authzEngine,
		authCloser:             httpAuthCloser,
		metricsCloser:          metricsCloser,
		metricsRegisterer:      metricsRegisterer,
		metrics:                metrics,
		partitionTableNotifier: partitionTableNotifier,
		eventSourceNotifier:    eventSourceNotifier,
		webhookSourceNotifier:  webhookSourceNotifier,
		secretNotifier:         secretNotifier,
		caRootNotifier:         caRootNotifier,
		joinTokenNotifier:      joinTokenNotifier,
		lpOwnersNotifier:       lpOwnersNotifier,
		tenantNotifier:         tenantNotifier,
		tenantDEKNotifier:      tenantDEKNotifier,
		platformConfigNotifier: platformConfigNotifier,
		tenantDEKs:             tenantDEKs,
		tenantOIDC:             tenantOIDC,
		logger:                 logger,
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
	authzIc connect.Interceptor,
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
	path, handler := srv.NewHandler(connect.WithInterceptors(authzIc))
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
	authzIc connect.Interceptor,
	secrets *secretstore.Resolver,
	enforcer quota.Enforcer,
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
		Addr:             cfg.Ingress.Addr,
		TLS:              lc.ServerTLSConfig,
		Log:              logger,
		Middleware:       mw,
		AuthzInterceptor: authzIc,
		Enforcer:         enforcer,
	}
	icfg.ExtraRoutes = func(srv *ingress.Server) []connectserver.Route {
		var routes []connectserver.Route
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
	cfg                    Config
	eh                     *engine.Host
	adminEnabled           bool
	shards                 []uint64
	snapshotTriggers       map[uint64]chan struct{}
	deliverySrv            *connectserver.Server
	deliveryClient         *delivery.Client
	deliveryCreds          *creds.ListenerCreds
	adminCreds             *creds.ListenerCreds
	handlerSigner          *creds.Signer
	httpAuthMW             func(http.Handler) http.Handler
	authzInterceptor       connect.Interceptor
	authzEngine            *authz.Engine
	authCloser             func() error
	metricsCloser          func() error
	metricsRegisterer      prometheus.Registerer
	metrics                *observability.Metrics
	partitionTableNotifier *cluster.TableNotifier
	eventSourceNotifier    *cluster.TableNotifier
	webhookSourceNotifier  *cluster.TableNotifier
	secretNotifier         *cluster.TableNotifier
	caRootNotifier         *cluster.TableNotifier
	joinTokenNotifier      *cluster.TableNotifier
	lpOwnersNotifier       *cluster.TableNotifier
	tenantNotifier         *cluster.TableNotifier
	tenantDEKNotifier      *cluster.TableNotifier
	platformConfigNotifier *cluster.TableNotifier
	tenantDEKs             *secretstore.TenantDEKResolver
	tenantOIDC             *auth.TenantOIDCReconciler
	logger                 *slog.Logger
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
	authzInterceptor := d.authzInterceptor
	authzEngine := d.authzEngine
	authCloser := d.authCloser
	metricsCloser := d.metricsCloser
	metricsRegisterer := d.metricsRegisterer
	metrics := d.metrics
	partitionTableNotifier := d.partitionTableNotifier
	eventSourceNotifier := d.eventSourceNotifier
	webhookSourceNotifier := d.webhookSourceNotifier
	secretNotifier := d.secretNotifier
	lpOwnersNotifier := d.lpOwnersNotifier
	platformConfigNotifier := d.platformConfigNotifier
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
		adminSrv      *connectserver.Server
		bootstrapSrv  *connectserver.Server
		bootstrapCred *creds.ListenerCreds
		snapshotCxl   context.CancelFunc
		snapshotRepo  *snapshot.BlobRepository
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
		// proposalPrincipalInterceptor lifts auth.Principal into the
		// engine proposer's ctx key so every Raft proposal originating
		// from an admin/config Connect call stamps Envelope.Header.principal
		// — the durable audit trail's "who".
		opts := connect.WithInterceptors(d.authzInterceptor, proposalPrincipalInterceptor{})
		clusterPath, clusterH := clusterSrv.NewHandler(opts)
		configPath, configH := configSrv.NewHandler(opts)
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

	// PartitionTable Reconciler converges this node's running-shard set
	// with the cluster's shard-0 PartitionTable. Wakes on the notifier
	// bump (any UpdatePartitionTable / EvictNode / rebalance step
	// apply) or the 5s ticker (catches the post-snapshot-recovery case
	// where dragonboat replaces on-disk state without firing the apply
	// path). The first SyncRead is a no-op until shard 0 bootstraps.
	go func() {
		reader := partitionTableReader{host: eh}
		if rerr := engine.RunPartitionTableReconciler(ctx, partitionTableNotifier.Subscribe(), reader, eh, logger); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: partition table reconcile loop exited", "err", rerr)
		}
	}()

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

	// Authz Reconciler converges the Cedar engine's live policy set with
	// shard 0's PlatformConfigRecord. Empty row keeps the in-binary
	// foundational set; a policy that fails to compile keeps the previous one
	// — a bad reconcile can neither open the cluster up nor lock it out.
	go func() {
		reader := clusterAuthzReader{host: eh}
		if rerr := authzEngine.RunReconciler(ctx, platformConfigNotifier.Subscribe(), reader, logger); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: authz reconcile loop exited", "err", rerr)
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

	// Bootstrap listener: opt-in, TLS-without-client-cert. Hosts the
	// MeshSign service. The ClusterIssuer requires shard 0 to carry an
	// "active" CARoot row; without one we log + skip so an operator can
	// `reflowd config ca init` and restart to bring the listener up.
	if !cfg.Bootstrap.Disabled && cfg.Bootstrap.Addr != "" {
		bsCreds, perr := creds.Build(cfg.Bootstrap.Creds, logger)
		if perr != nil {
			return nil, fmt.Errorf("reflow: bootstrap creds: %w", perr)
		}
		bootstrapCred = bsCreds
		// The bootstrap port intentionally never requires a client
		// cert — joiners haven't been issued one yet. Force-clear
		// ClientAuth on the server side so an operator-shipped creds
		// spec with client_auth=true doesn't silently render the
		// bootstrap port unreachable from a virgin joiner.
		if bsCreds.ServerTLSConfig != nil {
			bsCreds.ServerTLSConfig.ClientAuth = tls.NoClientCert
			bsCreds.ServerTLSConfig.ClientCAs = nil
		}
		mode, merr := certmgrSigningMode(cfg.PKI.Builtin.SigningMode)
		if merr != nil {
			return nil, fmt.Errorf("reflow: pki.builtin.signing_mode: %w", merr)
		}
		issueCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		issuer, ierr := certmgr.NewClusterIssuer(issueCtx, certmgr.ClusterOptions{
			Reader:      caRootReader{host: eh},
			Keys:        secrets,
			SigningMode: mode,
			KMSKeyURI:   cfg.PKI.Builtin.KMSKeyURI,
			Principal:   "node/" + strconv.FormatUint(cfg.Node.ID, 10),
			Validity:    cfg.Bootstrap.LeafValidity,
		})
		cancel()
		if ierr != nil {
			logger.Warn("reflow: bootstrap listener skipped — no CA active yet",
				"err", ierr,
				"hint", "run `reflowd config ca init` to mint the cluster CA")
		} else {
			// Once the cluster CA is active, the same ClusterIssuer
			// powers IssueOperator on the Config admin RPC. Late-bind so
			// the operator-facing `reflowd config issue-operator` flow
			// works without a restart.
			configSrv.SetOperatorIssuer(issuer)
			bsServer, berr := bootstrap.NewServer(bootstrap.Config{
				Host:         eh,
				Runner:       runner,
				Issuer:       issuer,
				Log:          logger,
				LeafValidity: cfg.Bootstrap.LeafValidity,
			})
			if berr != nil {
				return nil, fmt.Errorf("reflow: bootstrap server: %w", berr)
			}
			bsPath, bsH := bsServer.NewHandler()
			bs, lErr := connectserver.New(ctx, connectserver.Config{
				Addr: cfg.Bootstrap.Addr,
				TLS:  bsCreds.ServerTLSConfig,
				Log:  logger,
			}, connectserver.Route{Path: bsPath, Handler: bsH})
			if lErr != nil {
				return nil, fmt.Errorf("reflow: bootstrap listener: %w", lErr)
			}
			bootstrapSrv = bs
			logger.Info("reflow: bootstrap listening", "addr", bs.Addr(),
				"driver", string(bsCreds.Driver))
		}
	}

	// TenantDEKResolver was constructed pre-HostConfig and handed to
	// the encstore wrapper at the StoreFactory closures. The reconcile
	// loop starts here so the Reader adapter has a live *engine.Host.
	// Until the first reconcile completes, encstore.Lookup returns
	// ErrTenantDEKUnavailable for any non-default tenant — apply path
	// reads observe Unavailable instead of corruption.
	go func() {
		reader := tenantDEKReader{host: eh}
		if rerr := d.tenantDEKs.RunReconciler(ctx, d.tenantDEKNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: tenant_dek reconcile loop exited", "err", rerr)
		}
	}()

	// TenantTable wake fan-out. TableNotifier is single-subscriber by
	// design — the propose-then-Subscribe test pattern relies on the
	// buffered-1 channel (see internal/engine/cluster/notifier.go).
	// When multiple consumers need the same wake (TenantTable drives
	// both the OIDC reconciler and the quota reconciler), pkg/reflow
	// subscribes once and relays to N dedicated buffered-1 channels.
	tenantWakeOIDC := make(chan struct{}, 1)
	tenantWakeQuota := make(chan struct{}, 1)
	go relayWake(ctx, d.tenantNotifier.Subscribe(), tenantWakeOIDC, tenantWakeQuota)

	// TenantOIDCReconciler keeps the jwtVerifier's byIssuer snapshot
	// aligned with the union of cluster-default issuers and per-tenant
	// issuers carried on TenantRecord.OidcIssuers. Reconciler is a
	// no-op when jwtVerifier is nil (no cluster-default OIDC and no
	// tenants yet — possible in SPIFFE-only deployments).
	go func() {
		reader := tenantReader{host: eh}
		if rerr := d.tenantOIDC.RunReconciler(ctx, tenantWakeOIDC, reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: tenant_oidc reconcile loop exited", "err", rerr)
		}
	}()

	// Construct the quota Manager before the ingress listener so the
	// Enforcer can be threaded into ingress.Config. NoopEnforcer is the
	// fallback when something downstream short-circuits.
	quotaMgr := quota.New(metricsRegisterer, logger)
	go func() {
		reader := tenantReader{host: eh}
		if rerr := quotaMgr.RunReconciler(ctx, tenantWakeQuota, reader, eh); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflow: quota reconcile loop exited", "err", rerr)
		}
	}()

	multiNode := len(cfg.Cluster.Peers) > 1
	ingressRT, ingressCreds, webhookMgr, err := startIngressListener(ctx, eh, cfg, multiNode, httpAuthMW, authzInterceptor, secrets, quotaMgr, metrics, metricsRegisterer, logger)
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
		bootstrapSrv:   bootstrapSrv,
		bootstrapCreds: bootstrapCred,
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
type clusterAuthzReader struct {
	host *engine.Host
}

func (r clusterAuthzReader) ClusterAuthzPolicy(ctx context.Context) (string, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := r.host.ClusterAuthzPolicy(readCtx)
	if err != nil {
		return "", 0, err
	}
	var text string
	if res.Record != nil {
		text = res.Record.GetClusterAuthzPolicyText()
	}
	return text, res.TableRevision, nil
}

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

// certmgrSigningMode maps the koanf-facing string ("local" or
// "kms_remote", empty defaults to "local") to the certmgr enum. Any
// other value returns an error so an operator typo doesn't silently
// fall back to local mode and contradict the configured KMS URI.
func certmgrSigningMode(s string) (certmgr.SigningMode, error) {
	switch s {
	case "", "local":
		return certmgr.SigningModeLocal, nil
	case "kms_remote":
		return certmgr.SigningModeRemote, nil
	default:
		return 0, fmt.Errorf("unknown signing mode %q (want \"local\" or \"kms_remote\")", s)
	}
}

// caRootReader is the certmgr.CARootReader adapter the bootstrap
// listener's ClusterIssuer uses to pull the active CA snapshot from
// shard 0.
type caRootReader struct {
	host *engine.Host
}

func (r caRootReader) CARoots(ctx context.Context) ([]*enginev1.CARootRecord, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.CARoots(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Records, list.TableRevision, nil
}

// tenantDEKReader is the Reader adapter the TenantDEKResolver uses
// to pull the latest set of TenantDEKRecord rows from shard 0.
type tenantDEKReader struct {
	host *engine.Host
}

func (r tenantDEKReader) ListTenantDEKs(ctx context.Context) ([]*enginev1.TenantDEKRecord, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.TenantDEKs(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Records, list.TableRevision, nil
}

// relayWake forwards each receive on src to a non-blocking send on
// every out channel. Used when a single TableNotifier needs to feed
// multiple consumer goroutines (TableNotifier is single-subscriber
// by design — see internal/engine/cluster/notifier.go — so the
// fan-out lives at this layer where the consumers are wired). Each
// out channel must be buffered-1 to preserve the coalesce-bursts
// contract; the relay drops sends when the consumer hasn't yet
// drained, identical to TableNotifier.Bump semantics. Exits on ctx
// cancel.
func relayWake(ctx context.Context, src <-chan struct{}, out ...chan<- struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-src:
			for _, c := range out {
				select {
				case c <- struct{}{}:
				default:
				}
			}
		}
	}
}

// tenantReader is the Reader adapter the TenantOIDCReconciler uses to
// pull the latest set of TenantRecord rows from shard 0.
type tenantReader struct {
	host *engine.Host
}

func (r tenantReader) ListTenants(ctx context.Context) ([]*enginev1.TenantRecord, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.Tenants(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Tenants, list.TableRevision, nil
}

// partitionTableReader is the Reader adapter the PartitionTable
// reconciler uses to pull the latest shard-0 PartitionTable. SyncReads
// via host.PartitionTable, which already pins a deadline internally for
// dragonboat.
type partitionTableReader struct {
	host *engine.Host
}

func (r partitionTableReader) SnapshotPartitionTable(ctx context.Context) (*enginev1.PartitionTable, error) {
	return r.host.PartitionTable(ctx)
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
	// Audit log retention. Pointer disambiguates "operator left it unset"
	// (nil → default 90d) from "operator explicitly disabled retention"
	// (non-nil zero → leader-scoped GC goroutine never spawns). Same
	// convention as RebalanceConfig.MinSecondsBetweenTransfers.
	if cfg.Audit.RetentionDuration == nil {
		def := 90 * 24 * time.Hour
		cfg.Audit.RetentionDuration = &def
	}
	if cfg.Audit.GcInterval == 0 {
		cfg.Audit.GcInterval = time.Hour
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
