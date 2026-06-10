package reflw

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	connect "connectrpc.com/connect"
	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflwos/capability"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/auth"
	"github.com/twinfer/reflw/internal/authz"
	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/cluster"
	"github.com/twinfer/reflw/internal/engine/delivery"
	"github.com/twinfer/reflw/internal/engine/rebalance"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/engine/snapshot"
	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/internal/observability"
	"github.com/twinfer/reflw/internal/secretstore"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/handler/wire"
	hcvaultkms "github.com/twinfer/reflw/pkg/kms/hcvault"
	"github.com/twinfer/reflw/pkg/reflw/creds"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	"github.com/twinfer/reflw/pkg/reflwclient"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"

	// KMS providers always-linked, config-gated. Each subpackage's
	// init() calls registry.RegisterKMSClient under a sync.Once.
	// AWS / GCP self-register and read the standard credential chain;
	// Vault registers via the explicit hcvaultkms.Register call below
	// (it needs a token file). BlobKMS is the no-managed-KMS fallback.
	_ "github.com/twinfer/reflw/pkg/kms/awskms"
	_ "github.com/twinfer/reflw/pkg/kms/blob"
	_ "github.com/twinfer/reflw/pkg/kms/gcpkms"
)

// Run starts a reflw node from cfg and returns a Host. The Host owns
// goroutines and TCP listeners; call Host.Close (or cancel ctx) to shut down.
//
// Run is the only public entrypoint user binaries need. Typical usage:
//
//	cfg := reflw.Config{
//	    Node:    reflw.NodeConfig{ID: 1, RaftAddr: "127.0.0.1:5410"},
//	    Storage: reflw.StorageConfig{DataDir: "/var/lib/reflw"},
//	}
//	cfg.Handlers.Endpoints = []reflw.HandlerEndpoint{{URL: "http://localhost:9000"}}
//	host, err := reflw.Run(ctx, cfg)
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
			return nil, fmt.Errorf("reflw: hcvault register: %w", err)
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
			return nil, fmt.Errorf("reflw: handler signer: %w", sErr)
		}
		handlerSigner = hs
	}

	// Shard-0 TableNotifiers fan out apply-path commits to local
	// subsystems (secret + authz Reconcilers). Constructed before
	// engine.NewHost so the FSM picks them up at start; their
	// Subscribe() ends are handed to subsystem goroutines later.
	partitionTableNotifier := cluster.NewTableNotifier()
	secretNotifier := cluster.NewTableNotifier()
	modelNotifier := cluster.NewTableNotifier()
	lpOwnersNotifier := cluster.NewTableNotifier()
	rebalanceDrainNotifier := cluster.NewTableNotifier()
	platformConfigNotifier := cluster.NewTableNotifier()

	// Node-global Pebble caches, shared across every shard DB. Pebble's
	// per-DB default cache would otherwise multiply by shard count on
	// reflw's one-DB-per-shard layout, fragmenting the working set into
	// N independent LRUs. Owned for the Host's lifetime and Unref'd by
	// Host.Close after the engine closes its DBs; the deferred cleanup
	// below Unrefs them on any early-return error before that handoff.
	pebbleCache, pebbleFileCache := storage.NewSharedCaches(cfg.Storage.PebbleCacheBytes, int(numShards))
	cachesAdopted := false
	defer func() {
		if !cachesAdopted {
			pebbleCache.Unref()
			pebbleFileCache.Unref()
		}
	}()

	// Disk-stall handling. A DiskSlow event whose duration crosses
	// maxSyncDuration crashes the process once: a wedged disk can't make
	// progress, and staying in the Raft quorum while silently not
	// applying is worse than a crash that lets the orchestrator restart
	// us. <= 0 disables the crash (events are still logged + counted).
	var maxSyncDuration time.Duration
	if cfg.Storage.MaxSyncDurationMs > 0 {
		maxSyncDuration = time.Duration(cfg.Storage.MaxSyncDurationMs) * time.Millisecond
	}
	var stallOnce sync.Once
	pebbleTuning := storage.PebbleTuning{
		Cache:     pebbleCache,
		FileCache: pebbleFileCache,
		EventListener: metrics.NewPebbleEventListener(logger, maxSyncDuration, func(info pebble.DiskSlowInfo) {
			stallOnce.Do(func() {
				logger.Error("reflw: pebble disk stall — exiting to drop out of quorum",
					"path", info.Path, "duration", info.Duration)
				os.Exit(1)
			})
		}),
	}

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
		AdminEndpoint:      adminAdvertised(cfg),
		Peers:              toEnginePeers(cfg.Cluster.Peers),
		JoinExisting:       cfg.Cluster.JoinExisting,
		NumPartitionShards: numShards,
		Metrics:            metrics,
		PebbleOptions:      pebbleTuning.Options,
		EagerStateMaxBytes: cfg.Handlers.EagerStateMaxBytes,
		ClusterNotifiers: cluster.Notifiers{
			PartitionTable:      partitionTableNotifier,
			SecretTable:         secretNotifier,
			ModelTable:          modelNotifier,
			LPOwnersTable:       lpOwnersNotifier,
			RebalanceDrainTable: rebalanceDrainNotifier,
			PlatformConfigTable: platformConfigNotifier,
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
	// — surfaces as "reflw/creds: nil Signer" on the first
	// engine→handler dispatch.
	if handlerSigner != nil {
		hcfg.HandlerSigner = handlerSigner
	}
	// Process execution: install the reflwos engine adapter as the ProcessEngine
	// and register the capability-bridge handler into the in-process registry —
	// before the InProcDialer below captures it, so service tasks are
	// dispatchable. The resolver is either operator-injected (Process.Models,
	// embedded) or, when Process.Enabled with no injected resolver, a
	// table-backed resolver fed by the shard-0 ModelTable (its reconciler is
	// spawned in finishStartup once the host exists).
	var modelResolver processengine.ModelResolver
	var modelTableResolver *processengine.TableResolver
	if cfg.Process.Models != nil {
		modelResolver = cfg.Process.Models
	} else if cfg.Process.Enabled {
		modelTableResolver = processengine.NewTableResolver(logger)
		modelResolver = modelTableResolver
	}
	// modelPlanner backs config.RegisterModelSet with the real reflwos planner
	// whenever the process plane is active (injected or table-backed): it parses
	// every entry, derives each model's bundle (decisions/children/imports), and
	// validates the set ∪ existing table is dependency-closed and cycle-free — so
	// a broken or unresolved-import model is rejected at registration instead of
	// failing silently per-node at reconcile. Nil otherwise → config falls back
	// to its shallow well-formed-XML check with empty bundles.
	var modelPlanner admin.PlanModelSetFunc
	if modelResolver != nil {
		modelPlanner = processengine.PlanModelSet
	}
	if modelResolver != nil {
		hcfg.ProcessEngine = processengine.New(modelResolver)
		capReg := cfg.Process.Capabilities
		if capReg == nil {
			capReg = capability.NewRegistry()
		}
		if cfg.Handlers.InProcess == nil {
			cfg.Handlers.InProcess = handler.NewRegistry()
		}
		if err := processengine.RegisterBridge(cfg.Handlers.InProcess, capReg); err != nil {
			if handlerSigner != nil {
				handlerSigner.Close()
			}
			if metricsCloser != nil {
				_ = metricsCloser()
			}
			return nil, fmt.Errorf("reflw: register reflwos capability bridge: %w", err)
		}
	}
	// In-process handler: register an inproc transport so the engine
	// dispatches to the embedded Registry directly, no HTTP. The bridge
	// lives in inproc.go (internal/engine can't import pkg/handler).
	if cfg.Handlers.InProcess != nil {
		hcfg.InProcDialer = inprocDialer(cfg.Handlers.InProcess, wire.DefaultCodec())
	}
	eh, err := engine.NewHost(ctx, hcfg)
	if err != nil {
		if handlerSigner != nil {
			handlerSigner.Close()
		}
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, fmt.Errorf("reflw: NewHost: %w", err)
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
		nodeIdentity   *creds.NodeIdentity
		httpAuthCloser func() error
		httpAuthMW     func(http.Handler) http.Handler
	)
	bail := func(err error) (*Host, error) {
		if httpAuthCloser != nil {
			_ = httpAuthCloser()
		}
		_ = creds.CloseAll(deliveryCreds, adminCreds)
		if nodeIdentity != nil {
			_ = nodeIdentity.Close()
		}
		if handlerSigner != nil {
			handlerSigner.Close()
		}
		_ = eh.Close()
		if metricsCloser != nil {
			_ = metricsCloser()
		}
		return nil, err
	}

	var oidcCfg *auth.OIDCConfig
	if cfg.Auth.OIDC.Enabled() {
		oidcCfg = &auth.OIDCConfig{
			Issuer:      cfg.Auth.OIDC.Issuer,
			Audience:    cfg.Auth.OIDC.Audience,
			GroupsClaim: cfg.Auth.OIDC.GroupsClaim,
			ClaimKeys:   cfg.Auth.OIDC.ClaimKeys,
		}
		logger.Info("reflw: OIDC bearer auth enabled", "issuer", cfg.Auth.OIDC.Issuer)
	}
	mw, mwCloser, mwErr := auth.HTTPMiddleware(ctx, logger, oidcCfg)
	if mwErr != nil {
		return bail(fmt.Errorf("reflw: auth middleware: %w", mwErr))
	}
	httpAuthMW = mw
	httpAuthCloser = mwCloser

	// Cedar authorization engine, shared by every Connect service via the
	// authz interceptor. Seeded with the in-binary foundational policies
	// until PR3 moves policy text onto shard 0.
	authzEngine, azErr := authz.NewEngine([]byte(authz.FoundationalClusterPolicies))
	if azErr != nil {
		return bail(fmt.Errorf("reflw: authz engine: %w", azErr))
	}
	authzInterceptor := authz.NewInterceptor(authzEngine, logger, cfg.Auth.OIDC.Enabled())

	// Node mesh identity: when a cluster CA is configured, this node
	// self-issues its own node/<id> leaf from the config CA + KMS-wrapped
	// key, and the mesh listeners (admin, delivery) + the SelfJoin client
	// all present it. No central issuer, no join token, no bootstrap port.
	// When unset (single-node / dev) the listeners fall back to their own
	// X.Creds specs below.
	if cfg.ClusterCA.Enabled() {
		id, idErr := buildNodeIdentity(ctx, cfg, logger)
		if idErr != nil {
			return bail(fmt.Errorf("reflw: cluster CA identity: %w", idErr))
		}
		nodeIdentity = id
	}

	if crossShard {
		if nodeIdentity != nil {
			deliveryCreds = creds.MeshListenerCreds(nodeIdentity, true)
		} else {
			dc, derr := creds.Build(cfg.Delivery.Creds, logger)
			if derr != nil {
				return bail(fmt.Errorf("reflw: delivery creds: %w", derr))
			}
			deliveryCreds = dc
		}
		recordListenerSecurity(metrics, "delivery", deliveryCreds)
		if deliveryCreds.SecurityLevel == credentials.NoSecurity {
			logger.Warn("reflw: multi-node delivery using insecure transport — " +
				"node-to-node traffic is unauthenticated and unencrypted")
		}
	}
	if adminEnabled {
		if nodeIdentity != nil {
			adminCreds = creds.MeshListenerCreds(nodeIdentity, true)
		} else {
			ac, aerr := creds.Build(cfg.Admin.Creds, logger)
			if aerr != nil {
				return bail(fmt.Errorf("reflw: admin creds: %w", aerr))
			}
			adminCreds = ac
		}
		recordListenerSecurity(metrics, "admin", adminCreds)
	}

	var (
		deliverySrv    *connectserver.Server
		deliveryClient *delivery.Client
	)
	if crossShard {
		ds, dc, derr := startDeliveryListener(ctx, eh, cfg, deliveryCreds, httpAuthMW, authzInterceptor, metrics, logger)
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
		nodeIdentity:           nodeIdentity,
		handlerSigner:          handlerSigner,
		httpAuthMW:             httpAuthMW,
		authzInterceptor:       authzInterceptor,
		authzEngine:            authzEngine,
		authCloser:             httpAuthCloser,
		metricsCloser:          metricsCloser,
		metricsRegisterer:      metricsRegisterer,
		metrics:                metrics,
		partitionTableNotifier: partitionTableNotifier,
		secretNotifier:         secretNotifier,
		modelNotifier:          modelNotifier,
		modelTableResolver:     modelTableResolver,
		modelPlanner:           modelPlanner,
		lpOwnersNotifier:       lpOwnersNotifier,
		platformConfigNotifier: platformConfigNotifier,
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
	// The Host adopts the shared Pebble caches; Host.Close Unrefs them
	// after the engine closes its DBs. Suppress the deferred cleanup.
	host.pebbleCache = pebbleCache
	host.pebbleFileCache = pebbleFileCache
	cachesAdopted = true
	return host, nil
}

// startDeliveryListener builds the Delivery client (so partitions get a
// Sender on startup) and the Delivery Connect server. The two share one
// creds.Spec (mTLS or insecure) so the cluster forms a closed trust loop.
// The auth middleware is the same instance admin and ingress use; the
// starter policy gates /reflw.delivery.v1.Delivery/* to node/* principals.
func startDeliveryListener(
	ctx context.Context,
	eh *engine.Host,
	cfg Config,
	lc *creds.ListenerCreds,
	mw func(http.Handler) http.Handler,
	authzIc connect.Interceptor,
	metrics *observability.Metrics,
	logger *slog.Logger,
) (*connectserver.Server, *delivery.Client, error) {
	dc, err := delivery.NewClient(delivery.ClientConfig{
		Resolver:        eh,
		Log:             logger,
		ClientTLSConfig: lc.ClientTLSConfig,
		Metrics:         metrics,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("reflw: delivery client: %w", err)
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
		return nil, nil, fmt.Errorf("reflw: listen delivery %s: %w", cfg.Node.DeliveryAddr, cerr)
	}
	logger.Info("reflw: delivery listening", "addr", cs.Addr(),
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
	taskSchema ingress.TaskSchemaResolver,
	metrics *observability.Metrics,
	logger *slog.Logger,
) (*ingress.Runtime, *creds.ListenerCreds, error) {
	if err := validateWebhooks(cfg.Webhooks); err != nil {
		return nil, nil, fmt.Errorf("reflw: webhook config: %w", err)
	}
	if cfg.Ingress.Disabled {
		if len(cfg.Webhooks) > 0 {
			logger.Warn("reflw: webhooks configured but ingress is disabled; webhook routes will not be served",
				"count", len(cfg.Webhooks))
		}
		logger.Info("reflw: ingress disabled (cfg.ingress.disabled=true); clients must use a separate ingress fleet")
		return nil, nil, nil
	}
	lc, err := creds.Build(cfg.Ingress.Creds, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("reflw: ingress creds: %w", err)
	}
	recordListenerSecurity(metrics, "ingress", lc)
	if multiNode && lc.SecurityLevel == credentials.NoSecurity {
		logger.Warn("reflw: ingress is running on an insecure listener — multi-node deployments should configure cfg.Ingress.Creds")
	}
	icfg := ingress.Config{
		Addr:               cfg.Ingress.Addr,
		TLS:                lc.ServerTLSConfig,
		Log:                logger,
		Middleware:         mw,
		AuthzInterceptor:   authzIc,
		TaskSchemaResolver: taskSchema,
		CORS: ingress.CORSConfig{
			AllowedOrigins: cfg.Ingress.CORS.AllowedOrigins,
			AllowedHeaders: cfg.Ingress.CORS.AllowedHeaders,
			MaxAgeSeconds:  cfg.Ingress.CORS.MaxAgeSeconds,
		},
	}
	if len(cfg.Webhooks) > 0 {
		icfg.ExtraRoutes = webhookRoutes(cfg.Webhooks, secrets, logger)
	}
	rt, err := ingress.Start(ctx, eh, icfg)
	if err != nil {
		_ = creds.CloseAll(lc)
		return nil, nil, fmt.Errorf("reflw: ingress start: %w", err)
	}
	logger.Info("reflw: ingress listening",
		"addr", rt.Addr(),
		"driver", string(lc.Driver))
	return rt, lc, nil
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
	nodeIdentity           *creds.NodeIdentity
	handlerSigner          *creds.Signer
	httpAuthMW             func(http.Handler) http.Handler
	authzInterceptor       connect.Interceptor
	authzEngine            *authz.Engine
	authCloser             func() error
	metricsCloser          func() error
	metricsRegisterer      prometheus.Registerer
	metrics                *observability.Metrics
	partitionTableNotifier *cluster.TableNotifier
	secretNotifier         *cluster.TableNotifier
	modelNotifier          *cluster.TableNotifier
	modelTableResolver     *processengine.TableResolver
	modelPlanner           admin.PlanModelSetFunc
	lpOwnersNotifier       *cluster.TableNotifier
	platformConfigNotifier *cluster.TableNotifier
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
	nodeIdentity := d.nodeIdentity
	handlerSigner := d.handlerSigner
	httpAuthMW := d.httpAuthMW
	authzInterceptor := d.authzInterceptor
	authzEngine := d.authzEngine
	authCloser := d.authCloser
	metricsCloser := d.metricsCloser
	metricsRegisterer := d.metricsRegisterer
	metrics := d.metrics
	partitionTableNotifier := d.partitionTableNotifier
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
		if err := callSelfJoin(ctx, cfg, eh, nodeIdentity, logger); err != nil {
			return nil, fmt.Errorf("reflw: SelfJoin: %w", err)
		}
	}

	// Start shard 0 before partition shards so the partition table is
	// established as partitions come up.
	runner, err := eh.StartMetadataShard()
	if err != nil {
		return nil, fmt.Errorf("reflw: StartMetadataShard: %w", err)
	}
	logger.Info("reflw: metadata shard started", "shard", 0)

	for _, sh := range shards {
		if _, err := eh.StartPartition(sh); err != nil {
			return nil, fmt.Errorf("reflw: StartPartition(%d): %w", sh, err)
		}
		logger.Info("reflw: partition started", "shard", sh)
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
			return nil, fmt.Errorf("reflw: open snapshot bucket: %w", err)
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
					Metrics:    metrics,
				})
			}
			logger.Info("reflw: snapshot producer started",
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
					Metrics:       metrics,
				})
			}
			logger.Info("reflw: snapshot reaper started",
				"retain", cfg.Snapshot.Retain,
				"retention_age", cfg.Snapshot.RetentionAge,
				"tiered_daily", cfg.Snapshot.TieredDaily,
				"tiered_weekly", cfg.Snapshot.TieredWeekly,
				"tiered_monthly", cfg.Snapshot.TieredMonthly,
				"shards", shards)
		}
	}

	// Build the in-process Admin server unconditionally — it is the engine
	// proposer + deployment glue that autoSeedEndpoints needs, even when no
	// external listener is configured. The Connect listener only goes up when
	// adminEnabled.
	adminCfg := admin.Config{
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
		// With the process plane on, back RegisterModelSet with the real reflwos
		// planner (derives bundles + validates the dependency closure); nil
		// otherwise → the shallow XML check with empty bundles.
		PlanModelSet: d.modelPlanner,
	}
	// Avoid the typed-nil interface trap: only assign the Signer field when the
	// underlying *creds.Signer is non-nil.
	if handlerSigner != nil {
		adminCfg.Signer = handlerSigner
	}
	adminSvc, aErr := admin.NewServer(adminCfg)
	if aErr != nil {
		if snapshotCxl != nil {
			snapshotCxl()
		}
		if snapshotRepo != nil {
			_ = snapshotRepo.Close()
		}
		return nil, fmt.Errorf("reflw: admin server: %w", aErr)
	}

	if adminEnabled {
		// proposalPrincipalInterceptor lifts auth.Principal into the
		// engine proposer's ctx key so every Raft proposal originating
		// from an admin Connect call stamps Envelope.Header.principal
		// — the durable audit trail's "who".
		opts := connect.WithInterceptors(d.authzInterceptor, proposalPrincipalInterceptor{})
		adminPath, adminH := adminSvc.NewHandler(opts)
		cs, lErr := connectserver.New(ctx, connectserver.Config{
			Addr: cfg.Admin.Addr,
			TLS:  adminCreds.ServerTLSConfig,
			Log:  logger,
		},
			connectserver.Route{Path: adminPath, Handler: httpAuthMW(adminH)},
		)
		if lErr != nil {
			if snapshotCxl != nil {
				snapshotCxl()
			}
			if snapshotRepo != nil {
				_ = snapshotRepo.Close()
			}
			return nil, fmt.Errorf("reflw: admin listener: %w", lErr)
		}
		adminSrv = cs
		logger.Info("reflw: admin listening", "addr", cs.Addr(),
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
			logger.Warn("reflw: partition table reconcile loop exited", "err", rerr)
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
			logger.Warn("reflw: routing reconcile loop exited", "err", rerr)
		}
	}()

	// Authz Reconciler converges the Cedar engine's live policy set with
	// shard 0's PlatformConfigRecord. Empty row keeps the in-binary
	// foundational set; a policy that fails to compile keeps the previous one
	// — a bad reconcile can neither open the cluster up nor lock it out.
	go func() {
		reader := clusterAuthzReader{host: eh}
		if rerr := authzEngine.RunReconciler(ctx, platformConfigNotifier.Subscribe(), reader, logger); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflw: authz reconcile loop exited", "err", rerr)
		}
	}()

	// SecretStore Resolver runs alongside the ingress listener so the
	// webhook Manager can Lookup(name) on each reconcile pass. The
	// Resolver itself is hot-path safe — no per-call work in Lookup.
	secrets := secretstore.New(metricsRegisterer, logger)
	go func() {
		reader := secretReader{host: eh}
		if rerr := secrets.RunReconciler(ctx, secretNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
			logger.Warn("reflw: secretstore reconcile loop exited", "err", rerr)
		}
	}()

	// Table-backed model resolver (durable process plane): reparse the shard-0
	// ModelTable on each notifier wake, serving parsed graphs from an in-memory
	// cache. Only spawned when Process.Enabled selected the table resolver.
	if d.modelTableResolver != nil {
		go func() {
			reader := modelReader{host: eh}
			if rerr := d.modelTableResolver.RunReconciler(ctx, d.modelNotifier.Subscribe(), reader); rerr != nil && !errors.Is(rerr, context.Canceled) {
				logger.Warn("reflw: model resolver reconcile loop exited", "err", rerr)
			}
		}()
	}

	multiNode := len(cfg.Cluster.Peers) > 1
	// GET /v1/tasks/{token} returns a parked task's submission schema when the
	// active model resolver can derive it; the table-backed resolver does. Absent or
	// non-capable → the read returns the descriptor only. Guarded so a nil
	// *TableResolver never becomes a non-nil interface wrapping a nil pointer.
	var taskSchema ingress.TaskSchemaResolver
	if d.modelTableResolver != nil {
		taskSchema = d.modelTableResolver
	}
	ingressRT, ingressCreds, err := startIngressListener(ctx, eh, cfg, multiNode, httpAuthMW, authzInterceptor, secrets, taskSchema, metrics, logger)
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

	// Auto-seed remote-handler deployments from config. Spawned AFTER
	// the last error-returning step so a failed Run doesn't leave this
	// goroutine running with no Host to attach to. Each failure inside
	// the seed loop is logged and the next endpoint is tried; ctx is the
	// Run caller's context — cancelling it cancels the seed loop.
	if len(cfg.Handlers.Endpoints) > 0 {
		go autoSeedEndpoints(ctx, adminSvc, runner, cfg.Handlers.Endpoints, logger)
	}
	if cfg.Handlers.InProcess != nil {
		go autoSeedInProc(ctx, adminSvc, runner, cfg.Handlers.InProcess, logger)
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
		nodeIdentity:   nodeIdentity,
		authCloser:     authCloser,
		snapshotCxl:    snapshotCxl,
		snapshotRepo:   snapshotRepo,
		handlerSigner:  handlerSigner,
	}, nil
}

// clusterAuthzReader is the Reader adapter the cluster authz policy
// reconciler uses to pull desired state.
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

// modelReader adapts engine.Host.Models to the processengine.ModelTableReader the
// TableResolver reconciler reads each ModelTable wake.
type modelReader struct {
	host *engine.Host
}

func (r modelReader) ListModels(ctx context.Context) ([]*enginev1.ModelRecord, uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	list, err := r.host.Models(readCtx)
	if err != nil {
		return nil, 0, err
	}
	return list.Records, list.TableRevision, nil
}

// ClusterCAKeyAAD binds the KMS-wrapped CA key ciphertext to the
// cluster-CA slot. `reflwd config ca init` seals with this AAD and
// buildNodeIdentity unwraps with it, so a ciphertext sealed for some
// other secret (which uses its name as AAD) can't be replayed here.
// Exported so the CLI seal path and this unwrap path share one value.
const ClusterCAKeyAAD = "reflw-cluster-ca/v1"

// buildNodeIdentity resolves the cluster CA (public cert from config,
// KMS-wrapped key unwrapped from the config blob URI) and builds this
// node's self-issued mesh identity. Called once at startup; every mesh
// listener + the SelfJoin client share the returned identity.
func buildNodeIdentity(ctx context.Context, cfg Config, log *slog.Logger) (*creds.NodeIdentity, error) {
	certPEM, err := loadClusterCACert(cfg.ClusterCA)
	if err != nil {
		return nil, err
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	keyPEM, err := secretstore.ResolveRemoteEncrypted(resolveCtx, &enginev1.RemoteEncryptedSecret{
		BlobUri: cfg.ClusterCA.KeyBlobURI,
		KekUri:  cfg.ClusterCA.KeyKEKURI,
	}, []byte(ClusterCAKeyAAD), nil)
	if err != nil {
		return nil, fmt.Errorf("unwrap CA key: %w", err)
	}
	dir := cfg.ClusterCA.CertCacheDir
	if dir == "" {
		dir = filepath.Join(cfg.Storage.DataDir, "mesh")
	}
	idStr := strconv.FormatUint(cfg.Node.ID, 10)
	return creds.BuildNodeIdentity(ctx, creds.NodeIdentityOptions{
		CACertPEM:    certPEM,
		CAKeyPEM:     keyPEM,
		NodeID:       idStr,
		Principal:    "node/" + idStr,
		Hosts:        meshLeafHosts(cfg),
		Validity:     cfg.ClusterCA.LeafValidity,
		CertCacheDir: dir,
		Logger:       log,
	})
}

// meshLeafHosts returns the SANs to embed in the self-issued node leaf:
// the host parts of the node's advertised raft / delivery / admin
// addresses plus any operator-declared cfg.ClusterCA.LeafHosts. The mesh
// verifies peers by CN, but external admin-CLI clients verify hostname,
// so the leaf must cover the addresses they dial. Wildcard binds
// (0.0.0.0 / ::) and empties are dropped.
func meshLeafHosts(cfg Config) []string {
	raw := []string{
		hostOnly(cfg.Node.RaftAdvertisedAddr),
		hostOnly(cfg.Node.RaftAddr),
		hostOnly(cfg.Node.DeliveryAddr),
		hostOnly(cfg.Admin.Addr),
	}
	raw = append(raw, cfg.ClusterCA.LeafHosts...)
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, h := range raw {
		if h == "" || h == "0.0.0.0" || h == "::" || seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	return out
}

// adminAdvertised returns the admin endpoint to publish via gossip: the
// explicit AdvertisedAddr, or the bind Addr when unset. A joiner's
// SelfJoin resolves the leader through this value, so it must be
// routable — a 0.0.0.0 bind with no AdvertisedAddr is undiallable.
func adminAdvertised(cfg Config) string {
	if cfg.Admin.AdvertisedAddr != "" {
		return cfg.Admin.AdvertisedAddr
	}
	return cfg.Admin.Addr
}

// hostOnly strips a :port suffix, returning the host part (or the input
// unchanged when it has no port).
func hostOnly(addr string) string {
	if addr == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// loadClusterCACert returns the CA cert PEM from the inline value or the
// configured file. The cert is public — no KMS involved.
func loadClusterCACert(c ClusterCAConfig) ([]byte, error) {
	if c.CACertPEM != "" {
		return []byte(c.CACertPEM), nil
	}
	pem, err := os.ReadFile(c.CACertFile)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %s: %w", c.CACertFile, err)
	}
	return pem, nil
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
			log.Warn("reflw: auto-seed endpoints: shard 0 leadership not reached within 2m; skipping",
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
			log.Warn("reflw: auto-seed endpoint failed", "url", ep.URL, "err", err)
			continue
		}
		log.Info("reflw: auto-seed endpoint registered",
			"url", ep.URL, "deployment_id", depID)
	}
}

// inprocDeploymentURL is the synthetic URL an in-process handler deployment
// is registered under. The "inproc" scheme routes to the in-process dialer
// installed on the engine's handlerclient Registry (HostConfig.InProcDialer).
const inprocDeploymentURL = "inproc://local"

// autoSeedInProc waits for shard 0 leadership, then registers the embedded
// handler Registry as a single inproc:// deployment. Discovery is
// synthesized locally via handler.LocalDiscovery — the engine cannot dial an
// inproc URL to run the usual /discover probe. Fire-and-forget; mirrors
// autoSeedEndpoints' leadership wait.
func autoSeedInProc(ctx context.Context, srv *admin.Server, runner *engine.MetadataRunner, reg *handler.Registry, log *slog.Logger) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if runner != nil && runner.IsLeader() {
			break
		}
		if time.Now().After(deadline) {
			log.Warn("reflw: auto-seed in-process handler: shard 0 leadership not reached within 2m; skipping")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	regCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	depID, err := srv.AutoSeedLocal(regCtx, inprocDeploymentURL, handler.LocalDiscovery(reg).GetHandlers(), 0)
	if err != nil {
		log.Warn("reflw: auto-seed in-process handler failed", "err", err)
		return
	}
	log.Info("reflw: auto-seed in-process handler registered",
		"deployment_id", depID, "handlers", reg.Len())
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
		return errors.New("reflw: Node.ID must be > 0")
	}
	if cfg.Node.RaftAddr == "" {
		return errors.New("reflw: Node.RaftAddr is required")
	}
	if cfg.Storage.DataDir == "" {
		return errors.New("reflw: Storage.DataDir is required")
	}
	return nil
}

func withDefaults(cfg Config) Config {
	if !cfg.Metrics.Disabled && cfg.Metrics.Addr == "" {
		cfg.Metrics.Addr = ":9090"
	}
	// Ingress is started by Run unless cfg.Ingress.Disabled. If the
	// operator left Addr empty, fall back to the well-known reflw port
	// so `reflw.Run` works out of the box.
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
	if cfg.Storage.MaxSyncDurationMs == 0 {
		// Unset → arm disk-stall detection at the storage default (20s).
		// A negative value disables it and is left untouched.
		cfg.Storage.MaxSyncDurationMs = int64(storage.DefaultMaxSyncDuration / time.Millisecond)
	}
	return cfg
}

func buildLogger(cfg LoggingConfig) *slog.Logger {
	if cfg.Handler != nil {
		return slog.New(cfg.Handler).With("service", "reflw")
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
		log.Info("reflw: metrics listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("reflw: metrics server exited", "err", err)
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
// surfaces as an error so the process supervisor restarts reflwd.
//
// CallWithLeaderRedirect handles per-attempt LeaderHint chasing (the
// gossip view was stale by one heartbeat); the outer retry here handles
// transient cluster-wide Unavailable conditions during cold start.
func callSelfJoin(ctx context.Context, cfg Config, host *engine.Host, id *creds.NodeIdentity, log *slog.Logger) error {
	req := &adminv1.AddNodeRequest{
		NodeId:       cfg.Node.ID,
		RaftAddr:     cfg.Node.RaftAddr,
		GossipAddr:   cfg.Node.GossipAdvAddr,
		GrpcEndpoint: cfg.Node.DeliveryAddr,
	}
	if req.GossipAddr == "" {
		req.GossipAddr = cfg.Node.GossipBindAddr
	}
	// The joiner authenticates with its self-issued mesh leaf. When no
	// cluster CA is configured (single-node-style dev cluster being
	// extended) fall back to the admin creds spec.
	dialOpts := func(addr string) reflwclient.DialOptions {
		if id != nil {
			return reflwclient.DialOptions{Addr: addr, ClientTLSConfig: id.ClientTLSConfig()}
		}
		return reflwclient.DialOptions{Addr: addr, Creds: cfg.Admin.Creds}
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
		err = reflwclient.CallWithLeaderRedirect(ctx, dialOpts(leaderAddr),
			3, func(rctx context.Context, cli *reflwclient.Client) error {
				_, e := cli.Admin.SelfJoin(rctx, connect.NewRequest(req))
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
