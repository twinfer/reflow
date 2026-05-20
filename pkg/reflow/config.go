// Package reflow is the public entrypoint for embedding the reflow durable
// execution engine in a Go binary. Construct a Config (programmatically or
// via pkg/reflow/config loaders), declare handler deployments via
// Config.Handlers.Endpoints (each entry is a URL the engine dials over
// HTTP/2), then call Run. Internal types in /internal/engine are not
// part of the stable API.
package reflow

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/reflow/creds"
)

// EventSourcesConfig is re-exported from internal/ingress/eventsource so
// the public Config surface stays in pkg/reflow while the implementation
// lives next to its consumers.
type EventSourcesConfig = eventsource.Config

// Config is the typed configuration for a reflow node. All fields are
// optional except Node.ID, Node.RaftAddr, and Storage.DataDir. Defaults
// are filled in by Run.
//
// Field-name notes: koanf tags use snake_case so the same config can be
// expressed identically in YAML, JSON, and env vars (the env provider
// translates REFLOW_INGRESS_GRPC_ADDR → ingress.grpc_addr).
//
// Transport security plugs in per-listener via creds.Spec; the zero
// spec is insecure. Multi-node deployments are allowed on insecure
// transport but Run emits a WARN log.
type Config struct {
	Node         NodeConfig         `koanf:"node"`
	Cluster      ClusterConfig      `koanf:"cluster"`
	Storage      StorageConfig      `koanf:"storage"`
	Ingress      IngressConfig      `koanf:"ingress"`
	Metrics      MetricsConfig      `koanf:"metrics"`
	Logging      LoggingConfig      `koanf:"logging"`
	Delivery     DeliveryConfig     `koanf:"delivery"`
	Admin        AdminConfig        `koanf:"admin"`
	Auth         AuthConfig         `koanf:"auth"`
	Snapshot     SnapshotConfig     `koanf:"snapshot"`
	Handlers     HandlersConfig     `koanf:"handlers"`
	EventSources EventSourcesConfig `koanf:"event_sources"`
}

// HandlersConfig groups the handler-related knobs. Endpoints lists
// handler-deployment URLs reflow.Run auto-registers at metadata-leader
// bootstrap. Each URL goes through the same admin.RegisterDeployment
// path operators would call by hand, so the engine discovers handlers,
// persists a DeploymentRecord on shard 0, and surfaces the
// deployment_id for inbound invocations to pin to.
type HandlersConfig struct {
	// Endpoints lists handler URLs to auto-register at bootstrap. Each
	// URL is dialed for protocol discovery (GET /discover over HTTP/2)
	// and the resulting handlers are persisted via
	// Command_RegisterDeployment.
	Endpoints []HandlerEndpoint `koanf:"endpoints"`

	// EagerStateMaxBytes caps the eager-state snapshot the engine ships
	// in StartMessage.state_map. Zero means "use the engine default"
	// (64 KiB). Raise it for state-heavy handlers that read many keys per
	// invocation (every key that fits avoids a 2-slot lazy fetch); lower
	// it to cap per-session wire + handler memory at the cost of more
	// lazy round-trips. Snapshots that exceed the cap surface as
	// StartMessage.partial_state=true and the SDK falls back to
	// GetLazyState.
	EagerStateMaxBytes uint32 `koanf:"eager_state_max_bytes"`
}

// HandlerEndpoint is one remote-handler URL the operator wants Run to
// register at startup. URL must be parseable and use http:// (h2c) or
// https:// (HTTP/2 + TLS).
type HandlerEndpoint struct {
	URL string `koanf:"url"`
}

// DeliveryConfig configures the cross-shard Delivery gRPC surface used
// for node-to-node outbox dispatch. Addr is the listen address (also
// advertised via gossip). Creds selects the transport-security driver
// for both the server and the outbound client.
type DeliveryConfig struct {
	Creds creds.Spec `koanf:"creds"`
}

// AdminConfig configures the admin gRPC server.
type AdminConfig struct {
	// Addr is the listen address for the admin gRPC server. Empty
	// disables the server (single-node deployments rarely need it).
	Addr string `koanf:"addr"`
	// Disabled forces the admin server off even when Addr is set. Used
	// by tests + multi-process embedders that ship their own surface.
	Disabled bool `koanf:"disabled"`
	// Creds selects the transport-security driver for the admin
	// listener.
	Creds creds.Spec `koanf:"creds"`
}

// AuthConfig drives the authentication + authorization interceptor
// stack shared by Admin and Delivery listeners.
type AuthConfig struct {
	// TrustDomain is the SPIFFE trust domain expected on TLS leaves
	// (and reflected in JWT claim mapping later). Empty defaults to
	// DefaultTrustDomain.
	TrustDomain string `koanf:"trust_domain"`
	// PolicyFile, when non-empty, points at a JSON authz policy that
	// is hot-reloaded via grpc-go's authz.FileWatcher. Empty installs
	// the embedded starter policy (see internal/auth).
	PolicyFile string `koanf:"policy_file"`
}

// SnapshotConfig configures the per-partition DR snapshot producer,
// the archive repository, and the retention reaper.
//
// The URL selects the gocloud.dev/blob driver:
//
//	file:///mnt/reflow-snaps       local filesystem (dev / NFS / shared volume)
//	s3://bucket?prefix=reflow/     AWS S3
//	gs://bucket?prefix=reflow/     Google Cloud Storage
//	azblob://container?prefix=…    Azure Blob Storage
//	mem://                         in-memory (tests only)
//
// Provider-native flags (SSE-KMS, CMEK, Azure SSE, regions, custom
// endpoints) ride on the URL query string; reflow doesn't interpret
// them — see https://gocloud.dev/concepts/urls/.
type SnapshotConfig struct {
	// URL identifies the snapshot bucket. Empty disables archiving;
	// admin snapshot RPCs return FailedPrecondition.
	URL string `koanf:"url"`
	// Interval between automatic snapshot cycles per partition shard.
	// Zero disables the producer; admin RPC CreateSnapshot still works.
	Interval time.Duration `koanf:"interval"`
	// Retain is the per-shard count retention enforced inline on Put.
	// 0 means "retain all". Combine with RetentionAge for time-based GC.
	Retain int `koanf:"retain"`
	// RetentionAge drops archives whose mod time is older than this.
	// 0 disables age-based reaping. The reaper polls at hourly cadence.
	RetentionAge time.Duration `koanf:"retention_age"`
	// TieredDaily / TieredWeekly / TieredMonthly enable GFS-style
	// retention: keep one archive per recent UTC day / ISO week /
	// calendar month, up to the configured slot count. Any non-zero
	// value puts the shard's reaper into tiered mode and disables both
	// Retain and RetentionAge for that shard.
	//
	// A common policy — "last 7 daily, 4 weekly, 12 monthly" — is:
	//   TieredDaily: 7, TieredWeekly: 4, TieredMonthly: 12.
	TieredDaily   int `koanf:"tiered_daily"`
	TieredWeekly  int `koanf:"tiered_weekly"`
	TieredMonthly int `koanf:"tiered_monthly"`
	// ScratchDir is where dragonboat Exported snapshots land before
	// archiving. Empty falls back to $TMPDIR/reflow-snapshot-scratch.
	ScratchDir string `koanf:"scratch_dir"`
}

// DefaultTrustDomain is the SPIFFE trust domain used when
// AuthConfig.TrustDomain is empty.
const DefaultTrustDomain = creds.DefaultTrustDomain

// trustDomainOrDefault returns cfg.Auth.TrustDomain or
// DefaultTrustDomain when unset.
func (c AuthConfig) trustDomainOrDefault() string {
	if c.TrustDomain == "" {
		return DefaultTrustDomain
	}
	return c.TrustDomain
}

// NodeConfig identifies this node in the cluster.
type NodeConfig struct {
	// ID is the replica ID for this node. Must be > 0. In single-node
	// deployments use 1.
	ID uint64 `koanf:"id"`
	// RaftAddr is the host:port dragonboat advertises for inter-node Raft
	// traffic. For single-node use a localhost port.
	RaftAddr string `koanf:"raft_addr"`
	// GossipBindAddr is the address dragonboat's gossip layer binds to
	// (host:port). Required when Cluster.Peers is non-empty.
	GossipBindAddr string `koanf:"gossip_bind_addr"`
	// GossipAdvAddr is the address advertised to peers for NAT traversal.
	// Falls back to GossipBindAddr when empty.
	GossipAdvAddr string `koanf:"gossip_adv_addr"`
	// DeliveryAddr is the host:port for this node's reflow Delivery gRPC
	// listener (cross-partition outbox dispatch). Required when
	// Cluster.Peers is non-empty; advertised via gossip NodeHostMeta so
	// peers can resolve it.
	DeliveryAddr string `koanf:"delivery_addr"`
}

// Peer is one entry in a static cluster topology. GossipAddr is required
// when Cluster.Peers is non-empty (every peer entry, including self);
// NodeHostID is optional and defaults to a stable derivation from NodeID.
type Peer struct {
	NodeID     uint64 `koanf:"node_id"`
	RaftAddr   string `koanf:"raft_addr"`
	GossipAddr string `koanf:"gossip_addr"`
	NodeHostID string `koanf:"node_host_id"`
	// AdminAddr, when set, is this peer's reflow Admin gRPC endpoint.
	// Not consulted by the joiner SelfJoin path (which discovers the
	// leader via gossip-published NodeHostMeta.admin_endpoint), but
	// kept here so the `reflowd cluster ...` CLI and tests can resolve peers
	// by NodeID from config without hard-coding host:port. Optional.
	AdminAddr string `koanf:"admin_addr"`
}

// ClusterConfig describes the multi-node cluster. Single-node deployments
// leave Peers empty.
type ClusterConfig struct {
	// Shards lists the partition shard IDs this node should host. Defaults
	// to []uint64{1} when empty.
	Shards []uint64 `koanf:"shards"`
	// Peers is the static topology — non-empty for multi-node bootstrap.
	Peers []Peer `koanf:"peers"`
	// JoinExisting, when true, starts this node as a joiner of an
	// already-running cluster: dragonboat StartOnDiskReplica is called
	// with (nil, join=true) so the node catches up from a Raft snapshot
	// instead of seeding the initial membership. The operator must have
	// run `reflowd cluster add-node` (which proposes RegisterNode +
	// PROMOTE_TO_VOTER) against an existing leader first; that workflow
	// is what makes this ReplicaID a known member of every shard's
	// configuration. Default false preserves the static-bootstrap path.
	JoinExisting bool `koanf:"join_existing"`
}

// StorageConfig configures the on-disk state.
type StorageConfig struct {
	// DataDir holds per-partition state and dragonboat's Raft log.
	// Layout: <DataDir>/raft/, <DataDir>/p{shardID}/state/.
	DataDir string `koanf:"data_dir"`
}

// IngressConfig configures the client-facing Connect listener. One
// HTTP/2 port serves Connect / gRPC / gRPC-Web / HTTP-JSON via Connect
// content negotiation. reflow.Run starts ingress unless Disabled is
// true — it is how clients submit invocations.
type IngressConfig struct {
	// Disabled skips starting the ingress server on this node. Use when
	// clients submit via a separate ingress fleet; the engine still
	// serves internal cluster traffic (delivery, admin) normally.
	Disabled bool `koanf:"disabled"`
	// Addr is the listen address. Empty defaults to ":8080".
	Addr string `koanf:"addr"`
	// Creds selects the transport-security driver. Zero spec is
	// insecure; multi-node deployments emit a startup warning so the
	// operator knows the client surface is unauthenticated.
	Creds creds.Spec `koanf:"creds"`
	// HTTP toggles the REST-style facade mounted on the same listener
	// under /v1/*. Same Connect-typed ingress.Server, same auth
	// middleware, no second port.
	HTTP IngressHTTPConfig `koanf:"http"`
}

// IngressHTTPConfig configures the REST surface mounted at /v1/* on the
// ingress listener. The REST handlers delegate to the same in-process
// ingress.Server as Connect, so routing / idempotency / error semantics
// are identical.
type IngressHTTPConfig struct {
	// Disabled skips mounting the /v1/* router. The Connect ingress at
	// /reflow.ingress.v1.Ingress/* is unaffected.
	Disabled bool `koanf:"disabled"`
	// MaxBodyBytes caps inbound request bodies on /v1/*. Zero falls back
	// to a 1 MiB default. Requests larger than the cap are rejected with
	// 413 Payload Too Large before any handler logic runs.
	MaxBodyBytes int64 `koanf:"max_body_bytes"`
	// MaxPollMs caps long-poll endpoints (/v1/attach, /v1/call) at the
	// REST surface. Zero falls back to 30_000 (30s), chosen below common
	// LB idle timeouts (ALB / ingress-nginx default 60s) so REST clients
	// don't see surprise 502s. The Connect surface continues to cap at
	// 60s independently.
	MaxPollMs int `koanf:"max_poll_ms"`
	// TrustedProxies is the CIDR allowlist of immediate-peer addresses
	// permitted to set X-Forwarded-Host / RFC 7239 Forwarded headers.
	// Empty (secure default) means forwarded-host headers are ignored.
	// Reserved for future hostmux wiring; not consumed by the v1 runtime
	// path.
	TrustedProxies []string `koanf:"trusted_proxies"`
}

// MetricsConfig configures the Prometheus collector and the optional
// /metrics HTTP server.
type MetricsConfig struct {
	// Addr is the listen address for the /metrics HTTP server. Empty
	// means use the default ":9090". Set Disabled to skip the HTTP server
	// entirely while keeping the collectors registered.
	Addr string `koanf:"addr"`
	// Registry is the Prometheus registry to register against. nil uses
	// prometheus.DefaultRegisterer. Not loaded from config.
	Registry *prometheus.Registry `koanf:"-"`
	// Disabled skips both registering collectors and starting the HTTP
	// server. Use when embedding into a process that owns its own
	// Prometheus stack.
	Disabled bool `koanf:"disabled"`
}

// LoggingConfig configures slog. If Handler is nil a JSON handler writing to
// stderr is used at Level. If both fields are zero an info-level JSON logger
// is installed.
type LoggingConfig struct {
	Level slog.Level `koanf:"level"`
	// Handler is programmatic-only: callers wanting a custom slog.Handler
	// inject it after loading.
	Handler slog.Handler `koanf:"-"`
}
