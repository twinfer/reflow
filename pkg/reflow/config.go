// Package reflow is the public entrypoint for embedding the reflow durable
// execution engine in a Go binary. Construct a Config (programmatically or
// via pkg/reflow/config loaders), register handlers on Config.Handlers,
// then call Run.
//
// Phase 2 surface only. Internal types in /internal/engine are not part of
// the stable API.
package reflow

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/pkg/sdk"
)

// Config is the typed configuration for a reflow node. All fields are
// optional except Node.ID, Node.RaftAddr, and Storage.DataDir. Defaults
// are filled in by Run.
//
// Field-name notes: koanf tags use snake_case so the same config can be
// expressed identically in YAML, JSON, and env vars (the env provider
// translates REFLOW_INGRESS_GRPC_ADDR → ingress.grpc_addr).
type Config struct {
	Node    NodeConfig    `koanf:"node"`
	Cluster ClusterConfig `koanf:"cluster"`
	Storage StorageConfig `koanf:"storage"`
	Ingress IngressConfig `koanf:"ingress"`
	Metrics MetricsConfig `koanf:"metrics"`
	Logging LoggingConfig `koanf:"logging"`
	// TLS is the single-CA mTLS configuration. Required when
	// Cluster.Peers is non-empty — Delivery (between nodes) and Admin
	// (between operators and nodes) both terminate mTLS using the same
	// node-leaf cert. Role distinction is enforced by SPIFFE URI SANs
	// on each leaf (see pkg/reflow/tls.go).
	TLS TLSConfig `koanf:"tls"`
	// Admin is the cluster admin gRPC surface. Phase 4.2.
	Admin AdminConfig `koanf:"admin"`
	// Snapshot configures the off-host DR snapshot archive. Phase 4.2.
	Snapshot SnapshotConfig `koanf:"snapshot"`
	Handlers *sdk.Registry  `koanf:"-"`
}

// AdminConfig configures the admin gRPC server. Phase 4.2.
type AdminConfig struct {
	// Addr is the listen address for the admin gRPC server. Empty
	// disables the server (single-node deployments rarely need it).
	Addr string `koanf:"addr"`
	// Disabled forces the admin server off even when Addr is set. Used
	// by tests + multi-process embedders that ship their own surface.
	Disabled bool `koanf:"disabled"`
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

// TLSConfig groups the PEM file paths that drive reflow's mTLS plus the
// SPIFFE trust domain stamped on every issued leaf. See pkg/reflow/tls.go
// for the single-CA + URI-SAN contract.
type TLSConfig struct {
	CAFile      string `koanf:"ca_file"`
	CertFile    string `koanf:"cert_file"`
	KeyFile     string `koanf:"key_file"`
	TrustDomain string `koanf:"trust_domain"`
}

// DefaultTrustDomain is the SPIFFE trust domain used when TLSConfig
// leaves it empty.
const DefaultTrustDomain = "reflow.local"

// TrustDomainOrDefault returns the configured trust domain or
// DefaultTrustDomain when unset.
func (c TLSConfig) TrustDomainOrDefault() string {
	if c.TrustDomain == "" {
		return DefaultTrustDomain
	}
	return c.TrustDomain
}

// files renders TLSConfig as the internal TLSFiles struct used by the
// helper builders. Kept as a small projection so the public Config
// surface doesn't depend on tls.go internals.
func (c TLSConfig) files() TLSFiles {
	return TLSFiles{
		CAFile:   c.CAFile,
		CertFile: c.CertFile,
		KeyFile:  c.KeyFile,
	}
}

// IsZero reports whether no TLS file paths have been configured. Used by
// Run to gate "multi-node requires TLS" validation. TrustDomain is
// intentionally ignored — it has a sensible default and shouldn't on
// its own flip cluster-mode wiring.
func (c TLSConfig) IsZero() bool {
	return c.CAFile == "" && c.CertFile == "" && c.KeyFile == ""
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
	// (host:port). Required when Cluster.Peers is non-empty. Phase 4.1.
	GossipBindAddr string `koanf:"gossip_bind_addr"`
	// GossipAdvAddr is the address advertised to peers for NAT traversal.
	// Falls back to GossipBindAddr when empty. Phase 4.1.
	GossipAdvAddr string `koanf:"gossip_adv_addr"`
	// DeliveryAddr is the host:port for this node's reflow Delivery gRPC
	// listener (cross-partition outbox dispatch). Required when
	// Cluster.Peers is non-empty; advertised via gossip NodeHostMeta so
	// peers can resolve it. Phase 4.1.
	DeliveryAddr string `koanf:"delivery_addr"`
}

// BootstrapMode determines how the node joins the cluster.
type BootstrapMode int

const (
	// BootstrapSingleNode runs the node alone. Phase 2 default.
	BootstrapSingleNode BootstrapMode = iota
	// BootstrapStaticPeers reads peers from ClusterConfig.Peers. Phase 4.
	BootstrapStaticPeers
	// BootstrapDiscovery uses gossip/discovery to find peers. Phase 5.
	BootstrapDiscovery
)

// Peer is one entry in a static cluster topology. Phase 4.1: GossipAddr
// is required when Cluster.Peers is non-empty (every peer entry,
// including self); NodeHostID is optional and defaults to a stable
// derivation from NodeID.
type Peer struct {
	NodeID     uint64 `koanf:"node_id"`
	RaftAddr   string `koanf:"raft_addr"`
	GossipAddr string `koanf:"gossip_addr"`
	NodeHostID string `koanf:"node_host_id"`
}

// ClusterConfig describes the multi-node cluster. Phase 2 ignores Peers,
// BootstrapMode, and RaftTLSCert (single-node only); Phase 4 wires them.
//
// Secrets-as-config: RaftTLSCert is the actual cert material loaded by a
// koanf provider (Vault, AWS SM, GCP SM, file, env). No inline ${secret:}
// template — the chosen koanf provider populates the field directly.
type ClusterConfig struct {
	// Shards lists the partition shard IDs this node should host. Phase 2
	// defaults to []uint64{1} when empty.
	Shards []uint64 `koanf:"shards"`
	// Peers is the static topology for BootstrapStaticPeers. Phase 4.
	Peers []Peer `koanf:"peers"`
	// BootstrapMode selects how the cluster forms. Phase 4.
	BootstrapMode BootstrapMode `koanf:"bootstrap_mode"`
	// RaftTLSCert is the PEM-encoded TLS cert for inter-node Raft
	// transport, typically populated from a secret-store provider
	// (Vault, AWS SM). Phase 4+.
	RaftTLSCert []byte `koanf:"raft_tls_cert"`
	// RaftTLSKey is the matching private key. Phase 4+.
	RaftTLSKey []byte `koanf:"raft_tls_key"`
	// JoinExisting, when true, starts this node as a joiner of an
	// already-running cluster: dragonboat StartOnDiskReplica is called
	// with (nil, join=true) so the node catches up from a Raft snapshot
	// instead of seeding the initial membership. The operator must have
	// run `reflow-cluster add-node` (which proposes RegisterNode +
	// PROMOTE_TO_VOTER) against an existing leader first; that workflow
	// is what makes this ReplicaID a known member of every shard's
	// configuration. Default false preserves the static-bootstrap path.
	JoinExisting bool `koanf:"join_existing"`
}

// SnapshotRepository abstracts the destination for partition snapshots
// (SAD §6.12). Phase 2 does not ship any driver; nil means "local only".
// The interface body lands when the first driver does, in Phase 2.5+.
type SnapshotRepository any

// StorageConfig configures the on-disk state.
type StorageConfig struct {
	// DataDir holds per-partition state and dragonboat's Raft log.
	// Layout: <DataDir>/raft/, <DataDir>/p{shardID}/state/.
	DataDir string `koanf:"data_dir"`
	// SnapshotRepo is the optional remote snapshot destination. nil means
	// local snapshots only. Not loaded from config — set programmatically.
	SnapshotRepo SnapshotRepository `koanf:"-"`
}

// IngressConfig configures the client-facing gRPC + HTTP/JSON gateway
// (which also hosts the SessionService stub for out-of-process SDKs).
type IngressConfig struct {
	GRPCAddr string      `koanf:"grpc_addr"`
	HTTPAddr string      `koanf:"http_addr"`
	TLS      *tls.Config `koanf:"-"` // assembled from TLSCert/TLSKey at Run time
	// TLSCert is the PEM-encoded server cert for the ingress endpoint.
	// Loaded as bytes from any koanf provider (file, Vault, AWS SM).
	TLSCert []byte `koanf:"tls_cert"`
	// TLSKey is the matching private key.
	TLSKey   []byte `koanf:"tls_key"`
	Disabled bool   `koanf:"disabled"`
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
