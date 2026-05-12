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
	Node     NodeConfig    `koanf:"node"`
	Cluster  ClusterConfig `koanf:"cluster"`
	Storage  StorageConfig `koanf:"storage"`
	Ingress  IngressConfig `koanf:"ingress"`
	Metrics  MetricsConfig `koanf:"metrics"`
	Logging  LoggingConfig `koanf:"logging"`
	Handlers *sdk.Registry `koanf:"-"`
}

// NodeConfig identifies this node in the cluster.
type NodeConfig struct {
	// ID is the replica ID for this node. Must be > 0. In single-node
	// deployments use 1.
	ID uint64 `koanf:"id"`
	// RaftAddr is the host:port dragonboat advertises for inter-node Raft
	// traffic. For single-node use a localhost port.
	RaftAddr string `koanf:"raft_addr"`
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

// Peer is one entry in a static cluster topology.
type Peer struct {
	NodeID   uint64 `koanf:"node_id"`
	RaftAddr string `koanf:"raft_addr"`
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
