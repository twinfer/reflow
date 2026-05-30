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

	"github.com/twinfer/reflow/pkg/reflow/creds"
)

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
	Bootstrap    BootstrapConfig    `koanf:"bootstrap"`
	Auth         AuthConfig         `koanf:"auth"`
	Snapshot     SnapshotConfig     `koanf:"snapshot"`
	Handlers     HandlersConfig     `koanf:"handlers"`
	KMS          KMSConfig          `koanf:"kms"`
	PKI          PKIConfig          `koanf:"pki"`
	Rebalance    RebalanceConfig    `koanf:"rebalance"`
	Audit        AuditConfig        `koanf:"audit"`
}

// AuditConfig configures the cluster-FSM config-change audit log.
// Every operator-initiated mutation on shard 0 (Upsert*, Delete*, ...)
// is written into an append-only AuditLogTable in the same Pebble
// batch as the mutation, and optionally pushed to Logger for SIEM
// fan-out. The durable Pebble write is always performed; Logger is
// the slog escape hatch.
//
// Reflow ships no sink. Operators wire Logger themselves and compose
// fan-out via Go 1.26 stdlib slog.NewMultiHandler — see
// internal/audit for the recipe. The retention loop on the metadata
// leader periodically range-deletes rows older than RetentionDuration
// via a Raft-proposed Command_GcAuditLog; the cadence is GcInterval
// (default 1h).
type AuditConfig struct {
	// Logger is programmatic-only: operators wanting slog fan-out
	// inject a *slog.Logger after loading koanf config. Nil disables
	// the slog path; the durable AuditLogTable Pebble write is
	// unconditional.
	Logger *slog.Logger `koanf:"-"`

	// RetentionDuration bounds how long audit rows persist on shard 0.
	// The metadata leader proposes Command_GcAuditLog{before_ts_ms}
	// every GcInterval to delete older rows.
	//
	// Pointer to disambiguate "unset" (nil → withDefaults fills 90d)
	// from "explicit 0" (non-nil pointer to 0 → retention disabled,
	// rows accumulate indefinitely — fine for tests). Same convention
	// as RebalanceConfig.MinSecondsBetweenTransfers.
	RetentionDuration *time.Duration `koanf:"retention_duration"`

	// GcInterval is the cadence at which the leader-scoped GC
	// goroutine proposes a retention pass. Zero defaults to 1 hour.
	// Retention precision is bounded by GcInterval, not by the storage
	// layer.
	GcInterval time.Duration `koanf:"gc_interval"`
}

// KMSConfig configures the KMS providers Reflow registers in Tink's
// process-global KMSClient registry. Four providers ship in-binary:
//
//   - blobkms+<gocloud-uri> — no-managed-KMS fallback (self-registered)
//   - aws-kms://...          — AWS KMS via AWS SDK v2 credential chain
//   - gcp-kms://...          — GCP Cloud KMS via Application Default Credentials
//   - hcvault://...          — HashiCorp Vault Transit, opt-in via Vault config below
//
// AWS and GCP self-register from package init() and pick up
// credentials from the host environment (env vars, IAM role, workload
// identity, etc.) — no Reflow config knobs needed. Vault is opt-in
// because it requires an explicit token; populate VaultKMSConfig.TokenFile
// to enable it.
type KMSConfig struct {
	Vault VaultKMSConfig `koanf:"vault"`
}

// PKIConfig configures how the cluster CA signs leaves. Today the
// only Issuer implementation is "builtin" (the CARootTable-backed
// ClusterIssuer); the sub-scope leaves room for future Issuer plugins
// (ACME, smallstep, k8s cert-manager) without renaming fields.
type PKIConfig struct {
	Builtin BuiltinPKIConfig `koanf:"builtin"`
}

// BuiltinPKIConfig tunes the shard-0-backed ClusterIssuer. SigningMode
// picks the CA signing-key source:
//
//   - "local" (default): the CA private key is fetched from shard 0's
//     SecretTable as PEM bytes on every Refresh. KEK-wrapped at rest;
//     plaintext key bytes appear in process memory for as long as the
//     active CA snapshot is held.
//   - "kms_remote": KMSKeyURI is dispatched through certmgr's
//     RemoteSigner registry to obtain a crypto.Signer that proxies
//     every Sign() call to the KMS. The CA private key NEVER enters
//     process memory. Operators wire concrete provider factories
//     (AWS KMS, GCP Cloud KMS, Vault Transit, Azure Key Vault, …) at
//     startup via certmgr.RegisterRemoteSigner before reflow.Run.
//
// kms_remote is cluster-wide: the cert PEM in CARootTable is bound to
// the KMS-held key, so every node MUST be configured with the same
// signing mode + URI. Mixing modes within one cluster will trip the
// cert-vs-signer public-key check at Refresh time and the node will
// refuse to issue leaves.
type BuiltinPKIConfig struct {
	// SigningMode selects "local" or "kms_remote". Empty defaults to
	// "local" for backwards-compatibility with pre-PR-5 deployments.
	SigningMode string `koanf:"signing_mode"`

	// KMSKeyURI is the URI of the KMS-managed signing key handle, e.g.
	// "aws-kms://arn:aws:kms:us-east-1:123:key/abc",
	// "gcp-kms://projects/p/locations/l/keyRings/r/cryptoKeys/k",
	// "hcvault://addr/transit/keys/name". Required when
	// SigningMode="kms_remote"; ignored otherwise.
	KMSKeyURI string `koanf:"kms_key_uri"`
}

// VaultKMSConfig configures the HashiCorp Vault Transit KMS provider.
// Empty TokenFile disables Vault registration entirely.
type VaultKMSConfig struct {
	// Address optionally narrows the registered URI prefix. Empty
	// registers the broad "hcvault://" prefix (any Vault host is
	// accepted via the URI). Set e.g. to "vault.prod:8200" to constrain
	// the registered prefix to "hcvault://vault.prod:8200".
	Address string `koanf:"address"`

	// TokenFile is the path to a file holding the Vault token. Required
	// (empty disables Vault registration). Read once at startup; token
	// rotation requires a restart in v1.
	TokenFile string `koanf:"token_file"`
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

// BootstrapConfig configures the kubeadm-style joiner credential
// exchange (MeshSign). The bootstrap listener is opt-in and segregated
// from Admin so a freshly-installed joiner with no mesh leaf can reach
// it without bypassing the Admin RequireAndVerifyClientCert gate. The
// join token in the request is the authorization fact, not a TLS peer
// cert. Operators typically enable the bootstrap port on a couple of
// nodes behind a firewall rule that only allows freshly-imaged hosts
// to reach it.
type BootstrapConfig struct {
	// Addr is the listen address. Empty disables the bootstrap server.
	Addr string `koanf:"addr"`
	// Disabled forces the bootstrap server off even when Addr is set.
	Disabled bool `koanf:"disabled"`
	// Creds selects the transport-security driver. TLS without client
	// auth is the supported mode; the bootstrap server rejects start
	// when ClientAuth would be required.
	Creds creds.Spec `koanf:"creds"`
	// LeafValidity is the validity stamped onto signed bootstrap
	// leaves. Zero defaults to 24h, matching certmgr's per-renewal
	// default — joiners renew via their per-node ClusterIssuer once
	// running.
	LeafValidity time.Duration `koanf:"leaf_validity"`
}

// AuthConfig drives the authentication + authorization interceptor
// stack shared by Admin, Delivery, and Ingress listeners. Authentication
// is mesh-leaf mTLS (principal from the verified leaf CN); a request with
// no client cert produces an anonymous principal, and the Cedar
// authorization interceptor (internal/authz) decides whether anonymous is
// acceptable for each procedure. The struct is currently empty — per-
// listener transport security is configured via the *.Creds fields.
type AuthConfig struct{}

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

// RebalanceConfig configures the autonomous LP rebalancer. Lives on the
// metadata-shard (shard 0) leader; one node in the cluster runs the loop
// at a time, naturally chosen by the Raft leader election that already
// drives metadataRebalancer and lpMover.
//
// Mode is the master switch:
//
//   - "off"      — control loop not started; no observation, no decisions.
//   - "advisory" — loop runs, emits metrics, logs candidate moves with
//     outcome=would_transfer, but never proposes a transfer.
//     Use this to validate the planner against production-
//     shaped traffic before flipping to "auto".
//   - "auto"     — full automation. Candidate moves above SkewEngagePct
//     are proposed through the same Command_InitiateLPTransfer
//     path the manual `reflowd cluster transfer-lp` CLI uses,
//     so they show up in ListLPTransfers with no extra
//     plumbing.
//
// Triggers in this version are limited to:
//   - membership change (NodeRegistry update reshapes the planner's
//     input shard set)
//   - operator-requested drain (RebalanceDrainTable on shard 0; drained
//     shards are subtracted from the planner's input)
//
// Load-based triggers (per-shard QPS, p99) and capacity circuit breakers
// (Pebble L0, write amp) are out of scope for PR 5.0.
//
// Defaults are conservative — basically "do one move a minute, never
// engage unless seriously skewed."
type RebalanceConfig struct {
	// Mode is "off" (default), "advisory", or "auto". Anything else is
	// treated as "off" with a startup warning.
	Mode string `koanf:"mode"`
	// MaxConcurrentTransfers caps the count of non-terminal rows in
	// LPTransferTable the rebalancer is willing to maintain. Zero
	// defaults to 1.
	MaxConcurrentTransfers uint32 `koanf:"max_concurrent_transfers"`
	// MinSecondsBetweenTransfers is the minimum gap between two new
	// transfer initiations, measured against the most recent
	// LPTransferRecord.started_at_ms. Pointer so an explicit 0
	// (operator wants no cooldown) is distinguishable from "unset" —
	// koanf decodes a present `min_seconds_between_transfers: 0`
	// into a non-nil pointer to 0, an absent key leaves the field
	// nil and withDefaults fills 60.
	MinSecondsBetweenTransfers *uint32 `koanf:"min_seconds_between_transfers"`
	// SkewEngagePct is the mis-placement percentage (0..100) above
	// which the rebalancer engages and starts proposing moves. Zero
	// defaults to 15. Skew is computed as
	//
	//   len(routing.Diff(current, desired)) / total_LPs * 100
	//
	// — the fraction of LPs whose current shard != the planner's
	// desired shard.
	SkewEngagePct uint32 `koanf:"skew_engage_pct"`
	// SkewDisengagePct is the threshold below which the rebalancer
	// disengages once engaged. Must be < SkewEngagePct; the gap is
	// the hysteresis band. Zero defaults to 8.
	SkewDisengagePct uint32 `koanf:"skew_disengage_pct"`
}

// RebalanceMode constants match the koanf-decoded string values for
// RebalanceConfig.Mode. Exported so callers and tests can compare
// without bare strings.
const (
	RebalanceModeOff      = "off"
	RebalanceModeAdvisory = "advisory"
	RebalanceModeAuto     = "auto"
)

// NodeConfig identifies this node in the cluster.
type NodeConfig struct {
	// ID is the replica ID for this node. Must be > 0. In single-node
	// deployments use 1.
	ID uint64 `koanf:"id"`
	// RaftAddr is the host:port dragonboat binds to for inter-node Raft
	// traffic. For single-node use a localhost port. When
	// RaftAdvertisedAddr is empty, RaftAddr also serves as the address
	// gossiped to peers (today's combined-bind-and-advertise behavior).
	RaftAddr string `koanf:"raft_addr"`
	// RaftAdvertisedAddr, when non-empty, is the address gossiped to
	// peers; bind stays at RaftAddr. Required when the node sits behind
	// NAT, a load balancer, or (in the e2e harness) a Toxiproxy listener.
	// Empty preserves combined bind+advertise behavior.
	RaftAdvertisedAddr string `koanf:"raft_advertised_addr"`
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
	// NumPartitionShards is the cluster-wide partition shard count (shard
	// ids 1..S), independent of peer count and replication factor. 0 means
	// auto: the peer count (1 for solo). Every shard is replicated on every
	// peer (RF=N); physical shard ids are always contiguous 1..S.
	NumPartitionShards uint64 `koanf:"num_partition_shards"`
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
