//go:build e2e

package e2e

// ContainerClusterOptions configures a fresh ContainerCluster.
//
// Defaults (when zero): N=3 reflowd nodes, NumShards=1. The smoke
// tier is insecure; mTLS lands in a follow-up PR. Toggles are
// additive on this same struct, never new constructors.
type ContainerClusterOptions struct {
	// N is the number of reflowd nodes in the cluster. Defaults to 3.
	N int

	// NumShards is the partition-shard modulus. Defaults to 1.
	NumShards uint64

	// WithToxiproxy installs one Toxiproxy sidecar per reflowd node and
	// routes every node's raft traffic through its own sidecar. When
	// enabled, ContainerCluster.Tx is non-nil and exposes Cut/Heal per
	// unordered or ordered pair. The bufconn PartitionMatrix this
	// replaces was symmetric per-pair; this preserves that semantic.
	WithToxiproxy bool
}
