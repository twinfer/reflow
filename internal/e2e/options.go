//go:build e2e

package e2e

// ContainerClusterOptions configures a fresh ContainerCluster.
//
// Defaults (when zero): N=3 reflowd nodes, NumShards=1. The smoke
// tier is insecure; mTLS lands in a follow-up PR. Once Toxiproxy and
// mTLS arrive, those toggles will be additive options on this same
// struct, never new constructors.
type ContainerClusterOptions struct {
	// N is the number of reflowd nodes in the cluster. Defaults to 3.
	N int

	// NumShards is the partition-shard modulus. Defaults to 1.
	NumShards uint64
}
