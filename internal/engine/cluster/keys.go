// Package cluster implements the metadata Raft group (shard 0) FSM and its
// per-shard Pebble store. Shard 0 holds the cluster-wide partition table
// and node membership records; shards 1..N are the partition state
// machines defined in package engine.
//
// Keys live in their own DB (a separate Pebble directory under the host's
// DataDir), so the codec here does NOT prefix with a shard id.
//
// Namespaces (top-level prefixes):
//
//	meta                            -> PartitionMeta singleton (reuses the
//	                                   partition meta proto; only applied_index
//	                                   and latest_announced_epoch are populated)
//	node/<8-byte BE node_id>        -> NodeMembership
//	partition_table                 -> PartitionTable singleton
//	deployment/<deployment_id ascii> -> DeploymentRecord
//	deployment_idx/<service>\x00<handler> -> deployment_id (ascii)
//
// All multi-byte integers are big-endian so lexicographic byte order
// matches numeric order — same convention as internal/storage/keys.
package cluster

import (
	"encoding/binary"
)

const (
	metaPrefix            = "meta"
	nodePrefix            = "node/"
	partitionTabKey       = "partition_table"
	deploymentPrefix      = "deployment/"
	deploymentIndexPrefix = "deployment_idx/"
)

// MetaKey returns the singleton key for the metadata shard's PartitionMeta.
func MetaKey() []byte { return []byte(metaPrefix) }

// NodePrefix returns the node/ namespace prefix. Used for iteration.
func NodePrefix() []byte { return []byte(nodePrefix) }

// NodeKey returns node/<8-byte BE node_id>.
func NodeKey(nodeID uint64) []byte {
	out := make([]byte, 0, len(nodePrefix)+8)
	out = append(out, nodePrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nodeID)
	return append(out, buf[:]...)
}

// PartitionTableKey returns the singleton key for the PartitionTable.
func PartitionTableKey() []byte { return []byte(partitionTabKey) }

// DeploymentPrefix returns the deployment/ namespace prefix. Used for
// iteration via a forward range scan.
func DeploymentPrefix() []byte { return []byte(deploymentPrefix) }

// DeploymentKey returns deployment/<id>. Deployment ids are caller-shaped
// strings (UUIDv4 for operator-registered deployments, "inproc-<16-hex>"
// for the synthetic inproc deployment).
func DeploymentKey(id string) []byte {
	out := make([]byte, 0, len(deploymentPrefix)+len(id))
	out = append(out, deploymentPrefix...)
	return append(out, id...)
}

// DeploymentIndexKey returns deployment_idx/<service>\x00<handler>.
// The NUL separator can't appear in proto string fields, so the byte
// boundary is unambiguous. Value: the deployment_id (ascii) currently
// answering for (service, handler).
func DeploymentIndexKey(service, handler string) []byte {
	out := make([]byte, 0, len(deploymentIndexPrefix)+len(service)+1+len(handler))
	out = append(out, deploymentIndexPrefix...)
	out = append(out, service...)
	out = append(out, 0x00)
	return append(out, handler...)
}
