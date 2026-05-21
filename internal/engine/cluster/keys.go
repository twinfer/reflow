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
//	eventsrc/<name>                 -> EventSourceRecord
//	webhooksrc/<name>               -> WebhookSourceRecord
//	secret/<name>                   -> SecretRecord
//	lpowner/<4-byte BE lp>          -> LPOwnerRecord (lp → shard_id
//	                                   routing entry; per-node reconcilers
//	                                   List/Snapshot this on each table
//	                                   notifier wake to refresh the
//	                                   Partitioner's atomic snapshot)
//	tablerev/<table_name>           -> TableRevision singleton (CAS guard
//	                                   for cluster-managed config tables;
//	                                   separate top-level namespace so it
//	                                   never sits inside the data range
//	                                   iterated by List())
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
	eventSourcePrefix     = "eventsrc/"
	webhookSourcePrefix   = "webhooksrc/"
	secretPrefix          = "secret/"
	lpOwnerPrefix         = "lpowner/"
	tableRevisionPrefix   = "tablerev/"
)

// Canonical short names for revision-tracked tables. Used as the
// argument to RevisionKey; persisted on disk, so renaming is an
// upgrade-incompat change.
const (
	RevisionTableEventSource   = "eventsrc"
	RevisionTableWebhookSource = "webhooksrc"
	RevisionTableSecret        = "secret"
	RevisionTableLPOwners      = "lpowners"
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

// DeploymentKey returns deployment/<id>. Deployment ids are UUIDv4
// strings minted by the admin RegisterDeployment RPC.
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

// EventSourcePrefix returns the eventsrc/ namespace prefix. Used for
// iteration via a forward range scan.
func EventSourcePrefix() []byte { return []byte(eventSourcePrefix) }

// EventSourceKey returns eventsrc/<name>. Name uniqueness is enforced
// by the admin RPC validator; the apply arm trusts it.
func EventSourceKey(name string) []byte {
	out := make([]byte, 0, len(eventSourcePrefix)+len(name))
	out = append(out, eventSourcePrefix...)
	return append(out, name...)
}

// WebhookSourcePrefix returns the webhooksrc/ namespace prefix.
func WebhookSourcePrefix() []byte { return []byte(webhookSourcePrefix) }

// WebhookSourceKey returns webhooksrc/<name>. Name uniqueness (and
// path uniqueness across rows) is enforced by the admin RPC validator;
// the apply arm trusts it.
func WebhookSourceKey(name string) []byte {
	out := make([]byte, 0, len(webhookSourcePrefix)+len(name))
	out = append(out, webhookSourcePrefix...)
	return append(out, name...)
}

// SecretPrefix returns the secret/ namespace prefix. Used for iteration.
func SecretPrefix() []byte { return []byte(secretPrefix) }

// SecretKey returns secret/<name>. Name uniqueness is enforced by the
// admin RPC validator; the apply arm trusts it.
func SecretKey(name string) []byte {
	out := make([]byte, 0, len(secretPrefix)+len(name))
	out = append(out, secretPrefix...)
	return append(out, name...)
}

// LPOwnerPrefix returns the lpowner/ namespace prefix. Used for forward
// range iteration; rows sort in lp order because the 4-byte BE encoding
// of lp follows the prefix.
func LPOwnerPrefix() []byte { return []byte(lpOwnerPrefix) }

// LPOwnerKey returns lpowner/<4-byte BE lp>. Big-endian so lexicographic
// byte order matches numeric lp order — range scans iterate LPs in
// ascending sequence.
func LPOwnerKey(lp uint32) []byte {
	out := make([]byte, 0, len(lpOwnerPrefix)+4)
	out = append(out, lpOwnerPrefix...)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], lp)
	return append(out, buf[:]...)
}

// RevisionKey returns the CAS singleton key for a table identified by
// its canonical short name (e.g. RevisionTableEventSource). Lives in a
// separate top-level namespace from the table's own data so List/range
// iteration over the data never observes the revision row.
func RevisionKey(tableName string) []byte {
	out := make([]byte, 0, len(tableRevisionPrefix)+len(tableName))
	out = append(out, tableRevisionPrefix...)
	return append(out, tableName...)
}
