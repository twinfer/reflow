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
//	lptransfer/<transfer_id ascii>  -> LPTransferRecord (in-progress LP
//	                                   transfer saga; lpMover reads/writes
//	                                   on the metadata leader)
//	rebalance_drain/<8-byte BE shard_id> -> RebalanceDrainRecord
//	                                   (operator-requested drains;
//	                                   autonomous rebalancer subtracts
//	                                   drained shards from the planner's
//	                                   input set)
//	tenant/<4-byte BE id>           -> TenantRecord (id=0 reserved for
//	                                   the default-tenant sentinel and
//	                                   never persisted)
//	tenant_name_idx/<name>          -> 4-byte BE tenant_id (name → id
//	                                   secondary index; the Config
//	                                   server resolves create-vs-update
//	                                   by reading this on the leader
//	                                   before propose)
//	tenant_dek/<4-byte BE tenant_id> -> TenantDEKRecord (per-tenant
//	                                   data-encryption-key reference;
//	                                   per-node TenantDEKResolver
//	                                   SyncRead-iterates this on each
//	                                   table notifier wake to refresh
//	                                   the in-memory tenant_id→AEAD
//	                                   map; the DEK plaintext never
//	                                   leaves the resolving node's
//	                                   process memory)
//	auditlog/<8-byte BE raft_index> -> AuditLogRecord (append-only
//	                                   config-change audit; written
//	                                   in the same Batch as the
//	                                   audited mutation. raft_index
//	                                   is monotonic and unique across
//	                                   the cluster lifetime, so the
//	                                   key never collides. Retention
//	                                   GC range-deletes by raft_index
//	                                   span derived from ts_ms.)
//	caroot/<name>                   -> CARootRecord (one row per
//	                                   cluster CA root; "active" is
//	                                   the conventional row name).
//	jointoken/<hex token_hash>      -> JoinTokenRecord (kubeadm-style
//	                                   one-time bootstrap credential;
//	                                   plaintext only at create time,
//	                                   persisted as sha256 hash; the
//	                                   MeshSign apply path consumes
//	                                   single_use=true rows atomically).
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
	platformConfigKey     = "platform_config"
	deploymentPrefix      = "deployment/"
	deploymentIndexPrefix = "deployment_idx/"
	eventSourcePrefix     = "eventsrc/"
	webhookSourcePrefix   = "webhooksrc/"
	secretPrefix          = "secret/"
	lpOwnerPrefix         = "lpowner/"
	lpTransferPrefix      = "lptransfer/"
	rebalanceDrainPrefix  = "rebalance_drain/"
	tenantPrefix          = "tenant/"
	tenantNameIndexPrefix = "tenant_name_idx/"
	tenantDEKPrefix       = "tenant_dek/"
	auditLogPrefix        = "auditlog/"
	caRootPrefix          = "caroot/"
	joinTokenPrefix       = "jointoken/"
	tableRevisionPrefix   = "tablerev/"
)

// Canonical short names for revision-tracked tables. Used as the
// argument to RevisionKey; persisted on disk, so renaming is an
// upgrade-incompat change.
const (
	RevisionTableDeployment     = "deployment"
	RevisionTableEventSource    = "eventsrc"
	RevisionTableWebhookSource  = "webhooksrc"
	RevisionTableSecret         = "secret"
	RevisionTableLPOwners       = "lpowners"
	RevisionTableLPTransfers    = "lptransfers"
	RevisionTableRebalanceDrain = "rebalance_drain"
	RevisionTableTenant         = "tenant"
	RevisionTableTenantDEK      = "tenant_dek"
	RevisionTableCARoot         = "caroot"
	RevisionTableJoinToken      = "jointoken"
	RevisionTablePlatformConfig = "platformconfig"
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

// PlatformConfigKey returns the singleton key for the PlatformConfigRecord.
func PlatformConfigKey() []byte { return []byte(platformConfigKey) }

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

// CARootPrefix returns the caroot/ namespace prefix. Used for iteration.
func CARootPrefix() []byte { return []byte(caRootPrefix) }

// CARootKey returns caroot/<name>. The conventional row name is
// "active"; rotation appends rows whose name encodes the rotation
// epoch so the historical chain remains queryable.
func CARootKey(name string) []byte {
	out := make([]byte, 0, len(caRootPrefix)+len(name))
	out = append(out, caRootPrefix...)
	return append(out, name...)
}

// JoinTokenPrefix returns the jointoken/ namespace prefix. Used for
// iteration; the bootstrap server scans this on each MeshSign call to
// locate the redeemed token by its sha256 hash.
func JoinTokenPrefix() []byte { return []byte(joinTokenPrefix) }

// JoinTokenKey returns jointoken/<hex-token-hash>. The hex encoding
// keeps the key printable for audit logs and avoids embedding raw
// binary bytes in the Pebble key namespace.
func JoinTokenKey(tokenHash []byte) []byte {
	const hextab = "0123456789abcdef"
	out := make([]byte, 0, len(joinTokenPrefix)+2*len(tokenHash))
	out = append(out, joinTokenPrefix...)
	for _, b := range tokenHash {
		out = append(out, hextab[b>>4], hextab[b&0x0F])
	}
	return out
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

// LPTransferPrefix returns the lptransfer/ namespace prefix. Used by
// LPTransferTable.List for the lpMover's per-tick scan.
func LPTransferPrefix() []byte { return []byte(lpTransferPrefix) }

// LPTransferKey returns lptransfer/<transfer_id>. transfer_id is a
// UUIDv4 string minted by Admin/TransferLP; rows sort lexicographically
// (no scan ordering requirement — the lpMover advances each row
// independently).
func LPTransferKey(transferID string) []byte {
	out := make([]byte, 0, len(lpTransferPrefix)+len(transferID))
	out = append(out, lpTransferPrefix...)
	return append(out, transferID...)
}

// RebalanceDrainPrefix returns the rebalance_drain/ namespace prefix.
// Used for forward range iteration; rows sort in shard_id order because
// the 8-byte BE encoding of shard_id follows the prefix.
func RebalanceDrainPrefix() []byte { return []byte(rebalanceDrainPrefix) }

// RebalanceDrainKey returns rebalance_drain/<8-byte BE shard_id>.
// Big-endian so lexicographic byte order matches numeric shard order.
func RebalanceDrainKey(shardID uint64) []byte {
	out := make([]byte, 0, len(rebalanceDrainPrefix)+8)
	out = append(out, rebalanceDrainPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], shardID)
	return append(out, buf[:]...)
}

// TenantPrefix returns the tenant/ namespace prefix. Used for forward
// range iteration; rows sort in id order because the 4-byte BE encoding
// of id follows the prefix.
func TenantPrefix() []byte { return []byte(tenantPrefix) }

// TenantKey returns tenant/<4-byte BE id>. Big-endian so lexicographic
// byte order matches numeric id order. id==0 is the default-tenant
// sentinel and must never be persisted (the FSM rejects it).
func TenantKey(id uint32) []byte {
	out := make([]byte, 0, len(tenantPrefix)+4)
	out = append(out, tenantPrefix...)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)
	return append(out, buf[:]...)
}

// TenantNameIndexPrefix returns the tenant_name_idx/ namespace prefix.
// Used for iteration; the Config server scans this to resolve a
// create-vs-update decision by name without loading every full
// TenantRecord row.
func TenantNameIndexPrefix() []byte { return []byte(tenantNameIndexPrefix) }

// TenantNameIndexKey returns tenant_name_idx/<name>. Value is the
// 4-byte BE tenant_id. Maintained by the UpsertTenant/DeleteTenant
// apply arms; the FSM trusts the Config server to have validated
// uniqueness via the read-then-CAS round-trip.
func TenantNameIndexKey(name string) []byte {
	out := make([]byte, 0, len(tenantNameIndexPrefix)+len(name))
	out = append(out, tenantNameIndexPrefix...)
	return append(out, name...)
}

// TenantDEKPrefix returns the tenant_dek/ namespace prefix. Used for
// forward range iteration; rows sort in tenant_id order because the
// 4-byte BE encoding of tenant_id follows the prefix.
func TenantDEKPrefix() []byte { return []byte(tenantDEKPrefix) }

// TenantDEKKey returns tenant_dek/<4-byte BE tenant_id>. Big-endian
// so lexicographic byte order matches numeric id order. tenant_id==0
// is the default-tenant sentinel and must never be persisted here —
// the default tenant uses a built-in cluster-wide AEAD, not a
// resolver-fetched DEK.
func TenantDEKKey(tenantID uint32) []byte {
	out := make([]byte, 0, len(tenantDEKPrefix)+4)
	out = append(out, tenantDEKPrefix...)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], tenantID)
	return append(out, buf[:]...)
}

// AuditLogPrefix returns the auditlog/ namespace prefix. Forward
// range iteration yields rows in raft_index ascending order because
// the 8-byte BE encoding of raft_index follows the prefix.
func AuditLogPrefix() []byte { return []byte(auditLogPrefix) }

// AuditLogKey returns auditlog/<8-byte BE raft_index>. raft_index is
// the dragonboat entry index that produced the audited mutation;
// monotonic and unique across the cluster lifetime, so the key never
// collides.
func AuditLogKey(raftIndex uint64) []byte {
	out := make([]byte, 0, len(auditLogPrefix)+8)
	out = append(out, auditLogPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], raftIndex)
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
