package cluster

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// MetaTable is the metadata shard's applied-index + leader-epoch singleton.
// Reuses the PartitionMeta proto so callers can share the
// internal/engine/leadership.go epoch wiring. next_outbox_seq is unused on
// shard 0 (no outbox).
//
// S is the read-only handle; both storage.Store (commit-state view) and
// storage.Batch (in-flight view with read-your-writes coherence) satisfy
// it. Apply-path callers bind to the batch so multi-entry batches see
// each other's writes — same pattern as partition.go.
type MetaTable struct{ S storage.Reader }

func (t MetaTable) Get() (*enginev1.PartitionMeta, error) {
	val, closer, err := t.S.Get(MetaKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &enginev1.PartitionMeta{}, nil
		}
		return nil, err
	}
	defer closer.Close()
	var m enginev1.PartitionMeta
	if err := proto.Unmarshal(val, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (t MetaTable) Put(b storage.Batch, m *enginev1.PartitionMeta) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Set(MetaKey(), buf)
}

// MembershipTable holds NodeMembership rows keyed by node_id.
type MembershipTable struct{ S storage.Reader }

func (t MembershipTable) Get(nodeID uint64) (*enginev1.NodeMembership, error) {
	val, closer, err := t.S.Get(NodeKey(nodeID))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var m enginev1.NodeMembership
	if err := proto.Unmarshal(val, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (t MembershipTable) Put(b storage.Batch, m *enginev1.NodeMembership) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Set(NodeKey(m.GetNodeId()), buf)
}

// List returns every NodeMembership row, sorted by NodeID.
func (t MembershipTable) List() ([]*enginev1.NodeMembership, error) {
	prefix := NodePrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.NodeMembership
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var m enginev1.NodeMembership
		if err := proto.Unmarshal(iter.Value(), &m); err != nil {
			return nil, err
		}
		out = append(out, &m)
	}
	return out, iter.Error()
}

// PartitionTableTable persists the cluster's PartitionTable singleton.
// (The name is verbose to keep grep-ability with other *Table accessors.)
type PartitionTableTable struct{ S storage.Reader }

func (t PartitionTableTable) Get() (*enginev1.PartitionTable, error) {
	val, closer, err := t.S.Get(PartitionTableKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var pt enginev1.PartitionTable
	if err := proto.Unmarshal(val, &pt); err != nil {
		return nil, err
	}
	return &pt, nil
}

func (t PartitionTableTable) Put(b storage.Batch, pt *enginev1.PartitionTable) error {
	buf, err := proto.Marshal(pt)
	if err != nil {
		return err
	}
	return b.Set(PartitionTableKey(), buf)
}

// DeploymentTable persists DeploymentRecord rows keyed by deployment id.
// Lives on shard 0 alongside MembershipTable and PartitionTableTable.
type DeploymentTable struct{ S storage.Reader }

func (t DeploymentTable) Get(id string) (*enginev1.DeploymentRecord, error) {
	val, closer, err := t.S.Get(DeploymentKey(id))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.DeploymentRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t DeploymentTable) Put(b storage.Batch, rec *enginev1.DeploymentRecord) error {
	if rec.GetId() == "" {
		return errors.New("DeploymentTable.Put: empty id")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(DeploymentKey(rec.GetId()), buf)
}

// Delete removes the deployment row for id. Delete-of-absent is a
// no-op (Pebble tolerates missing keys); the apply-arm still bumps the
// table revision so a CAS-roundtripping CLI sees forward progress.
// Callers must also Delete any (service, handler) → id entries in
// DeploymentIndexTable that pointed to this id — see
// applyDeleteDeployment in fsm.go.
func (t DeploymentTable) Delete(b storage.Batch, id string) error {
	if id == "" {
		return errors.New("DeploymentTable.Delete: empty id")
	}
	return b.Delete(DeploymentKey(id))
}

// List returns every DeploymentRecord row in lexicographic id order.
func (t DeploymentTable) List() ([]*enginev1.DeploymentRecord, error) {
	prefix := DeploymentPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.DeploymentRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.DeploymentRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// DeploymentIndexTable maps (service, handler) → deployment_id, the
// current deployment that should answer when an ingress request arrives
// without a pinned deployment_id. Lives on shard 0; written from the
// RegisterDeployment apply arm.
type DeploymentIndexTable struct{ S storage.Reader }

// Get returns the deployment_id for the (service, handler) pair, or
// "" + nil if no entry exists.
func (t DeploymentIndexTable) Get(service, handler string) (string, error) {
	val, closer, err := t.S.Get(DeploymentIndexKey(service, handler))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	defer closer.Close()
	return string(val), nil
}

// Put writes (service, handler) → id. Overwrites prior mappings — newer
// deployment registration wins. Pinned invocations still resolve via
// DeploymentTable.Get(id).
func (t DeploymentIndexTable) Put(b storage.Batch, service, handler, id string) error {
	if service == "" || handler == "" {
		return errors.New("DeploymentIndexTable.Put: empty service or handler")
	}
	if id == "" {
		return errors.New("DeploymentIndexTable.Put: empty deployment id")
	}
	return b.Set(DeploymentIndexKey(service, handler), []byte(id))
}

// Delete removes the (service, handler) → id mapping. No-op when the
// row is absent. Used by applyDeleteDeployment to evict stale routes
// after a deployment is removed.
func (t DeploymentIndexTable) Delete(b storage.Batch, service, handler string) error {
	if service == "" || handler == "" {
		return errors.New("DeploymentIndexTable.Delete: empty service or handler")
	}
	return b.Delete(DeploymentIndexKey(service, handler))
}

// RevisionTable persists the per-table TableRevision singletons used as
// CAS guards by cluster-managed config commands (UpsertEventSource,
// DeleteEventSource, ...). Each row lives at tablerev/<table_name> in a
// separate top-level namespace from the table data itself.
type RevisionTable struct{ S storage.Reader }

// Get returns the current revision for the named table. Returns 0 (not
// an error) when the row is absent — that's how "fresh table" looks.
func (t RevisionTable) Get(tableName string) (uint64, error) {
	val, closer, err := t.S.Get(RevisionKey(tableName))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	var rev enginev1.TableRevision
	if err := proto.Unmarshal(val, &rev); err != nil {
		return 0, err
	}
	return rev.GetRevision(), nil
}

// Bump reads the current revision, writes (current+1, nowMs), and
// returns the new value. nowMs is sourced from Envelope.Header.created_at_ms
// so the apply path stays deterministic against the proposer's wall
// clock (mirrors the partition pattern; see internal/engine/CLAUDE.md
// "transitions deterministic w.r.t. nowMs").
func (t RevisionTable) Bump(b storage.Batch, tableName string, nowMs uint64) (uint64, error) {
	cur, err := t.Get(tableName)
	if err != nil {
		return 0, err
	}
	next := cur + 1
	buf, err := proto.Marshal(&enginev1.TableRevision{
		Revision:    next,
		UpdatedAtMs: nowMs,
	})
	if err != nil {
		return 0, err
	}
	if err := b.Set(RevisionKey(tableName), buf); err != nil {
		return 0, err
	}
	return next, nil
}

// PlatformConfigTable persists the cluster-wide PlatformConfigRecord
// singleton on shard 0. The per-node authz Reconciler SyncRead-loads it on
// each TableNotifier wake to recompile the live Cedar policy set.
type PlatformConfigTable struct{ S storage.Reader }

// Get returns the PlatformConfigRecord singleton, or (nil, nil) when unset.
func (t PlatformConfigTable) Get() (*enginev1.PlatformConfigRecord, error) {
	val, closer, err := t.S.Get(PlatformConfigKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.PlatformConfigRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// Put replaces the singleton (last-writer-wins; CAS is enforced by the apply
// arm's precondition check, not here).
func (t PlatformConfigTable) Put(b storage.Batch, rec *enginev1.PlatformConfigRecord) error {
	if rec == nil {
		return errors.New("PlatformConfigTable.Put: nil record")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(PlatformConfigKey(), buf)
}

// SecretTable persists SecretRecord rows keyed by name. Lives on shard
// 0 alongside the other cluster-managed config tables. Per-node
// internal/secretstore Reconcilers SyncRead-iterate this table on each
// TableNotifier wake to refresh the in-memory name→bytes resolution
// map; the plaintext never leaves the resolving node's process memory.
type SecretTable struct{ S storage.Reader }

func (t SecretTable) Get(name string) (*enginev1.SecretRecord, error) {
	val, closer, err := t.S.Get(SecretKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.SecretRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t SecretTable) Put(b storage.Batch, rec *enginev1.SecretRecord) error {
	if rec.GetName() == "" {
		return errors.New("SecretTable.Put: empty name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(SecretKey(rec.GetName()), buf)
}

// Delete removes the row for name. Delete-of-absent is a no-op.
func (t SecretTable) Delete(b storage.Batch, name string) error {
	if name == "" {
		return errors.New("SecretTable.Delete: empty name")
	}
	return b.Delete(SecretKey(name))
}

// List returns every SecretRecord in lexicographic name order.
func (t SecretTable) List() ([]*enginev1.SecretRecord, error) {
	prefix := SecretPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.SecretRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.SecretRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// ModelTable persists ModelRecord rows keyed by (kind, name, version). Lives on
// shard 0 alongside the other cluster-managed config tables. Per-node
// iflowengine TableResolvers SyncRead-iterate this table on each TableNotifier
// wake to re-parse each BPMN/CMMN model into an in-memory graph cache.
type ModelTable struct{ S storage.Reader }

func (t ModelTable) Get(kind, name, version string) (*enginev1.ModelRecord, error) {
	val, closer, err := t.S.Get(ModelKey(kind, name, version))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.ModelRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t ModelTable) Put(b storage.Batch, rec *enginev1.ModelRecord) error {
	ref := rec.GetModelRef()
	if ref.GetKind() == "" || ref.GetName() == "" {
		return errors.New("ModelTable.Put: empty model_ref kind/name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(ModelKey(ref.GetKind(), ref.GetName(), ref.GetVersion()), buf)
}

// Delete removes the row for (kind, name, version). Delete-of-absent is a no-op.
func (t ModelTable) Delete(b storage.Batch, kind, name, version string) error {
	if kind == "" || name == "" {
		return errors.New("ModelTable.Delete: empty kind/name")
	}
	return b.Delete(ModelKey(kind, name, version))
}

// List returns every ModelRecord in lexicographic key order.
func (t ModelTable) List() ([]*enginev1.ModelRecord, error) {
	prefix := ModelPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.ModelRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.ModelRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// CARootTable persists CARootRecord rows keyed by name. Lives on shard
// 0 alongside the other cluster-managed config tables. The row holds
// the CA cert PEM and a pointer (key_secret_name) into SecretTable
// where the AEAD-wrapped signing key lives; the key never traverses
// Raft. Per-node certmgr.ClusterIssuer instances SyncRead-iterate this
// table on each TableNotifier wake to refresh the active CA snapshot.
type CARootTable struct{ S storage.Reader }

func (t CARootTable) Get(name string) (*enginev1.CARootRecord, error) {
	val, closer, err := t.S.Get(CARootKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.CARootRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t CARootTable) Put(b storage.Batch, rec *enginev1.CARootRecord) error {
	if rec.GetName() == "" {
		return errors.New("CARootTable.Put: empty name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(CARootKey(rec.GetName()), buf)
}

// Delete removes the row for name. Delete-of-absent is a no-op.
func (t CARootTable) Delete(b storage.Batch, name string) error {
	if name == "" {
		return errors.New("CARootTable.Delete: empty name")
	}
	return b.Delete(CARootKey(name))
}

// List returns every CARootRecord in lexicographic name order.
func (t CARootTable) List() ([]*enginev1.CARootRecord, error) {
	prefix := CARootPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.CARootRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.CARootRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// JoinTokenTable persists JoinTokenRecord rows keyed by the
// sha256(token_plaintext). Lives on shard 0; the bootstrap server
// SyncReads it on each MeshSign call to locate the redeemed token,
// then ConsumeJoinToken-proposes to atomically mark single_use rows
// consumed before issuing a leaf.
type JoinTokenTable struct{ S storage.Reader }

func (t JoinTokenTable) Get(tokenHash []byte) (*enginev1.JoinTokenRecord, error) {
	val, closer, err := t.S.Get(JoinTokenKey(tokenHash))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.JoinTokenRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t JoinTokenTable) Put(b storage.Batch, rec *enginev1.JoinTokenRecord) error {
	if len(rec.GetTokenHash()) == 0 {
		return errors.New("JoinTokenTable.Put: empty token_hash")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(JoinTokenKey(rec.GetTokenHash()), buf)
}

// Delete removes the row for tokenHash. Delete-of-absent is a no-op.
func (t JoinTokenTable) Delete(b storage.Batch, tokenHash []byte) error {
	if len(tokenHash) == 0 {
		return errors.New("JoinTokenTable.Delete: empty token_hash")
	}
	return b.Delete(JoinTokenKey(tokenHash))
}

// List returns every JoinTokenRecord in lexicographic token-hash order.
func (t JoinTokenTable) List() ([]*enginev1.JoinTokenRecord, error) {
	prefix := JoinTokenPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.JoinTokenRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.JoinTokenRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// LPOwnersTable persists LPOwnerRecord rows keyed by lp ∈ [0, LPCount).
// Lives on shard 0 alongside the other cluster-managed config tables.
// Per-node routing Reconcilers SyncRead the table on each TableNotifier
// wake to refresh the Partitioner's atomic snapshot; lookup on the
// routing hot path is a single atomic.Pointer load with no per-call work.
type LPOwnersTable struct{ S storage.Reader }

func (t LPOwnersTable) Get(lp uint32) (*enginev1.LPOwnerRecord, error) {
	val, closer, err := t.S.Get(LPOwnerKey(lp))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.LPOwnerRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t LPOwnersTable) Put(b storage.Batch, rec *enginev1.LPOwnerRecord) error {
	if rec.GetShardId() == 0 {
		return errors.New("LPOwnersTable.Put: zero shard_id")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(LPOwnerKey(rec.GetLp()), buf)
}

// Delete removes the row for lp. Delete-of-absent is a no-op.
func (t LPOwnersTable) Delete(b storage.Batch, lp uint32) error {
	return b.Delete(LPOwnerKey(lp))
}

// List returns every LPOwnerRecord in ascending lp order.
func (t LPOwnersTable) List() ([]*enginev1.LPOwnerRecord, error) {
	prefix := LPOwnerPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.LPOwnerRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.LPOwnerRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// Snapshot returns the full lp → shard_id map. Used by per-node routing
// reconcilers to swap the Partitioner's atomic snapshot in one call.
func (t LPOwnersTable) Snapshot() (map[uint32]uint64, error) {
	list, err := t.List()
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]uint64, len(list))
	for _, rec := range list {
		out[rec.GetLp()] = rec.GetShardId()
	}
	return out, nil
}

// LPTransferTable persists LPTransferRecord rows keyed by transfer_id.
// Lives on shard 0 alongside LPOwnersTable. The lpMover goroutine on
// the metadata leader reads the full table via List on each tick (the
// row count is bounded by the number of in-flight transfers, typically
// 0..few) and advances each non-terminal row by one phase per pass.
type LPTransferTable struct{ S storage.Reader }

func (t LPTransferTable) Get(transferID string) (*enginev1.LPTransferRecord, error) {
	val, closer, err := t.S.Get(LPTransferKey(transferID))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.LPTransferRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t LPTransferTable) Put(b storage.Batch, rec *enginev1.LPTransferRecord) error {
	if rec.GetTransferId() == "" {
		return errors.New("LPTransferTable.Put: empty transfer_id")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(LPTransferKey(rec.GetTransferId()), buf)
}

// Delete removes the row for transfer_id. Delete-of-absent is a no-op.
func (t LPTransferTable) Delete(b storage.Batch, transferID string) error {
	return b.Delete(LPTransferKey(transferID))
}

// List returns every LPTransferRecord in lexicographic transfer_id order.
func (t LPTransferTable) List() ([]*enginev1.LPTransferRecord, error) {
	prefix := LPTransferPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.LPTransferRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.LPTransferRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// RebalanceDrainTable persists RebalanceDrainRecord rows keyed by
// shard_id. Lives on shard 0 alongside the other cluster-managed config
// tables. The autonomous rebalancer's advisor reads the full table on
// each tick (count is bounded by operator drain actions, typically
// 0..few) and subtracts drained shards from the planner's input set.
type RebalanceDrainTable struct{ S storage.Reader }

func (t RebalanceDrainTable) Get(shardID uint64) (*enginev1.RebalanceDrainRecord, error) {
	val, closer, err := t.S.Get(RebalanceDrainKey(shardID))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.RebalanceDrainRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t RebalanceDrainTable) Put(b storage.Batch, rec *enginev1.RebalanceDrainRecord) error {
	if rec.GetShardId() == 0 {
		return errors.New("RebalanceDrainTable.Put: zero shard_id")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(RebalanceDrainKey(rec.GetShardId()), buf)
}

// Delete removes the row for shard_id. Delete-of-absent is a no-op.
func (t RebalanceDrainTable) Delete(b storage.Batch, shardID uint64) error {
	return b.Delete(RebalanceDrainKey(shardID))
}

// List returns every RebalanceDrainRecord in ascending shard_id order.
func (t RebalanceDrainTable) List() ([]*enginev1.RebalanceDrainRecord, error) {
	prefix := RebalanceDrainPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.RebalanceDrainRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.RebalanceDrainRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// prefixUpperBound is a local clone of keys.PrefixUpperBound to avoid an
// import cycle (internal/storage/keys is for the partition codec).
func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for len(out) > 0 && out[len(out)-1] == 0xFF {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return nil
	}
	out[len(out)-1]++
	return out
}
