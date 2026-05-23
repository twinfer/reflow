package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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

// EventSourceTable persists EventSourceRecord rows keyed by name. Lives
// on shard 0 alongside DeploymentTable. The Reconciler on every node
// SyncRead-iterates this table on each TableNotifier wake to converge
// the local dispatcher set.
type EventSourceTable struct{ S storage.Reader }

func (t EventSourceTable) Get(name string) (*enginev1.EventSourceRecord, error) {
	val, closer, err := t.S.Get(EventSourceKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.EventSourceRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t EventSourceTable) Put(b storage.Batch, rec *enginev1.EventSourceRecord) error {
	if rec.GetName() == "" {
		return errors.New("EventSourceTable.Put: empty name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(EventSourceKey(rec.GetName()), buf)
}

// Delete removes the row for name. Delete-of-absent is a no-op (Pebble's
// Delete tolerates missing keys); callers still bump the table revision
// so the operator's CAS-roundtrip CLI observes progress.
func (t EventSourceTable) Delete(b storage.Batch, name string) error {
	if name == "" {
		return errors.New("EventSourceTable.Delete: empty name")
	}
	return b.Delete(EventSourceKey(name))
}

// List returns every EventSourceRecord in lexicographic name order.
func (t EventSourceTable) List() ([]*enginev1.EventSourceRecord, error) {
	prefix := EventSourcePrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.EventSourceRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.EventSourceRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// WebhookSourceTable persists WebhookSourceRecord rows keyed by name.
// Lives on shard 0 alongside EventSourceTable. The Reconciler on every
// node SyncRead-iterates this table on each TableNotifier wake to
// converge the local route-snapshot.
type WebhookSourceTable struct{ S storage.Reader }

func (t WebhookSourceTable) Get(name string) (*enginev1.WebhookSourceRecord, error) {
	val, closer, err := t.S.Get(WebhookSourceKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.WebhookSourceRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t WebhookSourceTable) Put(b storage.Batch, rec *enginev1.WebhookSourceRecord) error {
	if rec.GetName() == "" {
		return errors.New("WebhookSourceTable.Put: empty name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(WebhookSourceKey(rec.GetName()), buf)
}

// Delete removes the row for name. Delete-of-absent is a no-op.
func (t WebhookSourceTable) Delete(b storage.Batch, name string) error {
	if name == "" {
		return errors.New("WebhookSourceTable.Delete: empty name")
	}
	return b.Delete(WebhookSourceKey(name))
}

// List returns every WebhookSourceRecord in lexicographic name order.
func (t WebhookSourceTable) List() ([]*enginev1.WebhookSourceRecord, error) {
	prefix := WebhookSourcePrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.WebhookSourceRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.WebhookSourceRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
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

// TenantTable persists TenantRecord rows keyed by 4-byte BE id. Lives
// on shard 0 alongside DeploymentTable. Rows sort in ascending id
// order, so List returns the lowest id first — useful for the Config
// server's "allocate max(id)+1" path during create.
//
// The 4-byte numeric id is the load-bearing key; `name` is carried
// for display only and is indexed via TenantNameIndexTable for
// create-vs-update resolution. Tenant id 0 is the default-tenant
// sentinel and must never be persisted (the FSM rejects it).
type TenantTable struct{ S storage.Reader }

func (t TenantTable) Get(id uint32) (*enginev1.TenantRecord, error) {
	val, closer, err := t.S.Get(TenantKey(id))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var rec enginev1.TenantRecord
	if err := proto.Unmarshal(val, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (t TenantTable) Put(b storage.Batch, rec *enginev1.TenantRecord) error {
	if rec.GetId() == 0 {
		return errors.New("TenantTable.Put: zero id (reserved for default-tenant sentinel)")
	}
	if rec.GetName() == "" {
		return errors.New("TenantTable.Put: empty name")
	}
	buf, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Set(TenantKey(rec.GetId()), buf)
}

// Delete removes the row for id. Delete-of-absent is a no-op; callers
// still bump the table revision so the operator's CAS-roundtrip CLI
// observes progress.
func (t TenantTable) Delete(b storage.Batch, id uint32) error {
	if id == 0 {
		return errors.New("TenantTable.Delete: zero id")
	}
	return b.Delete(TenantKey(id))
}

// List returns every TenantRecord in ascending id order.
func (t TenantTable) List() ([]*enginev1.TenantRecord, error) {
	prefix := TenantPrefix()
	upper := prefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.TenantRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var rec enginev1.TenantRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, iter.Error()
}

// TenantNameIndexTable maintains the name → id secondary index for
// TenantTable. Maintained by the apply arms (UpsertTenant inserts /
// re-points the row, DeleteTenant removes it). Read by the Config
// server to resolve create-vs-update by name without scanning every
// TenantRecord.
type TenantNameIndexTable struct{ S storage.Reader }

// Get returns the tenant id for name, or 0 if absent (0 is also the
// default-tenant sentinel — callers distinguish "not indexed" from
// "default tenant" by context: this table never holds the default
// tenant, so 0 always means "not found").
func (t TenantNameIndexTable) Get(name string) (uint32, error) {
	val, closer, err := t.S.Get(TenantNameIndexKey(name))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(val) != 4 {
		return 0, fmt.Errorf("TenantNameIndexTable.Get: malformed value len=%d", len(val))
	}
	return binary.BigEndian.Uint32(val), nil
}

func (t TenantNameIndexTable) Put(b storage.Batch, name string, id uint32) error {
	if name == "" {
		return errors.New("TenantNameIndexTable.Put: empty name")
	}
	if id == 0 {
		return errors.New("TenantNameIndexTable.Put: zero id")
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)
	return b.Set(TenantNameIndexKey(name), buf[:])
}

func (t TenantNameIndexTable) Delete(b storage.Batch, name string) error {
	if name == "" {
		return errors.New("TenantNameIndexTable.Delete: empty name")
	}
	return b.Delete(TenantNameIndexKey(name))
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
