package cluster

import (
	"bytes"
	"errors"

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
