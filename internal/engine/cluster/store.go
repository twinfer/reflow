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
type MetaTable struct{ S storage.Store }

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
type MembershipTable struct{ S storage.Store }

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
type PartitionTableTable struct{ S storage.Store }

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
