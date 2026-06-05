package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessSubIndexTable is the per-instance reverse index of message
// subscriptions, one row per parked catch at
// proc_sub_idx/<lp:4><24-byte process root id><node_id>, value = the originating
// ProcessSubscribe. It lives on the instance's own shard (the forward
// MessageSubscription lives on the message routing key's shard, generally a
// different LP), so finishProcessInstance can find every subscription an
// instance still holds with one bounded prefix scan and tear each down on its
// message partition. A torn-down catch (SignalUnsubscribe, which carries only a
// node id) resolves to its forward subscription via Get. The timer_idx analog
// for the subscription plane.
type ProcessSubIndexTable struct{ S storage.Reader }

// Put records that root's catch at ps.sub.node_id is subscribed; the value is the
// ProcessSubscribe so a later teardown can reconstruct the forward row + routing.
func (t ProcessSubIndexTable) Put(b storage.Batch, root *enginev1.InvocationId, ps *enginev1.ProcessSubscribe) error {
	k, err := keys.ProcessSubIndexKey(root, ps.GetSub().GetNodeId())
	if err != nil {
		return err
	}
	return putProto(b, k, ps)
}

// Get returns the ProcessSubscribe recorded for (root, nodeID). ok is false when
// the instance never subscribed that node (or already tore it down).
func (t ProcessSubIndexTable) Get(root *enginev1.InvocationId, nodeID string) (*enginev1.ProcessSubscribe, bool, error) {
	k, err := keys.ProcessSubIndexKey(root, nodeID)
	if err != nil {
		return nil, false, err
	}
	var ps enginev1.ProcessSubscribe
	if err := getProto(t.S, k, &ps); err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &ps, true, nil
}

// Delete removes the index row for (root, nodeID).
func (t ProcessSubIndexTable) Delete(b storage.Batch, root *enginev1.InvocationId, nodeID string) error {
	k, err := keys.ProcessSubIndexKey(root, nodeID)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// ScanByInstance visits every subscription parked by root, decoding each
// ProcessSubscribe in key order. Bounded by the instance's parked-catch count.
// Used by finishProcessInstance's terminal sweep. Read-only; the caller collects
// then mutates so it never writes the batch while iterating.
func (t ProcessSubIndexTable) ScanByInstance(root *enginev1.InvocationId, fn func(*enginev1.ProcessSubscribe) error) error {
	prefix, err := keys.ProcessSubIndexInstancePrefix(root)
	if err != nil {
		return err
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		ps := &enginev1.ProcessSubscribe{}
		if err := proto.Unmarshal(iter.Value(), ps); err != nil {
			return err
		}
		if err := fn(ps); err != nil {
			return err
		}
	}
	return iter.Error()
}
