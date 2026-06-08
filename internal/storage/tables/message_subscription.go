package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// MessageSubscriptionTable stores parked BPMN message/signal catches, one row per
// (message routing key, subscriber) at msgsub/<lp:4><corr_digest:32><sub_digest:32>.
// The <lp:4> is derived from the message routing key
// PartitionKey(message_name, correlation_key) — NOT the instance's own
// partition key — so the row is co-located with where DeliverProcessMessage
// routes and a correlation match is a single prefix range. Rides the LP-transfer
// scan via keys.MessageSubscriptionLPPrefix.
type MessageSubscriptionTable struct{ S storage.Reader }

// Put writes (or overwrites) the subscription row for sub on logical partition
// lp (= LPFromPartitionKey of the message routing key). A re-subscribe by the
// same node overwrites its own row (the sub digest is stable per subscriber).
func (t MessageSubscriptionTable) Put(b storage.Batch, lp uint32, sub *enginev1.MessageSubscription) error {
	return putProto(b, t.key(lp, sub), sub)
}

func (t MessageSubscriptionTable) key(lp uint32, sub *enginev1.MessageSubscription) []byte {
	return keys.MessageSubscriptionKey(lp, sub.GetMessageName(), sub.GetCorrelationKey(),
		sub.GetInstancePk(), sub.GetService(), sub.GetInstanceKey(), sub.GetNodeId())
}

// ScanByCorrelation visits every subscriber parked on (messageName,
// correlationKey) within lp, in key order. fn receives a private copy of the raw
// storage key (pass it to DeleteKey for one-shot consumption) and the decoded
// subscription. A non-nil error from fn aborts the scan and is returned.
func (t MessageSubscriptionTable) ScanByCorrelation(lp uint32, messageName, correlationKey string, fn func(key []byte, sub *enginev1.MessageSubscription) error) error {
	prefix := keys.MessageSubscriptionScanPrefix(lp, messageName, correlationKey)
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		sub := &enginev1.MessageSubscription{}
		if err := proto.Unmarshal(iter.Value(), sub); err != nil {
			continue
		}
		// Copy the key: the iterator reuses its buffer across Next().
		k := append([]byte(nil), iter.Key()...)
		if err := fn(k, sub); err != nil {
			return err
		}
	}
	return iter.Error()
}

// DeleteKey removes a subscription row by its raw storage key (as yielded by
// ScanByCorrelation) — the one-shot delete that consumes a delivered message.
func (t MessageSubscriptionTable) DeleteKey(b storage.Batch, key []byte) error {
	return b.Delete(key)
}

// Delete removes the subscription row for sub on lp by reconstructing its key
// from the same fields Put used (the sub digest is stable per subscriber). Used
// by onProcessUnsubscribe to tear down a parked catch.
func (t MessageSubscriptionTable) Delete(b storage.Batch, lp uint32, sub *enginev1.MessageSubscription) error {
	return b.Delete(t.key(lp, sub))
}
