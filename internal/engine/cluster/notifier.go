package cluster

// TableNotifier is the apply-path signal that one of shard 0's
// cluster-managed config tables has been mutated by a committed Raft
// batch. The FSM apply path calls Bump after batch.Commit; a per-table
// consumer goroutine (e.g. the event-source Reconciler) holds Subscribe()
// and reacts by SyncRead'ing the table and converging local state.
//
// Goroutine affinity:
//
//   - Bump: fires on the FSM apply goroutine (dragonboat Update),
//     STRICTLY POST-COMMIT and STRICTLY NON-BLOCKING. The send into the
//     buffered-1 channel is dropped when the buffer is already full —
//     that is fine because the consumer's next wake-up does a fresh
//     SyncRead and observes whatever the latest committed state is.
//     The buffer exists only to coalesce bursts.
//
//   - Subscribe: returns the receive end. v1 supports exactly one
//     subscriber per notifier; this matches the per-table model
//     (one Reconciler owns each subsystem). Re-subscribing returns the
//     same channel. When multiple consumers need to wake on the same
//     table (e.g. one table drives two subsystems), the second consumer
//     subscribes to a fan-out relay built in pkg/reflw — TableNotifier
//     itself stays
//     single-subscriber so the existing propose-then-Subscribe test
//     pattern continues to work (subscribe-after-bump still drains
//     the pending signal from the buffered-1 channel).
//
// Why not a callback closure: callbacks let consumers smuggle
// business logic onto the apply goroutine, which violates the
// internal/engine/CLAUDE.md "no ProposeSelf from apply" rule. The
// notify-then-pull shape makes the boundary explicit.
type TableNotifier struct {
	ch chan struct{}
}

// NewTableNotifier returns a notifier with a buffered-1 channel.
func NewTableNotifier() *TableNotifier {
	return &TableNotifier{ch: make(chan struct{}, 1)}
}

// Bump signals a subscriber that the underlying table changed. Non-blocking:
// if the buffer is full (a prior Bump is still pending consumption), the
// signal is dropped. The consumer's next SyncRead observes the merged
// effect of all dropped + delivered bumps.
func (n *TableNotifier) Bump() {
	if n == nil {
		return
	}
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// Subscribe returns the receive end. Single-consumer; calling multiple
// times returns the same channel.
func (n *TableNotifier) Subscribe() <-chan struct{} {
	if n == nil {
		return nil
	}
	return n.ch
}
