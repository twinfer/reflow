package loadgen

import (
	"context"
	"fmt"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Violation is a single failed post-run invariant.
type Violation struct {
	Kind    string
	Detail  string
	Subject *enginev1.InvocationId
}

// AwaitCompletion polls LookupInvocationStatus for every issued
// invocation until either every one has reached Completed or the
// deadline elapses. Returns the violations: invocations that never
// completed, or that completed with a non-empty failure_message.
//
// This is the post-run safety net: workload Run's inline poller may
// have missed completions if Duration expired, but the engine
// continues processing in the background — AwaitCompletion gives
// the journal time to settle before the harness reports failures.
func AwaitCompletion(ctx context.Context, c WorkloadCluster, issued []IssuedInvocation, timeout time.Duration) []Violation {
	if len(issued) == 0 || c == nil {
		return nil
	}
	live := c.AnyLiveNode()
	if live == nil {
		// No live node to issue lookups against; mark every
		// pending invocation as unknown rather than silently
		// returning success.
		out := make([]Violation, 0, len(issued))
		for _, inv := range issued {
			out = append(out, Violation{
				Kind:    "never_completed",
				Detail:  fmt.Sprintf("shard=%d state=no_live_node", inv.ShardID),
				Subject: inv.ID,
			})
		}
		return out
	}
	deadline := time.Now().Add(timeout)
	pending := make(map[string]IssuedInvocation, len(issued))
	for _, inv := range issued {
		pending[encodeKey(inv)] = inv
	}
	violations := make([]Violation, 0)

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

poll:
	for time.Now().Before(deadline) && len(pending) > 0 {
		for key, inv := range pending {
			lookupCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			st, err := live.DescribeInvocation(lookupCtx, inv.ID)
			cancel()
			if err != nil || st == nil {
				continue
			}
			if cs, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				if cs.Completed.GetFailureMessage() != "" {
					violations = append(violations, Violation{
						Kind:    "completed_with_failure",
						Detail:  cs.Completed.GetFailureMessage(),
						Subject: inv.ID,
					})
				}
				delete(pending, key)
			}
		}
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			break poll
		case <-tick.C:
		}
	}

	for _, inv := range pending {
		state := "unknown"
		// Use Background ctx for the final-state probe so that even if
		// the caller's ctx is already done, we can still observe whether
		// the row is actually Completed. Otherwise an exhausted parent
		// ctx surfaces as "lookup_err=invalid deadline" violations that
		// mask the real state.
		lookupCtx, lc := context.WithTimeout(context.Background(), 500*time.Millisecond)
		st, err := live.DescribeInvocation(lookupCtx, inv.ID)
		lc()
		switch {
		case err != nil:
			state = fmt.Sprintf("lookup_err=%v", err)
		case st == nil:
			state = "nil_status"
		case st.GetStatus() == nil:
			state = "no_kind"
		default:
			switch st.GetStatus().(type) {
			case *enginev1.InvocationStatus_Free:
				state = "Free"
			case *enginev1.InvocationStatus_Scheduled:
				state = "Scheduled"
			case *enginev1.InvocationStatus_Invoked:
				state = "Invoked"
			case *enginev1.InvocationStatus_Suspended:
				state = "Suspended"
			case *enginev1.InvocationStatus_Completed:
				state = "Completed(missed_by_poller)"
			default:
				state = fmt.Sprintf("%T", st.GetStatus())
			}
		}
		violations = append(violations, Violation{
			Kind:    "never_completed",
			Detail:  fmt.Sprintf("shard=%d id=%x:%x state=%s", inv.ShardID, inv.ID.GetPartitionKey(), inv.ID.GetUuid(), state),
			Subject: inv.ID,
		})
	}
	return violations
}
