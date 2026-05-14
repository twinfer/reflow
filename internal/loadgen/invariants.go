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
func AwaitCompletion(ctx context.Context, c *Cluster, issued []IssuedInvocation, timeout time.Duration) []Violation {
	if len(issued) == 0 || c == nil || len(c.Nodes) == 0 {
		return nil
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
			st, err := c.Nodes[0].Host.LookupInvocationStatus(lookupCtx, inv.ShardID, inv.ID)
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
		violations = append(violations, Violation{
			Kind:    "never_completed",
			Detail:  fmt.Sprintf("shard=%d", inv.ShardID),
			Subject: inv.ID,
		})
	}
	return violations
}
