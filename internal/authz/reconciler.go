package authz

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// reconcileInterval is the backstop tick. The PlatformConfigTable notifier
// wake (FSM post-commit Bump) is the primary signal; the ticker just catches
// a missed wake (snapshot-recovery edge case).
const reconcileInterval = 5 * time.Second

// PolicyReader is the seam the reconciler uses to fetch the cluster authz
// policy text + its CAS revision from shard 0. run.go wires a thin adapter
// over engine.Host.ClusterAuthzPolicy; tests hand in a fake.
type PolicyReader interface {
	ClusterAuthzPolicy(ctx context.Context) (policyText string, tableRevision uint64, err error)
}

// RunReconciler keeps the engine's live policy set converged with shard 0's
// PlatformConfigRecord. It wakes on the PlatformConfigTable notifier or a 5s
// ticker, reads the desired policy text, validates + compiles it, and
// atomically swaps the engine's policy set. Two safety properties:
//   - an empty row (fresh cluster, no upload yet) leaves the in-binary
//     foundational policy installed at NewEngine — it is never clobbered with
//     an empty set;
//   - a policy that fails to compile keeps the previous set, so a bad
//     reconcile can neither open the cluster up nor lock it out.
//
// Runs on its own goroutine; never the FSM apply path. Returns when ctx is
// cancelled.
func (e *Engine) RunReconciler(ctx context.Context, sub <-chan struct{}, reader PolicyReader, log *slog.Logger) error {
	if e == nil {
		return nil
	}
	if reader == nil {
		return errors.New("authz: reader is required for reconcile loop")
	}
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	e.reconcileOnce(ctx, reader, log)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			e.reconcileOnce(ctx, reader, log)
		case <-ticker.C:
			e.reconcileOnce(ctx, reader, log)
		}
	}
}

// reconcileOnce does one read + compile + swap pass. Errors are logged, never
// propagated.
func (e *Engine) reconcileOnce(ctx context.Context, reader PolicyReader, log *slog.Logger) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	text, _, err := reader.ClusterAuthzPolicy(readCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Warn("authz: read cluster policy", "err", err)
		}
		return
	}
	if text == "" {
		// Fresh cluster: no uploaded policy. Keep the in-binary foundational
		// set installed at NewEngine — don't clobber it with an empty policy.
		return
	}
	ps, err := e.CompileAndValidate([]byte(text))
	if err != nil {
		log.Warn("authz: cluster policy failed to compile; keeping previous set", "err", err)
		return
	}
	e.SetPolicies(ps)
}
