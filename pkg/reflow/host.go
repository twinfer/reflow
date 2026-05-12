package reflow

import (
	"context"
	"errors"

	"github.com/twinfer/reflow/internal/engine"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Host is a running reflow node. Construct one via Run; close it via Close.
//
// Host is a thin wrapper over the internal engine.Host. Callers should
// treat the engine package as private; Host is the stable surface.
type Host struct {
	engine        *engine.Host
	metricsCloser func() error
}

// Close stops every partition and the underlying NodeHost. Idempotent.
// Stops the metrics HTTP server too, if Run started one.
func (h *Host) Close() error {
	var firstErr error
	if h.engine != nil {
		if err := h.engine.Close(); err != nil {
			firstErr = err
		}
		h.engine = nil
	}
	if h.metricsCloser != nil {
		if err := h.metricsCloser(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.metricsCloser = nil
	}
	return firstErr
}

// AwaitLeader blocks until shardID has an elected leader on this node, or
// ctx expires. Useful for tests and bootstrap scripts.
func (h *Host) AwaitLeader(ctx context.Context, shardID uint64) error {
	if h.engine == nil {
		return errors.New("reflow: host closed")
	}
	return h.engine.AwaitLeader(ctx, shardID)
}

// LookupInvocationStatus performs a linearizable read of an invocation's
// status from the partition that owns it.
func (h *Host) LookupInvocationStatus(ctx context.Context, shardID uint64, id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	if h.engine == nil {
		return nil, errors.New("reflow: host closed")
	}
	return h.engine.LookupInvocationStatus(ctx, shardID, id)
}

// Engine returns the underlying internal engine.Host. Reserved for tests
// that need access to internal hooks (Partition, NodeHost). Not part of
// the stable API; future Phase 2 steps will move test helpers under this
// package and shrink the engine surface.
func (h *Host) Engine() *engine.Host { return h.engine }
