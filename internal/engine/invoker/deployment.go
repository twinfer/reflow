package invoker

import (
	"context"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// DeploymentResolver returns the DeploymentRecord for a stamped
// deployment_id. Implementations read it from shard 0 (locally on the
// metadata leader, via SyncRead elsewhere). The invoker calls Resolve
// from the apply goroutine before installing a session; ctx scopes the
// lookup (typically the invoker's context), so a resolver doing a
// SyncRead cancels on engine shutdown rather than running on its own
// wall clock.
//
// A nil return with nil error means "deployment not found"; the invoker
// drops the session install with a warn. A non-nil error short-circuits
// session install with a warn.
type DeploymentResolver interface {
	Resolve(ctx context.Context, deploymentID string) (*enginev1.DeploymentRecord, error)
}

// DeploymentResolverFunc adapts a plain function into DeploymentResolver.
type DeploymentResolverFunc func(ctx context.Context, deploymentID string) (*enginev1.DeploymentRecord, error)

// Resolve calls f.
func (f DeploymentResolverFunc) Resolve(ctx context.Context, deploymentID string) (*enginev1.DeploymentRecord, error) {
	return f(ctx, deploymentID)
}
