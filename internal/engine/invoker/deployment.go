package invoker

import (
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// DeploymentResolver returns the DeploymentRecord for a stamped
// deployment_id. Implementations may consult an in-memory cache (single
// node, synthetic inproc) or a remote shard-0 read (multi-node). The
// invoker calls Resolve from the apply goroutine before installing a
// session; resolvers should bound any I/O and return ("not found")
// rather than blocking indefinitely.
//
// A nil return with nil error means "deployment not found"; the invoker
// falls back to in-process registry lookup (legacy / synthetic inproc
// path). A non-nil error short-circuits session install with a warn.
type DeploymentResolver interface {
	Resolve(deploymentID string) (*enginev1.DeploymentRecord, error)
}

// DeploymentResolverFunc adapts a plain function into DeploymentResolver.
type DeploymentResolverFunc func(deploymentID string) (*enginev1.DeploymentRecord, error)

// Resolve calls f.
func (f DeploymentResolverFunc) Resolve(deploymentID string) (*enginev1.DeploymentRecord, error) {
	return f(deploymentID)
}
