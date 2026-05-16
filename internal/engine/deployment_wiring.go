package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/handlerclient/http2client"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// resolveDeployment looks up the persisted DeploymentRecord on shard 0
// by id. Returns (nil, nil) for a deployment the cluster has never
// seen — the invoker treats that as "drop the invocation".
//
// ctx scopes the SyncRead; the invoker passes its own context so
// shutdown cancels in-flight lookups rather than spinning until a
// wall-clock timeout. A bounded fallback deadline (2s) covers callers
// that pass a never-cancelled context.
//
// Implements invoker.DeploymentResolver. Bound at partition construction
// via invoker.Config.Deployments.
func (h *Host) resolveDeployment(ctx context.Context, deploymentID string) (*enginev1.DeploymentRecord, error) {
	if deploymentID == "" {
		return nil, nil
	}
	if h.nh == nil {
		return nil, nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeployment{ID: deploymentID})
	if err != nil {
		return nil, fmt.Errorf("host: SyncRead deployment %s: %w", deploymentID, err)
	}
	rec, ok := res.(*enginev1.DeploymentRecord)
	if !ok {
		return nil, fmt.Errorf("host: resolveDeployment: unexpected lookup type %T", res)
	}
	return rec, nil
}

// LookupDeploymentIDByHandler resolves (service, handler) → deployment_id
// against shard 0's deployment index. Returns "" + nil when no
// deployment claims the handler — callers decide whether that is an
// error condition. Also returns "" + nil when shard 0 is not hosted on
// this node.
//
// The deployment_id this returns identifies the *current* deployment
// for (service, handler); pinned invocations resolve their own
// deployment via the resolveDeployment id-keyed path. New ingress
// invocations should be stamped with the id returned here so the pin
// holds across future deployment swaps.
func (h *Host) LookupDeploymentIDByHandler(ctx context.Context, service, handler string) (string, error) {
	if service == "" || handler == "" {
		return "", errors.New("host: LookupDeploymentIDByHandler: empty service or handler")
	}
	if h.nh == nil {
		return "", nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	// Retry on transient errors: shard 0 may still be electing a leader
	// or replaying its log on restart. The caller (invoker dispatch) gets
	// blocked while we wait — that is preferable to silently dropping the
	// invocation because shard 0 wasn't ready for 100ms.
	var lastErr error
	for {
		res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeploymentByHandler{Service: service, Handler: handler})
		if err == nil {
			id, ok := res.(string)
			if !ok {
				return "", fmt.Errorf("host: LookupDeploymentIDByHandler: unexpected lookup type %T", res)
			}
			return id, nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", nil //nolint:nilerr // treat shard-0 unavailability as "no deployment"
		}
		select {
		case <-ctx.Done():
			_ = lastErr
			return "", nil //nolint:nilerr
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// openWireStream is the WireDispatcher implementation: ask the
// handlerclient.Registry for a cached Client against the deployment URL
// and open a fresh Stream addressed to (target.Service, target.Handler).
// The route is used by the HTTP/2 transport to build the URL path; the
// StartMessage frame echoes the same tuple as a sanity check.
func (h *Host) openWireStream(ctx context.Context, rec *enginev1.DeploymentRecord, target *enginev1.InvocationTarget) (handlerclient.Stream, error) {
	if rec == nil {
		return nil, errors.New("host: openWireStream: nil deployment record")
	}
	if target == nil {
		return nil, errors.New("host: openWireStream: nil invocation target")
	}
	if h.handlerRegistry == nil {
		return nil, errors.New("host: handlerclient registry not initialized")
	}
	client, err := h.handlerRegistry.Get(rec.GetId(), rec.GetUrl())
	if err != nil {
		return nil, fmt.Errorf("host: get handlerclient: %w", err)
	}
	return client.Invoke(ctx, handlerclient.Route{
		Service: target.GetServiceName(),
		Handler: target.GetHandlerName(),
	})
}

// newHandlerRegistry builds the engine-side handlerclient registry with
// the default transport dialers (raw HTTP/2 plain + TLS). Operators may
// install additional dialers post-construction via
// Host.HandlerClients().Register.
func newHandlerRegistry() *handlerclient.Registry {
	r := handlerclient.NewRegistry()
	http2client.Register(r)
	return r
}

// HandlerClients exposes the handlerclient registry so operators can
// install custom dialers or codecs in their own bootstrap (before
// remote deployments are registered). The reflowd binary uses the
// defaults from newHandlerRegistry.
func (h *Host) HandlerClients() *handlerclient.Registry {
	return h.handlerRegistry
}

// hostWireDispatcher adapts Host.openWireStream into the
// invoker.WireDispatcher interface so the invoker can hold a small,
// stable seam instead of a *Host reference.
type hostWireDispatcher struct{ h *Host }

func (d hostWireDispatcher) Open(ctx context.Context, rec *enginev1.DeploymentRecord, target *enginev1.InvocationTarget) (handlerclient.Stream, error) {
	return d.h.openWireStream(ctx, rec, target)
}
