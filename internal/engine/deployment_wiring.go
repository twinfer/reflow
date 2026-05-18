package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/handlerclient/connectclient"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// resolveDeployment looks up the persisted DeploymentRecord on shard 0
// by id. Returns (nil, nil) for a deployment the cluster has never
// seen — the invoker treats that as "drop the invocation".
//
// ctx scopes the SyncRead; the invoker passes its own context so
// shutdown cancels in-flight lookups rather than spinning until a
// wall-clock timeout. A bounded fallback deadline (5s) covers callers
// that pass a never-cancelled context.
//
// Retries on transient errors: post-restart, the partition leader may
// dispatch an invoker session before shard 0 finishes electing its
// leader. Mirror the LookupDeploymentIDByHandler retry shape so the
// session waits rather than failing the invocation terminally.
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
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	var lastErr error
	for {
		res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeployment{ID: deploymentID})
		if err == nil {
			rec, ok := res.(*enginev1.DeploymentRecord)
			if !ok {
				return nil, fmt.Errorf("host: resolveDeployment: unexpected lookup type %T", res)
			}
			return rec, nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("host: SyncRead deployment %s: %w", deploymentID, err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("host: SyncRead deployment %s: %w", deploymentID, lastErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// LookupDeploymentIDByHandler resolves (service, handler) → deployment_id
// against shard 0's deployment index. Returns "" + nil when no
// deployment claims the handler — callers decide whether that is an
// error condition. Returns "" + nil when shard 0 is not hosted on this
// node. Returns "" + a non-nil error when shard 0 is unreachable (e.g.
// election in progress, ctx expired) so the caller can distinguish
// "not registered" from "ask again later" and surface the right status
// code to the user.
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
	// or replaying its log on restart. The caller (ingress / invoker
	// dispatch) gets blocked while we wait — that is preferable to
	// silently dropping the invocation because shard 0 wasn't ready for
	// 100ms.
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
		// ctx-derived terminal conditions: surface the cause so callers
		// can map it to a transient gRPC status (Unavailable /
		// DeadlineExceeded) rather than the misleading "no deployment
		// registered" FailedPrecondition that an empty string produces
		// in ingress.SubmitInvocation.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("host: LookupDeploymentIDByHandler: shard 0 unavailable: %w", err)
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("host: LookupDeploymentIDByHandler: shard 0 unavailable: %w", lastErr)
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

// newHandlerRegistry builds the engine-side handlerclient registry
// with the default Connect transport dialers (plaintext h2c + TLS).
// Operators may install additional dialers post-construction via
// Host.HandlerClients().Register. signer, when non-nil, stamps every
// dispatched request with an Authorization: Bearer JWT.
func newHandlerRegistry(signer handlerclient.Signer) *handlerclient.Registry {
	r := handlerclient.NewRegistry()
	connectclient.Register(r, signer)
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
