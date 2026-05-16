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

// resolveDeployment lookups the persisted DeploymentRecord for the
// given id. The synthetic inproc record short-circuits via the
// in-memory id; everything else goes through dragonboat shard 0
// SyncRead with a bounded timeout. Returns (nil, nil) for a deployment
// the cluster has never seen — the caller treats that as "fall back to
// in-process registry".
//
// Implements invoker.DeploymentResolver. Bound at partition construction
// via invoker.Config.Deployments.
func (h *Host) resolveDeployment(deploymentID string) (*enginev1.DeploymentRecord, error) {
	if deploymentID == "" {
		return nil, nil
	}
	if rec := h.matchInprocDeployment(deploymentID); rec != nil {
		return rec, nil
	}
	if h.nh == nil {
		return nil, nil
	}
	// Shard 0 may not be hosted on this node (single-node deployments
	// have no metadata shard). SyncRead would return ErrShardNotFound;
	// treat that as "not in cluster" and fall back.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeployment{ID: deploymentID})
	if err != nil {
		// Probe-style: any error short-circuits to nil. The invoker
		// falls back to in-process registry, which is correct for the
		// synthetic inproc deployment running on a single-node host.
		return nil, nil //nolint:nilerr // intentional: shard 0 absence is not a caller error
	}
	rec, ok := res.(*enginev1.DeploymentRecord)
	if !ok {
		return nil, fmt.Errorf("host: resolveDeployment: unexpected lookup type %T", res)
	}
	return rec, nil
}

// matchInprocDeployment returns the synthetic in-proc record when
// deploymentID matches sdk.InprocDeploymentID(handlers), nil otherwise.
// Pure in-memory; no dragonboat.
func (h *Host) matchInprocDeployment(deploymentID string) *enginev1.DeploymentRecord {
	if h.InprocDeploymentID() != deploymentID {
		return nil
	}
	return h.InprocDeploymentRecord(uint64(time.Now().UnixMilli()))
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
