package reflw

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"

	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/delivery"
	"github.com/twinfer/reflw/internal/engine/snapshot"
	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/reflw/creds"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Host is a running reflw node. Construct one via Run; close it via Close.
//
// Host is a thin wrapper over the internal engine.Host. Callers should
// treat the engine package as private; Host is the stable surface.
type Host struct {
	engine         *engine.Host
	metricsCloser  func() error
	ingressRT      *ingress.Runtime
	ingressCreds   *creds.ListenerCreds
	deliverySrv    *connectserver.Server
	deliveryClient *delivery.Client
	deliveryCreds  *creds.ListenerCreds
	adminSrv       *connectserver.Server
	adminCreds     *creds.ListenerCreds
	// nodeIdentity is the self-issued mesh identity shared by the admin +
	// delivery listeners (built in Run when a cluster CA is configured).
	// nil for single-node / non-mesh deployments. Closed in Close.
	nodeIdentity  *creds.NodeIdentity
	authCloser    func() error
	snapshotCxl   context.CancelFunc
	snapshotRepo  *snapshot.BlobRepository
	handlerSigner *creds.Signer
	// pebbleCache / pebbleFileCache are the node-global Pebble caches
	// shared across every shard DB (built in Run). Close Unrefs them
	// after the engine closes its DBs — see Close.
	pebbleCache     *pebble.Cache
	pebbleFileCache *pebble.FileCache
}

// Close stops every partition and the underlying NodeHost. Idempotent.
// Stops the ingress server (closed first so client requests stop arriving
// before in-flight work drains), metrics HTTP server, admin + Delivery
// Connect servers, the snapshot producer goroutines, and the pooled
// delivery client.
func (h *Host) Close() error {
	var firstErr error
	if h.ingressRT != nil {
		if err := h.ingressRT.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.ingressRT = nil
	}
	if h.snapshotCxl != nil {
		h.snapshotCxl()
		h.snapshotCxl = nil
	}
	if h.snapshotRepo != nil {
		if err := h.snapshotRepo.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.snapshotRepo = nil
	}
	if h.adminSrv != nil {
		if err := h.adminSrv.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.adminSrv = nil
	}
	if h.deliverySrv != nil {
		if err := h.deliverySrv.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.deliverySrv = nil
	}
	if h.deliveryClient != nil {
		if err := h.deliveryClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.deliveryClient = nil
	}
	if h.engine != nil {
		if err := h.engine.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.engine = nil
	}
	// Unref the node-global Pebble caches only after the engine — and
	// therefore every shard DB that held a ref — has closed. Each is
	// owned by exactly one ref here, so this drops the last one and frees
	// the cache memory.
	if h.pebbleCache != nil {
		h.pebbleCache.Unref()
		h.pebbleCache = nil
	}
	if h.pebbleFileCache != nil {
		h.pebbleFileCache.Unref()
		h.pebbleFileCache = nil
	}
	if h.authCloser != nil {
		if err := h.authCloser(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.authCloser = nil
	}
	if err := creds.CloseAll(h.deliveryCreds, h.adminCreds, h.ingressCreds); err != nil && firstErr == nil {
		firstErr = err
	}
	h.deliveryCreds = nil
	h.adminCreds = nil
	h.ingressCreds = nil
	// nodeIdentity owns the CertMagic Manager shared by the mesh
	// listeners (their ListenerCreds.Close is nil), so close it here after
	// those listeners are down.
	if h.nodeIdentity != nil {
		if err := h.nodeIdentity.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.nodeIdentity = nil
	}
	if h.handlerSigner != nil {
		h.handlerSigner.Close()
		h.handlerSigner = nil
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
		return errors.New("reflw: host closed")
	}
	return h.engine.AwaitLeader(ctx, shardID)
}

// LookupInvocationStatus performs a linearizable read of an invocation's
// status from the partition that owns it.
func (h *Host) LookupInvocationStatus(ctx context.Context, shardID uint64, id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	if h.engine == nil {
		return nil, errors.New("reflw: host closed")
	}
	return h.engine.LookupInvocationStatus(ctx, shardID, id)
}

// Engine returns the underlying internal engine.Host. Reserved for tests
// that need access to internal hooks (Partition, NodeHost). Not part of
// the stable API.
func (h *Host) Engine() *engine.Host { return h.engine }
