package reflow

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Host is a running reflow node. Construct one via Run; close it via Close.
//
// Host is a thin wrapper over the internal engine.Host. Callers should
// treat the engine package as private; Host is the stable surface.
type Host struct {
	engine         *engine.Host
	metricsCloser  func() error
	deliverySrv    *grpc.Server
	deliveryLn     net.Listener
	deliveryClient *delivery.Client
	deliveryCreds  *creds.ListenerCreds
	adminSrv       *grpc.Server
	adminLn        net.Listener
	adminCreds     *creds.ListenerCreds
	authCloser     func() error
	snapshotCxl    context.CancelFunc
	snapshotRepo   *snapshot.BlobRepository
	handlerSigner  *creds.Signer
}

// Close stops every partition and the underlying NodeHost. Idempotent.
// Stops the metrics HTTP server, admin + Delivery gRPC servers, the
// snapshot producer goroutines, and the pooled delivery client.
func (h *Host) Close() error {
	var firstErr error
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
		h.adminSrv.GracefulStop()
		h.adminSrv = nil
	}
	if h.adminLn != nil {
		// adminLn closes first — firstErr is still nil; drop the
		// otherwise-tautological nil check (vet's `nilness` analyzer
		// would flag it).
		if err := h.adminLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			firstErr = err
		}
		h.adminLn = nil
	}
	if h.deliverySrv != nil {
		h.deliverySrv.GracefulStop()
		h.deliverySrv = nil
	}
	if h.deliveryLn != nil {
		if err := h.deliveryLn.Close(); err != nil && firstErr == nil && !errors.Is(err, net.ErrClosed) {
			firstErr = err
		}
		h.deliveryLn = nil
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
	if h.authCloser != nil {
		if err := h.authCloser(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.authCloser = nil
	}
	if err := creds.CloseAll(h.deliveryCreds, h.adminCreds); err != nil && firstErr == nil {
		firstErr = err
	}
	h.deliveryCreds = nil
	h.adminCreds = nil
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
// the stable API.
func (h *Host) Engine() *engine.Host { return h.engine }
