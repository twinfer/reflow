// Package ingress hosts the external-facing entrypoints (gRPC + HTTP/JSON
// via grpc-gateway) for submitting invocations, awaiting their results,
// resolving awakeables, and read-only admin queries.
//
// Phase 2 routes everything to a single partition (shard 1). The
// RouteToShard helper is the seam for Phase 4 consistent hashing.
package ingress

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// Phase2ShardID is the single partition hosted in Phase 2 deployments.
// Replaced by RouteToShard once Phase 4 lands consistent hashing.
const Phase2ShardID uint64 = 1

// Server implements ingressv1.IngressServer over an engine.Host. Constructed
// once per process and registered on both the gRPC server and the
// grpc-gateway runtime.ServeMux. Stateless apart from the host pointer and
// logger — concurrent requests are safe.
type Server struct {
	ingressv1.UnimplementedIngressServer

	host *engine.Host
	log  *slog.Logger
}

// NewServer builds an ingress Server bound to the given host. Log defaults
// to slog.Default if nil.
func NewServer(h *engine.Host, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{host: h, log: log}
}

// SubmitInvocation mints a fresh InvocationId, derives the owning partition
// (Phase 2: always shard 1), and proposes an InvokeCommand via the
// partition's ingress proposer. Returns the id; the caller must poll
// AwaitInvocation (or use SSE once Phase 5 lands).
func (s *Server) SubmitInvocation(ctx context.Context, req *ingressv1.SubmitInvocationRequest) (*ingressv1.SubmitInvocationResponse, error) {
	if req.GetService() == "" || req.GetHandler() == "" {
		return nil, status.Error(codes.InvalidArgument, "service and handler are required")
	}

	target := &enginev1.InvocationTarget{
		ServiceName: req.GetService(),
		HandlerName: req.GetHandler(),
		ObjectKey:   req.GetObjectKey(),
	}

	shardID := s.routeToShard(target)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no partition for shard %d", shardID)
	}

	// Phase 3 — optimistic idempotency lookup. If a prior submission with
	// the same (service, handler, object_key, idempotency_key) tuple has
	// already been accepted, surface its InvocationId directly without
	// minting a new one or proposing again. A losing race (two ingress
	// callers miss the lookup, both propose) is handled authoritatively
	// in the apply path's onInvoke: the second InvokeCommand is dropped.
	if ik := req.GetIdempotencyKey(); ik != "" {
		res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupIdempotency{
			Service:        target.GetServiceName(),
			Handler:        target.GetHandlerName(),
			ObjectKey:      target.GetObjectKey(),
			IdempotencyKey: ik,
		})
		if err == nil {
			if prior, ok := res.(*enginev1.InvocationId); ok && prior != nil {
				return &ingressv1.SubmitInvocationResponse{
					InvocationId:    prior,
					InvocationIdStr: FormatInvocationID(prior),
				}, nil
			}
		}
		// SyncRead errors fall through to propose; the apply-path dedup
		// is authoritative and idempotent on retries.
	}

	id, err := mintInvocationID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "mint invocation id: %v", err)
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId:   id,
		Target:         target,
		Input:          req.GetInput(),
		IdempotencyKey: req.GetIdempotencyKey(),
	}}}
	producerID := "http/" + FormatInvocationID(id)
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, status.Error(codes.Unavailable, "shard closed")
		}
		return nil, status.Errorf(codes.Internal, "propose invoke: %v", err)
	}
	return &ingressv1.SubmitInvocationResponse{
		InvocationId:    id,
		InvocationIdStr: FormatInvocationID(id),
	}, nil
}

// routeToShard picks the owning partition for a target. Phase 2 stub: always
// shard 1. Phase 4 will hash (service, object_key) into the partition table
// from the metadata shard.
func (s *Server) routeToShard(_ *enginev1.InvocationTarget) uint64 {
	return Phase2ShardID
}

// mintInvocationID generates a fresh 16-byte uuid v4 and packages it under
// the Phase 2 partition key (1). When multi-partition routing lands, this
// will derive partition_key from the target tuple before mint.
func mintInvocationID() (*enginev1.InvocationId, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	// Set RFC4122 v4 bits so the id is a well-formed UUID.
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return &enginev1.InvocationId{
		PartitionKey: Phase2ShardID,
		Uuid:         uuid,
	}, nil
}

// resolveID picks an InvocationId from either the proto field (preferred) or
// the string form. Returns an InvalidArgument error if neither is usable.
func resolveID(idStr string, idProto *enginev1.InvocationId) (*enginev1.InvocationId, error) {
	if idProto != nil && len(idProto.GetUuid()) == 16 {
		return idProto, nil
	}
	if idStr == "" {
		return nil, status.Error(codes.InvalidArgument, "invocation_id is required")
	}
	id, err := ParseInvocationID(idStr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse invocation_id: %v", err)
	}
	return id, nil
}
