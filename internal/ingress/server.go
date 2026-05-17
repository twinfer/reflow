// Package ingress hosts the external-facing entrypoints (gRPC + HTTP/JSON
// via grpc-gateway) for submitting invocations, awaiting their results,
// resolving awakeables, and read-only admin queries.
//
// Routing goes through the Host's Partitioner: SubmitInvocation hashes
// (service, object_key) into a partition_key and stamps it onto the new
// InvocationId; lookup handlers (await, describe, get-output) trust the
// partition_key already on the id.
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
	"github.com/twinfer/reflow/internal/engine/routing"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

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

// SubmitInvocation mints a fresh InvocationId stamped with the
// partition_key derived from (service, object_key), then proposes an
// InvokeCommand via the owning partition's ingress proposer. Returns the
// id; the caller may poll AwaitInvocation or use AttachInvocation to wait
// for the result.
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

	// Optimistic idempotency lookup. If a prior submission with the same
	// (service, handler, object_key, idempotency_key) tuple has already
	// been accepted, surface its InvocationId directly without minting a
	// new one or proposing again. A losing race (two ingress callers miss
	// the lookup, both propose) is handled authoritatively in the apply
	// path's onInvoke: the second InvokeCommand is dropped.
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

	id, err := mintInvocationID(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "mint invocation id: %v", err)
	}

	// Resolve (service, handler) → deployment_id via shard 0's index.
	// A non-empty result pins this invocation to that deployment for its
	// lifetime, even if a later registration overwrites the (service,
	// handler) mapping. Empty result means no deployment has been
	// registered for this handler — return FailedPrecondition so the
	// caller knows they need to RegisterDeployment first.
	//
	// A non-nil error from LookupDeploymentIDByHandler means shard 0 was
	// transiently unreachable (election in progress, ctx expired); map
	// to Unavailable / DeadlineExceeded so the client retries rather
	// than treating the dispatch as a permanent configuration error.
	deploymentID, err := s.host.LookupDeploymentIDByHandler(ctx, target.GetServiceName(), target.GetHandlerName())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "lookup deployment: %v", err)
		}
		if errors.Is(err, context.Canceled) {
			return nil, status.Errorf(codes.Canceled, "lookup deployment: %v", err)
		}
		return nil, status.Errorf(codes.Unavailable, "lookup deployment: %v", err)
	}
	if deploymentID == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"no deployment registered for %s/%s", target.GetServiceName(), target.GetHandlerName())
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId:   id,
		Target:         target,
		Input:          req.GetInput(),
		IdempotencyKey: req.GetIdempotencyKey(),
		DeploymentId:   deploymentID,
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

// routeToShard picks the owning partition for a target by hashing
// (service, object_key) through the Host's Partitioner.
func (s *Server) routeToShard(target *enginev1.InvocationTarget) uint64 {
	return s.host.Partitioner().ShardForTarget(target)
}

// shardForID returns the partition shard owning the given invocation id.
// The partition_key is stamped at mint time and is authoritative — an id
// with zero partition_key is malformed and rejected as InvalidArgument.
func (s *Server) shardForID(id *enginev1.InvocationId) (uint64, error) {
	pk := id.GetPartitionKey()
	if pk == 0 {
		return 0, status.Error(codes.InvalidArgument, "invocation id has no partition_key")
	}
	return s.host.Partitioner().ShardForKey(pk), nil
}

// mintInvocationID generates a fresh 16-byte UUIDv4 and stamps the
// partition_key derived from the target's (service, object_key) tuple,
// pinning the id to a specific shard for its lifetime.
func mintInvocationID(target *enginev1.InvocationTarget) (*enginev1.InvocationId, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	// Set RFC4122 v4 bits so the id is a well-formed UUID.
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
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
