// Package ingress hosts the external-facing entrypoints for submitting
// invocations, awaiting their results, resolving awakeables, and
// read-only admin queries. Served over Connect (HTTP/2) with content-
// negotiated Connect / gRPC / gRPC-Web / HTTP-JSON.
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

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/routing"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// Server implements ingressv1connect.IngressHandler over an engine.Host.
// Stateless apart from the host pointer and logger — concurrent requests
// are safe.
type Server struct {
	ingressv1connect.UnimplementedIngressHandler

	host *engine.Host
	log  *slog.Logger
	// schemaResolver, when set, derives a parked task's submission JSON Schema for
	// the GET /v1/tasks/{token} read (set by the ingress runtime from the active
	// model resolver). Held as an interface so this package stays model-agnostic —
	// it never imports reflwos. Nil → the read returns the descriptor without a
	// schema (no process engine, or a non-schema-capable resolver).
	schemaResolver TaskSchemaResolver
}

// NewServer builds an ingress Server bound to the given host. Log
// defaults to slog.Default if nil.
func NewServer(h *engine.Host, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{host: h, log: log}
}

// SubmitArgs is the transport-agnostic input to a durable submit — the
// fields the former SubmitInvocation RPC carried, shared by the Connect
// RPC shell and the REST kernel (invoke_http.go) / webhook adapter.
type SubmitArgs struct {
	Service        string
	Handler        string
	ObjectKey      string
	Input          []byte
	IdempotencyKey string
	Metadata       map[string]string
}

// Submit mints a fresh InvocationId stamped with the partition_key derived
// from (service, object_key), then proposes an InvokeCommand via the owning
// partition's ingress proposer. Returns the id (or a prior id on an
// idempotency / workflow-key hit). The non-RPC core extracted from the
// former SubmitInvocation RPC: errors are connect.Errors so every transport
// maps the same codes.
func (s *Server) Submit(ctx context.Context, a SubmitArgs) (*enginev1.InvocationId, error) {
	if a.Service == "" || a.Handler == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service and handler are required"))
	}

	target := &enginev1.InvocationTarget{
		ServiceName: a.Service,
		HandlerName: a.Handler,
		ObjectKey:   a.ObjectKey,
	}

	shardID := s.routeToShard(target)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	// Optimistic idempotency lookup. If a prior submission with the same
	// (service, handler, object_key, idempotency_key) tuple has already
	// been accepted, surface its InvocationId directly without minting a
	// new one or proposing again. A losing race (two ingress callers
	// miss the lookup, both propose) is handled authoritatively in the
	// apply path's onInvoke: the second InvokeCommand is dropped.
	if ik := a.IdempotencyKey; ik != "" {
		// Submit is reached over the REST facade and webhook adapter (which derives
		// an idempotency key), neither of which carries the Connect deadline
		// interceptor — and SyncRead requires a deadline.
		rctx, rcancel := ensureReadDeadline(ctx)
		res, err := s.host.NodeHost().SyncRead(rctx, shardID, engine.LookupIdempotency{
			Service:        target.GetServiceName(),
			Handler:        target.GetHandlerName(),
			ObjectKey:      target.GetObjectKey(),
			IdempotencyKey: ik,
		})
		rcancel()
		if err == nil {
			if prior, ok := res.(*enginev1.InvocationId); ok && prior != nil {
				return prior, nil
			}
		}
		// SyncRead errors fall through to propose; the apply-path dedup
		// is authoritative and idempotent on retries.
	}

	// Resolve (service, handler) → (deployment_id, kind) via shard 0's
	// index. deployment_id pins this invocation to that deployment for
	// its lifetime. kind drives workflow lifecycle decisions: a Run-kind
	// handler enables single-run-per-key submit dedup before we mint a
	// fresh InvocationId.
	info, err := s.host.LookupHandlerInfo(ctx, target.GetServiceName(), target.GetHandlerName())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("lookup handler: %w", err))
		}
		if errors.Is(err, context.Canceled) {
			return nil, connect.NewError(connect.CodeCanceled, fmt.Errorf("lookup handler: %w", err))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("lookup handler: %w", err))
	}
	if info == nil || info.DeploymentID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("no deployment registered for %s/%s", target.GetServiceName(), target.GetHandlerName()))
	}

	// Workflow single-run-per-key dedup. For Run handlers, a prior submission
	// to (service, workflow_key) already claimed the key — surface the
	// existing InvocationId. Apply-path authoritatively dedups losing races.
	if protocolv1.Kind(info.Kind) == protocolv1.Kind_KIND_WORKFLOW && target.GetObjectKey() != "" {
		rctx, rcancel := ensureReadDeadline(ctx)
		res, err := s.host.NodeHost().SyncRead(rctx, shardID, engine.LookupWorkflowRun{
			Service:     target.GetServiceName(),
			WorkflowKey: target.GetObjectKey(),
		})
		rcancel()
		if err == nil {
			if prior, ok := res.(*enginev1.InvocationId); ok && prior != nil {
				return prior, nil
			}
		}
		// SyncRead errors fall through to propose; apply-path dedup is
		// authoritative and idempotent on retries.
	}

	id, err := mintInvocationID(target)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("mint invocation id: %w", err))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId:   id,
		Target:         target,
		Input:          a.Input,
		IdempotencyKey: a.IdempotencyKey,
		DeploymentId:   info.DeploymentID,
		Kind:           info.Kind,
		Metadata:       a.Metadata,
	}}}
	producerID := "http/" + FormatInvocationID(id)
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose invoke: %w", err))
	}
	return id, nil
}

// routeToShard picks the owning partition for a target by hashing
// (service, object_key) through the Host's Partitioner.
func (s *Server) routeToShard(target *enginev1.InvocationTarget) uint64 {
	return s.host.Partitioner().ShardForTarget(target)
}

// shardForID returns the partition shard owning the given invocation
// id. The partition_key is stamped at mint time and is authoritative —
// an id with zero partition_key is malformed and rejected as
// InvalidArgument.
func (s *Server) shardForID(id *enginev1.InvocationId) (uint64, error) {
	pk := id.GetPartitionKey()
	if pk == 0 {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("invocation id has no partition_key"))
	}
	return s.host.Partitioner().ShardForKey(pk), nil
}

// mintInvocationID generates a fresh 16-byte UUIDv4 and stamps the
// partition_key derived from (service, object_key), pinning the id to a
// specific shard for its lifetime.
func mintInvocationID(target *enginev1.InvocationTarget) (*enginev1.InvocationId, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
		Uuid:         uuid,
	}, nil
}

// resolveID picks an InvocationId from either the proto field
// (preferred) or the string form. Returns an InvalidArgument connect
// error if neither is usable.
func resolveID(idStr string, idProto *enginev1.InvocationId) (*enginev1.InvocationId, error) {
	if idProto != nil && len(idProto.GetUuid()) == 16 {
		return idProto, nil
	}
	if idStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invocation_id is required"))
	}
	id, err := ParseInvocationID(idStr)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("parse invocation_id: %w", err))
	}
	return id, nil
}
