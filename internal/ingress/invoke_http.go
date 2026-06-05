package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/observability"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// The REST data-plane facade ("/v1/*"): a single HTTP→durable-invocation kernel
// shared by the generic invoke routes and the process routes, and (via the
// Invoker seam) by the webhook adapter. It owns header lifting, body limits,
// the await/send split, error→HTTP mapping, and the IngressRESTRequests metric.
// The Connect ingress RPCs live alongside it on the same listener; a root-level
// "/{service}/{handler}" would conflict with the Connect subtree, hence "/v1/".

const (
	defaultMaxBodyBytes int64 = 4 << 20 // 4 MiB

	metaHeaderPrefix   = "Reflw-Meta-"
	idempotencyHeader  = "Idempotency-Key"
	invocationIDHeader = "X-Reflw-Invocation-Id"

	// Cedar action ids + plane group authorized by the REST routes — the same
	// ids the RPC path uses in procMap, so policy treats REST identically.
	actionSubmitInvocation = "SubmitInvocation"
	actionStartProcess     = "StartProcess"
	groupIngressActions    = "IngressActions"
)

// Invoker is the durable-submit seam the REST kernel and webhook adapter use.
// *Server satisfies it.
type Invoker interface {
	Submit(ctx context.Context, a SubmitArgs) (*enginev1.InvocationId, error)
	Await(ctx context.Context, id *enginev1.InvocationId, timeoutMs uint32) (*Outcome, error)
}

// ProcessStarter starts a BPMN/CMMN instance. *Server satisfies it.
type ProcessStarter interface {
	StartProcessCore(ctx context.Context, a StartProcessArgs) (uint64, string, error)
}

// IngressAuthorizer authorizes a REST-facade ingress call by Cedar action id.
// *authz.Interceptor satisfies it; kept as an interface so this package needs
// no authz import and the kernel is unit-testable with a fake.
type IngressAuthorizer interface {
	AuthorizeIngressAction(ctx context.Context, action string, groups ...string) error
}

var (
	_ Invoker        = (*Server)(nil)
	_ ProcessStarter = (*Server)(nil)
)

// InvokeConfig wires the REST data-plane kernel.
type InvokeConfig struct {
	Invoker      Invoker
	Starter      ProcessStarter
	Authorizer   IngressAuthorizer
	Metrics      *observability.Metrics
	MaxBodyBytes int64
	Log          *slog.Logger
}

func (c InvokeConfig) maxBody() int64 {
	if c.MaxBodyBytes > 0 {
		return c.MaxBodyBytes
	}
	return defaultMaxBodyBytes
}

// InvokeHTTP builds the handler for POST /v1/{service}/{handler} and
// /v1/{service}/{key}/{handler} (keyed). Await-by-default: the call blocks for
// the result; ?mode=send returns 202 + the invocation id immediately; an await
// that outlives the server-side clamp also degrades to 202 + id.
func InvokeHTTP(cfg InvokeConfig, keyed bool) http.Handler {
	route := "/v1/{service}/{handler}"
	if keyed {
		route = "/v1/{service}/{key}/{handler}"
	}
	maxBody := cfg.maxBody()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := &statusCapture{ResponseWriter: w, status: http.StatusOK}
		defer func() { recordREST(cfg.Metrics, route, r.Method, sc.status) }()

		if r.Method != http.MethodPost {
			http.Error(sc, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Authorize before reading the body so an unauthorized caller never streams one.
		if err := cfg.Authorizer.AuthorizeIngressAction(r.Context(), actionSubmitInvocation, groupIngressActions); err != nil {
			writeConnErr(sc, err)
			return
		}
		args := SubmitArgs{
			Service:        r.PathValue("service"),
			Handler:        r.PathValue("handler"),
			IdempotencyKey: r.Header.Get(idempotencyHeader),
			Metadata:       liftMetaHeaders(r.Header),
		}
		if keyed {
			args.ObjectKey = r.PathValue("key")
		}
		body, err := io.ReadAll(http.MaxBytesReader(sc, r.Body, maxBody))
		if err != nil {
			http.Error(sc, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		args.Input = body

		id, err := cfg.Invoker.Submit(r.Context(), args)
		if err != nil {
			writeConnErr(sc, err)
			return
		}
		idStr := FormatInvocationID(id)
		if r.URL.Query().Get("mode") == "send" {
			writeJSON(sc, http.StatusAccepted, map[string]string{"invocation_id": idStr})
			return
		}
		out, err := cfg.Invoker.Await(r.Context(), id, parseTimeoutMs(r.URL.Query().Get("timeout_ms")))
		if err != nil {
			writeConnErr(sc, err)
			return
		}
		switch {
		case !out.Completed:
			// Outlived the await clamp — degrade to send semantics; the caller polls.
			writeJSON(sc, http.StatusAccepted, map[string]string{"invocation_id": idStr})
		case out.FailureMessage != "" || out.FailureCode != 0:
			writeJSON(sc, http.StatusUnprocessableEntity, map[string]any{
				"invocation_id": idStr,
				"failure":       out.FailureMessage,
				"failure_code":  out.FailureCode,
			})
		default:
			sc.Header().Set(invocationIDHeader, idStr)
			sc.Header().Set("Content-Type", "application/octet-stream")
			sc.WriteHeader(http.StatusOK)
			_, _ = sc.Write(out.Output)
		}
	})
}

// StartProcessHTTP builds the handler for POST /v1/processes/{name} (BPMN) and
// /v1/cases/{name} (CMMN). Send-only → 202 + {pk, instance_key}. An optional
// ?instance_key= makes the start idempotent.
func StartProcessHTTP(cfg InvokeConfig, isCase bool) http.Handler {
	route := "/v1/processes/{name}"
	kind := "bpmn"
	if isCase {
		route = "/v1/cases/{name}"
		kind = "cmmn"
	}
	maxBody := cfg.maxBody()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := &statusCapture{ResponseWriter: w, status: http.StatusOK}
		defer func() { recordREST(cfg.Metrics, route, r.Method, sc.status) }()

		if r.Method != http.MethodPost {
			http.Error(sc, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := cfg.Authorizer.AuthorizeIngressAction(r.Context(), actionStartProcess, groupIngressActions); err != nil {
			writeConnErr(sc, err)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(sc, r.Body, maxBody))
		if err != nil {
			http.Error(sc, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		pk, instanceKey, err := cfg.Starter.StartProcessCore(r.Context(), StartProcessArgs{
			Name:        r.PathValue("name"),
			Kind:        kind,
			InstanceKey: r.URL.Query().Get("instance_key"),
			Vars:        body,
		})
		if err != nil {
			writeConnErr(sc, err)
			return
		}
		writeJSON(sc, http.StatusAccepted, map[string]any{
			"pk":           strconv.FormatUint(pk, 10),
			"instance_key": instanceKey,
		})
	})
}

// statusCapture records the first status written, for the REST metric.
type statusCapture struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (c *statusCapture) WriteHeader(code int) {
	if !c.wroteHeader {
		c.status = code
		c.wroteHeader = true
	}
	c.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeConnErr maps a connect.Error to an HTTP status + plain message. Auth
// denials carry opaque messages ("unauthenticated"/"forbidden"), so this never
// leaks which check failed.
func writeConnErr(w http.ResponseWriter, err error) {
	code := connect.CodeOf(err)
	msg := code.String()
	var ce *connect.Error
	if errors.As(err, &ce) {
		msg = ce.Message()
	}
	http.Error(w, msg, HTTPStatusForCode(code))
}

// HTTPStatusForCode maps a Connect error code to the HTTP status the Connect
// protocol itself uses, so the REST facade and HTTP-JSON Connect agree.
func HTTPStatusForCode(code connect.Code) int {
	switch code {
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest
	case connect.CodeFailedPrecondition:
		return http.StatusPreconditionFailed
	case connect.CodeNotFound:
		return http.StatusNotFound
	case connect.CodeAlreadyExists:
		return http.StatusConflict
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	case connect.CodeCanceled:
		return 499 // client closed request
	default:
		return http.StatusInternalServerError
	}
}

// liftMetaHeaders lifts inbound Reflw-Meta-* headers (lowercased + stripped)
// into invocation metadata — the REST carrier for ctx.Metadata().
func liftMetaHeaders(h http.Header) map[string]string {
	var m map[string]string
	for k, vs := range h {
		if len(vs) == 0 {
			continue
		}
		if rest, ok := strings.CutPrefix(k, metaHeaderPrefix); ok && rest != "" {
			if m == nil {
				m = make(map[string]string)
			}
			m[strings.ToLower(rest)] = vs[0]
		}
	}
	return m
}

// parseTimeoutMs parses ?timeout_ms=; 0 (→ the server-side awaitMaxTimeout
// clamp) on empty/invalid.
func parseTimeoutMs(s string) uint32 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

func recordREST(m *observability.Metrics, route, method string, status int) {
	if m == nil || m.IngressRESTRequests == nil {
		return
	}
	m.IngressRESTRequests.WithLabelValues(route, method, statusClass(status)).Inc()
}

func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}
