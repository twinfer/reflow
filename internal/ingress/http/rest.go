package httpingress

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	connect "connectrpc.com/connect"
	"github.com/go-chi/chi/v5"

	"github.com/twinfer/reflow/internal/ingress"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// restHandlers binds chi route handlers to *ingress.Server. Every method
// is a thin URL parser: build the typed Connect request, delegate, map
// the response to a JSON envelope.
type restHandlers struct {
	srv *ingress.Server
	cfg Config
}

// submitResponseBody is the JSON envelope for /v1/send: the caller gets
// the minted invocation id and decides when to attach.
type submitResponseBody struct {
	InvocationID string `json:"invocation_id"`
}

// completedResponseBody is the JSON envelope for /v1/call (submit+await)
// and /v1/attach. Output bytes are JSON-encoded as a base64 string so
// arbitrary handler payloads (proto, binary, text) survive the
// JSON-round-trip unambiguously.
type completedResponseBody struct {
	InvocationID   string `json:"invocation_id"`
	Completed      bool   `json:"completed"`
	Output         []byte `json:"output,omitempty"` // base64 via json.Marshal
	FailureMessage string `json:"failure_message,omitempty"`
	FailureCode    uint32 `json:"failure_code,omitempty"`
}

// outputResponseBody is the JSON envelope for /v1/output (non-blocking
// status lookup). Status is the proto enum stringified for human reading.
type outputResponseBody struct {
	Status         string `json:"status"`
	Output         []byte `json:"output,omitempty"`
	FailureMessage string `json:"failure_message,omitempty"`
	FailureCode    uint32 `json:"failure_code,omitempty"`
}

// stateResponseBody is the JSON envelope for /v1/state. present
// distinguishes "absent" from "present-but-empty" — never inferred from
// len(value).
type stateResponseBody struct {
	Value   []byte `json:"value,omitempty"`
	Present bool   `json:"present"`
}

// errorBody is the canonical 4xx/5xx envelope. code mirrors Connect's
// snake_case naming so dashboards built against either system are
// comparable.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// resolvePayload is the request body for awakeable / promise resolution.
// Either Value or FailureMessage may be set (a failure wins when both
// are provided — same semantics as ResolveWorkflowPromise on Connect).
type resolvePayload struct {
	Value          []byte `json:"value,omitempty"`
	FailureMessage string `json:"failure_message,omitempty"`
}

func (h *restHandlers) call(w http.ResponseWriter, r *http.Request) {
	h.callImpl(w, r, chi.URLParam(r, "service"), "", chi.URLParam(r, "handler"))
}

func (h *restHandlers) callKeyed(w http.ResponseWriter, r *http.Request) {
	h.callImpl(w, r, chi.URLParam(r, "service"), chi.URLParam(r, "key"), chi.URLParam(r, "handler"))
}

// callImpl implements submit + long-poll await. The two-step is more
// straightforward than trying to extend SubmitInvocation server-side and
// preserves the Connect contract verbatim. Errors at either step map to
// the same HTTP envelope.
func (h *restHandlers) callImpl(w http.ResponseWriter, r *http.Request, service, key, handler string) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}

	submitResp, err := h.srv.SubmitInvocation(r.Context(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:        service,
		Handler:        handler,
		ObjectKey:      key,
		Input:          body,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Metadata:       collectMetadata(r.Header),
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	idStr := submitResp.Msg.GetInvocationIdStr()

	timeoutMs := parseTimeoutMs(r, h.cfg.MaxPollMs)
	awaitResp, err := h.srv.AwaitInvocation(r.Context(), connect.NewRequest(&ingressv1.AwaitInvocationRequest{
		InvocationId: idStr,
		TimeoutMs:    timeoutMs,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &completedResponseBody{
		InvocationID:   idStr,
		Completed:      awaitResp.Msg.GetCompleted(),
		Output:         awaitResp.Msg.GetOutput(),
		FailureMessage: awaitResp.Msg.GetFailureMessage(),
		FailureCode:    awaitResp.Msg.GetFailureCode(),
	})
}

func (h *restHandlers) send(w http.ResponseWriter, r *http.Request) {
	h.sendImpl(w, r, chi.URLParam(r, "service"), "", chi.URLParam(r, "handler"))
}

func (h *restHandlers) sendKeyed(w http.ResponseWriter, r *http.Request) {
	h.sendImpl(w, r, chi.URLParam(r, "service"), chi.URLParam(r, "key"), chi.URLParam(r, "handler"))
}

func (h *restHandlers) sendImpl(w http.ResponseWriter, r *http.Request, service, key, handler string) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	resp, err := h.srv.SubmitInvocation(r.Context(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:        service,
		Handler:        handler,
		ObjectKey:      key,
		Input:          body,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Metadata:       collectMetadata(r.Header),
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &submitResponseBody{InvocationID: resp.Msg.GetInvocationIdStr()})
}

func (h *restHandlers) attach(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "invocation_id")
	timeoutMs := parseTimeoutMs(r, h.cfg.MaxPollMs)
	resp, err := h.srv.AttachInvocation(r.Context(), connect.NewRequest(&ingressv1.AttachInvocationRequest{
		InvocationId: idStr,
		TimeoutMs:    timeoutMs,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &completedResponseBody{
		InvocationID:   idStr,
		Completed:      resp.Msg.GetCompleted(),
		Output:         resp.Msg.GetOutput(),
		FailureMessage: resp.Msg.GetFailureMessage(),
		FailureCode:    resp.Msg.GetFailureCode(),
	})
}

func (h *restHandlers) output(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "invocation_id")
	resp, err := h.srv.GetInvocationOutput(r.Context(), connect.NewRequest(&ingressv1.GetInvocationOutputRequest{
		InvocationId: idStr,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &outputResponseBody{
		Status:         outputStatusString(resp.Msg.GetStatus()),
		Output:         resp.Msg.GetOutput(),
		FailureMessage: resp.Msg.GetFailureMessage(),
		FailureCode:    resp.Msg.GetFailureCode(),
	})
}

func (h *restHandlers) cancel(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "invocation_id")
	resp, err := h.srv.CancelInvocation(r.Context(), connect.NewRequest(&ingressv1.CancelInvocationRequest{
		InvocationId: idStr,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": resp.Msg.GetAccepted()})
}

func (h *restHandlers) resolveAwakeable(w http.ResponseWriter, r *http.Request) {
	awkID := chi.URLParam(r, "awakeable_id")
	var p resolvePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "decode body: "+err.Error())
		return
	}
	resp, err := h.srv.ResolveAwakeable(r.Context(), connect.NewRequest(&ingressv1.ResolveAwakeableRequest{
		AwakeableId:    awkID,
		Value:          p.Value,
		FailureMessage: p.FailureMessage,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"resolved": resp.Msg.GetResolved()})
}

func (h *restHandlers) resolveWorkflowPromise(w http.ResponseWriter, r *http.Request) {
	service := chi.URLParam(r, "service")
	wkey := chi.URLParam(r, "workflow_key")
	name := chi.URLParam(r, "name")
	var p resolvePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "decode body: "+err.Error())
		return
	}
	resp, err := h.srv.ResolveWorkflowPromise(r.Context(), connect.NewRequest(&ingressv1.ResolveWorkflowPromiseRequest{
		Service:        service,
		WorkflowKey:    wkey,
		PromiseName:    name,
		Value:          p.Value,
		FailureMessage: p.FailureMessage,
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": resp.Msg.GetAccepted()})
}

func (h *restHandlers) getState(w http.ResponseWriter, r *http.Request) {
	resp, err := h.srv.GetObjectState(r.Context(), connect.NewRequest(&ingressv1.GetObjectStateRequest{
		Service:   chi.URLParam(r, "service"),
		ObjectKey: chi.URLParam(r, "key"),
		StateKey:  chi.URLParam(r, "state_key"),
	}))
	if err != nil {
		writeConnectErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &stateResponseBody{
		Value:   resp.Msg.GetValue(),
		Present: resp.Msg.GetPresent(),
	})
}

// readBody returns the request body bytes, surfacing oversize errors as
// 413 and any other read error as 400. On error, the response is fully
// written and the caller must not write anything further.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}
	body, err := io.ReadAll(r.Body)
	if err == nil {
		return body, true
	}
	// http.MaxBytesReader wraps the cause in *http.MaxBytesError.
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeError(w, http.StatusRequestEntityTooLarge, "resource_exhausted",
			"request body exceeds configured cap")
		return nil, false
	}
	writeError(w, http.StatusBadRequest, "invalid_argument", "read body: "+err.Error())
	return nil, false
}

// writeJSON serializes v as JSON and sets the standard headers. Any
// encode failure is logged via the response writer's underlying error
// path — by that point status has already been written, so there is no
// way to surface it to the client.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError emits the canonical errorBody envelope at the given HTTP
// status with a Connect-style snake_case code and a human message.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, &errorBody{Code: code, Message: msg})
}

// writeConnectErr maps a Connect error from a delegated handler call
// into the HTTP error envelope, using the Connect code → HTTP status
// table.
func writeConnectErr(w http.ResponseWriter, err error) {
	cerr := new(connect.Error)
	if errors.As(err, &cerr) {
		code := cerr.Code()
		writeError(w, httpStatusFor(code), connectCodeName(code), cerr.Message())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal", err.Error())
}

// httpStatusFor maps Connect codes to HTTP status codes per the design
// doc (see plan: "Error mapping").
func httpStatusFor(c connect.Code) int {
	switch c {
	case connect.CodeInvalidArgument, connect.CodeOutOfRange:
		return http.StatusBadRequest
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeNotFound:
		return http.StatusNotFound
	case connect.CodeAlreadyExists, connect.CodeAborted:
		return http.StatusConflict
	case connect.CodeFailedPrecondition:
		return http.StatusPreconditionFailed
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	case connect.CodeCanceled:
		return http.StatusRequestTimeout
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeUnimplemented:
		return http.StatusNotImplemented
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// connectCodeName returns the snake_case identifier for a Connect code,
// matching Connect's wire-level convention (see connectrpc.com/connect
// Code.String).
func connectCodeName(c connect.Code) string { return c.String() }

// metadataHeaderPrefix is the canonical HTTP header prefix the REST
// ingress lifts into SubmitInvocationRequest.metadata. Convention:
// "Reflow-Meta-Event-Id: evt_42" → metadata["event-id"] = "evt_42".
// Operator middleware (webhook signature verifiers, tenant resolvers,
// …) stamps verified facts here so the durable handler can route
// without re-verifying.
//
// Keys are lowercased before storage because Go's net/http canonicalizes
// header names (first letter uppercase, rest lowercase per dash segment)
// — relying on the canonical form makes handler reads depend on Go's
// MIME rules, which is brittle. Lowercased keys are stable across
// languages and stable through the HTTP/proto/handler hops.
const metadataHeaderPrefix = "Reflow-Meta-"

// collectMetadata extracts every header whose canonical name starts
// with metadataHeaderPrefix, returning a (lowercased-suffix → value)
// map. Multiple values per header are joined with ", " (Go's
// http.Header.Get returns only the first; the join surfaces the rest
// deterministically). Nil when no matching headers are present so the
// proto field stays empty.
func collectMetadata(h http.Header) map[string]string {
	var out map[string]string
	for k, vs := range h {
		if !strings.HasPrefix(k, metadataHeaderPrefix) {
			continue
		}
		if out == nil {
			out = make(map[string]string, 4)
		}
		out[strings.ToLower(k[len(metadataHeaderPrefix):])] = strings.Join(vs, ", ")
	}
	return out
}

// outputStatusString stringifies the GetInvocationOutputResponse_Status
// proto enum for the JSON envelope. The proto numeric values aren't
// stable across SDKs — the snake_case forms are.
func outputStatusString(s ingressv1.GetInvocationOutputResponse_Status) string {
	switch s {
	case ingressv1.GetInvocationOutputResponse_PENDING:
		return "pending"
	case ingressv1.GetInvocationOutputResponse_COMPLETED_OK:
		return "completed_ok"
	case ingressv1.GetInvocationOutputResponse_COMPLETED_FAILED:
		return "completed_failed"
	case ingressv1.GetInvocationOutputResponse_UNKNOWN:
		return "unknown"
	default:
		return "unknown"
	}
}
