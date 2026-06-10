package ingress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// --- fakes -------------------------------------------------------------------

type fakeInvoker struct {
	submitID  *enginev1.InvocationId
	submitErr error
	out       *ingress.Outcome
	awaitErr  error

	gotArgs      ingress.SubmitArgs
	submitCalled bool
	awaitCalled  bool
}

func (f *fakeInvoker) Submit(_ context.Context, a ingress.SubmitArgs) (*enginev1.InvocationId, error) {
	f.submitCalled = true
	f.gotArgs = a
	return f.submitID, f.submitErr
}

func (f *fakeInvoker) Await(_ context.Context, _ *enginev1.InvocationId, _ uint32) (*ingress.Outcome, error) {
	f.awaitCalled = true
	return f.out, f.awaitErr
}

type fakeStarter struct {
	pk          uint64
	instanceKey string
	err         error

	gotArgs ingress.StartProcessArgs
	called  bool
}

func (f *fakeStarter) StartProcessCore(_ context.Context, a ingress.StartProcessArgs) (uint64, string, error) {
	f.called = true
	f.gotArgs = a
	return f.pk, f.instanceKey, f.err
}

type fakeAuthorizer struct {
	err       error
	gotAction string
}

func (f *fakeAuthorizer) AuthorizeIngressAction(_ context.Context, action string, _ ...string) error {
	f.gotAction = action
	return f.err
}

type fakeReader struct {
	present      bool
	events       []*enginev1.ProcessHistoryEvent
	nextAfterSeq uint64
	err          error

	instance    *ingressv1.GetProcessInstanceResponse // GetProcessInstanceCore canned result
	instanceErr error

	task    *ingressv1.GetTaskResponse // GetTaskCore canned result
	taskErr error

	gotName, gotKey string
	gotToken        string
	gotAfterSeq     uint64
	gotLimit        int
	called          bool
}

func (f *fakeReader) GetProcessInstanceCore(_ context.Context, name, instanceKey string) (*ingressv1.GetProcessInstanceResponse, error) {
	f.called = true
	f.gotName, f.gotKey = name, instanceKey
	return f.instance, f.instanceErr
}

func (f *fakeReader) GetProcessInstanceHistoryCore(_ context.Context, name, instanceKey string, afterSeq uint64, limit int) (bool, []*enginev1.ProcessHistoryEvent, uint64, error) {
	f.called = true
	f.gotName, f.gotKey, f.gotAfterSeq, f.gotLimit = name, instanceKey, afterSeq, limit
	return f.present, f.events, f.nextAfterSeq, f.err
}

func (f *fakeReader) GetTaskCore(_ context.Context, resumeToken string) (*ingressv1.GetTaskResponse, error) {
	f.called = true
	f.gotToken = resumeToken
	return f.task, f.taskErr
}

type fakeDeliverer struct {
	pk  uint64
	err error

	gotArgs ingress.DeliverProcessEventArgs
	called  bool
}

func (f *fakeDeliverer) DeliverProcessEventCore(_ context.Context, a ingress.DeliverProcessEventArgs) (uint64, error) {
	f.called = true
	f.gotArgs = a
	return f.pk, f.err
}

type fakeCompleter struct {
	pk  uint64
	err error

	gotArgs ingress.CompleteTaskArgs
	called  bool
}

func (f *fakeCompleter) CompleteTaskCore(_ context.Context, a ingress.CompleteTaskArgs) (uint64, error) {
	f.called = true
	f.gotArgs = a
	return f.pk, f.err
}

// --- helpers -----------------------------------------------------------------

func restMux(ic ingress.InvokeConfig) *http.ServeMux {
	m := http.NewServeMux()
	m.Handle("POST /v1/processes/{name}", ingress.StartProcessHTTP(ic, false))
	m.Handle("POST /v1/cases/{name}", ingress.StartProcessHTTP(ic, true))
	m.Handle("POST /v1/processes/{name}/{key}/events", ingress.DeliverProcessEventHTTP(ic))
	m.Handle("GET /v1/processes/{name}/{key}", ingress.GetProcessInstanceHTTP(ic))
	m.Handle("GET /v1/processes/{name}/{key}/history", ingress.GetProcessHistoryHTTP(ic))
	m.Handle("GET /v1/tasks/{token}", ingress.GetTaskHTTP(ic))
	m.Handle("POST /v1/tasks/{token}", ingress.CompleteTaskHTTP(ic))
	m.Handle("POST /v1/{service}/{key}/{handler}", ingress.InvokeHTTP(ic, true))
	m.Handle("POST /v1/{service}/{handler}", ingress.InvokeHTTP(ic, false))
	return m
}

func doReq(m http.Handler, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, req)
	return rec
}

func sampleID() *enginev1.InvocationId { return makeID(0xabcd, bytes.Repeat([]byte{0x11}, 16)) }

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode json %q: %v", rec.Body.String(), err)
	}
	return m
}

// --- generic invoke kernel ---------------------------------------------------

func TestInvokeHTTP_AwaitSuccess(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID(), out: &ingress.Outcome{Output: []byte("echo:hi"), Completed: true}}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: authz}), "POST", "/v1/Echo/echo", []byte("hi"), nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "echo:hi" {
		t.Fatalf("body = %q, want %q", got, "echo:hi")
	}
	if rec.Header().Get("X-Reflw-Invocation-Id") == "" {
		t.Fatalf("missing X-Reflw-Invocation-Id header")
	}
	if inv.gotArgs.Service != "Echo" || inv.gotArgs.Handler != "echo" || string(inv.gotArgs.Input) != "hi" {
		t.Fatalf("submit args = %+v", inv.gotArgs)
	}
	if authz.gotAction != "SubmitInvocation" {
		t.Fatalf("authorized action = %q, want SubmitInvocation", authz.gotAction)
	}
}

func TestInvokeHTTP_SendModeSkipsAwait(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID()}
	rec := doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}}), "POST", "/v1/Echo/echo?mode=send", []byte("hi"), nil)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if inv.awaitCalled {
		t.Fatalf("mode=send must not call Await")
	}
	if decodeJSON(t, rec)["invocation_id"] == "" {
		t.Fatalf("missing invocation_id in %q", rec.Body.String())
	}
}

func TestInvokeHTTP_KeyedRoute(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID(), out: &ingress.Outcome{Completed: true}}
	doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}}), "POST", "/v1/Obj/k1/h", []byte("x"), nil)
	if inv.gotArgs.ObjectKey != "k1" {
		t.Fatalf("object_key = %q, want k1", inv.gotArgs.ObjectKey)
	}
}

func TestInvokeHTTP_MetaAndIdempotencyHeaders(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID(), out: &ingress.Outcome{Completed: true}}
	doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}}), "POST", "/v1/Echo/echo", []byte("x"),
		map[string]string{"Reflw-Meta-Foo": "bar", "Idempotency-Key": "idem-1"})
	if inv.gotArgs.Metadata["foo"] != "bar" {
		t.Fatalf("metadata = %v, want foo=bar", inv.gotArgs.Metadata)
	}
	if inv.gotArgs.IdempotencyKey != "idem-1" {
		t.Fatalf("idempotency_key = %q, want idem-1", inv.gotArgs.IdempotencyKey)
	}
}

func TestInvokeHTTP_FailureOutcome(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID(), out: &ingress.Outcome{Completed: true, FailureMessage: "boom", FailureCode: 42}}
	rec := doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}}), "POST", "/v1/Echo/echo", []byte("x"), nil)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if decodeJSON(t, rec)["failure"] != "boom" {
		t.Fatalf("failure body = %q", rec.Body.String())
	}
}

func TestInvokeHTTP_AwaitTimeoutDegradesToSend(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID(), out: &ingress.Outcome{Completed: false}}
	rec := doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}}), "POST", "/v1/Echo/echo", []byte("x"), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (await-timeout degrades to send)", rec.Code)
	}
	if decodeJSON(t, rec)["invocation_id"] == "" {
		t.Fatalf("missing invocation_id on timeout")
	}
}

func TestInvokeHTTP_AuthzDenyBeforeSubmit(t *testing.T) {
	cases := []struct {
		name string
		code connect.Code
		want int
	}{
		{"unauthenticated", connect.CodeUnauthenticated, http.StatusUnauthorized},
		{"permission_denied", connect.CodePermissionDenied, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := &fakeInvoker{submitID: sampleID()}
			authz := &fakeAuthorizer{err: connect.NewError(tc.code, errors.New(tc.name))}
			rec := doReq(restMux(ingress.InvokeConfig{Invoker: inv, Authorizer: authz}), "POST", "/v1/Echo/echo", []byte("x"), nil)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if inv.submitCalled {
				t.Fatalf("Submit must not run after authz denial")
			}
		})
	}
}

func TestInvokeHTTP_BodyTooLarge(t *testing.T) {
	inv := &fakeInvoker{submitID: sampleID()}
	ic := ingress.InvokeConfig{Invoker: inv, Authorizer: &fakeAuthorizer{}, MaxBodyBytes: 8}
	rec := doReq(restMux(ic), "POST", "/v1/Echo/echo", bytes.Repeat([]byte("x"), 100), nil)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if inv.submitCalled {
		t.Fatalf("Submit must not run on oversize body")
	}
}

// --- process kernel ----------------------------------------------------------

func TestStartProcessHTTP_BPMNandCMMN(t *testing.T) {
	for _, tc := range []struct {
		path, wantKind string
		isCase         bool
	}{
		{"/v1/processes/order", "bpmn", false},
		{"/v1/cases/claim", "cmmn", true},
	} {
		st := &fakeStarter{pk: 7, instanceKey: "p-1"}
		authz := &fakeAuthorizer{}
		rec := doReq(restMux(ingress.InvokeConfig{Starter: st, Authorizer: authz}), "POST", tc.path+"?instance_key=abc", []byte("{}"), nil)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s: status = %d, want 202 (%q)", tc.path, rec.Code, rec.Body.String())
		}
		body := decodeJSON(t, rec)
		if body["pk"] != "7" || body["instance_key"] != "p-1" {
			t.Fatalf("%s: body = %v", tc.path, body)
		}
		want := tc.path[len("/v1/processes/"):]
		if tc.isCase {
			want = tc.path[len("/v1/cases/"):]
		}
		if st.gotArgs.Name != want || st.gotArgs.Kind != tc.wantKind || string(st.gotArgs.Vars) != "{}" || st.gotArgs.InstanceKey != "abc" {
			t.Fatalf("%s: start args = %+v", tc.path, st.gotArgs)
		}
		if authz.gotAction != "StartProcess" {
			t.Fatalf("%s: authorized action = %q, want StartProcess", tc.path, authz.gotAction)
		}
	}
}

func TestStartProcessHTTP_AuthzDeny(t *testing.T) {
	st := &fakeStarter{}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Starter: st, Authorizer: authz}), "POST", "/v1/processes/order", []byte("{}"), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if st.called {
		t.Fatalf("StartProcessCore must not run after authz denial")
	}
}

// TestGetProcessHistoryHTTP_Success: the GET history route authorizes against
// GetProcessInstanceHistory, threads name/key/cursor/limit into the reader, and
// renders the timeline as protojson (uint64→string, enum→name).
func TestGetProcessHistoryHTTP_Success(t *testing.T) {
	rd := &fakeReader{present: true, nextAfterSeq: 2, events: []*enginev1.ProcessHistoryEvent{
		{Seq: 1, Kind: enginev1.ProcessHistoryKind_PROCESS_HISTORY_STARTED},
		{Seq: 2, Kind: enginev1.ProcessHistoryKind_PROCESS_HISTORY_SUBSCRIBED, NodeId: "wait"},
	}}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/processes/order/o-1/history?after_seq=5&limit=10", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", rec.Code, rec.Body.String())
	}
	if authz.gotAction != "GetProcessInstanceHistory" {
		t.Fatalf("authorized action = %q, want GetProcessInstanceHistory", authz.gotAction)
	}
	if rd.gotName != "order" || rd.gotKey != "o-1" || rd.gotAfterSeq != 5 || rd.gotLimit != 10 {
		t.Fatalf("reader args = name=%q key=%q after=%d limit=%d", rd.gotName, rd.gotKey, rd.gotAfterSeq, rd.gotLimit)
	}
	body := decodeJSON(t, rec)
	if body["present"] != true {
		t.Fatalf("present = %v, want true (%q)", body["present"], rec.Body.String())
	}
	if body["nextAfterSeq"] != "2" { // protojson renders uint64 as a string
		t.Fatalf("nextAfterSeq = %v, want \"2\"", body["nextAfterSeq"])
	}
	evs, ok := body["events"].([]any)
	if !ok || len(evs) != 2 {
		t.Fatalf("events = %v", body["events"])
	}
	if ev0 := evs[0].(map[string]any); ev0["kind"] != "PROCESS_HISTORY_STARTED" {
		t.Fatalf("events[0].kind = %v, want PROCESS_HISTORY_STARTED", ev0["kind"])
	}
}

func TestGetProcessHistoryHTTP_NotFound(t *testing.T) {
	rd := &fakeReader{present: false}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/processes/order/o-1/history", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%q)", rec.Code, rec.Body.String())
	}
}

func TestGetProcessHistoryHTTP_AuthzDeny(t *testing.T) {
	rd := &fakeReader{present: true}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/processes/order/o-1/history", nil, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if rd.called {
		t.Fatalf("reader must not run after authz denial")
	}
}

// TestDeliverProcessEventHTTP_Success: the events route authorizes against
// DeliverProcessEvent, threads name/key/?kind/body into the deliverer, and returns
// 202 + {pk, accepted}.
func TestDeliverProcessEventHTTP_Success(t *testing.T) {
	dl := &fakeDeliverer{pk: 42}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Deliverer: dl, Authorizer: authz}),
		"POST", "/v1/processes/order/o-1/events?kind=UserTaskCompleted", []byte(`{"NodeID":"u"}`), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%q)", rec.Code, rec.Body.String())
	}
	if authz.gotAction != "DeliverProcessEvent" {
		t.Fatalf("authorized action = %q, want DeliverProcessEvent", authz.gotAction)
	}
	if dl.gotArgs.Name != "order" || dl.gotArgs.InstanceKey != "o-1" ||
		dl.gotArgs.EventKind != "UserTaskCompleted" || string(dl.gotArgs.Payload) != `{"NodeID":"u"}` {
		t.Fatalf("deliver args = %+v", dl.gotArgs)
	}
	if body := decodeJSON(t, rec); body["pk"] != "42" || body["accepted"] != true {
		t.Fatalf("body = %v", body)
	}
}

// TestDeliverProcessEventHTTP_MissingKind: ?kind is required (400, deliverer untouched).
func TestDeliverProcessEventHTTP_MissingKind(t *testing.T) {
	dl := &fakeDeliverer{}
	rec := doReq(restMux(ingress.InvokeConfig{Deliverer: dl, Authorizer: &fakeAuthorizer{}}),
		"POST", "/v1/processes/order/o-1/events", []byte(`{}`), nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if dl.called {
		t.Fatalf("deliverer must not run without a kind")
	}
}

func TestDeliverProcessEventHTTP_AuthzDeny(t *testing.T) {
	dl := &fakeDeliverer{}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Deliverer: dl, Authorizer: authz}),
		"POST", "/v1/processes/order/o-1/events?kind=UserTaskCompleted", []byte(`{}`), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if dl.called {
		t.Fatalf("deliverer must not run after authz denial")
	}
}

// TestGetProcessInstanceHTTP_Success: the GET instance route authorizes against
// GetProcessInstance, threads name/key into the reader, and renders the state as
// protojson — including awaiting_tasks, whose resume_token survives the JSON
// round-trip and decodes back to (name, key, node_id).
func TestGetProcessInstanceHTTP_Success(t *testing.T) {
	tok, err := keys.MintResumeToken(0xab, "order", "o-1", "u")
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	rd := &fakeReader{instance: &ingressv1.GetProcessInstanceResponse{
		Present: true,
		Status:  enginev1.ProcessStatus_PROCESS_STATUS_RUNNING,
		Kind:    enginev1.ProcessKind_PROCESS_KIND_BPMN,
		AwaitingTasks: []*ingressv1.AwaitingTaskInfo{
			{NodeId: "u", Name: "Approve", ResumeToken: tok},
		},
	}}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/processes/order/o-1", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", rec.Code, rec.Body.String())
	}
	if authz.gotAction != "GetProcessInstance" {
		t.Fatalf("authorized action = %q, want GetProcessInstance", authz.gotAction)
	}
	if rd.gotName != "order" || rd.gotKey != "o-1" {
		t.Fatalf("reader args = name=%q key=%q", rd.gotName, rd.gotKey)
	}
	body := decodeJSON(t, rec)
	if body["present"] != true || body["status"] != "PROCESS_STATUS_RUNNING" {
		t.Fatalf("body = %v (%q)", body, rec.Body.String())
	}
	tasks, ok := body["awaitingTasks"].([]any)
	if !ok || len(tasks) != 1 {
		t.Fatalf("awaitingTasks = %v", body["awaitingTasks"])
	}
	t0 := tasks[0].(map[string]any)
	if t0["nodeId"] != "u" || t0["name"] != "Approve" {
		t.Fatalf("awaitingTasks[0] = %v", t0)
	}
	gotTok, _ := t0["resumeToken"].(string)
	tgt, err := keys.DecodeResumeToken(gotTok)
	if err != nil {
		t.Fatalf("decode rendered token %q: %v", gotTok, err)
	}
	if tgt.Service != "order" || tgt.InstanceKey != "o-1" || tgt.NodeID != "u" {
		t.Fatalf("decoded token = %+v", tgt)
	}
}

func TestGetProcessInstanceHTTP_NotFound(t *testing.T) {
	rd := &fakeReader{instance: &ingressv1.GetProcessInstanceResponse{Present: false}}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/processes/order/o-1", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%q)", rec.Code, rec.Body.String())
	}
}

func TestGetProcessInstanceHTTP_AuthzDeny(t *testing.T) {
	rd := &fakeReader{instance: &ingressv1.GetProcessInstanceResponse{Present: true}}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/processes/order/o-1", nil, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if rd.called {
		t.Fatalf("reader must not run after authz denial")
	}
}

// --- complete-by-resume-token kernel -----------------------------------------

// TestCompleteTaskHTTP_Success: POST /v1/tasks/{token} authorizes against
// DeliverProcessEvent, threads the path token + body (output vars) into the
// completer as a non-fail completion, and returns 202 + {pk, accepted}.
func TestCompleteTaskHTTP_Success(t *testing.T) {
	cp := &fakeCompleter{pk: 99}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Completer: cp, Authorizer: authz}),
		"POST", "/v1/tasks/rpt_abc", []byte(`{"approved":true}`), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%q)", rec.Code, rec.Body.String())
	}
	if authz.gotAction != "DeliverProcessEvent" {
		t.Fatalf("authorized action = %q, want DeliverProcessEvent", authz.gotAction)
	}
	if cp.gotArgs.ResumeToken != "rpt_abc" || string(cp.gotArgs.Output) != `{"approved":true}` || cp.gotArgs.Fail {
		t.Fatalf("complete args = %+v", cp.gotArgs)
	}
	if body := decodeJSON(t, rec); body["pk"] != "99" || body["accepted"] != true {
		t.Fatalf("body = %v", body)
	}
}

// TestCompleteTaskHTTP_FailAction: ?action=fail&failure= drives the fail path —
// Fail set, FailureMessage threaded, body ignored as output.
func TestCompleteTaskHTTP_FailAction(t *testing.T) {
	cp := &fakeCompleter{pk: 1}
	rec := doReq(restMux(ingress.InvokeConfig{Completer: cp, Authorizer: &fakeAuthorizer{}}),
		"POST", "/v1/tasks/rpt_abc?action=fail&failure=nope", []byte(`{}`), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%q)", rec.Code, rec.Body.String())
	}
	if !cp.gotArgs.Fail || cp.gotArgs.FailureMessage != "nope" {
		t.Fatalf("complete args = %+v", cp.gotArgs)
	}
}

func TestCompleteTaskHTTP_InvalidAction(t *testing.T) {
	cp := &fakeCompleter{}
	rec := doReq(restMux(ingress.InvokeConfig{Completer: cp, Authorizer: &fakeAuthorizer{}}),
		"POST", "/v1/tasks/rpt_abc?action=bogus", []byte(`{}`), nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if cp.called {
		t.Fatalf("completer must not run on an invalid action")
	}
}

func TestCompleteTaskHTTP_AuthzDeny(t *testing.T) {
	cp := &fakeCompleter{}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Completer: cp, Authorizer: authz}),
		"POST", "/v1/tasks/rpt_abc", []byte(`{}`), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if cp.called {
		t.Fatalf("completer must not run after authz denial")
	}
}

// TestCompleteTaskHTTP_StaleToken: a stale/forged token surfaces as
// FailedPrecondition from the core → 412 (the boundary-validation contract).
func TestCompleteTaskHTTP_StaleToken(t *testing.T) {
	cp := &fakeCompleter{err: connect.NewError(connect.CodeFailedPrecondition, errors.New("not awaiting"))}
	rec := doReq(restMux(ingress.InvokeConfig{Completer: cp, Authorizer: &fakeAuthorizer{}}),
		"POST", "/v1/tasks/rpt_abc", []byte(`{}`), nil)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412 (%q)", rec.Code, rec.Body.String())
	}
}

// --- resolve-task-by-resume-token kernel -------------------------------------

// TestGetTaskHTTP_SuccessWithSchema: GET /v1/tasks/{token} authorizes against
// GetProcessInstance (a read), threads the path token into the reader, and renders
// the descriptor + submission schema as protojson (the schema struct inline).
func TestGetTaskHTTP_SuccessWithSchema(t *testing.T) {
	sch, err := structpb.NewStruct(map[string]any{"type": "object", "required": []any{"approved"}})
	if err != nil {
		t.Fatalf("build schema struct: %v", err)
	}
	rd := &fakeReader{task: &ingressv1.GetTaskResponse{
		Service: "order", InstanceKey: "o-1", NodeId: "u", Name: "Approve", Schema: sch,
	}}
	authz := &fakeAuthorizer{}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/tasks/rpt_xyz", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", rec.Code, rec.Body.String())
	}
	if authz.gotAction != "GetProcessInstance" {
		t.Fatalf("authorized action = %q, want GetProcessInstance", authz.gotAction)
	}
	if rd.gotToken != "rpt_xyz" {
		t.Fatalf("reader token = %q, want rpt_xyz", rd.gotToken)
	}
	body := decodeJSON(t, rec)
	if body["service"] != "order" || body["instanceKey"] != "o-1" || body["nodeId"] != "u" || body["name"] != "Approve" {
		t.Fatalf("descriptor body = %v (%q)", body, rec.Body.String())
	}
	schema, ok := body["schema"].(map[string]any)
	if !ok || schema["type"] != "object" {
		t.Fatalf("schema = %v (%q)", body["schema"], rec.Body.String())
	}
}

// TestGetTaskHTTP_DescriptorOnly: with no resolver wired the core returns a
// schema-less response; protojson omits the schema field entirely.
func TestGetTaskHTTP_DescriptorOnly(t *testing.T) {
	rd := &fakeReader{task: &ingressv1.GetTaskResponse{
		Service: "order", InstanceKey: "o-1", NodeId: "u", Name: "Approve",
	}}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/tasks/rpt_xyz", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec)
	if _, present := body["schema"]; present {
		t.Fatalf("schema must be omitted when absent: %q", rec.Body.String())
	}
	if body["nodeId"] != "u" {
		t.Fatalf("descriptor body = %v", body)
	}
}

// TestGetTaskHTTP_StaleToken: a stale/consumed token surfaces as FailedPrecondition
// from the core → 412 (the same liveness gate the completion path applies).
func TestGetTaskHTTP_StaleToken(t *testing.T) {
	rd := &fakeReader{taskErr: connect.NewError(connect.CodeFailedPrecondition, errors.New("not awaiting"))}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/tasks/rpt_xyz", nil, nil)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412 (%q)", rec.Code, rec.Body.String())
	}
}

// TestGetTaskHTTP_InvalidToken: a malformed token surfaces as InvalidArgument → 400.
func TestGetTaskHTTP_InvalidToken(t *testing.T) {
	rd := &fakeReader{taskErr: connect.NewError(connect.CodeInvalidArgument, errors.New("resume token"))}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/tasks/bogus", nil, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (%q)", rec.Code, rec.Body.String())
	}
}

func TestGetTaskHTTP_AuthzDeny(t *testing.T) {
	rd := &fakeReader{task: &ingressv1.GetTaskResponse{NodeId: "u"}}
	authz := &fakeAuthorizer{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: authz}),
		"GET", "/v1/tasks/rpt_xyz", nil, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if rd.called {
		t.Fatalf("reader must not run after authz denial")
	}
}

func TestGetProcessHistoryHTTP_InvalidCursor(t *testing.T) {
	rd := &fakeReader{present: true}
	rec := doReq(restMux(ingress.InvokeConfig{Reader: rd, Authorizer: &fakeAuthorizer{}}),
		"GET", "/v1/processes/order/o-1/history?after_seq=abc", nil, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if rd.called {
		t.Fatalf("reader must not run on a malformed cursor")
	}
}

// --- end-to-end through the real ingress listener ----------------------------

// TestREST_InvokeAwaitEcho_E2E proves the /v1/ routes, authn middleware, Cedar
// authorization, and the real Submit→Await path are wired end-to-end: a plain
// HTTP POST to /v1/Echo/echo blocks for the result and returns the echoed body.
func TestREST_InvokeAwaitEcho_E2E(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, rt, _ := bringUpHostWithIngress(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+rt.Addr()+"/v1/Echo/echo", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%q)", resp.StatusCode, string(body))
	}
	if string(body) != "echo:hello" {
		t.Fatalf("body = %q, want %q", string(body), "echo:hello")
	}
	if resp.Header.Get("X-Reflw-Invocation-Id") == "" {
		t.Fatalf("missing X-Reflw-Invocation-Id header")
	}
}
