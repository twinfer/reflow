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

	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
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

// --- helpers -----------------------------------------------------------------

func restMux(ic ingress.InvokeConfig) *http.ServeMux {
	m := http.NewServeMux()
	m.Handle("POST /v1/processes/{name}", ingress.StartProcessHTTP(ic, false))
	m.Handle("POST /v1/cases/{name}", ingress.StartProcessHTTP(ic, true))
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
