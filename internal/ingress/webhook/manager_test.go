package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// stubSubmitter captures the last SubmitInvocation request so tests
// can assert input + metadata flowed through correctly.
type stubSubmitter struct {
	mu        sync.Mutex
	gotReq    *ingressv1.SubmitInvocationRequest
	returnErr error
	idStr     string
}

func (s *stubSubmitter) SubmitInvocation(_ context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotReq = req.Msg
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	id := s.idStr
	if id == "" {
		id = "inv_test_id"
	}
	return connect.NewResponse(&ingressv1.SubmitInvocationResponse{
		InvocationIdStr: id,
		InvocationId:    &enginev1.InvocationId{Uuid: []byte("test")},
	}), nil
}

func (s *stubSubmitter) last() *ingressv1.SubmitInvocationRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gotReq
}

// signGitHubBody signs body with secret per GitHub's
// X-Hub-Signature-256 spec. Used here (not in factory_github_test.go's
// helper) to keep manager tests self-contained.
func signGitHubBody(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestManager_DispatchesVerifiedGitHubWebhook(t *testing.T) {
	secret := []byte("ghs_manager_test")
	sub := &stubSubmitter{idStr: "inv_xyz"}
	m, err := NewManager([]SourceConfig{{
		Path:     "/webhooks/github",
		Verifier: "github",
		Secret:   secret,
		Service:  "github-events",
		Handler:  "receive",
		Metadata: map[string]string{"tenant": "acme"},
	}}, sub, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	routes := m.Routes()
	if len(routes) != 1 {
		t.Fatalf("len(routes)=%d; want 1", len(routes))
	}
	if routes[0].Path != "/webhooks/" {
		t.Errorf("path=%q; want /webhooks/ (subtree catch-all)", routes[0].Path)
	}

	body := []byte(`{"action":"opened","number":7}`)
	r := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signGitHubBody(secret, body))
	r.Header.Set("X-GitHub-Event", "pull_request")
	r.Header.Set("X-GitHub-Delivery", "abc-123")
	w := httptest.NewRecorder()
	routes[0].Handler.ServeHTTP(w, r)

	if w.Result().StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d; want 202; body=%s", w.Result().StatusCode, w.Body.String())
	}
	if got := w.Body.String(); got != "inv_xyz" {
		t.Errorf("body=%q; want inv_xyz", got)
	}
	got := sub.last()
	if got == nil {
		t.Fatal("submitter never called")
	}
	if got.GetService() != "github-events" || got.GetHandler() != "receive" {
		t.Errorf("service/handler=%q/%q", got.GetService(), got.GetHandler())
	}
	if !bytes.Equal(got.GetInput(), body) {
		t.Errorf("input bytes mismatch")
	}
	// Static + verifier-stamped metadata must both be present.
	want := map[string]string{
		"tenant":          "acme",
		"webhook_vendor":  "github",
		"github_event":    "pull_request",
		"github_delivery": "abc-123",
	}
	for k, v := range want {
		if got.GetMetadata()[k] != v {
			t.Errorf("metadata[%q]=%q; want %q", k, got.GetMetadata()[k], v)
		}
	}
}

func TestManager_RejectsBadSignature(t *testing.T) {
	sub := &stubSubmitter{}
	m, err := NewManager([]SourceConfig{{
		Path:     "/webhooks/github",
		Verifier: "github",
		Secret:   []byte("right-secret"),
		Service:  "x",
		Handler:  "y",
	}}, sub, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	body := []byte(`{"a":1}`)
	r := httptest.NewRequest("POST", "/webhooks/github", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signGitHubBody([]byte("WRONG"), body))
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d; want 401", w.Result().StatusCode)
	}
	if sub.last() != nil {
		t.Error("submitter should not have been called")
	}
}

func TestManager_SubmitFailurePropagates(t *testing.T) {
	secret := []byte("s")
	sub := &stubSubmitter{returnErr: connect.NewError(connect.CodeUnavailable, errors.New("engine busy"))}
	m, err := NewManager([]SourceConfig{{
		Path: "/wh", Verifier: "github", Secret: secret, Service: "s", Handler: "h",
	}}, sub, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	body := []byte(`{}`)
	r := httptest.NewRequest("POST", "/wh", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signGitHubBody(secret, body))
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d; want 503 (CodeUnavailable)", w.Result().StatusCode)
	}
}

func TestManager_NonPOSTRejected(t *testing.T) {
	m, err := NewManager([]SourceConfig{{
		Path: "/wh", Verifier: "github", Secret: []byte("s"), Service: "s", Handler: "h",
	}}, &stubSubmitter{}, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	r := httptest.NewRequest("GET", "/wh", nil)
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Result().StatusCode)
	}
	if allow := w.Result().Header.Get("Allow"); allow != "POST" {
		t.Errorf("Allow=%q; want POST", allow)
	}
}

func TestManager_DuplicatePathRejected(t *testing.T) {
	_, err := NewManager([]SourceConfig{
		{Path: "/wh", Verifier: "github", Secret: []byte("s"), Service: "s", Handler: "h"},
		{Path: "/wh", Verifier: "stripe", Secret: []byte("s"), Service: "s", Handler: "h"},
	}, &stubSubmitter{}, nil)
	if err == nil {
		t.Fatal("expected duplicate-path error")
	}
}

func TestManager_UnknownVerifierRejected(t *testing.T) {
	_, err := NewManager([]SourceConfig{{
		Path: "/wh", Verifier: "nonexistent-vendor",
		Secret: []byte("s"), Service: "s", Handler: "h",
	}}, &stubSubmitter{}, nil)
	if err == nil {
		t.Fatal("expected unknown-verifier error")
	}
}

func TestManager_RequiredFieldsValidated(t *testing.T) {
	cases := map[string]SourceConfig{
		"empty-path":     {Path: "", Verifier: "github", Secret: []byte("s"), Service: "s", Handler: "h"},
		"relative-path":  {Path: "no-slash", Verifier: "github", Secret: []byte("s"), Service: "s", Handler: "h"},
		"empty-verifier": {Path: "/x", Verifier: "", Secret: []byte("s"), Service: "s", Handler: "h"},
		"empty-service":  {Path: "/x", Verifier: "github", Secret: []byte("s"), Service: "", Handler: "h"},
		"empty-handler":  {Path: "/x", Verifier: "github", Secret: []byte("s"), Service: "s", Handler: ""},
		"empty-secret":   {Path: "/x", Verifier: "github", Secret: nil, Service: "s", Handler: "h"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewManager([]SourceConfig{cfg}, &stubSubmitter{}, nil)
			if err == nil {
				t.Errorf("expected validation error for %s", name)
			}
		})
	}
}

func TestManager_EmptySourcesReturnsNoOp(t *testing.T) {
	m, err := NewManager(nil, nil, nil)
	if err != nil {
		t.Fatalf("NewManager(nil): %v", err)
	}
	// Empty-snapshot Manager still mounts the catch-all so operators
	// can wire ExtraRoutes unconditionally. Every request 404s.
	routes := m.Routes()
	if len(routes) != 1 || routes[0].Path != "/webhooks/" {
		t.Fatalf("routes=%v; want one /webhooks/ entry", routes)
	}
	r := httptest.NewRequest("POST", "/webhooks/anything", nil)
	w := httptest.NewRecorder()
	routes[0].Handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("status=%d; want 404 on empty snapshot", w.Result().StatusCode)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestManager_StripeIntegration end-to-end exercises the stripe
// verifier through the manager dispatch path with metadata merging.
func TestManager_StripeIntegration(t *testing.T) {
	secret := []byte("whsec_test")
	tsUnix := time.Now().Unix()
	body := []byte(`{"id":"evt_42","type":"charge.succeeded"}`)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(tsUnix, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	sig := fmt.Sprintf("t=%d,v1=%s", tsUnix, hex.EncodeToString(mac.Sum(nil)))

	sub := &stubSubmitter{}
	m, err := NewManager([]SourceConfig{{
		Path:     "/webhooks/stripe",
		Verifier: "stripe",
		Secret:   secret,
		Service:  "stripe-events",
		Handler:  "receive",
		Metadata: map[string]string{"environment": "prod"},
	}}, sub, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	r := httptest.NewRequest("POST", "/webhooks/stripe", bytes.NewReader(body))
	r.Header.Set("Stripe-Signature", sig)
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)

	if w.Result().StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d; body=%s", w.Result().StatusCode, w.Body.String())
	}
	got := sub.last()
	if got == nil {
		t.Fatal("submitter not called")
	}
	if !bytes.Equal(got.GetInput(), body) {
		t.Error("input bytes don't match verified body")
	}
	if got.GetMetadata()["webhook_vendor"] != "stripe" {
		t.Errorf("vendor=%q", got.GetMetadata()["webhook_vendor"])
	}
	if got.GetMetadata()["environment"] != "prod" {
		t.Errorf("static metadata not merged: %v", got.GetMetadata())
	}
	if got.GetMetadata()["stripe_signed_timestamp"] != strconv.FormatInt(tsUnix, 10) {
		t.Errorf("stripe_signed_timestamp=%q", got.GetMetadata()["stripe_signed_timestamp"])
	}
}

// Verify the manager satisfies the Submitter contract — guards
// against accidental signature changes.
var _ Submitter = (*stubSubmitter)(nil)

// silence unused import for io when verifier tests are stripped.
var _ = io.Discard
