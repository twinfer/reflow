package reflow

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// fakeSubmitter records the SubmitInvocation requests the webhook
// adapter builds and returns a canned response (or err, when set).
type fakeSubmitter struct {
	reqs []*ingressv1.SubmitInvocationRequest
	err  error
}

func (f *fakeSubmitter) SubmitInvocation(_ context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error) {
	f.reqs = append(f.reqs, req.Msg)
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&ingressv1.SubmitInvocationResponse{InvocationIdStr: "inv-test-1"}), nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testStripeWebhook() WebhookConfig {
	return WebhookConfig{
		Provider:   "stripe",
		Path:       "/webhooks/stripe",
		SecretName: "stripe-signing",
		Service:    "WebhookRouter",
		Handler:    "OnStripeEvent",
	}
}

// fakeSecrets is a secretLookuper returning a fixed secret (ok=true) or a
// miss (ok=false).
type fakeSecrets struct {
	secret []byte
	ok     bool
}

func (f fakeSecrets) Lookup(string) ([]byte, bool) { return f.secret, f.ok }

// stripeSignedRequest builds a POST carrying a valid Stripe-Signature
// for body under secret, timestamped at ts (use time.Now for the
// registered verifier's 5-minute tolerance).
func stripeSignedRequest(t *testing.T, path string, secret, body []byte, ts int64) *http.Request {
	t.Helper()
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", ts, sig))
	return r
}

func TestWebhookHandler_Success(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"id":"evt_1","type":"invoice.payment_succeeded","data":{"object":{"id":"cus_42"}}}`)
	fake := &fakeSubmitter{}
	h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{secret: secret, ok: true}, discardLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, stripeSignedRequest(t, "/webhooks/stripe", secret, body, time.Now().Unix()))

	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s; want 202", w.Code, w.Body.String())
	}
	if len(fake.reqs) != 1 {
		t.Fatalf("submitter calls=%d; want 1", len(fake.reqs))
	}
	got := fake.reqs[0]
	if got.GetService() != "WebhookRouter" || got.GetHandler() != "OnStripeEvent" {
		t.Errorf("target=%s/%s; want WebhookRouter/OnStripeEvent", got.GetService(), got.GetHandler())
	}
	if !bytes.Equal(got.GetInput(), body) {
		t.Errorf("input mismatch: got %s", got.GetInput())
	}
	if got.GetIdempotencyKey() != "evt_1" {
		t.Errorf("idempotency_key=%q; want evt_1 (Stripe event id)", got.GetIdempotencyKey())
	}
	if got.GetMetadata()["webhook_vendor"] != "stripe" {
		t.Errorf("metadata[webhook_vendor]=%q; want stripe", got.GetMetadata()["webhook_vendor"])
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["invocation_id"] == "" {
		t.Errorf("response missing invocation_id: %s", w.Body.String())
	}
}

func TestWebhookHandler_BadSignature(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"id":"evt_1"}`)
	fake := &fakeSubmitter{}
	h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{secret: secret, ok: true}, discardLogger())

	// Sign with the wrong secret.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, stripeSignedRequest(t, "/webhooks/stripe", []byte("wrong-secret"), body, time.Now().Unix()))

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d; want 401", w.Code)
	}
	if len(fake.reqs) != 0 {
		t.Errorf("submitter called %d times on bad signature; want 0", len(fake.reqs))
	}
}

func TestWebhookHandler_MissingSecret(t *testing.T) {
	body := []byte(`{"id":"evt_1"}`)
	fake := &fakeSubmitter{}
	h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{ok: false}, discardLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, stripeSignedRequest(t, "/webhooks/stripe", []byte("whsec"), body, time.Now().Unix()))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d; want 503", w.Code)
	}
	if len(fake.reqs) != 0 {
		t.Errorf("submitter called on missing secret")
	}
}

func TestWebhookHandler_NonPOST(t *testing.T) {
	fake := &fakeSubmitter{}
	h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{secret: []byte("x"), ok: true}, discardLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/webhooks/stripe", nil))

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d; want 405", w.Code)
	}
	if len(fake.reqs) != 0 {
		t.Errorf("submitter called on GET")
	}
}

func TestWebhookHandler_SubmitErrorStatus(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"id":"evt_1","type":"x"}`)
	cases := []struct {
		code connect.Code
		want int
	}{
		{connect.CodeUnavailable, http.StatusServiceUnavailable},
		{connect.CodeInvalidArgument, http.StatusBadRequest},
		{connect.CodeInternal, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		fake := &fakeSubmitter{err: connect.NewError(tc.code, fmt.Errorf("boom"))}
		h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{secret: secret, ok: true}, discardLogger())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, stripeSignedRequest(t, "/webhooks/stripe", secret, body, time.Now().Unix()))
		if w.Code != tc.want {
			t.Errorf("submit code=%s → status=%d; want %d", tc.code, w.Code, tc.want)
		}
	}
}

// TestWebhookHandler_ForwardsIdempotencyKey confirms the adapter stamps
// the same event id on every delivery of the same event — the engine
// then dedups on it (engine-side dedup is covered by ingress tests).
func TestWebhookHandler_ForwardsIdempotencyKey(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"id":"evt_dup","type":"invoice.payment_succeeded","data":{"object":{"id":"cus_1"}}}`)
	fake := &fakeSubmitter{}
	h := webhookHandler(testStripeWebhook(), fake, fakeSecrets{secret: secret, ok: true}, discardLogger())

	for i := range 2 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, stripeSignedRequest(t, "/webhooks/stripe", secret, body, time.Now().Unix()))
		if w.Code != http.StatusAccepted {
			t.Fatalf("delivery %d status=%d; want 202", i, w.Code)
		}
	}
	if len(fake.reqs) != 2 {
		t.Fatalf("submitter calls=%d; want 2", len(fake.reqs))
	}
	if k0, k1 := fake.reqs[0].GetIdempotencyKey(), fake.reqs[1].GetIdempotencyKey(); k0 != "evt_dup" || k1 != "evt_dup" {
		t.Errorf("idempotency keys=%q,%q; want both evt_dup", k0, k1)
	}
}

func TestValidateWebhooks(t *testing.T) {
	base := func() WebhookConfig {
		return WebhookConfig{Provider: "stripe", Path: "/webhooks/stripe", SecretName: "s", Service: "S", Handler: "H"}
	}
	if err := validateWebhooks([]WebhookConfig{base()}); err != nil {
		t.Errorf("good config rejected: %v", err)
	}

	mut := func(f func(*WebhookConfig)) []WebhookConfig {
		w := base()
		f(&w)
		return []WebhookConfig{w}
	}
	bad := map[string][]WebhookConfig{
		"unknown provider": mut(func(w *WebhookConfig) { w.Provider = "nope" }),
		"empty provider":   mut(func(w *WebhookConfig) { w.Provider = "" }),
		"no leading slash": mut(func(w *WebhookConfig) { w.Path = "webhooks/x" }),
		"ingress prefix":   mut(func(w *WebhookConfig) { w.Path = ingressRPCPrefix + "x" }),
		"empty secret":     mut(func(w *WebhookConfig) { w.SecretName = "" }),
		"empty service":    mut(func(w *WebhookConfig) { w.Service = "" }),
		"empty handler":    mut(func(w *WebhookConfig) { w.Handler = "" }),
		"duplicate paths":  {base(), base()},
	}
	for name, cfg := range bad {
		if err := validateWebhooks(cfg); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestWebhookHTTPStatus(t *testing.T) {
	cases := map[connect.Code]int{
		connect.CodeInvalidArgument:  http.StatusBadRequest,
		connect.CodeUnavailable:      http.StatusServiceUnavailable,
		connect.CodeUnauthenticated:  http.StatusForbidden,
		connect.CodePermissionDenied: http.StatusForbidden,
		connect.CodeInternal:         http.StatusInternalServerError,
		connect.CodeNotFound:         http.StatusInternalServerError,
	}
	for code, want := range cases {
		if got := webhookHTTPStatus(code); got != want {
			t.Errorf("webhookHTTPStatus(%s)=%d; want %d", code, got, want)
		}
	}
}
