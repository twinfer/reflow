package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	connect "connectrpc.com/connect"
)

// stripeFixture is the standard request shape stripeVerifier expects:
// signed payload is `t` + "." + body. Tests adjust individual fields
// to exercise verifier paths.
type stripeFixture struct {
	secret []byte
	body   []byte
	tsUnix int64
	now    time.Time
}

func defaultStripeFixture() stripeFixture {
	now := time.Unix(1_700_000_000, 0)
	return stripeFixture{
		secret: []byte("whsec_test_secret"),
		body:   []byte(`{"id":"evt_1","type":"payment_intent.succeeded"}`),
		tsUnix: now.Unix(),
		now:    now,
	}
}

func (f stripeFixture) sign(t *testing.T) string {
	t.Helper()
	mac := hmac.New(sha256.New, f.secret)
	mac.Write([]byte(strconv.FormatInt(f.tsUnix, 10)))
	mac.Write([]byte("."))
	mac.Write(f.body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (f stripeFixture) request(t *testing.T, sigHeader string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/webhooks/stripe", bytes.NewReader(f.body))
	r.Header.Set("Stripe-Signature", sigHeader)
	return r
}

func newTestStripeVerifier(now time.Time) *stripeVerifier {
	v := NewStripeVerifier(0).(*stripeVerifier)
	v.now = func() time.Time { return now }
	return v
}

func TestStripeVerifier_ValidSignature(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	ev, err := v.Verify(context.Background(), r, f.secret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !bytes.Equal(ev.Body, f.body) {
		t.Errorf("Body bytes mismatch")
	}
	if ev.Metadata["webhook_vendor"] != "stripe" {
		t.Errorf("vendor=%q; want stripe", ev.Metadata["webhook_vendor"])
	}
	if ev.Metadata["stripe_signed_timestamp"] != strconv.FormatInt(f.tsUnix, 10) {
		t.Errorf("signed_timestamp=%q; want %d", ev.Metadata["stripe_signed_timestamp"], f.tsUnix)
	}
}

func TestStripeVerifier_TamperedBody(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	// Replace body with different bytes after signing.
	tampered := defaultStripeFixture()
	tampered.body = []byte(`{"id":"evt_1","type":"payment_intent.payment_failed"}`)
	r := tampered.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error on tampered body")
	}
}

func TestStripeVerifier_WrongSecret(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, []byte("wrong-secret")); err == nil {
		t.Fatal("expected error on wrong secret")
	}
}

func TestStripeVerifier_ExpiredTimestamp(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	r := f.request(t, header)
	// "now" is 1 hour ahead of the signed timestamp; default tolerance 5min.
	v := newTestStripeVerifier(f.now.Add(time.Hour))
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error on expired timestamp")
	}
}

func TestStripeVerifier_FutureTimestamp(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	r := f.request(t, header)
	// "now" is 1 hour behind — also out of tolerance.
	v := newTestStripeVerifier(f.now.Add(-time.Hour))
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error on future timestamp")
	}
}

func TestStripeVerifier_MissingHeader(t *testing.T) {
	f := defaultStripeFixture()
	r := f.request(t, "")
	r.Header.Del("Stripe-Signature")
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error on missing header")
	}
}

func TestStripeVerifier_EmptySecret(t *testing.T) {
	f := defaultStripeFixture()
	sig := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, sig)
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, nil); err == nil {
		t.Fatal("expected error on empty secret")
	}
}

func TestStripeVerifier_MultipleV1SignaturesAnyMatches(t *testing.T) {
	f := defaultStripeFixture()
	// Real Stripe rotation: two valid v1 signatures (one with the
	// current secret, one with an old secret). We provide the
	// correct one as the second entry to confirm OR-matching.
	correct := f.sign(t)
	header := fmt.Sprintf("t=%d,v1=%s,v1=%s", f.tsUnix, "deadbeef00", correct)
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, f.secret); err != nil {
		t.Errorf("Verify with rotated-pair header: %v", err)
	}
}

func TestStripeVerifier_V0Ignored(t *testing.T) {
	f := defaultStripeFixture()
	// v0 is the deprecated pre-2019 scheme. A header with only v0
	// must be rejected (no v1 signatures means nothing to verify).
	header := fmt.Sprintf("t=%d,v0=deadbeef", f.tsUnix)
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error when only v0 signatures present")
	}
}

func TestStripeVerifier_MalformedTimestamp(t *testing.T) {
	f := defaultStripeFixture()
	header := fmt.Sprintf("t=not-a-number,v1=%s", f.sign(t))
	r := f.request(t, header)
	v := newTestStripeVerifier(f.now)
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected error on malformed timestamp")
	}
}

func TestStripeVerifier_ReturnsConnectCodeUnauthenticated(t *testing.T) {
	f := defaultStripeFixture()
	r := f.request(t, "")
	r.Header.Del("Stripe-Signature")
	v := newTestStripeVerifier(f.now)
	_, err := v.Verify(context.Background(), r, f.secret)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("CodeOf=%v; want CodeUnauthenticated", connect.CodeOf(err))
	}
}

// TestStripeVerifier_OversizedBodyRejected confirms the MaxBytesReader
// kill switch trips when a malicious client sends an unreasonably
// large body.
func TestStripeVerifier_OversizedBodyRejected(t *testing.T) {
	f := defaultStripeFixture()
	// 2 MiB > default 1 MiB cap.
	huge := make([]byte, 2<<20)
	r := httptest.NewRequest("POST", "/v1/webhooks/stripe", io.NopCloser(bytes.NewReader(huge)))
	// Manually compute the right signature for "huge" so the only
	// rejection reason is body-size, not signature mismatch — keeps
	// the test self-contained even if size cap moves.
	mac := hmac.New(sha256.New, f.secret)
	mac.Write([]byte(strconv.FormatInt(f.tsUnix, 10)))
	mac.Write([]byte("."))
	mac.Write(huge)
	header := fmt.Sprintf("t=%d,v1=%s", f.tsUnix, hex.EncodeToString(mac.Sum(nil)))
	r.Header.Set("Stripe-Signature", header)
	v := newTestStripeVerifier(f.now)
	_, err := v.Verify(context.Background(), r, f.secret)
	if err == nil {
		t.Fatal("expected oversized-body rejection")
	}
	if !strings.Contains(err.Error(), "body read") && !strings.Contains(err.Error(), "signature") {
		t.Logf("err=%v (acceptable as long as it's a hard reject)", err)
	}
}
