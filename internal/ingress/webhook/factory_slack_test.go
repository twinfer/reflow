package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	connect "connectrpc.com/connect"
)

type slackFixture struct {
	secret []byte
	body   []byte
	tsUnix int64
	now    time.Time
}

func defaultSlackFixture() slackFixture {
	now := time.Unix(1_700_000_000, 0)
	return slackFixture{
		secret: []byte("slack-signing-secret"),
		body:   []byte("token=xyz&team_id=T123&command=/test&text=hello"),
		tsUnix: now.Unix(),
		now:    now,
	}
}

func (f slackFixture) sign(t *testing.T) string {
	t.Helper()
	mac := hmac.New(sha256.New, f.secret)
	mac.Write([]byte("v0:"))
	mac.Write([]byte(strconv.FormatInt(f.tsUnix, 10)))
	mac.Write([]byte(":"))
	mac.Write(f.body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func (f slackFixture) request(t *testing.T, sig, ts string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/webhooks/slack", bytes.NewReader(f.body))
	if sig != "" {
		r.Header.Set("X-Slack-Signature", sig)
	}
	if ts != "" {
		r.Header.Set("X-Slack-Request-Timestamp", ts)
	}
	return r
}

func newTestSlackVerifier(now time.Time) *slackVerifier {
	v := NewSlackVerifier(0).(*slackVerifier)
	v.now = func() time.Time { return now }
	return v
}

func TestSlackVerifier_ValidSignature(t *testing.T) {
	f := defaultSlackFixture()
	r := f.request(t, f.sign(t), strconv.FormatInt(f.tsUnix, 10))
	ev, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !bytes.Equal(ev.Body, f.body) {
		t.Errorf("body mismatch")
	}
	if ev.Metadata["webhook_vendor"] != "slack" {
		t.Errorf("vendor=%q", ev.Metadata["webhook_vendor"])
	}
	if ev.Metadata["slack_signed_timestamp"] != strconv.FormatInt(f.tsUnix, 10) {
		t.Errorf("slack_signed_timestamp=%q", ev.Metadata["slack_signed_timestamp"])
	}
}

func TestSlackVerifier_TamperedBody(t *testing.T) {
	f := defaultSlackFixture()
	sig := f.sign(t)
	// Tamper after signing.
	tampered := defaultSlackFixture()
	tampered.body = []byte("token=xyz&team_id=T999&command=/test&text=evil")
	r := tampered.request(t, sig, strconv.FormatInt(f.tsUnix, 10))
	if _, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected tampering rejection")
	}
}

func TestSlackVerifier_ExpiredTimestamp(t *testing.T) {
	f := defaultSlackFixture()
	r := f.request(t, f.sign(t), strconv.FormatInt(f.tsUnix, 10))
	v := newTestSlackVerifier(f.now.Add(time.Hour)) // 1h in future, 5min tolerance
	if _, err := v.Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected expired-timestamp rejection")
	}
}

func TestSlackVerifier_MissingSignatureHeader(t *testing.T) {
	f := defaultSlackFixture()
	r := f.request(t, "", strconv.FormatInt(f.tsUnix, 10))
	if _, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected missing-signature rejection")
	}
}

func TestSlackVerifier_MissingTimestampHeader(t *testing.T) {
	f := defaultSlackFixture()
	r := f.request(t, f.sign(t), "")
	if _, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected missing-timestamp rejection")
	}
}

func TestSlackVerifier_MissingV0Prefix(t *testing.T) {
	f := defaultSlackFixture()
	// Sign correctly but strip v0= prefix.
	full := f.sign(t)
	stripped := full[len("v0="):]
	r := f.request(t, stripped, strconv.FormatInt(f.tsUnix, 10))
	if _, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret); err == nil {
		t.Fatal("expected missing-prefix rejection")
	}
}

func TestSlackVerifier_ConnectCodeUnauthenticated(t *testing.T) {
	f := defaultSlackFixture()
	r := f.request(t, "", "")
	_, err := newTestSlackVerifier(f.now).Verify(context.Background(), r, f.secret)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("code=%v; want CodeUnauthenticated", connect.CodeOf(err))
	}
}
