package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	connect "connectrpc.com/connect"
)

func signGitHub(t *testing.T, secret, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func newGitHubRequest(t *testing.T, body []byte, sig, event, delivery string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/webhooks/github", bytes.NewReader(body))
	if sig != "" {
		r.Header.Set("X-Hub-Signature-256", sig)
	}
	if event != "" {
		r.Header.Set("X-GitHub-Event", event)
	}
	if delivery != "" {
		r.Header.Set("X-GitHub-Delivery", delivery)
	}
	return r
}

func TestGitHubVerifier_ValidSignature(t *testing.T) {
	secret := []byte("ghs_test")
	body := []byte(`{"action":"opened"}`)
	sig := signGitHub(t, secret, body)
	r := newGitHubRequest(t, body, sig, "pull_request", "d-uuid-123")
	v := NewGitHubVerifier()
	ev, err := v.Verify(context.Background(), r, secret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !bytes.Equal(ev.Body, body) {
		t.Errorf("body mismatch")
	}
	if ev.Metadata["webhook_vendor"] != "github" {
		t.Errorf("vendor=%q", ev.Metadata["webhook_vendor"])
	}
	if ev.Metadata["github_event"] != "pull_request" {
		t.Errorf("github_event=%q", ev.Metadata["github_event"])
	}
	if ev.Metadata["github_delivery"] != "d-uuid-123" {
		t.Errorf("github_delivery=%q", ev.Metadata["github_delivery"])
	}
}

func TestGitHubVerifier_WrongSecret(t *testing.T) {
	secret := []byte("right")
	body := []byte(`{"x":1}`)
	r := newGitHubRequest(t, body, signGitHub(t, secret, body), "push", "x")
	v := NewGitHubVerifier()
	if _, err := v.Verify(context.Background(), r, []byte("wrong")); err == nil {
		t.Fatal("expected mismatch")
	}
}

func TestGitHubVerifier_TamperedBody(t *testing.T) {
	secret := []byte("s")
	r := newGitHubRequest(t, []byte("tampered"), signGitHub(t, secret, []byte("original")), "", "")
	v := NewGitHubVerifier()
	if _, err := v.Verify(context.Background(), r, secret); err == nil {
		t.Fatal("expected tampering rejection")
	}
}

func TestGitHubVerifier_MissingHeader(t *testing.T) {
	r := newGitHubRequest(t, []byte("x"), "", "", "")
	v := NewGitHubVerifier()
	if _, err := v.Verify(context.Background(), r, []byte("s")); err == nil {
		t.Fatal("expected missing-header rejection")
	}
}

func TestGitHubVerifier_MissingPrefix(t *testing.T) {
	secret := []byte("s")
	body := []byte("x")
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	// Send hex without the "sha256=" prefix.
	r := newGitHubRequest(t, body, hex.EncodeToString(mac.Sum(nil)), "", "")
	v := NewGitHubVerifier()
	if _, err := v.Verify(context.Background(), r, secret); err == nil {
		t.Fatal("expected prefix rejection")
	}
}

func TestGitHubVerifier_MalformedHex(t *testing.T) {
	r := newGitHubRequest(t, []byte("x"), "sha256=not-hex", "", "")
	v := NewGitHubVerifier()
	if _, err := v.Verify(context.Background(), r, []byte("s")); err == nil {
		t.Fatal("expected malformed-hex rejection")
	}
}

func TestGitHubVerifier_ConnectCodeUnauthenticated(t *testing.T) {
	r := newGitHubRequest(t, []byte("x"), "", "", "")
	v := NewGitHubVerifier()
	_, err := v.Verify(context.Background(), r, []byte("s"))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("code=%v; want CodeUnauthenticated", connect.CodeOf(err))
	}
}
