package webhook_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/internal/ingress/webhook"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// recSubmitter counts SubmitInvocation calls.
type recSubmitter struct {
	mu      sync.Mutex
	calls   int
	lastReq *ingressv1.SubmitInvocationRequest
	err     error
}

func (r *recSubmitter) SubmitInvocation(_ context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastReq = req.Msg
	if r.err != nil {
		return nil, r.err
	}
	return connect.NewResponse(&ingressv1.SubmitInvocationResponse{InvocationIdStr: "inv_x"}), nil
}

func (r *recSubmitter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func signGitHub(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func newMgr(t *testing.T, sub webhook.Submitter) *webhook.Manager {
	t.Helper()
	// SecretStore Resolver is nil here: every Reconcile call below
	// supplies SourceConfig.Secret directly, so the Manager never
	// needs to Lookup. The production code path goes through
	// reconcileFromReader which does call secrets.Lookup; that path
	// is covered by secret store + integration tests.
	m, err := webhook.New(sub, nil, prometheus.NewRegistry(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func postSigned(t *testing.T, m *webhook.Manager, path string, secret, body []byte) *http.Response {
	t.Helper()
	r := httptest.NewRequest("POST", path, bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signGitHub(secret, body))
	r.Header.Set("X-GitHub-Event", "ping")
	r.Header.Set("X-GitHub-Delivery", "d-1")
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)
	return w.Result()
}

func TestReconcile_AddRemoveChange(t *testing.T) {
	t.Parallel()
	sub := &recSubmitter{}
	m := newMgr(t, sub)
	secret := []byte("s1")

	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{{
		Name: "a", Path: "/webhooks/a", Verifier: "github", Secret: secret,
		Service: "svc", Handler: "on",
	}}); err != nil {
		t.Fatal(err)
	}
	if resp := postSigned(t, m, "/webhooks/a", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("a accepted: status=%d", resp.StatusCode)
	}

	// Add a second source; the first stays live.
	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{
		{Name: "a", Path: "/webhooks/a", Verifier: "github", Secret: secret, Service: "svc", Handler: "on"},
		{Name: "b", Path: "/webhooks/b", Verifier: "github", Secret: secret, Service: "svc", Handler: "on"},
	}); err != nil {
		t.Fatal(err)
	}
	if resp := postSigned(t, m, "/webhooks/a", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("a still works: status=%d", resp.StatusCode)
	}
	if resp := postSigned(t, m, "/webhooks/b", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("b accepted: status=%d", resp.StatusCode)
	}

	// Remove a; only b remains. POST to a → 404.
	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{
		{Name: "b", Path: "/webhooks/b", Verifier: "github", Secret: secret, Service: "svc", Handler: "on"},
	}); err != nil {
		t.Fatal(err)
	}
	if resp := postSigned(t, m, "/webhooks/a", secret, []byte(`{}`)); resp.StatusCode != http.StatusNotFound {
		t.Errorf("a removed: status=%d; want 404", resp.StatusCode)
	}
	if resp := postSigned(t, m, "/webhooks/b", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Errorf("b still works: status=%d", resp.StatusCode)
	}
}

func TestReconcile_DuplicatePathDedup(t *testing.T) {
	t.Parallel()
	sub := &recSubmitter{}
	m := newMgr(t, sub)
	secret := []byte("dup")
	// Two records claim "/webhooks/dup"; "alpha" wins (sorted by name).
	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{
		{Name: "beta", Path: "/webhooks/dup", Verifier: "github", Secret: secret, Service: "loser", Handler: "on"},
		{Name: "alpha", Path: "/webhooks/dup", Verifier: "github", Secret: secret, Service: "winner", Handler: "on"},
	}); err != nil {
		t.Fatal(err)
	}
	if resp := postSigned(t, m, "/webhooks/dup", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("dispatch: status=%d", resp.StatusCode)
	}
	if got := sub.lastReq.GetService(); got != "winner" {
		t.Fatalf("winning service=%q; want winner (alpha sorts before beta)", got)
	}
}

func TestReconcile_AtomicSnapshotSoak(t *testing.T) {
	t.Parallel()
	sub := &recSubmitter{}
	m := newMgr(t, sub)
	secret := []byte("soak")
	base := webhook.SourceConfig{
		Name: "n", Path: "/webhooks/soak", Verifier: "github", Secret: secret,
		Service: "svc", Handler: "on",
	}
	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{base}); err != nil {
		t.Fatal(err)
	}
	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)
	// 8 dispatcher goroutines hammer the live snapshot.
	for range 8 {
		wg.Go(func() {
			for !stop.Load() {
				resp := postSigned(t, m, "/webhooks/soak", secret, []byte(`{}`))
				if resp.StatusCode != http.StatusAccepted &&
					resp.StatusCode != http.StatusNotFound {
					// 404 is acceptable across a swap that briefly empties
					// snapshot; anything else is a data race.
					t.Errorf("unexpected status %d", resp.StatusCode)
				}
			}
		})
	}
	// 50 swaps in parallel; each rebuilds the snapshot from scratch.
	for range 50 {
		wg.Go(func() {
			_ = m.Reconcile(context.Background(), []webhook.SourceConfig{base})
		})
	}
	time.Sleep(150 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if sub.count() == 0 {
		t.Fatal("expected at least one accepted POST during soak")
	}
}

// fakeReader serves a pre-set list of records.
type fakeReader struct {
	mu      sync.Mutex
	records []*enginev1.WebhookSourceRecord
	rev     uint64
	err     error
}

func (r *fakeReader) ListWebhookSources(_ context.Context) ([]*enginev1.WebhookSourceRecord, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, 0, r.err
	}
	out := make([]*enginev1.WebhookSourceRecord, len(r.records))
	copy(out, r.records)
	return out, r.rev, nil
}

func (r *fakeReader) set(rev uint64, records ...*enginev1.WebhookSourceRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rev = rev
	r.records = records
}

// Secret rotation + preserve-prev-on-error coverage lives in
// secret_remote_encrypted_test.go — same shape, against the only
// supported SecretRef variant (remote_encrypted).

func TestRunReconciler_BadFactoryDoesNotKillSiblings(t *testing.T) {
	t.Parallel()
	sub := &recSubmitter{}
	m := newMgr(t, sub)
	// "bad" references an unregistered verifier; "good" should still
	// come up on the same reconcile pass.
	if err := m.Reconcile(context.Background(), []webhook.SourceConfig{
		{Name: "bad", Path: "/webhooks/bad", Verifier: "nonexistent-vendor", Secret: []byte("s"), Service: "svc", Handler: "on"},
		{Name: "good", Path: "/webhooks/good", Verifier: "github", Secret: []byte("g"), Service: "svc", Handler: "on"},
	}); err != nil {
		t.Fatalf("Reconcile returned error; per-source failures should be absorbed: %v", err)
	}
	if resp := postSigned(t, m, "/webhooks/good", []byte("g"), []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Errorf("good: status=%d", resp.StatusCode)
	}
	if resp := postSigned(t, m, "/webhooks/bad", []byte("s"), []byte(`{}`)); resp.StatusCode != http.StatusNotFound {
		t.Errorf("bad path should 404; status=%d", resp.StatusCode)
	}
}

func TestServe_UnknownPath404s(t *testing.T) {
	t.Parallel()
	sub := &recSubmitter{}
	m := newMgr(t, sub)
	r := httptest.NewRequest("POST", "/webhooks/unknown", bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()
	m.Routes()[0].Handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("unknown path: status=%d", w.Result().StatusCode)
	}
	if sub.count() != 0 {
		t.Errorf("submitter should not be called for unknown path")
	}
}

// waitForCount retries posting until the count crosses threshold or the
// timeout elapses. Used to wait for the reconcile loop's initial pass.
func waitForCount(t *testing.T, sub *recSubmitter, m *webhook.Manager, path string, secret []byte, threshold int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		before := sub.count()
		resp := postSigned(t, m, path, secret, []byte(`{}`))
		if resp.StatusCode == http.StatusAccepted && sub.count() >= before+1 && sub.count() >= threshold {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// waitForReject retries until the verifier rejects the post.
func waitForReject(t *testing.T, m *webhook.Manager, path string, secret []byte, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp := postSigned(t, m, path, secret, []byte(`{}`))
		if resp.StatusCode == http.StatusUnauthorized {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// Ensure the package-level interfaces compile.
var (
	_ webhook.Submitter = (*recSubmitter)(nil)
	_ webhook.Reader    = (*fakeReader)(nil)
	_                   = errors.New
)
