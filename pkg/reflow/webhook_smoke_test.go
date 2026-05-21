package reflow_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/twinfer/reflow/pkg/reflow"
)

// TestRun_WebhookSigVerifyAndDispatch wires a real reflow node with a
// GitHub webhook source, then posts a signed request. The end-to-end
// flow exercised here:
//
//   - cfg.Webhooks.Sources flows through pkg/reflow.buildWebhookSources
//     into internal/ingress/webhook.NewManager.
//   - Manager mounts the route on the existing ingress listener via
//     ingress.Config.ExtraRoutes.
//   - The HTTP POST hits the manager's handler, GitHub verifier
//     re-derives the HMAC, and the manager attempts SubmitInvocation
//     on the in-process *ingress.Server.
//
// The SubmitInvocation will fail with CodeFailedPrecondition (no
// handler deployment is registered in this single-node test), which
// the manager surfaces as HTTP 400 — confirming the signature
// passed and the dispatch happened.
func TestRun_WebhookSigVerifyAndDispatch(t *testing.T) {
	raftAddr := freeAddr(t)
	ingressAddr := freeAddr(t)
	secret := "ghs_smoke_test_secret"
	t.Setenv("REFLOW_TEST_GITHUB_SECRET", secret)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: raftAddr},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflow.IngressConfig{Addr: ingressAddr},
		Metrics: reflow.MetricsConfig{Disabled: true},
		Webhooks: reflow.WebhooksConfig{
			Sources: []reflow.WebhookSource{{
				Name:      "github-smoke",
				Path:      "/webhooks/github",
				Verifier:  "github",
				SecretEnv: "REFLOW_TEST_GITHUB_SECRET",
				Invocation: reflow.WebhookInvocation{
					Service:  "github-events",
					Handler:  "receive",
					Metadata: map[string]string{"tenant": "smoke"},
				},
			}},
		},
	}
	ctx := t.Context()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflow.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	awaitCtx, awaitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer awaitCancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	body := []byte(`{"action":"opened","pull_request":{"number":1}}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	url := "http://" + ingressAddr + "/webhooks/github"

	// Cluster-managed config: the seed loop proposes the source, the
	// reconciler picks it up on the next notifier wake. Both run in
	// background goroutines after Run returns. Poll until the route
	// is live (not 404) before dispatching the subtests.
	probeDeadline := time.Now().Add(8 * time.Second)
	for {
		probeReq, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		probeReq.Header.Set("X-Hub-Signature-256", sig)
		probeReq.Header.Set("X-GitHub-Event", "ping")
		probeReq.Header.Set("X-GitHub-Delivery", "probe")
		resp, err := http.DefaultClient.Do(probeReq)
		if err == nil && resp.StatusCode != http.StatusNotFound {
			_ = resp.Body.Close()
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(probeDeadline) {
			t.Fatal("webhook route never became live (seed loop or reconciler stuck)")
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Run("valid-signature-dispatches", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Hub-Signature-256", sig)
		req.Header.Set("X-GitHub-Event", "pull_request")
		req.Header.Set("X-GitHub-Delivery", "test-delivery-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		// Signature verifies → manager submits → no handler
		// registered → CodeFailedPrecondition → HTTP 400.
		// (We can't get a 202 here without registering a handler
		// deployment, which is heavier than this smoke needs.)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status=%d body=%q; want 400 (verifier passed, no handler registered)", resp.StatusCode, string(respBody))
		}
	})

	t.Run("bad-signature-rejected", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		// Sign with the wrong secret to force a mismatch.
		badMAC := hmac.New(sha256.New, []byte("wrong-secret"))
		badMAC.Write(body)
		req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(badMAC.Sum(nil)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status=%d; want 401", resp.StatusCode)
		}
	})

	t.Run("method-not-post", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status=%d; want 405", resp.StatusCode)
		}
	})
}

// TestRun_WebhookConfigValidatedBeforeListen exercises the pre-flight
// validation: an invalid webhook config aborts Run before binding the
// listener.
func TestRun_WebhookConfigValidatedBeforeListen(t *testing.T) {
	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflow.IngressConfig{Addr: freeAddr(t)},
		Metrics: reflow.MetricsConfig{Disabled: true},
		Webhooks: reflow.WebhooksConfig{
			Sources: []reflow.WebhookSource{{
				Path:      "/webhooks/typo",
				Verifier:  "stripee", // typo
				SecretEnv: "REFLOW_TEST_FAKE_SECRET",
				Invocation: reflow.WebhookInvocation{
					Service: "s", Handler: "h",
				},
			}},
		},
	}
	_, err := reflow.Run(t.Context(), cfg)
	if err == nil {
		t.Fatal("expected Run to reject typo'd verifier")
	}
}
