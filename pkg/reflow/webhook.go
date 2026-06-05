package reflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/webhook"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// ingressRPCPrefix is the path the Connect ingress handler mounts on.
// Webhook routes must not collide with it.
const ingressRPCPrefix = "/reflow.ingress.v1.Ingress/"

// webhookSubmitTimeout bounds the durable submit independent of the
// vendor's connection: a sender disconnect must not abort an accepted
// submission.
const webhookSubmitTimeout = 15 * time.Second

// invocationSubmitter is the slice of *ingress.Server the webhook adapter
// uses — just SubmitInvocation. Narrowing to an interface lets the
// adapter be unit-tested with a fake submitter, no engine host required.
type invocationSubmitter interface {
	SubmitInvocation(ctx context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error)
}

var _ invocationSubmitter = (*ingress.Server)(nil)

// secretLookuper is the seam the webhook adapter uses to fetch a signing
// secret by name. Production wiring is secretstore.Resolver.Lookup; tests
// hand in a fake. (Mirrors certmgr.CASigningKeyResolver.)
type secretLookuper interface {
	Lookup(name string) ([]byte, bool)
}

// webhookRoutes returns an ingress ExtraRoutes builder mounting one
// POST route per configured webhook. Each route verifies the vendor
// signature (via the registered pkg/webhook verifier), then submits the
// verified body to the configured durable handler. The routes are
// mounted OUTSIDE the auth middleware and Cedar interceptor — the HMAC
// signature is the authentication gate, and submissions land on the
// untenanted band (band 0). validateWebhooks must have already run.
//
// secrets resolves a secret name to its bytes; reflow.Run passes the
// per-node *secretstore.Resolver.
func webhookRoutes(whs []WebhookConfig, secrets secretLookuper, log *slog.Logger) func(*ingress.Server) []connectserver.Route {
	return func(srv *ingress.Server) []connectserver.Route {
		routes := make([]connectserver.Route, 0, len(whs))
		for _, wh := range whs {
			routes = append(routes, connectserver.Route{
				Path:    wh.Path,
				Handler: webhookHandler(wh, srv, secrets, log),
			})
		}
		return routes
	}
}

// webhookHandler builds the http.Handler for one webhook source:
// verify signature → derive idempotency key → SubmitInvocation.
func webhookHandler(wh WebhookConfig, submit invocationSubmitter, secrets secretLookuper, log *slog.Logger) http.Handler {
	// Resolve the verifier once; validateWebhooks proved it exists.
	verifier, _ := webhook.LookupVerifier(wh.Provider)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if verifier == nil { // defensive — should not happen post-validation
			http.Error(w, "webhook provider not registered", http.StatusInternalServerError)
			return
		}
		secret, ok := secrets.Lookup(wh.SecretName)
		if !ok || len(secret) == 0 {
			// Secret not resolved yet (or missing). Transient from the
			// vendor's view — 503 lets it retry; the idempotency key
			// makes the retry safe.
			log.Warn("webhook: signing secret unavailable",
				"provider", wh.Provider, "secret_name", wh.SecretName, "path", wh.Path)
			http.Error(w, "webhook secret unavailable", http.StatusServiceUnavailable)
			return
		}
		ev, err := verifier.Verify(r.Context(), r, secret)
		if err != nil {
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}
		// Decouple the durable submit from the vendor's connection.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), webhookSubmitTimeout)
		defer cancel()
		resp, err := submit.SubmitInvocation(ctx, connect.NewRequest(&ingressv1.SubmitInvocationRequest{
			Service:        wh.Service,
			Handler:        wh.Handler,
			ObjectKey:      wh.ObjectKey,
			Input:          ev.Body,
			IdempotencyKey: ev.IdempotencyKey,
			Metadata:       ev.Metadata,
		}))
		if err != nil {
			code := connect.CodeOf(err)
			log.Error("webhook: submit failed",
				"provider", wh.Provider, "path", wh.Path, "code", code.String(), "err", err)
			http.Error(w, "submit failed", webhookHTTPStatus(code))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"invocation_id": resp.Msg.GetInvocationIdStr()})
	})
}

// webhookHTTPStatus maps a submit error's Connect code to an HTTP
// status. The split matters to vendors: 5xx invites a retry (safe under
// the idempotency key), 4xx tells the vendor to stop.
func webhookHTTPStatus(code connect.Code) int {
	switch code {
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

// validateWebhooks checks the webhook config before any listener binds,
// so a typo fails Run fast rather than 500ing per request. Secret
// presence is not checked here (it resolves asynchronously) — a missing
// secret surfaces as a per-request 503.
func validateWebhooks(whs []WebhookConfig) error {
	seen := make(map[string]struct{}, len(whs))
	for i, wh := range whs {
		if wh.Provider == "" {
			return fmt.Errorf("webhooks[%d]: provider is required", i)
		}
		if _, err := webhook.LookupVerifier(wh.Provider); err != nil {
			return fmt.Errorf("webhooks[%d]: %w (registered: %s)", i, err, strings.Join(webhook.RegisteredNames(), ", "))
		}
		if !strings.HasPrefix(wh.Path, "/") {
			return fmt.Errorf("webhooks[%d]: path %q must start with /", i, wh.Path)
		}
		if strings.HasPrefix(wh.Path, ingressRPCPrefix) {
			return fmt.Errorf("webhooks[%d]: path %q collides with the ingress RPC prefix %q", i, wh.Path, ingressRPCPrefix)
		}
		if _, dup := seen[wh.Path]; dup {
			return fmt.Errorf("webhooks[%d]: duplicate path %q", i, wh.Path)
		}
		seen[wh.Path] = struct{}{}
		if wh.SecretName == "" {
			return fmt.Errorf("webhooks[%d] (%s): secret_name is required", i, wh.Path)
		}
		if wh.Service == "" || wh.Handler == "" {
			return fmt.Errorf("webhooks[%d] (%s): service and handler are required", i, wh.Path)
		}
	}
	return nil
}
