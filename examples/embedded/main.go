// embedded is the canonical "single-binary" example: one main() runs both
// the reflow engine and the Go handlers in the same process, with NO HTTP
// hop between them. The handlers are registered in a handler.Registry handed
// to reflow.Run via Config.Handlers.InProcess; the engine dispatches to them
// directly over an in-process transport.
//
// Architecture:
//
//	┌────────────────────────────────────────────────┐
//	│ embedded (this main)                             │
//	│  ┌────────────────────┐                          │
//	│  │ handler.Registry   │ ◄── in-process call ───┐ │
//	│  └────────────────────┘                        │ │
//	│  ┌────────────────────┐                        │ │
//	│  │ reflow.Run engine  │ ── Handlers.InProcess ──┘ │
//	│  │  + ingress HTTP    │                           │
//	│  └────────────────────┘                           │
//	└────────────────────────────────────────────────┘
//	                       │
//	                       │ HTTP POST /invocation/Greeter/hello
//	                       ▼
//	                  curl / your client
//
// Usage:
//
//	go run ./examples/embedded
//	# in another shell:
//	curl -X POST \
//	  -H 'content-type: application/json' \
//	  -d '{"input":"d29ybGQ="}' \  # base64("world")
//	  http://127.0.0.1:8080/invocation/Greeter/hello
//
// This example also mounts a Stripe webhook at POST /webhooks/stripe (see
// cfg.Webhooks): the route verifies the Stripe-Signature HMAC with the
// "stripe-signing" secret, then submits to WebhookRouter/OnStripeEvent,
// which fans out to a per-customer PaymentTracker object. That route needs
// the secret in the secret store — without it it answers 503; the
// greet/echo flow above needs no secret.
//
// customwebhook.go adds a CUSTOM verifier ("acme") for a vendor Reflow
// ships nothing for — implement webhook.Verifier, RegisterVerifier it
// before reflow.Run, and name it in cfg.Webhooks.
//
// To run handlers as a separate process instead (the remote model), host a
// handler.NewServer on a listener and register its URL via
// Config.Handlers.Endpoints. cmd/reflowd is the production engine binary.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/reflow"
	"github.com/twinfer/reflw/pkg/webhook"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. Build a registry and register the handlers in this process.
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Greeter", "hello", greet); err != nil {
		log.Error("register Greeter/hello", "err", err)
		os.Exit(1)
	}
	if err := reg.RegisterService("Echo", "echo", echo); err != nil {
		log.Error("register Echo/echo", "err", err)
		os.Exit(1)
	}
	// WebhookRouter is the durable entry point for the Stripe webhook
	// configured in cfg.Webhooks below. It mirrors Restate's
	// WebhookCallbackRouter: the edge already verified the signature, so
	// this handler just parses the event and fans out to a per-customer
	// PaymentTracker virtual object.
	if err := reg.RegisterService("WebhookRouter", "OnStripeEvent", onStripeEvent); err != nil {
		log.Error("register WebhookRouter/OnStripeEvent", "err", err)
		os.Exit(1)
	}
	if err := reg.RegisterObject("PaymentTracker", "OnPaymentSucceeded", onPaymentSucceeded); err != nil {
		log.Error("register PaymentTracker/OnPaymentSucceeded", "err", err)
		os.Exit(1)
	}
	if err := reg.RegisterObject("PaymentTracker", "OnPaymentFailed", onPaymentFailed); err != nil {
		log.Error("register PaymentTracker/OnPaymentFailed", "err", err)
		os.Exit(1)
	}

	// Register a CUSTOM webhook verifier for a vendor Reflow ships no
	// built-in for (see customwebhook.go). Must happen before reflow.Run,
	// which validates cfg.Webhooks against the verifier registry.
	webhook.RegisterVerifier(acmeVerifier{})

	// 2. Start the engine with the handlers running in-process. Run
	// registers the Registry as a single inproc:// deployment at
	// metadata-leader bootstrap and dispatches to it directly — no HTTP,
	// no second listener.
	dataDir, err := os.MkdirTemp("", "reflow-embedded-")
	if err != nil {
		log.Error("mkdir tmp", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dataDir)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{
			ID:       1,
			RaftAddr: "127.0.0.1:5410",
		},
		Storage: reflow.StorageConfig{
			DataDir: filepath.Join(dataDir, "node1"),
		},
		Ingress: reflow.IngressConfig{
			Addr: "127.0.0.1:8080",
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
		Handlers: reflow.HandlersConfig{
			InProcess: reg,
		},
		// Mount a Stripe webhook at POST /webhooks/stripe. The route
		// verifies the Stripe-Signature HMAC using the secret named
		// "stripe-signing", then submits to WebhookRouter/OnStripeEvent
		// on the untenanted band. The signature is the auth gate — the
		// route sits outside the mesh auth/authz chain.
		//
		// NOTE: "stripe-signing" must exist in the secret store (configure
		// cfg.KMS + a secret of that name); until it resolves the route
		// answers 503. The greet/echo handlers above work without it.
		Webhooks: []reflow.WebhookConfig{{
			Provider:   "stripe",
			Path:       "/webhooks/stripe",
			SecretName: "stripe-signing",
			Service:    "WebhookRouter",
			Handler:    "OnStripeEvent",
		}, {
			// "acme" is the CUSTOM verifier registered above — Reflow ships
			// no built-in for it. Verified events submit to Echo/echo here
			// for the demo; needs an "acme-signing" secret in the store.
			Provider:   "acme",
			Path:       "/webhooks/acme",
			SecretName: "acme-signing",
			Service:    "Echo",
			Handler:    "echo",
		}},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		log.Error("reflow.Run", "err", err)
		os.Exit(1)
	}
	log.Info("embedded: engine + in-process handlers live; submit via POST http://127.0.0.1:8080/invocation/Greeter/hello")

	// 3. Wait for SIGINT/SIGTERM, then drain the engine.
	<-ctx.Done()
	log.Info("embedded: shutting down")
	if err := host.Close(); err != nil {
		log.Warn("engine close", "err", err)
	}
}

// greet returns "hello, <input>" — useful for a quick smoke test:
//
//	curl -d '{"input":"d29ybGQ="}' http://127.0.0.1:8080/invocation/Greeter/hello
//
// (the JSON `input` field is base64-encoded raw bytes).
func greet(_ handler.Context, in []byte) ([]byte, error) {
	return fmt.Appendf(nil, "hello, %s", in), nil
}

// echo returns its input unchanged.
func echo(_ handler.Context, in []byte) ([]byte, error) { return in, nil }

// onStripeEvent is a durable webhook router mirroring Restate's
// WebhookCallbackRouter. pkg/webhook already verified the Stripe
// signature at the edge and stamped vendor facts into ctx.Metadata()
// (e.g. ctx.Metadata()["webhook_vendor"] == "stripe"); this handler only
// parses the event and fans out to the per-customer PaymentTracker
// object. The fan-out is exactly-once by durable execution — OneWayCall
// is journaled, so a crash-replay never double-sends.
func onStripeEvent(ctx handler.Context, in []byte) ([]byte, error) {
	var evt struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				ID string `json:"id"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(in, &evt); err != nil {
		// Malformed body is terminal — a retry won't fix it.
		return nil, handler.NewFailure(400, "stripe: bad event json: "+err.Error())
	}
	id := evt.Data.Object.ID
	if id == "" {
		return nil, nil // nothing to route
	}
	switch evt.Type {
	case "invoice.payment_succeeded":
		return nil, ctx.OneWayCall(handler.Target{Service: "PaymentTracker", Handler: "OnPaymentSucceeded", Key: id}, in)
	case "invoice.payment_failed":
		return nil, ctx.OneWayCall(handler.Target{Service: "PaymentTracker", Handler: "OnPaymentFailed", Key: id}, in)
	default:
		return nil, nil // ignore other event types
	}
}

// onPaymentSucceeded / onPaymentFailed are PaymentTracker virtual-object
// handlers, locked per customer id (the object key). They record the
// latest event in durable per-object state.
func onPaymentSucceeded(ctx handler.Context, in []byte) ([]byte, error) {
	return nil, ctx.SetState("last_succeeded_event", in)
}

func onPaymentFailed(ctx handler.Context, in []byte) ([]byte, error) {
	return nil, ctx.SetState("last_failed_event", in)
}
