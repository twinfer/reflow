package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"

	"github.com/twinfer/reflow/pkg/webhook"
)

// acmeVerifier demonstrates supporting a vendor Reflow ships no built-in
// verifier for. Three steps to add your own:
//
//  1. Implement webhook.Verifier (Name + Verify), as below.
//  2. Register it before reflow.Run with webhook.RegisterVerifier(acmeVerifier{})
//     (see main). The built-in stripe/github/slack verifiers self-register
//     the same way from their package init.
//  3. Reference it by Name() in reflow.Config.Webhooks[].Provider ("acme").
//
// "acme" here is HMAC-SHA256 of the raw body, hex-encoded in an
// X-Acme-Signature header — swap the header names / algorithm for your
// vendor. The signing secret arrives via the secret store (the SecretName
// on the WebhookConfig), so it never lives in code.
type acmeVerifier struct{}

func (acmeVerifier) Name() string { return "acme" }

func (acmeVerifier) Verify(_ context.Context, r *http.Request, secret []byte) (*webhook.VerifiedEvent, error) {
	if len(secret) == 0 {
		return nil, errors.New("acme: empty secret")
	}
	// Cap the body, as the built-in verifiers do.
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err != nil {
		return nil, errors.New("acme: body too large or unreadable")
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	want := mac.Sum(nil)
	got, decErr := hex.DecodeString(r.Header.Get("X-Acme-Signature"))
	// Any returned error makes the adapter respond 401. Return a
	// connect.NewError(connect.CodeUnauthenticated, ...) instead if you
	// want the Connect-coded status; a plain error works too.
	if decErr != nil || !hmac.Equal(want, got) {
		return nil, errors.New("acme: signature mismatch")
	}
	return &webhook.VerifiedEvent{
		Body:     body,
		Metadata: map[string]string{webhook.MetadataKeyVendor: "acme"},
		// If the vendor sends a stable delivery id, surface it so the
		// engine dedups retries; empty disables submit-level dedup.
		IdempotencyKey: r.Header.Get("X-Acme-Delivery"),
	}, nil
}
