// Package server hosts a reflow handler-side endpoint over the
// protocolv1 wire. Operators register handlers in an *sdk.Registry,
// wrap it in NewHTTP2, and Serve on a listener. The reflow engine
// discovers the deployment over GET /discover, opens a session via
// POST /invoke/<service>/<handler>, and drives the handler through
// StartMessage / InputCommandMessage frames.
//
// The current implementation supports the minimum-viable session shape
// engine-side wire_session understands: StartMessage + InputCommandMessage
// in, OutputCommandMessage / ErrorMessage / EndMessage out. Context
// methods that journal durable side effects (Sleep, Run, Call, State,
// Awakeable) return ErrWireNotImplemented — the wire-protocol expansion
// for those primitives lands as the wire session matures.
package server

import (
	"errors"
	"fmt"
	"net"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/pkg/sdk"
)

// Server is the handler-side endpoint. NewHTTP2 is the one constructor;
// the engine dials the listener over raw HTTP/2 (h2c plaintext or HTTPS
// + TLS, depending on the registered deployment URL scheme).
type Server interface {
	// Serve blocks accepting sessions on ln until ln is closed or
	// Shutdown is called. Returns the listener error on close.
	Serve(ln net.Listener) error

	// Shutdown stops accepting new sessions and waits for in-flight
	// sessions to terminate. Idempotent.
	Shutdown() error
}

// Config groups constructor inputs shared by every transport. Registry
// is required; the others have sensible defaults.
type Config struct {
	// Registry holds the handlers this server is willing to serve. The
	// lookup is concurrency-safe; the same registry instance can back
	// multiple Servers (e.g. h2c and HTTPS on different ports).
	Registry *sdk.Registry

	// Codec governs inner-payload encoding for protocolv1 messages.
	// Defaults to protobuf. Both sides of the session must agree; the
	// engine's handlerclient.Codec is the matching half.
	Codec handlerclient.Codec

	// RootCAs, when non-nil, enables JWT verification of every /invoke
	// and /discover request via Authorization: Bearer <jwt>. The bundle
	// is the PEM-encoded set of CAs trusted to sign the caller's leaf;
	// the engine signs with a leaf rooted at one of these.
	RootCAs []byte

	// AllowedSPIFFE is the exact-match allowlist of caller SPIFFE URIs
	// (e.g. "spiffe://reflow.local/node/1"). Required when RootCAs is
	// set; leave empty when RootCAs is nil.
	AllowedSPIFFE []string

	// TrustDomain governs SPIFFE URI extraction from the caller's leaf.
	// Empty falls back to creds.DefaultTrustDomain ("reflow.local").
	// Only consulted when RootCAs is set.
	TrustDomain string

	// ExpectedAudience, when non-empty, requires the JWT aud claim to
	// match. Empty skips the aud check (chain + SPIFFE + exp/iat still
	// run). The SDK handler typically doesn't know its own deployment_id
	// (engine-assigned), so this is opt-in.
	ExpectedAudience string
}

// ErrWireNotImplemented is returned by wireContext methods whose
// engine-side wiring is not yet complete (SendSignal). The state-write
// and combinator primitives are fully wired; only the explicit
// cross-invocation signal path remains.
var ErrWireNotImplemented = errors.New("sdk/server: durable primitive not yet supported on wire path")

// ErrLazyStateUnavailable is returned by wireContext.GetState when the
// engine signaled partial_state (eager preload exceeded the cap) and
// the requested key was not in the snapshot. Lazy state fetch via
// GetLazyStateCommandMessage isn't wired yet — handlers see this in
// place of the eventual completion future.
var ErrLazyStateUnavailable = errors.New("sdk/server: state preload incomplete; lazy fetch not implemented")

// validateConfig fills defaults, rejects obviously broken inputs, and
// builds the verifier when RootCAs is set. Returns the verifier (nil
// when auth is disabled). Shared by every transport so the same
// diagnostic surfaces regardless of how the server was constructed.
func validateConfig(cfg *Config) (*creds.Verifier, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("sdk/server: Config.Registry is required")
	}
	if cfg.Codec == nil {
		cfg.Codec = handlerclient.DefaultCodec()
	}
	if cfg.RootCAs == nil {
		// Catch operator typos: auth fields set without the on/off
		// switch are almost certainly a misconfiguration.
		if len(cfg.AllowedSPIFFE) > 0 || cfg.TrustDomain != "" || cfg.ExpectedAudience != "" {
			return nil, errors.New("sdk/server: auth fields set without RootCAs; either set RootCAs to enable verification or remove the other auth fields")
		}
		return nil, nil
	}
	v, err := creds.NewVerifier(cfg.RootCAs, cfg.AllowedSPIFFE, cfg.TrustDomain, cfg.ExpectedAudience)
	if err != nil {
		return nil, fmt.Errorf("sdk/server: build verifier: %w", err)
	}
	return v, nil
}
