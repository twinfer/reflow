// Package server hosts a reflow handler-side endpoint over the
// protocolv1 wire. Operators register handlers in an *sdk.Registry,
// wrap it in NewGRPC or NewHTTP2, and Serve on a listener. The reflow
// engine discovers the deployment, opens a session, and drives the
// handler through StartMessage / InputCommandMessage frames.
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
	"github.com/twinfer/reflow/pkg/sdk"
)

// Server is the transport-neutral handler-side endpoint. NewGRPC and
// NewHTTP2 return concrete servers backed by the same Registry +
// wireContext path; pick one based on which transport the engine will
// dial (deployment URL scheme).
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
	// same registry instance can back multiple Servers (e.g. gRPC + HTTP/2
	// on different ports) — the lookup is concurrency-safe.
	Registry *sdk.Registry

	// Codec governs inner-payload encoding for protocolv1 messages.
	// Defaults to protobuf. Both sides of the session must agree; the
	// engine's handlerclient.Codec is the matching half.
	Codec handlerclient.Codec
}

// ErrWireNotImplemented is returned by every wireContext method that
// represents a durable-execution primitive (Sleep, Run, Call, State,
// Awakeable, signals). The engine-side wire session does not yet handle
// the corresponding command/notification frames; handlers running on the
// wire path must stick to input/output for now.
var ErrWireNotImplemented = errors.New("sdk/server: durable primitive not yet supported on wire path")

// validateConfig fills defaults and rejects obviously broken inputs.
// Shared by both transports so the same diagnostic surfaces regardless
// of how the server was constructed.
func validateConfig(cfg *Config) error {
	if cfg.Registry == nil {
		return fmt.Errorf("sdk/server: Config.Registry is required")
	}
	if cfg.Codec == nil {
		cfg.Codec = handlerclient.DefaultCodec()
	}
	return nil
}
