// Package creds is the operator-facing transport-credential layer for
// reflow. Each listener (Delivery, Admin, Ingress) and each handler-dial
// endpoint takes a Spec; Build returns a ListenerCreds carrying matching
// server + client *tls.Config values, an optional PerRPCCredentials, and
// a Close that releases any background resources (cert provider
// goroutines, token refreshers, …).
//
// The zero Spec (Driver == "") builds the insecure listener — that
// default is load-bearing for "go run ./cmd/reflowd" to start cert-free.
// Multi-node + insecure is allowed but emits a loud WARN at startup
// (see pkg/reflow/run.go); this package does not enforce the policy.
package creds

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/credentials"
)

// Driver names the wire-security backend that builds a listener's
// transport credentials. Each value maps to one file in this package
// (insecure.go, tls.go, certprovider.go, alts.go, google.go, local.go,
// oauth.go, jwt.go, sts.go).
type Driver string

const (
	// DriverInsecure is the default — no transport security. Build
	// returns this for the zero Spec.
	DriverInsecure Driver = "insecure"
	// DriverTLS terminates TLS using PEM files on disk with reflow's
	// SPIFFE URI-SAN convention. Replaces the legacy TLSConfig path.
	DriverTLS Driver = "tls"
	// DriverCertProvider terminates TLS using grpc-go's certprovider
	// plug-in framework. Use when an external agent rotates certs
	// (SPIFFE/SPIRE workload API, in-cluster cert manager).
	DriverCertProvider Driver = "tls_certprovider"
	// DriverALTS uses Google's ALTS (Application Layer Transport
	// Security) — GCE/GKE-only.
	DriverALTS Driver = "alts"
	// DriverGoogle is the bundled-credentials path that picks ALTS on
	// GCE and TLS elsewhere; the client also attaches a Google access
	// token.
	DriverGoogle Driver = "google"
	// DriverLocal uses grpc-go's local transport credentials (UDS or
	// loopback only). Operator-facing test / sidecar option.
	DriverLocal Driver = "local"
	// DriverOAuth attaches a bearer access token via PerRPCCredentials.
	// Server side stays TLS — OAuth is layered over an existing
	// transport-secure listener.
	DriverOAuth Driver = "oauth"
	// DriverJWT attaches a service-account JWT via PerRPCCredentials.
	// Same caveat as DriverOAuth — requires an underlying transport.
	DriverJWT Driver = "jwt"
	// DriverSTS performs an OAuth 2 token-exchange (RFC 8693) and
	// attaches the resulting access token via PerRPCCredentials.
	DriverSTS Driver = "sts"
)

// Spec is the koanf-decodable shape that selects one driver and carries
// its driver-specific options. Exactly one of the nested *Spec pointers
// must match Driver; non-matching pointers are ignored. A zero Spec
// (Driver == "") is treated as DriverInsecure.
type Spec struct {
	Driver       Driver            `koanf:"driver"`
	Insecure     *InsecureSpec     `koanf:"insecure"`
	TLS          *TLSSpec          `koanf:"tls"`
	CertProvider *CertProviderSpec `koanf:"tls_certprovider"`
	ALTS         *ALTSSpec         `koanf:"alts"`
	Google       *GoogleSpec       `koanf:"google"`
	Local        *LocalSpec        `koanf:"local"`
	OAuth        *OAuthSpec        `koanf:"oauth"`
	JWT          *JWTSpec          `koanf:"jwt"`
	STS          *STSSpec          `koanf:"sts"`
}

// ListenerCreds is what Build returns: everything one listener (or one
// dial-out client) needs to terminate or originate a secure connection.
//
// Reflow's transport layer is HTTP/2 (Connect RPC for admin/delivery/
// ingress; HTTP/2 with optional TLS for engine→handler). The *tls.Config
// pair is the primary interface.
type ListenerCreds struct {
	// ServerTLSConfig is the *tls.Config for the listening side
	// (Connect ingress / admin / delivery, h2c when nil). Populated by
	// the TLS and CertProvider drivers; nil for insecure and PerRPC-
	// only drivers.
	ServerTLSConfig *tls.Config
	// ClientTLSConfig is the *tls.Config for dial-out HTTP/2 clients
	// (pkg/reflowclient, pkg/ingressclient, internal/engine/delivery
	// Client). Mirror of ServerTLSConfig's client side. Nil for
	// insecure / PerRPC-only.
	ClientTLSConfig *tls.Config
	// PerRPC is a call-level credential when the driver attaches a
	// bearer token (OAuth, JWT, STS). Currently unused on the Connect
	// path; preserved as a forward-compat seam for Connect interceptor
	// integration.
	PerRPC credentials.PerRPCCredentials
	// Driver echoes Spec.Driver after defaulting (i.e. "insecure" for
	// the zero spec). Useful for metrics labelling.
	Driver Driver
	// SecurityLevel is the credentials.SecurityLevel of the produced
	// server credentials. Used to drive the SecurityLevel gauge and
	// the multi-node insecure WARN log.
	SecurityLevel credentials.SecurityLevel
	// Close releases any background resources spawned by Build
	// (certprovider goroutines, token refreshers). Safe to call once;
	// safe to call when nil via the package-level CloseAll helper.
	Close func() error
}

// Build constructs a ListenerCreds from a Spec. A zero Spec (or one with
// Driver == "") returns the insecure listener. log is used for
// driver-internal warnings only; nil is allowed and falls back to
// slog.Default.
func Build(s Spec, log *slog.Logger) (*ListenerCreds, error) {
	if log == nil {
		log = slog.Default()
	}
	driver := s.Driver
	if driver == "" {
		driver = DriverInsecure
	}
	switch driver {
	case DriverInsecure:
		return buildInsecure(s.Insecure)
	case DriverTLS:
		return buildTLS(s.TLS, log)
	case DriverCertProvider:
		return buildCertProvider(s.CertProvider, log)
	case DriverALTS:
		return buildALTS(s.ALTS)
	case DriverGoogle:
		return buildGoogle(s.Google)
	case DriverLocal:
		return buildLocal(s.Local)
	case DriverOAuth:
		return buildOAuth(s.OAuth)
	case DriverJWT:
		return buildJWT(s.JWT)
	case DriverSTS:
		return buildSTS(s.STS)
	default:
		return nil, fmt.Errorf("reflow/creds: unknown driver %q", driver)
	}
}

// CloseAll is the deferred-friendly helper a Host uses to release every
// ListenerCreds it built; the first error is returned and the remainder
// are still closed. nil entries and nil Close hooks are no-ops.
func CloseAll(lcs ...*ListenerCreds) error {
	var firstErr error
	for _, lc := range lcs {
		if lc == nil || lc.Close == nil {
			continue
		}
		if err := lc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// errMissingSpec is returned when a driver is selected but the matching
// nested spec pointer is nil. Driver factories all share the same
// shape, so the error string is centralised here.
func errMissingSpec(d Driver) error {
	return fmt.Errorf("reflow/creds: driver %q selected but spec is nil", d)
}

// errEmptyField is the common error for required string fields.
func errEmptyField(d Driver, field string) error {
	return fmt.Errorf("reflow/creds: driver %q: %s is required", d, field)
}

// ErrPerRPCRequiresTransport is returned when a PerRPC-only driver
// (OAuth, JWT, STS) is selected without an underlying transport
// configuration. Reflow does not bundle a default TLS spec under those
// drivers because the operator should be explicit about the trust
// material; instead Build refuses the combo so the misconfiguration
// surfaces at startup, not on the first RPC.
var ErrPerRPCRequiresTransport = errors.New(
	"reflow/creds: oauth/jwt/sts drivers require an underlying transport — wrap with creds.WithTransport or configure tls/tls_certprovider on the listener")
