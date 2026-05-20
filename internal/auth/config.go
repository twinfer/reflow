package auth

import "time"

// Config is the internal-auth view of the public reflow.AuthConfig.
// pkg/reflow.Run translates the user-facing struct into this one at
// the boundary so internal/auth never imports pkg/reflow.
type Config struct {
	TrustDomain string
	PolicyFile  string
	OIDC        []OIDCIssuerConfig
}

// OIDCIssuerConfig mirrors pkg/reflow.OIDCIssuer. See that struct's
// doc comments for field semantics.
type OIDCIssuerConfig struct {
	Name           string
	IssuerURL      string
	JWKSFile       string
	Audiences      []string
	PrincipalClaim string
	PrincipalKind  string
	KindClaim      string
	RequiredClaims map[string]string
	AllowedClaims  []string
	ClockSkew      time.Duration
	EagerDiscovery bool
}
