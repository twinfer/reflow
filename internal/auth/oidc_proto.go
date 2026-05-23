package auth

import (
	"maps"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ProtoToOIDCIssuerConfig converts a wire-shape OIDCIssuerConfig from
// enginev1 (carried on TenantRecord.OidcIssuers) into the internal
// auth-package shape used by JWTVerifier. The two structs are
// field-equivalent modulo a couple of casing/unit differences:
//
//   - enginev1.JwksFile  ↔ auth.JWKSFile
//   - enginev1.IssuerUrl ↔ auth.IssuerURL
//   - enginev1.ClockSkewMs (int64 millis) ↔ auth.ClockSkew (time.Duration)
//
// nil input returns the zero value so callers can use this in a
// per-record loop without nil-checking.
func ProtoToOIDCIssuerConfig(p *enginev1.OIDCIssuerConfig) OIDCIssuerConfig {
	if p == nil {
		return OIDCIssuerConfig{}
	}
	return OIDCIssuerConfig{
		Name:           p.GetName(),
		IssuerURL:      p.GetIssuerUrl(),
		JWKSFile:       p.GetJwksFile(),
		Audiences:      append([]string(nil), p.GetAudiences()...),
		PrincipalClaim: p.GetPrincipalClaim(),
		KindClaim:      p.GetKindClaim(),
		RequiredClaims: copyStringMap(p.GetRequiredClaims()),
		AllowedClaims:  append([]string(nil), p.GetAllowedClaims()...),
		ClockSkew:      time.Duration(p.GetClockSkewMs()) * time.Millisecond,
		EagerDiscovery: p.GetEagerDiscovery(),
	}
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}
