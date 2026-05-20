package auth

import (
	"context"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/authn"
	connect "connectrpc.com/connect"
	"github.com/cenkalti/backoff/v5"
	"github.com/coreos/go-oidc/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
)

// wwwAuthenticateBearerInvalid is the RFC 6750 §3 challenge value
// returned on a 401 when a bearer token was presented but failed
// verification. Clients that follow the spec retry with a refreshed
// token rather than the same bad one.
const wwwAuthenticateBearerInvalid = `Bearer error="invalid_token"`

// wwwAuthenticateBearer is the bare RFC 7235 challenge value used on
// 401s where no credentials were presented at all but bearer is an
// accepted scheme on this surface.
const wwwAuthenticateBearer = "Bearer"

// jwtVerifier dispatches an inbound Bearer token to the right issuer
// entry by inspecting the unverified `iss` claim, then delegates to
// that entry's go-oidc Verifier.
type jwtVerifier struct {
	byIssuer map[string]*issuerEntry
	log      *slog.Logger
}

// issuerEntry is one configured OIDC issuer plus its (possibly lazy)
// go-oidc Verifier.
type issuerEntry struct {
	cfg OIDCIssuerConfig
	log *slog.Logger

	// For lazy discovery: discoverOnce gates the first attempt;
	// verifier holds the result of a successful discovery. nextRetry
	// throttles subsequent attempts after a failure.
	mu        sync.Mutex
	verifier  *oidc.IDTokenVerifier
	nextRetry time.Time
	backoff   *backoff.ExponentialBackOff
}

// newJWTVerifier builds a multi-issuer verifier from the OIDC config
// slice. Issuers with JWKSFile fail-fast on file errors. Issuers with
// OIDC discovery are lazy unless EagerDiscovery is true. Empty config
// returns (nil, nil) — the AuthFunc skips bearer auth entirely.
func newJWTVerifier(ctx context.Context, cfg []OIDCIssuerConfig, log *slog.Logger) (*jwtVerifier, error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	if log == nil {
		log = slog.Default()
	}
	v := &jwtVerifier{byIssuer: make(map[string]*issuerEntry, len(cfg)), log: log}
	for i, ic := range cfg {
		if ic.IssuerURL == "" {
			return nil, fmt.Errorf("auth: oidc[%d]: issuer_url is required", i)
		}
		if len(ic.Audiences) == 0 {
			return nil, fmt.Errorf("auth: oidc[%d]: audiences must list at least one expected aud value", i)
		}
		if _, dup := v.byIssuer[ic.IssuerURL]; dup {
			return nil, fmt.Errorf("auth: oidc[%d]: duplicate issuer_url %q", i, ic.IssuerURL)
		}
		entry := &issuerEntry{cfg: ic, log: log.With("issuer", labelFor(ic))}
		entry.backoff = backoff.NewExponentialBackOff()
		entry.backoff.InitialInterval = 1 * time.Second
		entry.backoff.MaxInterval = 30 * time.Second

		switch {
		case ic.JWKSFile != "":
			verifier, err := buildStaticVerifier(ic)
			if err != nil {
				return nil, fmt.Errorf("auth: oidc[%d] (%q): %w", i, labelFor(ic), err)
			}
			entry.verifier = verifier
		case ic.EagerDiscovery:
			if err := entry.discover(ctx); err != nil {
				return nil, fmt.Errorf("auth: oidc[%d] (%q): eager discovery: %w", i, labelFor(ic), err)
			}
		}
		v.byIssuer[ic.IssuerURL] = entry
	}
	return v, nil
}

// verify runs the AuthFunc step for a Bearer token. Returns the
// Principal on success, a hard authn.Errorf otherwise. Token parse
// failures and verification failures both map to CodeUnauthenticated
// with an opaque message; full reasons go to the audit log.
func (v *jwtVerifier) verify(ctx context.Context, raw string) (Principal, error) {
	iss, err := unsafeReadIssuer(raw)
	if err != nil {
		v.log.Warn("jwt: malformed token", "err", err)
		return Principal{}, authn.Errorf("invalid token")
	}
	entry, ok := v.byIssuer[iss]
	if !ok {
		v.log.Warn("jwt: unknown issuer", "iss", iss)
		return Principal{}, authn.Errorf("invalid token")
	}
	return entry.verify(ctx, raw)
}

func (e *issuerEntry) verify(ctx context.Context, raw string) (Principal, error) {
	verifier, err := e.getVerifier(ctx)
	if err != nil {
		e.log.Warn("jwt: verifier unavailable", "err", err)
		return Principal{}, authn.Errorf("invalid token")
	}
	tok, err := verifier.Verify(ctx, raw)
	if err != nil {
		e.log.Warn("jwt: signature/claim verification failed", "err", err)
		return Principal{}, authn.Errorf("invalid token")
	}
	if !audienceMatches(tok.Audience, e.cfg.Audiences) {
		e.log.Warn("jwt: audience mismatch", "got", tok.Audience, "want_any", e.cfg.Audiences)
		return Principal{}, authn.Errorf("invalid token")
	}
	allClaims := map[string]any{}
	if err := tok.Claims(&allClaims); err != nil {
		e.log.Warn("jwt: claims unmarshal", "err", err)
		return Principal{}, authn.Errorf("invalid token")
	}
	for k, want := range e.cfg.RequiredClaims {
		if got, _ := allClaims[k].(string); got != want {
			e.log.Warn("jwt: required claim mismatch", "claim", k)
			return Principal{}, authn.Errorf("invalid token")
		}
	}
	var subject string
	if e.cfg.PrincipalClaim == "" {
		subject = tok.Subject
	} else {
		subject = lookupStringClaim(allClaims, e.cfg.PrincipalClaim)
	}
	if subject == "" {
		e.log.Warn("jwt: principal claim missing or empty", "claim", e.cfg.PrincipalClaim)
		return Principal{}, authn.Errorf("invalid token")
	}
	subject = sanitizeSubject(subject)
	kind := e.cfg.PrincipalKind
	if kind == "" {
		kind = "user"
	}
	if e.cfg.KindClaim != "" {
		if k := lookupStringClaim(allClaims, e.cfg.KindClaim); k != "" {
			kind = sanitizeSubject(k)
		}
	}
	var attached map[string]string
	if len(e.cfg.AllowedClaims) > 0 {
		attached = make(map[string]string, len(e.cfg.AllowedClaims))
		for _, name := range e.cfg.AllowedClaims {
			if s := lookupStringClaim(allClaims, name); s != "" {
				attached[name] = s
			}
		}
	}
	return Principal{
		Kind:    kind,
		Subject: subject,
		URI:     "oidc://" + tok.Issuer + "#" + subject,
		Raw:     kind + "/" + subject,
		Claims:  attached,
	}, nil
}

// getVerifier returns the cached verifier, or attempts lazy discovery
// when one isn't built yet. Discovery failures back off: subsequent
// requests within the backoff window get the cached error without
// hammering the IdP.
func (e *issuerEntry) getVerifier(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.verifier != nil {
		return e.verifier, nil
	}
	if !e.nextRetry.IsZero() && time.Now().Before(e.nextRetry) {
		return nil, errors.New("discovery in backoff")
	}
	if err := e.discoverLocked(ctx); err != nil {
		if d := e.backoff.NextBackOff(); d != backoff.Stop {
			e.nextRetry = time.Now().Add(d)
		}
		return nil, err
	}
	e.backoff.Reset()
	e.nextRetry = time.Time{}
	return e.verifier, nil
}

func (e *issuerEntry) discover(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.discoverLocked(ctx)
}

func (e *issuerEntry) discoverLocked(ctx context.Context) error {
	provider, err := oidc.NewProvider(ctx, e.cfg.IssuerURL)
	if err != nil {
		return err
	}
	e.verifier = provider.Verifier(verifierConfig(e.cfg))
	return nil
}

// buildStaticVerifier wires an oidc.StaticKeySet from a JWKS file.
// JWKSFile failures abort startup.
func buildStaticVerifier(ic OIDCIssuerConfig) (*oidc.IDTokenVerifier, error) {
	data, err := os.ReadFile(ic.JWKSFile)
	if err != nil {
		return nil, fmt.Errorf("read jwks_file: %w", err)
	}
	var jwks jose.JSONWebKeySet
	if err := json.Unmarshal(data, &jwks); err != nil {
		return nil, fmt.Errorf("parse jwks_file: %w", err)
	}
	if len(jwks.Keys) == 0 {
		return nil, errors.New("jwks_file contains no keys")
	}
	keys := make([]crypto.PublicKey, 0, len(jwks.Keys))
	for i, k := range jwks.Keys {
		if k.Key == nil {
			return nil, fmt.Errorf("jwks_file key[%d] has no public key material", i)
		}
		keys = append(keys, k.Key)
	}
	keySet := &oidc.StaticKeySet{PublicKeys: keys}
	return oidc.NewVerifier(ic.IssuerURL, keySet, verifierConfig(ic)), nil
}

// verifierConfig builds the go-oidc Config that disables ClientID
// matching (we validate audience ourselves against the configured
// slice) and applies the clock-skew leeway.
func verifierConfig(ic OIDCIssuerConfig) *oidc.Config {
	cfg := &oidc.Config{SkipClientIDCheck: true}
	if ic.ClockSkew > 0 {
		skew := ic.ClockSkew
		cfg.Now = func() time.Time { return time.Now().Add(-skew) }
	}
	return cfg
}

// unsafeReadIssuer base64-decodes the JWT payload segment and reads
// the `iss` claim WITHOUT signature verification. Used only to route
// to the right issuerEntry; the entry's Verifier re-validates `iss`
// against its configured value during signed verification.
func unsafeReadIssuer(raw string) (string, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("jwt: expected 3 segments, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("jwt: payload base64: %w", err)
	}
	var hdr struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &hdr); err != nil {
		return "", fmt.Errorf("jwt: payload json: %w", err)
	}
	if hdr.Iss == "" {
		return "", errors.New("jwt: missing iss claim")
	}
	return hdr.Iss, nil
}

func audienceMatches(got, want []string) bool {
	for _, g := range got {
		if slices.Contains(want, g) {
			return true
		}
	}
	return false
}

// lookupStringClaim returns claims[name] as string, or "" when the
// claim is absent / not-a-string. Callers decide what missing means
// (reject vs fall back) by checking the empty return.
func lookupStringClaim(claims map[string]any, name string) string {
	if v, ok := claims[name]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// sanitizeSubject strips '/' (and surrounding whitespace) so an
// IdP-controlled subject can't traverse principal-glob matching in
// Policy.Allow (path.Match treats '/' as a separator).
func sanitizeSubject(s string) string {
	s = strings.TrimSpace(s)
	return strings.ReplaceAll(s, "/", "_")
}

// labelFor returns the audit-log label for an issuer: explicit Name,
// or IssuerURL when Name is empty.
func labelFor(ic OIDCIssuerConfig) string {
	if ic.Name != "" {
		return ic.Name
	}
	return ic.IssuerURL
}

// bearerAuthFunc returns an authn.AuthFunc that pulls "Authorization:
// Bearer ..." and verifies it via the configured jwtVerifier. When
// the header is absent it returns (nil, nil) so the caller falls
// through to anonymous. Header present but verification failed
// returns a hard CodeUnauthenticated error.
func bearerAuthFunc(v *jwtVerifier) authn.AuthFunc {
	if v == nil {
		return func(_ context.Context, _ *http.Request) (any, error) { return nil, nil }
	}
	return func(ctx context.Context, r *http.Request) (any, error) {
		token, ok := authn.BearerToken(r)
		if !ok {
			return nil, nil
		}
		p, err := v.verify(ctx, token)
		if err != nil {
			// RFC 6750 §3: a 401 from a bearer-protected resource MUST
			// (the spec says "MUST" for error code values) carry a
			// WWW-Authenticate challenge so the client knows which
			// scheme to retry with and that the token, not the
			// scheme, is the problem.
			if cerr, ok := err.(*connect.Error); ok {
				cerr.Meta().Set("WWW-Authenticate", wwwAuthenticateBearerInvalid)
			}
			return nil, err
		}
		return p, nil
	}
}
