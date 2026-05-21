package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/authn"
	connect "connectrpc.com/connect"
)

// PrincipalHeader is the canonical HTTP header reflow stamps with the
// server-verified principal (e.g. "node/3", "operator/alice"). Inbound
// values are stripped by the policy handler to prevent forgery — only
// the server-stamped header survives into downstream handlers.
const PrincipalHeader = "X-Reflow-Principal"

// FileWatcherReload is how often the policy reloader checks the policy
// file for mtime changes when a file is configured. Embedded-policy
// installations don't spawn the watcher.
const FileWatcherReload = 30 * time.Second

// HTTPMiddleware builds the inbound auth chain for the Connect-mounted
// HTTP handler. The chain has three steps:
//
//  1. authn.Middleware runs an AuthFunc that tries SPIFFE-from-mTLS
//     first, then Bearer-JWT (when cfg.OIDC is non-empty). Verification
//     failures emit connect.CodeUnauthenticated.
//  2. policyHandler reads the stamped Principal, sets the server-
//     controlled X-Reflow-Principal header (any inbound copy is
//     stripped first), and matches request URL.Path against the
//     configured Policy. Denial emits connect.CodeUnauthenticated for
//     anonymous principals or connect.CodePermissionDenied for known-
//     but-rejected principals.
//  3. The wrapped handler runs with the Principal attached to the
//     request context via ContextWithPrincipal.
//
// The closer releases the policy file watcher goroutine; safe to call
// when nil (embedded-policy installations have no resources to free).
func HTTPMiddleware(cfg Config, log *slog.Logger) (mw func(http.Handler) http.Handler, closer func() error, err error) {
	if log == nil {
		log = slog.Default()
	}
	pol, c, perr := loadPolicy(cfg.PolicyFile, log)
	if perr != nil {
		return nil, nil, perr
	}
	jwt, jerr := newJWTVerifier(context.Background(), cfg.OIDC, log)
	if jerr != nil {
		if c != nil {
			_ = c()
		}
		return nil, nil, jerr
	}
	auth := composeAuthFunc(cfg.TrustDomain, jwt, log)
	authnMW := authn.NewMiddleware(auth)
	errWriter := connect.NewErrorWriter()
	policy := policyHandler(pol, log, errWriter, jwt != nil)
	mw = func(next http.Handler) http.Handler {
		return authnMW.Wrap(policy(next))
	}
	return mw, c, nil
}

// composeAuthFunc chains the SPIFFE and Bearer authenticators per the
// composition rules in the refactor plan: mTLS wins when both are
// present (a leaked bearer cannot forge a peer-verified leaf). When
// the SPIFFE step returns a non-anonymous Principal the bearer is
// not consulted; a debug-level log notes the override.
func composeAuthFunc(td string, jwt *jwtVerifier, log *slog.Logger) authn.AuthFunc {
	spiffe := spiffeAuthFunc(td)
	bearer := bearerAuthFunc(jwt)
	return func(ctx context.Context, r *http.Request) (any, error) {
		info, err := spiffe(ctx, r)
		if err != nil {
			return nil, err
		}
		if info != nil {
			if _, hasBearer := authn.BearerToken(r); hasBearer {
				log.Debug("auth: bearer token ignored — verified mTLS leaf present",
					"path", r.URL.Path)
			}
			return info, nil
		}
		return bearer(ctx, r)
	}
}

// Policy is a parsed allow-list of (path-glob, principal-glob) pairs. A
// request matches when SOME allow rule's path glob matches r.URL.Path AND
// every principal entry under that rule matches the stamped X-Reflow-
// Principal value.
//
// This is a deliberately narrower model than grpc-go's authz package — we
// only need the path + principal-header combo today. If reflow grows
// header-on-request matching beyond x-reflow-principal we'll either extend
// here or pull in OPA.
type Policy struct {
	Rules []PolicyRule
}

// PolicyRule is one allow rule.
type PolicyRule struct {
	Name           string
	PathGlobs      []string // path.Match globs against r.URL.Path
	PrincipalGlobs []string // path.Match globs against Principal.Raw
}

// Allow reports whether p is allowed to access path under the policy.
func (po *Policy) Allow(p string, principal Principal) bool {
	if po == nil {
		return false
	}
	for _, r := range po.Rules {
		if !matchAny(r.PathGlobs, p) {
			continue
		}
		if len(r.PrincipalGlobs) == 0 {
			return true
		}
		if matchAny(r.PrincipalGlobs, principal.Raw) {
			return true
		}
	}
	return false
}

func matchAny(globs []string, s string) bool {
	for _, g := range globs {
		ok, err := path.Match(g, s)
		if err == nil && ok {
			return true
		}
	}
	return false
}

// policyDoc mirrors the subset of grpc-go's authz JSON we use:
//
//	{ "allow_rules": [
//	    { "name": "...",
//	      "request": {
//	        "paths": ["/svc/*"],
//	        "headers": [{"key": "x-reflow-principal", "values": ["operator/*"]}]
//	      }} ] }
type policyDoc struct {
	AllowRules []struct {
		Name    string `json:"name"`
		Request struct {
			Paths   []string `json:"paths"`
			Headers []struct {
				Key    string   `json:"key"`
				Values []string `json:"values"`
			} `json:"headers"`
		} `json:"request"`
	} `json:"allow_rules"`
}

// ParsePolicy decodes the grpc-go authz JSON layout into a Policy. Header
// matchers other than x-reflow-principal are ignored (the policy file is
// shared with the gRPC path which understands more headers; for Connect
// we only enforce the principal header).
func ParsePolicy(b []byte) (*Policy, error) {
	var doc policyDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("auth: parse policy: %w", err)
	}
	pol := &Policy{}
	for _, r := range doc.AllowRules {
		rule := PolicyRule{Name: r.Name, PathGlobs: r.Request.Paths}
		for _, h := range r.Request.Headers {
			if strings.EqualFold(h.Key, PrincipalHeader) {
				rule.PrincipalGlobs = append(rule.PrincipalGlobs, h.Values...)
			}
		}
		pol.Rules = append(pol.Rules, rule)
	}
	return pol, nil
}

// loadPolicy returns a live policy plus an optional closer. When file is
// empty the embedded starter policy is used and closer is nil. With file
// set a 30s polling loop refreshes from disk and the closer stops it.
func loadPolicy(file string, log *slog.Logger) (*atomic.Pointer[Policy], func() error, error) {
	holder := &atomic.Pointer[Policy]{}
	if file == "" {
		pol, err := ParsePolicy([]byte(StarterPolicyJSON))
		if err != nil {
			return nil, nil, err
		}
		holder.Store(pol)
		return holder, nil, nil
	}
	pol, err := readPolicyFile(file)
	if err != nil {
		return nil, nil, err
	}
	holder.Store(pol)

	stopCh := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(FileWatcherReload)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				p, err := readPolicyFile(file)
				if err != nil {
					log.Warn("auth: policy reload failed", "file", file, "err", err)
					continue
				}
				holder.Store(p)
			}
		}
	}()
	closer := func() error {
		once.Do(func() { close(stopCh) })
		return nil
	}
	return holder, closer, nil
}

func readPolicyFile(file string) (*Policy, error) {
	if file == "" {
		return nil, errors.New("auth: policy file path is empty")
	}
	b, err := readFile(file)
	if err != nil {
		return nil, err
	}
	return ParsePolicy(b)
}

// readFile is wrapped so unit tests can stub it without going through the
// filesystem. Indirection is intentional — the file watcher polls.
var readFile = os.ReadFile
