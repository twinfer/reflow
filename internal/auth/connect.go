package auth

import (
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
)

// HTTPMiddleware wraps a Connect-mounted http.Handler with the same
// principal-stamping + policy enforcement that gRPC's auth.NewServerInterceptors
// provides for the delivery surface. The middleware:
//
//  1. Extracts the SPIFFE Principal from r.TLS (or anonymous when there is
//     no TLS state).
//  2. Strips any inbound X-Reflow-Principal header (forgery defense).
//  3. Stamps the server-computed Principal.Raw onto the request header
//     and into the request context (handlers read via PrincipalFromContext).
//  4. Matches request URL.Path against the configured Policy. A request
//     that no allow rule matches is rejected with HTTP 403; a malformed
//     leaf cert surfaces as HTTP 401.
//
// The closer releases any FileWatcher goroutine the policy holds open;
// safe to call when nil (Static policies have no resources to free).
func HTTPMiddleware(td, policyFile string, log *slog.Logger) (mw func(http.Handler) http.Handler, closer func() error, err error) {
	if log == nil {
		log = slog.Default()
	}
	pol, c, perr := loadPolicy(policyFile, log)
	if perr != nil {
		return nil, nil, perr
	}
	mw = func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, perr := extractHTTPPrincipal(td, r)
			if perr != nil {
				log.Warn("auth: extractor rejected request",
					"path", r.URL.Path, "err", perr)
				http.Error(w, "auth: "+perr.Error(), http.StatusUnauthorized)
				return
			}
			r.Header.Del(PrincipalHeader)
			if !principal.IsAnonymous() {
				r.Header.Set(PrincipalHeader, principal.Raw)
			}
			if !pol.Load().Allow(r.URL.Path, principal) {
				log.Warn("auth: policy denied request",
					"path", r.URL.Path, "principal", principal.String())
				http.Error(w, "auth: forbidden", http.StatusForbidden)
				return
			}
			r = r.WithContext(ContextWithPrincipal(r.Context(), principal))
			next.ServeHTTP(w, r)
		})
	}
	return mw, c, nil
}

// extractHTTPPrincipal mirrors SPIFFEExtractor.Extract for *http.Request:
// reads the verified TLS leaf, decodes the URI SAN. Empty TLS or absent
// peer info yields the anonymous principal with no error so non-mTLS
// listeners are usable in tests / single-node dev.
func extractHTTPPrincipal(td string, r *http.Request) (Principal, error) {
	if r.TLS == nil {
		return Principal{}, nil
	}
	if len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return Principal{}, nil
	}
	leaf := r.TLS.VerifiedChains[0][0]
	if len(leaf.URIs) != 1 {
		return Principal{}, fmt.Errorf("leaf must carry exactly one URI SAN; got %d", len(leaf.URIs))
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host == "" {
		return Principal{}, fmt.Errorf("unrecognised URI %q", u.String())
	}
	if u.Host != td {
		return Principal{}, fmt.Errorf("leaf trust domain %q; want %q", u.Host, td)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Principal{}, fmt.Errorf("leaf URI %q does not match /<kind>/<name>", u.String())
	}
	return Principal{
		Kind:    parts[0],
		Subject: parts[1],
		URI:     u.String(),
		Raw:     parts[0] + "/" + parts[1],
	}, nil
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

// ChainHTTP composes 0..N HTTP middlewares left-to-right (first runs
// outermost). Used by run.go to layer auth + logging on a Connect mux.
func ChainHTTP(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] != nil {
			h = mws[i](h)
		}
	}
	return h
}
