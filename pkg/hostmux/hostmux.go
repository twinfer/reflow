// Package hostmux dispatches HTTP requests to per-host handlers.
//
// Intended use is operator-owned routing in front of reflow's ingress:
// an operator's main() constructs a Mux, mounts per-tenant or
// per-vendor http.Handler trees under their respective hosts, and
// hands the Mux to reflow as one of ingress.Config.ExtraRoutes (or
// serves it on its own listener). Reflow's engine itself never reaches
// for this package — it is a primitive, not a feature.
//
// Capabilities are intentionally narrow: trust-aware host resolution,
// exact + wildcard lookup, atomic-swappable table for live reconfig
// (Mux.Set), and a JSON 404 envelope when no host matches.
//
// Multi-tenant SaaS pattern: the operator's tenant manager (config
// file watcher, control-plane stream, polling loop against a tenant
// DB) reacts to add/remove/rotate events by calling Mux.Set with the
// new (host, http.Handler) table. The swap is lock-free for inflight
// requests and updates take effect on the next request. Reflow does
// not durably store tenant config; secrets live in the operator's
// secret store and are bound into the per-host handler chain.
//
// Trust model — secure by default: X-Forwarded-Host and RFC 7239
// Forwarded headers are only honored when the immediate peer's IP
// falls inside a configured TrustPolicy.Proxies CIDR. With no trusted
// proxies configured, hostmux uses r.Host verbatim and ignores
// forwarded headers even if present. Deploy behind a TLS-terminating
// proxy you control and put its CIDR(s) in TrustPolicy.Proxies.
package hostmux

import (
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"sync/atomic"
)

// TrustPolicy controls which peers are allowed to set X-Forwarded-Host /
// Forwarded host=... headers. Proxies is the allowlist of immediate-peer
// CIDRs; an empty Proxies slice means forwarded-host headers are ignored
// entirely, regardless of source.
type TrustPolicy struct {
	Proxies []netip.Prefix
}

// Mux is a host-based HTTP dispatcher. It is safe for concurrent use:
// ServeHTTP reads via an atomic snapshot of the routing table, and Set
// replaces the table atomically without blocking inflight requests.
type Mux struct {
	table atomic.Pointer[table]
	trust TrustPolicy
}

// New constructs an empty Mux with the given trust policy. Set must be
// called at least once before requests are routed — until then every
// request falls through to a JSON 404.
func New(trust TrustPolicy) *Mux {
	m := &Mux{trust: trust}
	m.table.Store(&table{})
	return m
}

// Set replaces the routing table atomically. exact entries are matched
// case-insensitively on the full host; wildcard entries match by replacing
// the first dotted label with "*" (e.g. "*.acme.io" matches "stripe.acme.io"
// but not "acme.io" or "a.b.acme.io"). def is the catch-all handler when
// no host matches; nil def yields a JSON 404 instead.
func (m *Mux) Set(exact, wildcard map[string]http.Handler, def http.Handler) {
	t := &table{def: def}
	if len(exact) > 0 {
		t.exact = make(map[string]http.Handler, len(exact))
		for k, v := range exact {
			t.exact[strings.ToLower(k)] = v
		}
	}
	if len(wildcard) > 0 {
		t.wildcard = make(map[string]http.Handler, len(wildcard))
		for k, v := range wildcard {
			t.wildcard[strings.ToLower(k)] = v
		}
	}
	m.table.Store(t)
}

// Hosts returns the sorted list of exact + wildcard host keys currently
// installed. Useful for /healthz introspection and operator tooling. The
// catch-all default is not included.
func (m *Mux) Hosts() []string {
	t := m.table.Load()
	out := make([]string, 0, len(t.exact)+len(t.wildcard))
	for k := range t.exact {
		out = append(out, k)
	}
	for k := range t.wildcard {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ServeHTTP implements http.Handler. Resolution order: exact match first,
// then wildcard, then the catch-all default. If none match, a JSON 404 is
// emitted with no body besides the canonical error envelope.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := m.table.Load()
	host := m.resolveHost(r)
	if h, ok := t.exact[host]; ok {
		h.ServeHTTP(w, r)
		return
	}
	if wh := wildcardKey(host); wh != "" {
		if h, ok := t.wildcard[wh]; ok {
			h.ServeHTTP(w, r)
			return
		}
	}
	if t.def != nil {
		t.def.ServeHTTP(w, r)
		return
	}
	writeNotFound(w, host)
}

// table is the atomic snapshot of routing state.
type table struct {
	exact    map[string]http.Handler
	wildcard map[string]http.Handler
	def      http.Handler
}

// resolveHost applies TrustPolicy to derive the routing host. r.Host is
// the baseline; X-Forwarded-Host / RFC 7239 Forwarded headers are honored
// only when the immediate peer is in TrustPolicy.Proxies. The returned
// host is lowercased and port-stripped.
func (m *Mux) resolveHost(r *http.Request) string {
	host := r.Host
	if m.peerTrusted(r) {
		if h := r.Header.Get("X-Forwarded-Host"); h != "" {
			if comma := strings.IndexByte(h, ','); comma >= 0 {
				h = h[:comma]
			}
			host = strings.TrimSpace(h)
		} else if h := forwardedHost(r.Header.Get("Forwarded")); h != "" {
			host = h
		}
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(strings.TrimSpace(host))
}

// peerTrusted reports whether r.RemoteAddr's IP is inside any configured
// TrustPolicy.Proxies prefix. Empty Proxies → never trusted.
func (m *Mux) peerTrusted(r *http.Request) bool {
	if len(m.trust.Proxies) == 0 {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	for _, p := range m.trust.Proxies {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// forwardedHost parses an RFC 7239 Forwarded header and returns the
// host=... value, or "" if absent. Quoted values are unquoted.
func forwardedHost(header string) string {
	if header == "" {
		return ""
	}
	// Only the first element matters for host resolution.
	if comma := strings.IndexByte(header, ','); comma >= 0 {
		header = header[:comma]
	}
	for pair := range strings.SplitSeq(header, ";") {
		before, after, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key := strings.TrimSpace(before)
		if !strings.EqualFold(key, "host") {
			continue
		}
		val := strings.TrimSpace(after)
		val = strings.Trim(val, `"`)
		return val
	}
	return ""
}

// wildcardKey converts a resolved host into the matching wildcard key
// ("a.acme.io" → "*.acme.io"). Returns "" for bare top-level labels.
func wildcardKey(host string) string {
	dot := strings.IndexByte(host, '.')
	if dot < 0 {
		return ""
	}
	return "*" + host[dot:]
}

// writeNotFound emits the canonical JSON 404 used both by hostmux's own
// catch-all and (via the same shape) by the REST ingress error mapper, so
// clients hitting an unknown host see the same envelope as any /v1/*
// error.
func writeNotFound(w http.ResponseWriter, host string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":    "not_found",
		"message": "no route for host " + host,
	})
}
