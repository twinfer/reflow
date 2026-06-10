package ingress

import (
	"context"
	"maps"
	"net/http"
	"strings"
)

// metaHeaderPrefix is the inbound HTTP carrier for invocation metadata on the
// REST surface: a header `Reflw-Meta-Foo: bar` becomes metadata["foo"]="bar",
// surfaced to the durable handler via ctx.Metadata().
const metaHeaderPrefix = "Reflw-Meta-"

// metaCtxKey carries the lifted Reflw-Meta-* map from metaLiftHandler (the HTTP
// layer) to SubmitInvocation (the RPC layer), across the Vanguard transcoder.
// Vanguard derives the downstream request from the inbound one's context, so a
// value set here survives into the Connect handler.
type metaCtxKey struct{}

// metaLiftHandler lifts inbound Reflw-Meta-* headers into the request context so
// SubmitInvocation can merge them with the request body's metadata map. This is
// what preserves the REST header carrier through the Vanguard-transcoded path:
// the JSON body maps to the proto, and the headers ride the context alongside it.
func metaLiftHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m := liftMetaHeaders(r.Header); len(m) > 0 {
			r = r.WithContext(context.WithValue(r.Context(), metaCtxKey{}, m))
		}
		next.ServeHTTP(w, r)
	})
}

// metaFromContext returns the Reflw-Meta-* map lifted by metaLiftHandler, or nil.
func metaFromContext(ctx context.Context) map[string]string {
	m, _ := ctx.Value(metaCtxKey{}).(map[string]string)
	return m
}

// mergeMeta overlays header-lifted metadata onto the request body's map. Header
// values win: the Reflw-Meta-* carrier is the trusted seam operator middleware
// stamps verified facts into, so a body value must not override it. Either input
// may be nil; returns nil when both are empty.
func mergeMeta(header, body map[string]string) map[string]string {
	if len(header) == 0 {
		return body
	}
	out := make(map[string]string, len(header)+len(body))
	maps.Copy(out, body)
	maps.Copy(out, header)
	return out
}

// liftMetaHeaders lifts inbound Reflw-Meta-* headers (lowercased + stripped)
// into invocation metadata — the REST carrier for ctx.Metadata().
func liftMetaHeaders(h http.Header) map[string]string {
	var m map[string]string
	for k, vs := range h {
		if len(vs) == 0 {
			continue
		}
		if rest, ok := strings.CutPrefix(k, metaHeaderPrefix); ok && rest != "" {
			if m == nil {
				m = make(map[string]string)
			}
			m[strings.ToLower(rest)] = vs[0]
		}
	}
	return m
}
