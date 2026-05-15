package handlerclient

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// Dialer constructs a Client for a deployment URL. The transport layer
// supplies one Dialer per scheme; Registry routes by scheme.
type Dialer func(rawURL string, opts ...ClientOption) (Client, error)

// ClientOption configures a Client at construction. Options are merged
// in argument order; later options override earlier ones for the same
// field.
type ClientOption func(*ClientConfig)

// ClientConfig is the resolved set of options after WithX functions are
// applied. Dialer implementations consult these fields directly.
type ClientConfig struct {
	// Codec encodes inner protocolv1 message payloads. Default is
	// protobuf via DefaultCodec.
	Codec Codec
}

// WithCodec replaces the default protobuf codec with the supplied one.
// The handler-side server must accept the same codec.
func WithCodec(c Codec) ClientOption {
	return func(cfg *ClientConfig) { cfg.Codec = c }
}

// applyOptions resolves opts against the default config.
func applyOptions(opts []ClientOption) ClientConfig {
	cfg := ClientConfig{Codec: DefaultCodec()}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return cfg
}

// Registry maps deployment URL schemes to Dialer constructors and caches
// the resulting Clients by deployment_id. A Client is built lazily on
// first Get; subsequent Get calls with the same id return the cached
// Client. Cache eviction (on URL/transport change) is the caller's
// responsibility — Evict the id when the persisted DeploymentRecord
// changes.
type Registry struct {
	mu      sync.Mutex
	dialers map[string]Dialer
	cache   map[string]*cacheEntry
	opts    []ClientOption
}

type cacheEntry struct {
	url    string
	client Client
}

// NewRegistry returns an empty Registry with no dialers installed. Pass
// opts to apply default ClientOption values to every Client built by
// this registry; per-Get opts override.
func NewRegistry(opts ...ClientOption) *Registry {
	return &Registry{
		dialers: make(map[string]Dialer),
		cache:   make(map[string]*cacheEntry),
		opts:    opts,
	}
}

// Register installs a Dialer for the given URL scheme. Scheme matching
// is case-insensitive. Re-registering the same scheme replaces the prior
// Dialer.
func (r *Registry) Register(scheme string, d Dialer) {
	r.mu.Lock()
	r.dialers[strings.ToLower(scheme)] = d
	r.mu.Unlock()
}

// Get returns a cached Client for deploymentID, building one against
// rawURL on first call. Subsequent calls with the same id return the
// cached Client even if rawURL differs; callers must Evict the id first
// when the persisted URL changes.
func (r *Registry) Get(deploymentID, rawURL string, perCall ...ClientOption) (Client, error) {
	if deploymentID == "" {
		return nil, errors.New("handlerclient: empty deployment id")
	}
	r.mu.Lock()
	if e, ok := r.cache[deploymentID]; ok {
		r.mu.Unlock()
		return e.client, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("handlerclient: parse url %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	d, ok := r.dialers[scheme]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("handlerclient: no dialer registered for scheme %q", scheme)
	}
	merged := append([]ClientOption(nil), r.opts...)
	merged = append(merged, perCall...)
	c, err := d(rawURL, merged...)
	if err != nil {
		return nil, fmt.Errorf("handlerclient: dial %s: %w", rawURL, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check the cache in case a concurrent Get won the race; close
	// our newly-built client and return the winner.
	if e, ok := r.cache[deploymentID]; ok {
		_ = c.Close()
		return e.client, nil
	}
	r.cache[deploymentID] = &cacheEntry{url: rawURL, client: c}
	return c, nil
}

// Evict drops the cached Client for deploymentID, closing it. Called by
// the engine when a DeploymentRecord is overwritten and the URL changes;
// in-flight Streams owned by the evicted Client continue independently.
func (r *Registry) Evict(deploymentID string) {
	r.mu.Lock()
	e, ok := r.cache[deploymentID]
	delete(r.cache, deploymentID)
	r.mu.Unlock()
	if ok {
		_ = e.client.Close()
	}
}

// Close drops every cached Client. Safe to call multiple times.
func (r *Registry) Close() error {
	r.mu.Lock()
	cache := r.cache
	r.cache = make(map[string]*cacheEntry)
	r.mu.Unlock()
	for _, e := range cache {
		_ = e.client.Close()
	}
	return nil
}

// SchemeOf returns the lowercased scheme of rawURL or "" on parse error.
// Convenience for callers that need to validate a URL before storing
// the deployment record.
func SchemeOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme)
}
