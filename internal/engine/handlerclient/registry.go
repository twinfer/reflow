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
type Dialer func(rawURL string) (Client, error)

// Registry maps deployment URL schemes to Dialer constructors and caches
// the resulting Clients by deployment_id. A Client is built lazily on
// first Get; subsequent Get calls with the same id return the cached
// Client. On URL change for the same id the stale Client is closed and
// a fresh one is built (covers operator deployment swaps and the
// embedded-handler-restarts-on-new-loopback-port case).
type Registry struct {
	mu      sync.Mutex
	dialers map[string]Dialer
	cache   map[string]*cacheEntry
}

type cacheEntry struct {
	url    string
	client Client
}

// NewRegistry returns an empty Registry with no dialers installed.
func NewRegistry() *Registry {
	return &Registry{
		dialers: make(map[string]Dialer),
		cache:   make(map[string]*cacheEntry),
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
// rawURL on first call. When the cached entry's URL differs from rawURL
// the stale Client is closed and a fresh one is built.
func (r *Registry) Get(deploymentID, rawURL string) (Client, error) {
	if deploymentID == "" {
		return nil, errors.New("handlerclient: empty deployment id")
	}
	r.mu.Lock()
	if e, ok := r.cache[deploymentID]; ok && e.url == rawURL {
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
	c, err := d(rawURL)
	if err != nil {
		return nil, fmt.Errorf("handlerclient: dial %s: %w", rawURL, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check the cache: a concurrent Get with the same URL won the
	// race (return its winner, close ours); a stale entry with a
	// different URL gets replaced (close it, install ours).
	if e, ok := r.cache[deploymentID]; ok {
		if e.url == rawURL {
			_ = c.Close()
			return e.client, nil
		}
		_ = e.client.Close()
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
