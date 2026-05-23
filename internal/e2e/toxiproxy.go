//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Toxiproxy is the per-cluster network-chaos handle. It owns N sidecar
// toxiproxy containers (one per source reflowd node) and exposes Cut /
// Heal primitives that match the bufconn PartitionMatrix's API:
// `Cut(a, b)` is symmetric and unordered, equivalent to "the network
// link between nodes a and b is dropped"; `CutDir(from, to)` is the
// directional primitive used by asymmetric-partition tests later.
//
// Topology rationale (see toxiproxy_test.go in this package and the
// design doc):
//
//   - dragonboat's gossip publishes exactly one RaftAddress per node,
//     which every peer dials when sending raft traffic. That single
//     advertised string makes per-target partitioning natural but
//     per-source-target ("cut A→B but leave C→B alive") structurally
//     impossible at the address layer.
//
//   - We recover per-pair granularity by giving each reflowd container
//     its own per-source toxiproxy sidecar and rewriting peer hostnames
//     in that container's /etc/hosts to point at its sidecar's IP. The
//     advertised port differs per target so each sidecar can host one
//     proxy per (this source → that target) on a stable listen port.
//
//   - Disabling tox-from-A's `to-B` proxy cuts A→B without touching
//     anyone else's traffic to B. Cut(A, B) does both directions.
type Toxiproxy struct {
	containers map[uint64]testcontainers.Container // sidecar per source nodeID
	clients    map[uint64]*toxiclient.Client       // toxiproxy HTTP API per sidecar

	mu      sync.Mutex
	proxies map[proxyKey]*toxiclient.Proxy // (from, to) → live proxy handle
}

// proxyKey identifies a directional raft proxy: traffic from source
// nodeID to target nodeID.
type proxyKey struct{ from, to uint64 }

// targetHost returns the hostname every reflowd container uses to
// reach the raft listener of node `target`. The hostname is the same
// cluster-wide; the per-source ExtraHosts override is what redirects
// each source to its own sidecar. Bound to /etc/hosts via Docker
// HostConfig.ExtraHosts.
func targetHost(target uint64) string {
	return fmt.Sprintf("peer-target-%d", target)
}

// targetRaftPort returns the advertised raft port for node `target`.
// Each node gets a unique port so a sidecar's per-target proxies can
// listen on distinct ports without colliding. The bind port (where
// reflowd actually listens for raft) is still `raftPort` from
// cluster.go; toxiproxy upstreams point at <reflowd-node-N:raftPort>.
func targetRaftPort(target uint64) int {
	return 19000 + int(target)
}

// sidecarIP returns the static IPAM address of the toxiproxy sidecar
// assigned to source nodeID. Lives in the 10.X.0.20+ range (per-process
// X chosen in network.go) so it never collides with reflowd-nodeIP
// (10.X.0.10+).
func sidecarIP(source uint64) string {
	processSubnetMu.Lock()
	defer processSubnetMu.Unlock()
	return fmt.Sprintf("10.%d.0.%d", clusterOctet, 20+source)
}

// raftAdvertisedThrough is what reflowd publishes through gossip as its
// RaftAddress. Every peer's container resolves the hostname via the
// per-container ExtraHosts to the local sidecar; the port encodes
// which target the traffic is for.
func raftAdvertisedThrough(target uint64) string {
	return fmt.Sprintf("%s:%d", targetHost(target), targetRaftPort(target))
}

// peerExtraHosts builds the docker --add-host entries reflowd-node-K
// needs so that dragonboat's `dial peer-target-J` lands on tox-from-K.
// For self (J == K) the route still points at the sidecar — raft
// short-circuits self in practice, but a self-dial through the sidecar
// is also fine because tox-from-K hosts the to-K proxy as well so the
// sidecar can route a node's self-traffic.
func peerExtraHosts(source uint64) []string {
	ip := sidecarIP(source)
	// 8 is comfortably more than the chaos-test ceiling (N<=5 today).
	// Returning a slice keyed by the cluster size would couple this
	// helper to NewContainerCluster; we just emit a broad block of
	// peer-target-N → sidecar entries and the unused ones are harmless.
	out := make([]string, 0, 8)
	for j := uint64(1); j <= 8; j++ {
		out = append(out, fmt.Sprintf("%s:%s", targetHost(j), ip))
	}
	return out
}

// startToxiproxy brings up the N toxiproxy sidecars in parallel.
// Each sidecar gets a static IP and a stable DNS alias on `nw` so the
// reflowd containers' /etc/hosts overrides have a fixed target. After
// every sidecar is healthy (control API responds at /version), the
// per-source proxies are created via the toxiproxy HTTP client; the
// returned Toxiproxy can then mutate proxy state mid-test.
func startToxiproxy(t testing.TB, ctx context.Context, nw *testcontainers.DockerNetwork, n int) (*Toxiproxy, error) {
	t.Helper()
	tox := &Toxiproxy{
		containers: make(map[uint64]testcontainers.Container, n),
		clients:    make(map[uint64]*toxiclient.Client, n),
		proxies:    make(map[proxyKey]*toxiclient.Proxy),
	}

	// Sidecars in parallel — sidecar boot is ~1-3s each and they have
	// no inter-dependency. Cleanup is attached via t.Cleanup once each
	// container handle is captured.
	type result struct {
		nodeID uint64
		c      testcontainers.Container
		cli    *toxiclient.Client
		err    error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		nodeID := uint64(i + 1)
		go func(nodeID uint64) {
			c, cli, err := startOneSidecar(ctx, nw, nodeID)
			results <- result{nodeID: nodeID, c: c, cli: cli, err: err}
		}(nodeID)
	}
	var firstErr error
	for i := 0; i < n; i++ {
		r := <-results
		if r.c != nil {
			tox.containers[r.nodeID] = r.c
			t.Cleanup(func() {
				_ = testcontainers.TerminateContainer(r.c)
			})
		}
		if r.cli != nil {
			tox.clients[r.nodeID] = r.cli
		}
		if r.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("toxiproxy sidecar %d: %w", r.nodeID, r.err)
		}
	}
	if firstErr != nil {
		return tox, firstErr
	}

	// Create one proxy per (source, target) ordered pair, including the
	// self-pair. Self-traffic is harmless because raft short-circuits
	// self in dragonboat, but creating the proxy keeps the topology
	// uniform across sources (easier to reason about) at the cost of
	// N extra always-enabled proxies cluster-wide.
	for source := uint64(1); source <= uint64(n); source++ {
		cli := tox.clients[source]
		for target := uint64(1); target <= uint64(n); target++ {
			listen := fmt.Sprintf("0.0.0.0:%d", targetRaftPort(target))
			upstream := fmt.Sprintf("reflowd-node%d:%s", target, raftPort)
			name := fmt.Sprintf("from%d_to%d_raft", source, target)
			p, err := cli.CreateProxy(name, listen, upstream)
			if err != nil {
				return tox, fmt.Errorf("create proxy %s: %w", name, err)
			}
			tox.proxies[proxyKey{from: source, to: target}] = p
		}
	}
	return tox, nil
}

// startOneSidecar runs a single toxiproxy container on the cluster
// network with a stable IP + DNS alias. The control API on 8474 is
// the only port the test process talks to; proxy listen ports are
// internal-only (reachable from peer reflowd containers but never
// host-mapped — chaos tests don't need them visible from outside).
func startOneSidecar(ctx context.Context, nw *testcontainers.DockerNetwork, source uint64) (testcontainers.Container, *toxiclient.Client, error) {
	alias := fmt.Sprintf("tox-from-%d", source)
	ip := sidecarIP(source)
	req := testcontainers.ContainerRequest{
		Image:        toxiproxyImage,
		ExposedPorts: []string{"8474/tcp"},
		Cmd:          []string{"-host=0.0.0.0"},
		WaitingFor: wait.ForHTTP("/version").
			WithPort("8474/tcp").
			WithStartupTimeout(60 * time.Second),
	}
	gcr := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	if err := network.WithNetwork([]string{alias}, nw)(&gcr); err != nil {
		return nil, nil, fmt.Errorf("attach network: %w", err)
	}
	parsedIP, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, nil, fmt.Errorf("parse sidecar ip %q: %w", ip, err)
	}
	gcr.EndpointSettingsModifier = func(eps map[string]*mobynet.EndpointSettings) {
		for _, ep := range eps {
			ep.IPAMConfig = &mobynet.EndpointIPAMConfig{IPv4Address: parsedIP}
		}
	}
	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		return c, nil, fmt.Errorf("start: %w", err)
	}
	// The host-mapped control endpoint is how the test process drives
	// the toxiproxy API; the docker-internal IP is what reflowd
	// containers route through.
	host, err := c.Host(ctx)
	if err != nil {
		return c, nil, fmt.Errorf("host: %w", err)
	}
	port, err := c.MappedPort(ctx, "8474/tcp")
	if err != nil {
		return c, nil, fmt.Errorf("mapped port: %w", err)
	}
	cli := toxiclient.NewClient(fmt.Sprintf("http://%s:%s", host, port.Port()))
	return c, cli, nil
}

// Cut blocks raft traffic in both directions between a and b. No-op
// when a == b. Idempotent (toggling an already-disabled proxy is a
// silent success in toxiproxy).
func (t *Toxiproxy) Cut(a, b uint64) error {
	if a == b {
		return nil
	}
	if err := t.CutDir(a, b); err != nil {
		return err
	}
	return t.CutDir(b, a)
}

// Heal restores both directions of the link between a and b. No-op
// when a == b. Safe to call on a pair that was never Cut.
func (t *Toxiproxy) Heal(a, b uint64) error {
	if a == b {
		return nil
	}
	if err := t.HealDir(a, b); err != nil {
		return err
	}
	return t.HealDir(b, a)
}

// CutDir blocks raft traffic in the single direction from → to. Used
// by asymmetric-partition tests (later in the chaos PR sequence).
func (t *Toxiproxy) CutDir(from, to uint64) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return p.Disable()
}

// HealDir restores raft traffic in the single direction from → to.
func (t *Toxiproxy) HealDir(from, to uint64) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return p.Enable()
}

// Isolate cuts every link between `node` and the rest of the cluster.
// Equivalent to calling Cut(node, peer) for each peer in `peers`.
// Returns the first error encountered; later cuts still attempted so
// the partition is best-effort applied even on a partial-failure run.
func (t *Toxiproxy) Isolate(node uint64, peers []uint64) error {
	var firstErr error
	for _, p := range peers {
		if p == node {
			continue
		}
		if err := t.Cut(node, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HealAll re-enables every proxy in the topology AND removes every
// chaos_* toxic the helpers below installed. Convenience for tests
// that want to drop their chaos and let the cluster converge before
// the next phase.
func (t *Toxiproxy) HealAll() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for _, p := range t.proxies {
		if err := p.Enable(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := clearChaosToxics(p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Latency injects one-way delay (with optional jitter) on packets the
// `from` node sends to `to`. Replaces any existing `chaos_latency` toxic
// on the proxy. Both stream directions are independent — call twice
// (or use LatencyBoth) to slow both halves of a link.
//
// Toxiproxy spec: `latency` toxic attributes are `latency` (ms) and
// `jitter` (ms); see https://github.com/Shopify/toxiproxy#latency.
func (t *Toxiproxy) Latency(from, to uint64, latency, jitter time.Duration) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return replaceToxic(p, "chaos_latency", "latency", "upstream", toxiclient.Attributes{
		"latency": int(latency / time.Millisecond),
		"jitter":  int(jitter / time.Millisecond),
	})
}

// LatencyBoth applies Latency in both directions between a and b. Useful
// for symmetric "slow link" scenarios.
func (t *Toxiproxy) LatencyBoth(a, b uint64, latency, jitter time.Duration) error {
	if a == b {
		return nil
	}
	if err := t.Latency(a, b, latency, jitter); err != nil {
		return err
	}
	return t.Latency(b, a, latency, jitter)
}

// Bandwidth throttles the from→to direction to `rateKBps` kilobytes per
// second. Replaces any existing `chaos_bandwidth` toxic.
//
// Toxiproxy spec: `bandwidth` toxic attribute is `rate` (KB/s); see
// https://github.com/Shopify/toxiproxy#bandwidth.
func (t *Toxiproxy) Bandwidth(from, to uint64, rateKBps int) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return replaceToxic(p, "chaos_bandwidth", "bandwidth", "upstream", toxiclient.Attributes{
		"rate": rateKBps,
	})
}

// SlowClose delays TCP socket close on the from→to proxy by `delay`,
// emulating buggy peers that linger after a FIN. Replaces any existing
// `chaos_slow_close` toxic.
//
// Toxiproxy spec: `slow_close` toxic attribute is `delay` (ms); see
// https://github.com/Shopify/toxiproxy#slow_close.
func (t *Toxiproxy) SlowClose(from, to uint64, delay time.Duration) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return replaceToxic(p, "chaos_slow_close", "slow_close", "upstream", toxiclient.Attributes{
		"delay": int(delay / time.Millisecond),
	})
}

// ClearToxics removes every chaos_* toxic from the from→to proxy. Leaves
// the proxy enabled state untouched (use Heal to also re-enable a cut).
func (t *Toxiproxy) ClearToxics(from, to uint64) error {
	p, err := t.lookup(from, to)
	if err != nil {
		return err
	}
	return clearChaosToxics(p)
}

// replaceToxic deletes any existing toxic with `name` then re-adds it
// with the given parameters. Toxiproxy rejects AddToxic when a toxic
// of the same name already exists, so this idempotent variant is what
// tests want when iterating chaos intensity.
func replaceToxic(p *toxiclient.Proxy, name, kind, stream string, attrs toxiclient.Attributes) error {
	_ = p.RemoveToxic(name) // best-effort: 404 when absent
	_, err := p.AddToxic(name, kind, stream, 1.0, attrs)
	if err != nil {
		return fmt.Errorf("AddToxic %s (%s): %w", name, kind, err)
	}
	return nil
}

// clearChaosToxics removes every chaos_* toxic from one proxy. Used by
// both ClearToxics and HealAll. The `chaos_` prefix is the convention
// every helper above stamps so we never touch toxics installed by
// external code.
func clearChaosToxics(p *toxiclient.Proxy) error {
	toxics, err := p.Toxics()
	if err != nil {
		return fmt.Errorf("list toxics on %s: %w", p.Name, err)
	}
	var firstErr error
	for _, tx := range toxics {
		if len(tx.Name) < 6 || tx.Name[:6] != "chaos_" {
			continue
		}
		if err := p.RemoveToxic(tx.Name); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("remove %s: %w", tx.Name, err)
		}
	}
	return firstErr
}

func (t *Toxiproxy) lookup(from, to uint64) (*toxiclient.Proxy, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, ok := t.proxies[proxyKey{from: from, to: to}]
	if !ok {
		return nil, fmt.Errorf("toxiproxy: no proxy for %d->%d", from, to)
	}
	return p, nil
}

// toxiproxyImage is pinned for reproducibility. Override via
// REFLOW_E2E_TOXIPROXY_IMAGE; not exposed via the e2e API since this
// is a test-tier dependency, not something operators configure.
const toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"
