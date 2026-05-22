//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"text/template"
	"time"

	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Internal container ports — every reflowd container listens on these.
// Inter-container traffic stays inside the docker user-defined network;
// only ingress and admin are host-mapped (so the test process can dial
// them from outside).
const (
	raftPort     = "9091"
	gossipPort   = "9101"
	deliveryPort = "9100"
	ingressPort  = "8080"
	adminPort    = "8082"
)

// ContainerCluster owns the docker network and the per-node reflowd
// containers. Test code interacts with it the same way it does with
// loadgen.Cluster today: pick a node, submit invocations, poll results.
// Chaos primitives (Cut/Heal, KillNode) are stub for PR 3 — they land
// with the Toxiproxy + lifecycle-chaos PRs later in the sequence.
type ContainerCluster struct {
	Net   *testcontainers.DockerNetwork
	Nodes []*ContainerNode
}

// NewContainerCluster brings up an insecure reflowd cluster with N
// nodes on a fresh docker network. Defaults: N=3, NumShards=1. mTLS
// and Toxiproxy land additively in later PRs.
//
// Blocks until every container's ingress listener accepts TCP and
// some node reports shard 0 as having a leader (the metadata leader
// election finishing). On any failure t.Fatalf is called and cleanup
// fires via t.Cleanup.
func NewContainerCluster(t *testing.T, opts ContainerClusterOptions) *ContainerCluster {
	t.Helper()
	SkipUnlessDocker(t)
	image := ReflowdImage(t)

	if opts.N == 0 {
		opts.N = 3
	}
	if opts.NumShards == 0 {
		opts.NumShards = 1
	}

	nw := newDockerNetwork(t)
	cfgPath := writeClusterConfigYAML(t, opts.N, opts.NumShards)
	policyPath := writePermissivePolicy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Bring nodes up in parallel — boot is dominated by dragonboat
	// gossip rendezvous, which can't make progress until enough peers
	// are listening, so sequential starts would serialize the wait.
	nodes := make([]*ContainerNode, opts.N)
	var (
		wg       sync.WaitGroup
		startMu  sync.Mutex
		firstErr error
	)
	for i := 0; i < opts.N; i++ {
		i := i
		nodeID := uint64(i + 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := startReflowdContainer(ctx, t, image, nw, cfgPath, policyPath, nodeID)
			startMu.Lock()
			defer startMu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("node %d: %w", nodeID, err)
				}
				return
			}
			nodes[i] = n
		}()
	}
	wg.Wait()

	cluster := &ContainerCluster{Net: nw, Nodes: nodes}
	t.Cleanup(cluster.Close)
	if firstErr != nil {
		t.Fatalf("e2e: cluster bring-up: %v", firstErr)
	}

	// Wait until some partition shard reports a leader. We can't observe
	// shard 0 (metadata) via ingress.ListPartitions — that endpoint
	// reports Host.Partitions() which excludes the metadata shard — but
	// any partition leader implies the metadata leader has already
	// bootstrapped the partition table (partitions can't elect before
	// their initialMembers are written by the metadata FSM).
	if err := cluster.AwaitAnyPartitionLeader(ctx, 2*time.Minute); err != nil {
		t.Fatalf("e2e: await partition leader: %v", err)
	}
	return cluster
}

// Close terminates every node and releases the network via t.Cleanup
// chains. Idempotent.
func (c *ContainerCluster) Close() {
	if c == nil {
		return
	}
	for _, n := range c.Nodes {
		n.Close()
	}
}

// AwaitAnyPartitionLeader polls ListPartitions across every node until
// some node reports any partition shard with IsLeader=true. This is the
// composite "cluster is functional" signal we can observe via ingress
// — the ingress endpoint enumerates Host.Partitions() which excludes
// shard 0, so we can't directly check the metadata leader; but any
// partition shard with a leader implies the metadata leader already
// bootstrapped the partition table and the partition's initialMembers
// were committed (partitions can't elect before that).
func (c *ContainerCluster) AwaitAnyPartitionLeader(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n == nil {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			parts, err := n.ListPartitions(pctx)
			cancel()
			if err != nil {
				continue
			}
			for _, p := range parts {
				if p.ShardID != 0 && p.IsLeader {
					return nil
				}
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("no partition leader elected within %s", timeout)
}

// startReflowdContainer brings up one reflowd node attached to nw with
// stable DNS alias reflowd-node<N>, the shared cluster config
// bind-mounted, and per-node env vars layered on top. Blocks until the
// ingress port becomes reachable (basic engine-boot signal); deeper
// readiness (raft leader, deployment registered) is asserted at the
// cluster level via AwaitAnyMetadataLeader.
func startReflowdContainer(ctx context.Context, t *testing.T, image string, nw *testcontainers.DockerNetwork, cfgPath, policyPath string, nodeID uint64) (*ContainerNode, error) {
	t.Helper()
	alias := fmt.Sprintf("reflowd-node%d", nodeID)
	ip := nodeIP(nodeID)
	raftAdvertised := fmt.Sprintf("%s:%s", alias, raftPort)
	adminAdvertised := fmt.Sprintf("%s:%s", alias, adminPort)

	req := testcontainers.ContainerRequest{
		Image:        image,
		Cmd:          []string{"run"},
		ExposedPorts: []string{ingressPort + "/tcp", adminPort + "/tcp"},
		Env: map[string]string{
			"REFLOW_CONFIG":                    "/etc/reflowd/config.yaml",
			"REFLOW_NODE_ID":                   fmt.Sprintf("%d", nodeID),
			"REFLOW_NODE_RAFT_ADDR":            fmt.Sprintf("0.0.0.0:%s", raftPort),
			"REFLOW_NODE_RAFT_ADVERTISED_ADDR": raftAdvertised,
			// Gossip uses a fixed IP because dragonboat's memberlist
			// rejects hostnames in AdvertiseAddress
			// (config.isValidAdvertiseAddress). Bind to all interfaces
			// for inter-container traffic; advertise the static IPAM IP.
			"REFLOW_NODE_GOSSIP_BIND_ADDR": fmt.Sprintf("0.0.0.0:%s", gossipPort),
			"REFLOW_NODE_GOSSIP_ADV_ADDR":  fmt.Sprintf("%s:%s", ip, gossipPort),
			"REFLOW_NODE_DELIVERY_ADDR":    fmt.Sprintf("%s:%s", alias, deliveryPort),
			"REFLOW_INGRESS_ADDR":          fmt.Sprintf("0.0.0.0:%s", ingressPort),
			"REFLOW_ADMIN_ADDR":            fmt.Sprintf("0.0.0.0:%s", adminPort),
			"REFLOW_METRICS_DISABLED":      "true",
			// Permissive policy lets the test process call Config and
			// ClusterCtl RPCs without an operator-SPIFFE cert. Insecure
			// e2e only — the mTLS variant (later PR) uses an issued
			// operator/* cert and the starter policy unchanged.
			"REFLOW_AUTH_POLICY_FILE": "/etc/reflowd/auth-policy.json",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      cfgPath,
				ContainerFilePath: "/etc/reflowd/config.yaml",
				FileMode:          0o644,
			},
			{
				HostFilePath:      policyPath,
				ContainerFilePath: "/etc/reflowd/auth-policy.json",
				FileMode:          0o644,
			},
		},
		WaitingFor: wait.ForListeningPort(ingressPort + "/tcp").
			WithStartupTimeout(2 * time.Minute),
	}
	// Stream reflowd container output to t.Logf when REFLOW_E2E_LOGS=1.
	// Surfaces dragonboat / engine logs needed to debug bring-up failures
	// without leaving them on for every green run.
	if os.Getenv("REFLOW_E2E_LOGS") == "1" {
		req.LogConsumerCfg = &testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&tLogConsumer{t: t, prefix: alias}},
		}
	}
	gcr := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	if err := network.WithNetwork([]string{alias}, nw)(&gcr); err != nil {
		return nil, fmt.Errorf("attach network: %w", err)
	}
	// Pin the container's IPv4 on this network so gossip's advertise
	// value (which we baked into the config above) matches what Docker
	// hands the container at attach time.
	parsedIP, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, fmt.Errorf("parse static ip %q: %w", ip, err)
	}
	gcr.EndpointSettingsModifier = func(eps map[string]*mobynet.EndpointSettings) {
		for _, ep := range eps {
			ep.IPAMConfig = &mobynet.EndpointIPAMConfig{IPv4Address: parsedIP}
		}
	}

	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	ingressURL := resolveHostMapped(t, ctx, c, ingressPort+"/tcp")
	adminURL := resolveHostMapped(t, ctx, c, adminPort+"/tcp")
	return &ContainerNode{
		nodeID:          nodeID,
		raftAddr:        raftAdvertised,
		adminEndpoint:   adminAdvertised,
		ingressURL:      ingressURL,
		adminURLForTest: adminURL,
		container:       c,
	}, nil
}

// writeClusterConfigYAML writes the cluster-wide YAML config to a fresh
// path under t.TempDir() and returns the path. Same file is mounted
// into every node — per-node deltas (NODE_ID, advertised addrs) layer
// on top via REFLOW_* env vars.
func writeClusterConfigYAML(t *testing.T, n int, numShards uint64) string {
	t.Helper()
	type peer struct {
		NodeID uint64
		Raft   string
		Gossip string
	}
	type tmplData struct {
		Shards []uint64
		Peers  []peer
	}
	data := tmplData{}
	for i := uint64(1); i <= numShards; i++ {
		data.Shards = append(data.Shards, i)
	}
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		data.Peers = append(data.Peers, peer{
			NodeID: id,
			Raft:   fmt.Sprintf("reflowd-node%d:%s", id, raftPort),
			Gossip: fmt.Sprintf("%s:%s", nodeIP(id), gossipPort),
		})
	}
	tmpl := template.Must(template.New("cfg").Parse(clusterConfigTmpl))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		t.Fatalf("e2e: config tmpl: %v", err)
	}
	path := filepath.Join(t.TempDir(), "cluster-config.yaml")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("e2e: write cluster config: %v", err)
	}
	return path
}

// tLogConsumer routes container stdout/stderr to a *testing.T's Logf.
// Used only when REFLOW_E2E_LOGS=1 — silent otherwise.
type tLogConsumer struct {
	t      *testing.T
	prefix string
}

func (c *tLogConsumer) Accept(l testcontainers.Log) {
	c.t.Logf("[%s %s] %s", c.prefix, l.LogType, string(l.Content))
}

// writePermissivePolicy emits a JSON authz policy that allows every
// reflow surface unconditionally. Used by the insecure e2e tier so the
// test process can hit Config / ClusterCtl RPCs without an operator
// SPIFFE cert. The mTLS variant (later PR) keeps the embedded starter
// policy and issues an operator/* leaf instead.
func writePermissivePolicy(t *testing.T) string {
	t.Helper()
	const body = `{
  "name": "reflow_e2e_permissive",
  "deny_rules": [],
  "allow_rules": [
    {"name": "ingress",    "request": {"paths": ["/reflow.ingress.v1.Ingress/*"]}},
    {"name": "delivery",   "request": {"paths": ["/reflow.delivery.v1.Delivery/*"]}},
    {"name": "config",     "request": {"paths": ["/reflow.config.v1.Config/*"]}},
    {"name": "clusterctl", "request": {"paths": ["/reflow.clusterctl.v1.ClusterCtl/*"]}},
    {"name": "webhooks",   "request": {"paths": ["/webhooks/*", "/webhooks/*/*", "/webhooks/*/*/*"]}},
    {"name": "rest_v1",    "request": {"paths": ["/v1/*", "/v1/*/*", "/v1/*/*/*", "/v1/*/*/*/*"]}}
  ]
}`
	path := filepath.Join(t.TempDir(), "auth-policy.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("e2e: write policy file: %v", err)
	}
	return path
}

const clusterConfigTmpl = `cluster:
  shards: [{{range $i, $s := .Shards}}{{if $i}}, {{end}}{{$s}}{{end}}]
  peers:
{{- range .Peers}}
    - node_id: {{.NodeID}}
      raft_addr: {{.Raft}}
      gossip_addr: {{.Gossip}}
{{- end}}
`
