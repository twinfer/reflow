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

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/reflow/creds"
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
// Lifecycle chaos (KillNode) is rooted on ContainerNode.Kill; network
// chaos primitives live on the optional Tx handle (non-nil only when
// the cluster was started with WithToxiproxy=true).
type ContainerCluster struct {
	Net   *testcontainers.DockerNetwork
	Nodes []*ContainerNode

	// Tx is the per-cluster Toxiproxy handle when opts.WithToxiproxy
	// is true; nil otherwise. Exposes Cut / Heal / CutDir / HealDir,
	// the per-pair partition primitives that replace bufconn's
	// PartitionMatrix.
	Tx *Toxiproxy

	// Partitioner matches the cluster's shard modulus and is what
	// loadgen.WorkloadConfig uses to derive each invocation's
	// IssuedInvocation.ShardID. Construction mirrors loadgen.Cluster
	// (routing.NewPartitioner(NumPartitionShards)).
	Partitioner routing.Partitioner

	// certs is the cluster's mesh PKI (CA + operator/e2e leaf). The test
	// process dials the admin port as operator/* via certs.operatorSpec();
	// each node mounts its own node/<id> leaf for the mTLS delivery mesh.
	certs *meshCerts
}

// NewContainerCluster brings up an mTLS reflowd cluster with N nodes on a
// fresh docker network. Defaults: N=3, NumShards=1. Delivery + admin run mTLS
// off a per-cluster CA (node/<id> + operator/e2e leaves) so the foundational
// Cedar policy authorizes the mesh + admin without a permissive bootstrap
// policy; ingress stays plaintext (anonymous SubmitInvocation is open).
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
	cfgPath := writeClusterConfigYAML(t, opts.N, opts.NumShards, opts.WithToxiproxy)
	// Mesh PKI: the cluster runs full mTLS so the foundational Cedar policy
	// authorizes node/* (delivery mesh) and operator/* (admin) without a
	// permissive bootstrap policy. Per-node leaves are minted here on the test
	// goroutine — t.Fatalf isn't safe from the parallel start goroutines below.
	certs := newMeshCerts(t)
	ingressTLS := certs.operatorClientTLS(t)
	nodeCertPaths := make([][2]string, opts.N)
	for i := 0; i < opts.N; i++ {
		crt, key := certs.nodeLeaf(t, uint64(i+1))
		nodeCertPaths[i] = [2]string{crt, key}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Toxiproxy sidecars first when enabled — they must be listening
	// before reflowd containers start, because dragonboat's first
	// gossip+raft exchange races to dial advertised RaftAddresses, and
	// those addresses already point at the sidecars via ExtraHosts.
	var tx *Toxiproxy
	if opts.WithToxiproxy {
		t1, err := startToxiproxy(t, ctx, nw, opts.N)
		if err != nil {
			t.Fatalf("e2e: start toxiproxy sidecars: %v", err)
		}
		tx = t1
	}

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
			n, err := startReflowdContainer(ctx, t, image, nw, cfgPath, certs.caCertPath, nodeCertPaths[i][0], nodeCertPaths[i][1], nodeID, opts.WithToxiproxy, opts.ExtraEnv, certs.operatorSpec())
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

	cluster := &ContainerCluster{
		Net:         nw,
		Nodes:       nodes,
		Tx:          tx,
		Partitioner: *routing.NewPartitioner(opts.NumShards),
		certs:       certs,
	}
	t.Cleanup(cluster.Close)
	if firstErr != nil {
		t.Fatalf("e2e: cluster bring-up: %v", firstErr)
	}
	// Ingress runs mTLS too (see operatorClientTLS) — hand each node the
	// operator client config so SubmitInvocation / DescribeInvocation dial the
	// ingress port over HTTP/2-over-TLS rather than h2c.
	for _, n := range cluster.Nodes {
		if n != nil {
			n.ingressTLS = ingressTLS
		}
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

// AnyLiveNode returns the first non-terminated node, or nil when
// every node has been killed. Satisfies loadgen.WorkloadCluster so
// loadgen.WorkloadConfig / loadgen.AwaitCompletion can drive this
// cluster without forking the workload runner.
func (c *ContainerCluster) AnyLiveNode() loadgen.Node {
	if c == nil {
		return nil
	}
	for _, n := range c.Nodes {
		if n == nil || !n.IsLive() {
			continue
		}
		return n
	}
	return nil
}

// FindPartitionLeader returns the node currently reported as leader
// of `shardID`, or nil. Polls every live node — leadership can rotate
// at any time, so the caller should treat the result as a hint.
// shardID == 0 is the metadata shard and is NOT discoverable via
// ingress.ListPartitions (that endpoint enumerates Host.Partitions()
// which excludes shard 0); callers wanting the metadata leader must
// use the admin/discovery RPC instead.
func (c *ContainerCluster) FindPartitionLeader(ctx context.Context, shardID uint64) *ContainerNode {
	if c == nil {
		return nil
	}
	for _, n := range c.Nodes {
		if n == nil || !n.IsLive() {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		parts, err := n.ListPartitions(pctx)
		cancel()
		if err != nil {
			continue
		}
		for _, p := range parts {
			if p.ShardID == shardID && p.IsLeader {
				return n
			}
		}
	}
	return nil
}

// AwaitPartitionLeader is FindPartitionLeader with a polling deadline.
// Returns the elected leader or nil on timeout.
func (c *ContainerCluster) AwaitPartitionLeader(ctx context.Context, shardID uint64, timeout time.Duration) *ContainerNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n := c.FindPartitionLeader(ctx, shardID); n != nil {
			return n
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// PeerIDs returns the cluster's node IDs in slot order. Convenience
// for chaos primitives keyed by NodeID (the Toxiproxy Cut/Heal API).
func (c *ContainerCluster) PeerIDs() []uint64 {
	if c == nil {
		return nil
	}
	out := make([]uint64, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		out = append(out, n.NodeID())
	}
	return out
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
	var lastErr error // last NodeLeadership probe error, surfaced on timeout
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n == nil {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			parts, err := n.ListPartitions(pctx)
			cancel()
			if err != nil {
				lastErr = err
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
	// Surface the last probe error: a non-nil one means the NodeLeadership
	// admin-mTLS dial is failing (so the cluster's leadership is unobservable,
	// not necessarily absent); a nil one means probes succeeded but no
	// partition shard ever reported a leader (a real election failure).
	if lastErr != nil {
		return fmt.Errorf("no partition leader observed within %s; last NodeLeadership probe error: %w", timeout, lastErr)
	}
	return fmt.Errorf("no partition leader elected within %s", timeout)
}

// startReflowdContainer brings up one reflowd node attached to nw with
// stable DNS alias reflowd-node<N>, the shared cluster config
// bind-mounted, and per-node env vars layered on top. Blocks until the
// ingress port becomes reachable (basic engine-boot signal); deeper
// readiness (raft leader, deployment registered) is asserted at the
// cluster level via AwaitAnyMetadataLeader.
//
// When `withToxiproxy` is true the advertised raft address points at
// the per-node sidecar (peer-target-N:targetRaftPort(N)); ExtraHosts
// entries route every peer-target-* hostname to this node's sidecar
// IP so dragonboat's outbound raft dials land on tox-from-N's
// per-target proxies.
func startReflowdContainer(ctx context.Context, t *testing.T, image string, nw *testcontainers.DockerNetwork, cfgPath, caCertPath, nodeCertPath, nodeKeyPath string, nodeID uint64, withToxiproxy bool, extraEnv map[string]string, operatorCreds creds.Spec) (*ContainerNode, error) {
	t.Helper()
	alias := fmt.Sprintf("reflowd-node%d", nodeID)
	ip := nodeIP(nodeID)
	raftAdvertisedDefault := fmt.Sprintf("%s:%s", alias, raftPort)
	raftAdv := raftAdvertisedDefault
	if withToxiproxy {
		// Sidecar mode: publish a target-keyed hostname + per-node port
		// so other reflowd containers route their outbound raft dial
		// through their own tox-from-* sidecar (see toxiproxy.go).
		raftAdv = raftAdvertisedThrough(nodeID)
	}
	adminAdvertised := fmt.Sprintf("%s:%s", alias, adminPort)

	req := testcontainers.ContainerRequest{
		Image:        image,
		Cmd:          []string{"run"},
		ExposedPorts: []string{ingressPort + "/tcp", adminPort + "/tcp"},
		Env: map[string]string{
			"REFLOW_CONFIG":                    "/etc/reflowd/config.yaml",
			"REFLOW_NODE_ID":                   fmt.Sprintf("%d", nodeID),
			"REFLOW_NODE_RAFT_ADDR":            fmt.Sprintf("0.0.0.0:%s", raftPort),
			"REFLOW_NODE_RAFT_ADVERTISED_ADDR": raftAdv,
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
		},
		Files: []testcontainers.ContainerFile{
			{HostFilePath: cfgPath, ContainerFilePath: "/etc/reflowd/config.yaml", FileMode: 0o644},
			{HostFilePath: caCertPath, ContainerFilePath: containerCAPath, FileMode: 0o644},
			{HostFilePath: nodeCertPath, ContainerFilePath: containerCrtPath, FileMode: 0o644},
			// World-readable: testcontainers copies mounts as root, but reflowd
			// runs as the nonroot image user — a 0600 key would be unreadable
			// and delivery/admin creds.Build would bail at startup. Ephemeral
			// throwaway test key, so 0644 inside the container is fine.
			{HostFilePath: nodeKeyPath, ContainerFilePath: containerKeyPath, FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort(ingressPort + "/tcp").
			WithStartupTimeout(2 * time.Minute),
	}
	// Per-test ExtraEnv overrides win against harness defaults. Used to
	// inject REFLOW_REBALANCE_* knobs and similar subsystem toggles
	// that aren't surfaced by a typed ContainerClusterOptions field.
	for k, v := range extraEnv {
		req.Env[k] = v
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
	// In toxiproxy mode, redirect every peer-target-* hostname this
	// container might dial (raft advertised hosts are peer-target-*)
	// to this node's sidecar IP. The sidecar's per-target proxies
	// then forward to the actual reflowd-node-* listener.
	if withToxiproxy {
		hosts := peerExtraHosts(nodeID)
		gcr.HostConfigModifier = func(hc *mobycontainer.HostConfig) {
			hc.ExtraHosts = append(hc.ExtraHosts, hosts...)
		}
	}

	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	ingressURL := "https://" + stripScheme(resolveHostMapped(t, ctx, c, ingressPort+"/tcp"))
	adminURL := resolveHostMapped(t, ctx, c, adminPort+"/tcp")
	return &ContainerNode{
		nodeID:          nodeID,
		raftAddr:        raftAdv,
		adminEndpoint:   adminAdvertised,
		ingressURL:      ingressURL,
		adminURLForTest: adminURL,
		container:       c,
		operatorCreds:   operatorCreds,
	}, nil
}

// writeClusterConfigYAML writes the cluster-wide YAML config to a fresh
// path under t.TempDir() and returns the path. Same file is mounted
// into every node — per-node deltas (NODE_ID, advertised addrs) layer
// on top via REFLOW_* env vars. When `withToxiproxy` is true the peer
// raft_addr field uses the per-target hostname (peer-target-N) that
// ExtraHosts routes via the local sidecar; matches what each node
// publishes via REFLOW_NODE_RAFT_ADVERTISED_ADDR.
func writeClusterConfigYAML(t *testing.T, n int, numShards uint64, withToxiproxy bool) string {
	t.Helper()
	type peer struct {
		NodeID uint64
		Raft   string
		Gossip string
	}
	type tmplData struct {
		NumShards uint64
		Peers     []peer
	}
	data := tmplData{NumShards: numShards}
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		raft := fmt.Sprintf("reflowd-node%d:%s", id, raftPort)
		if withToxiproxy {
			raft = raftAdvertisedThrough(id)
		}
		data.Peers = append(data.Peers, peer{
			NodeID: id,
			Raft:   raft,
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

const clusterConfigTmpl = `cluster:
  num_partition_shards: {{.NumShards}}
  peers:
{{- range .Peers}}
    - node_id: {{.NodeID}}
      raft_addr: {{.Raft}}
      gossip_addr: {{.Gossip}}
{{- end}}
ingress:
  creds:
    driver: tls
    tls:
      ca_file: /etc/reflowd/certs/ca.crt
      cert_file: /etc/reflowd/certs/node.crt
      key_file: /etc/reflowd/certs/node.key
delivery:
  creds:
    driver: tls
    tls:
      ca_file: /etc/reflowd/certs/ca.crt
      cert_file: /etc/reflowd/certs/node.crt
      key_file: /etc/reflowd/certs/node.key
admin:
  creds:
    driver: tls
    tls:
      ca_file: /etc/reflowd/certs/ca.crt
      cert_file: /etc/reflowd/certs/node.crt
      key_file: /etc/reflowd/certs/node.key
`
