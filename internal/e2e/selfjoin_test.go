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

	connect "connectrpc.com/connect"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tink-crypto/tink-go/v2/core/registry"

	"github.com/twinfer/reflw/internal/certmgr"
	tinkkmsblob "github.com/twinfer/reflw/pkg/kms/blob"
	"github.com/twinfer/reflw/pkg/reflw"
	"github.com/twinfer/reflw/pkg/reflwclient"
	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
)

// Container-side paths for the cluster CA material every node mounts to
// self-issue its mesh leaf. The CA cert is public; the signing key is the
// KMS-wrapped ciphertext, unwrapped at startup via the blobkms KEK.
const (
	containerCADir       = "/etc/reflwd/ca"
	containerClusterCA   = containerCADir + "/ca.crt"
	containerCAKeyCipher = containerCADir + "/cakey.enc"
	containerKEK         = containerCADir + "/kek.bin"
)

// TestE2E_SelfIssueAndJoin proves the decentralized mesh end-to-end on a
// real reflwd image:
//
//   - Self-issue: a 3-node seed cluster is configured with cluster_ca
//     (public cert + KMS-wrapped key) and NO static leaf files. Each node
//     unwraps the CA key and self-issues its own node/<id> leaf at
//     startup; the cluster only forms (raft leader elected, admin mTLS
//     answerable) if those self-issued leaves verify against each other.
//   - Join: a 4th node boots with cluster.join_existing=true and empty of
//     any pre-issued credential. It self-issues its leaf, then SelfJoins
//     the metadata leader over mTLS (gossip-discovered) — the container
//     only reaches "ingress listening" once reflw.Run's in-startup
//     SelfJoin succeeds. We then assert the leader's ListNodes shows all
//     four and the rebalancer promoted node 4 into the partition shard.
//
// This is the first test to exercise the real SelfJoin-over-mTLS path
// (the engine integration test drives the FSM body directly without admin
// listeners — see TestMultiNode_JoinExistingCluster_OperatorAddNode).
func TestE2E_SelfIssueAndJoin(t *testing.T) {
	SkipUnlessDocker(t)
	image := ReflowdImage(t)

	const seedN = 3
	const joinerID = 4
	const numShards = 1

	// CA + operator client leaf (reused for the admin-port dials below),
	// plus the KMS-sealed CA key the nodes self-issue from.
	certs := newMeshCerts(t)
	caMountDir := sealClusterCA(t, certs.ca)

	nw := newDockerNetwork(t)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// Seeds list only nodes 1..3 (static bootstrap quorum); the joiner
	// lists 1..4 + join_existing so its gossip seeds the live cluster and
	// StartOnDiskReplica uses join=true.
	seedCfg := writeJoinConfigYAML(t, 1, seedN, false, numShards)
	joinerCfg := writeJoinConfigYAML(t, 1, joinerID, true, numShards)

	// Bring up the seed cluster in parallel — boot is gated on gossip
	// rendezvous, so sequential starts would serialize the wait.
	nodes := make([]*selfJoinNode, joinerID)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i := 0; i < seedN; i++ {
		i := i
		id := uint64(i + 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := startSelfIssuingNode(ctx, t, image, nw, seedCfg, caMountDir, id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("seed node %d: %w", id, err)
			}
			nodes[i] = n
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("e2e: seed bring-up: %v", firstErr)
	}

	// The seed cluster only answers admin mTLS once its self-issued leaves
	// verify against each other — so a successful ListNodes==seedN is the
	// self-issue assertion.
	seedAdmin := nodes[0].adminAddr
	awaitNodeCount(ctx, t, seedAdmin, certs, seedN, 90*time.Second)
	t.Logf("seed cluster up: %d nodes self-issued + meshed", seedN)

	// Boot the joiner. reflw.Run runs SelfJoin in finishStartup *before*
	// starting shards or the ingress listener, so the container reaching
	// "ingress listening" (the WaitingFor below) already proves the joiner
	// self-issued a valid mesh leaf and SelfJoined over mTLS.
	joiner, err := startSelfIssuingNode(ctx, t, image, nw, joinerCfg, caMountDir, joinerID)
	if err != nil {
		t.Fatalf("e2e: joiner bring-up (self-issue + SelfJoin) failed: %v", err)
	}
	nodes[joinerID-1] = joiner
	t.Logf("joiner node %d started: self-issued + SelfJoined over mTLS", joinerID)

	// Explicit membership assertion: the leader's ListNodes shows all four.
	awaitNodeCount(ctx, t, seedAdmin, certs, joinerID, 60*time.Second)

	// And the rebalancer drove PROMOTE_TO_VOTER: node 4 is a member of the
	// partition shard's replica set in the partition table.
	awaitPartitionMember(ctx, t, seedAdmin, certs, joinerID, 60*time.Second)
	t.Logf("node %d promoted into the partition shard — join complete", joinerID)
}

// selfJoinNode is the minimal handle this test keeps per container: the
// container (for cleanup) and the host-mapped admin address.
type selfJoinNode struct {
	id        uint64
	container testcontainers.Container
	adminAddr string
}

// startSelfIssuingNode brings up one reflwd container configured for
// cluster_ca self-issuance (no static leaf files). cfgPath selects the
// seed vs joiner config; caMountDir holds ca.crt + kek.bin + cakey.enc.
func startSelfIssuingNode(ctx context.Context, t *testing.T, image string, nw *testcontainers.DockerNetwork, cfgPath, caMountDir string, nodeID uint64) (*selfJoinNode, error) {
	t.Helper()
	alias := fmt.Sprintf("reflwd-node%d", nodeID)
	ip := nodeIP(nodeID)

	req := testcontainers.ContainerRequest{
		Image:        image,
		Cmd:          []string{"run"},
		ExposedPorts: []string{ingressPort + "/tcp", adminPort + "/tcp"},
		Env: map[string]string{
			"REFLW_CONFIG":                    "/etc/reflwd/config.yaml",
			"REFLW_NODE_ID":                   fmt.Sprintf("%d", nodeID),
			"REFLW_NODE_RAFT_ADDR":            fmt.Sprintf("0.0.0.0:%s", raftPort),
			"REFLW_NODE_RAFT_ADVERTISED_ADDR": fmt.Sprintf("%s:%s", alias, raftPort),
			"REFLW_NODE_GOSSIP_BIND_ADDR":     fmt.Sprintf("0.0.0.0:%s", gossipPort),
			"REFLW_NODE_GOSSIP_ADV_ADDR":      fmt.Sprintf("%s:%s", ip, gossipPort),
			"REFLW_NODE_DELIVERY_ADDR":        fmt.Sprintf("%s:%s", alias, deliveryPort),
			"REFLW_INGRESS_ADDR":              fmt.Sprintf("0.0.0.0:%s", ingressPort),
			"REFLW_ADMIN_ADDR":                fmt.Sprintf("0.0.0.0:%s", adminPort),
			// Advertise a routable admin endpoint via gossip so the joiner's
			// SelfJoin can dial the metadata leader (the bind is a wildcard).
			"REFLW_ADMIN_ADVERTISED_ADDR": fmt.Sprintf("%s:%s", alias, adminPort),
			"REFLW_METRICS_DISABLED":      "true",
		},
		Files: []testcontainers.ContainerFile{
			{HostFilePath: cfgPath, ContainerFilePath: "/etc/reflwd/config.yaml", FileMode: 0o644},
			{HostFilePath: filepath.Join(caMountDir, "ca.crt"), ContainerFilePath: containerClusterCA, FileMode: 0o644},
			{HostFilePath: filepath.Join(caMountDir, "kek.bin"), ContainerFilePath: containerKEK, FileMode: 0o644},
			{HostFilePath: filepath.Join(caMountDir, "cakey.enc"), ContainerFilePath: containerCAKeyCipher, FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort(ingressPort + "/tcp").WithStartupTimeout(2 * time.Minute),
	}
	if os.Getenv("REFLW_E2E_LOGS") == "1" {
		req.LogConsumerCfg = &testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&tLogConsumer{t: t, prefix: alias}},
		}
	}

	gcr := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	if err := network.WithNetwork([]string{alias}, nw)(&gcr); err != nil {
		return nil, fmt.Errorf("attach network: %w", err)
	}
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
	n := &selfJoinNode{
		id:        nodeID,
		container: c,
		adminAddr: stripScheme(resolveHostMapped(t, ctx, c, adminPort+"/tcp")),
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return n, nil
}

// awaitNodeCount polls the admin ListNodes RPC (over operator mTLS) until
// it returns want members or the deadline elapses.
func awaitNodeCount(ctx context.Context, t *testing.T, adminAddr string, certs *meshCerts, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	var lastErr error
	for time.Now().Before(deadline) {
		cli, err := reflwclient.Dial(ctx, reflwclient.DialOptions{Addr: adminAddr, Creds: certs.operatorSpec()})
		if err == nil {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			resp, rerr := cli.Cluster.ListNodes(rctx, connect.NewRequest(&clusterctlv1.ListNodesRequest{}))
			cancel()
			_ = cli.Close()
			if rerr == nil {
				last = len(resp.Msg.GetNodes())
				if last >= want {
					return
				}
				lastErr = nil
			} else {
				lastErr = rerr
			}
		} else {
			lastErr = err
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("e2e: ListNodes wait cancelled: %v", ctx.Err())
		}
	}
	t.Fatalf("e2e: expected %d nodes within %s; last=%d lastErr=%v", want, timeout, last, lastErr)
}

// awaitPartitionMember polls ListPartitions until nodeID appears in some
// partition shard's replica set — i.e. the rebalancer's PROMOTE_TO_VOTER
// saga committed for the joiner.
func awaitPartitionMember(ctx context.Context, t *testing.T, adminAddr string, certs *meshCerts, nodeID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		cli, err := reflwclient.Dial(ctx, reflwclient.DialOptions{Addr: adminAddr, Creds: certs.operatorSpec()})
		if err == nil {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			resp, rerr := cli.Cluster.ListPartitions(rctx, connect.NewRequest(&clusterctlv1.ListPartitionsRequest{}))
			cancel()
			_ = cli.Close()
			if rerr == nil {
				for _, rs := range resp.Msg.GetTable().GetShards() {
					for _, nid := range rs.GetNodeIds() {
						if nid == nodeID {
							return
						}
					}
				}
				lastErr = nil
			} else {
				lastErr = rerr
			}
		} else {
			lastErr = err
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("e2e: ListPartitions wait cancelled: %v", ctx.Err())
		}
	}
	t.Fatalf("e2e: node %d not a partition member within %s; lastErr=%v", nodeID, timeout, lastErr)
}

// sealClusterCA mints the KMS material the nodes self-issue from: it
// writes the public CA cert, a fresh blobkms KEK keyset, and the CA
// signing key sealed under that KEK (AAD = reflw.ClusterCAKeyAAD, the
// same value buildNodeIdentity unwraps with) into a host temp dir whose
// three files are bind-mounted into every container.
func sealClusterCA(t *testing.T, ca *certmgr.CA) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "ca.crt"), ca.CertPEM)

	kekRaw, err := tinkkmsblob.InitKEK()
	if err != nil {
		t.Fatalf("e2e: InitKEK: %v", err)
	}
	writeFile(t, filepath.Join(dir, "kek.bin"), kekRaw)

	// Seal using the host path to the KEK; the container reads the same
	// keyset bytes via its own mount path, so the derived AEAD matches.
	kekURI := tinkkmsblob.URIPrefix + "file://" + filepath.Join(dir, "kek.bin")
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("e2e: GetKMSClient(%q): %v", kekURI, err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("e2e: GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt(ca.KeyPEM, []byte(reflw.ClusterCAKeyAAD))
	if err != nil {
		t.Fatalf("e2e: seal CA key: %v", err)
	}
	writeFile(t, filepath.Join(dir, "cakey.enc"), ct)
	return dir
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("e2e: write %s: %v", path, err)
	}
}

// writeJoinConfigYAML renders the cluster_ca + peers config for nodes
// [1..peerN]. joinExisting=true is set on the joiner's copy. The CA
// material paths are the container mount points; leaf_hosts covers the
// host-mapped admin address the test process dials over operator mTLS.
func writeJoinConfigYAML(t *testing.T, firstID, peerN uint64, joinExisting bool, numShards uint64) string {
	t.Helper()
	type peer struct {
		NodeID uint64
		Raft   string
		Gossip string
	}
	type tmplData struct {
		NumShards    uint64
		JoinExisting bool
		Peers        []peer
		CACert       string
		KeyBlobURI   string
		KeyKEKURI    string
	}
	data := tmplData{
		NumShards:    numShards,
		JoinExisting: joinExisting,
		CACert:       containerClusterCA,
		KeyBlobURI:   "file://" + containerCAKeyCipher,
		KeyKEKURI:    tinkkmsblob.URIPrefix + "file://" + containerKEK,
	}
	for id := firstID; id <= peerN; id++ {
		data.Peers = append(data.Peers, peer{
			NodeID: id,
			Raft:   fmt.Sprintf("reflwd-node%d:%s", id, raftPort),
			Gossip: fmt.Sprintf("%s:%s", nodeIP(id), gossipPort),
		})
	}
	tmpl := template.Must(template.New("joincfg").Parse(joinConfigTmpl))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		t.Fatalf("e2e: join config tmpl: %v", err)
	}
	name := "seed-config.yaml"
	if joinExisting {
		name = "joiner-config.yaml"
	}
	path := filepath.Join(t.TempDir(), name)
	writeFile(t, path, buf.Bytes())
	return path
}

const joinConfigTmpl = `cluster:
  num_partition_shards: {{.NumShards}}
  join_existing: {{.JoinExisting}}
  peers:
{{- range .Peers}}
    - node_id: {{.NodeID}}
      raft_addr: {{.Raft}}
      gossip_addr: {{.Gossip}}
{{- end}}
cluster_ca:
  ca_cert_file: {{.CACert}}
  key_blob_uri: {{.KeyBlobURI}}
  key_kek_uri: {{.KeyKEKURI}}
  leaf_hosts:
    - localhost
    - 127.0.0.1
`
