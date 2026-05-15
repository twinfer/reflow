package engine_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/admin"
	enginesnap "github.com/twinfer/reflow/internal/engine/snapshot"

	"github.com/twinfer/reflow/internal/pki"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/pkg/sdk"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	"gocloud.dev/blob"
)

// adminRig augments a nodeRig with an admin gRPC server. Admin runs
// without the auth interceptor in tests because that interceptor requires
// mTLS-verified client certs — the dedicated TLS-rejection tests rebuild
// the rig with TLS+interceptor enabled.
type adminRig struct {
	nodeRig *nodeRig
	adminLn net.Listener
	adminGS *grpc.Server
}

func (a *adminRig) addr() string { return a.adminLn.Addr().String() }

func (a *adminRig) close() {
	if a.adminGS != nil {
		a.adminGS.GracefulStop()
	}
	if a.adminLn != nil {
		_ = a.adminLn.Close()
	}
	if a.nodeRig != nil {
		a.nodeRig.Close()
	}
}

// startAdminInsecure wires an admin gRPC server on the local Host
// without TLS. For tests that exercise admin functionality without
// caring about authentication.
func startAdminInsecure(t *testing.T, r *nodeRig) *adminRig {
	t.Helper()
	srv, err := admin.NewServer(admin.Config{
		Host:   r.Host,
		Runner: r.Host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", freeLocalAddr(t))
	if err != nil {
		t.Fatalf("listen admin: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() {
		if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("admin Serve exited: %v", err)
		}
	}()
	return &adminRig{nodeRig: r, adminLn: ln, adminGS: gs}
}

// dialInsecureAdmin opens a plaintext gRPC connection to addr and
// returns the typed client + a cleanup.
func dialInsecureAdmin(t *testing.T, addr string) (adminv1.AdminClient, func()) {
	t.Helper()
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return adminv1.NewAdminClient(cc), func() { _ = cc.Close() }
}

// awaitMetadataLeaderRig polls until a metadata leader is found and
// returns the rig that leads. Avoids races where the leader rotates
// between cluster bring-up and the test's first admin RPC.
func awaitMetadataLeaderRig(t *testing.T, rigs []*nodeRig, timeout time.Duration) *nodeRig {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range rigs {
			if mr := r.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
				return r
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no metadata leader within timeout")
	return nil
}

// awaitMembership polls Membership(0) until at least min rows show up.
func awaitMembership(t *testing.T, leader *nodeRig, min int, timeout time.Duration) []*enginev1.NodeMembership {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		members, err := leader.Host.Membership(ctx)
		cancel()
		if err == nil && len(members) >= min {
			return members
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("membership never reached >= %d rows", min)
	return nil
}

// TestAdminListNodes confirms the leader's admin surface
// returns every registered node. Membership is bootstrapped by the
// metadata-leader's RegisterNode propose loop.
func TestAdminListNodes(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	ar := startAdminInsecure(t, leader)
	defer ar.close()

	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ListNodes(ctx, &adminv1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(resp.GetNodes()) != 3 {
		t.Fatalf("ListNodes returned %d; want 3", len(resp.GetNodes()))
	}
	seen := map[uint64]bool{}
	for _, m := range resp.GetNodes() {
		seen[m.GetNodeId()] = true
	}
	for id := uint64(1); id <= 3; id++ {
		if !seen[id] {
			t.Errorf("ListNodes missing node %d: %v", id, seen)
		}
	}
}

// TestAdminListPartitions returns the bootstrap partition
// table observable via the admin surface.
func TestAdminListPartitions(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	ar := startAdminInsecure(t, leader)
	defer ar.close()
	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	// Allow the leader's bootstrap UpdatePartitionTable to land.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.ListPartitions(ctx, &adminv1.ListPartitionsRequest{})
		if err == nil && resp.GetTable() != nil && len(resp.GetTable().GetShards()) == 3 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("ListPartitions never returned a 3-shard table")
}

// TestAdminRemoveNode_LogicallyEvicts proposes EvictNode and
// asserts the apply path marks last_seen_ms=0 and enqueues DELETE_REPLICA
// steps for every shard the node hosted.
func TestAdminRemoveNode_LogicallyEvicts(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	// Pick a victim that is not the metadata leader (RemoveNode refuses
	// self-evict).
	victim := uint64(0)
	for _, r := range rigs {
		if r != leader {
			victim = r.Host.NodeID()
			break
		}
	}
	if victim == 0 {
		t.Fatal("could not pick a non-leader victim")
	}

	ar := startAdminInsecure(t, leader)
	defer ar.close()
	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.RemoveNode(ctx, &adminv1.RemoveNodeRequest{NodeId: victim}); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	// The apply arm: last_seen_ms == 0 + DELETE_REPLICA pending for any
	// shard the node was in.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		members, _ := leader.Host.Membership(ctx)
		pt, _ := leader.Host.PartitionTable(ctx)
		marked := false
		for _, m := range members {
			if m.GetNodeId() == victim && m.GetLastSeenMs() == 0 {
				marked = true
				break
			}
		}
		hasPending := false
		if pt != nil {
			for _, p := range pt.GetPending() {
				if p.GetKind() == enginev1.RebalanceStep_DELETE_REPLICA && p.GetRemoveNodeId() == victim {
					hasPending = true
					break
				}
			}
		}
		if marked && hasPending {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("EvictNode apply never zeroed last_seen_ms + enqueued DELETE_REPLICA for node %d", victim)
}

// TestAdminMutualTLS_RejectsUnsignedClient builds an admin
// server wired with operator-CA mTLS and asserts that a client without
// any cert cannot complete the handshake. Sanity check that the
// transport layer enforces auth.
func TestAdminMutualTLS_RejectsUnsignedClient(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)
	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)

	dir := t.TempDir()
	tlsSpec, opCert, opKey, caFile := writeAdminTLSFixtures(t, dir)

	// Build a TLS-protected admin server. The auth interceptor refuses
	// callers whose principal does not match the embedded policy, but
	// Require+VerifyClientCert at the TLS layer should reject empty
	// client certs first.
	srv, err := admin.NewServer(admin.Config{
		Host:   leader.Host,
		Runner: leader.Host.MetadataRunner(),
	})
	if err != nil {
		t.Fatal(err)
	}
	serverCreds, err := creds.Build(creds.Spec{Driver: creds.DriverTLS, TLS: &tlsSpec}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if serverCreds.Close != nil {
			_ = serverCreds.Close()
		}
	}()
	ln, err := net.Listen("tcp", freeLocalAddr(t))
	if err != nil {
		t.Fatal(err)
	}
	unaryIc, streamIc, authCloser, err := auth.NewServerInterceptors(auth.Config{
		Extractor: &auth.SPIFFEExtractor{TrustDomain: testTrustDomain},
	})
	if err != nil {
		t.Fatalf("NewServerInterceptors: %v", err)
	}
	defer func() {
		if authCloser != nil {
			_ = authCloser()
		}
	}()
	gs := grpc.NewServer(
		grpc.Creds(serverCreds.Server),
		grpc.ChainUnaryInterceptor(unaryIc),
		grpc.ChainStreamInterceptor(streamIc),
	)
	srv.Register(gs)
	go func() {
		_ = gs.Serve(ln)
	}()
	t.Cleanup(func() { gs.GracefulStop(); _ = ln.Close() })

	// 1) creds.Build refuses a TLS spec without a leaf keypair.
	if _, err := creds.Build(creds.Spec{
		Driver: creds.DriverTLS,
		TLS:    &creds.TLSSpec{CAFile: caFile, TrustDomain: testTrustDomain},
	}, nil); err == nil {
		t.Fatal("expected creds.Build to reject empty operator cert; got nil err")
	}

	// 2) Dial gRPC with a TLS config that trusts the server CA but
	//    presents no client cert. The gRPC call must fail because the
	//    server's ClientAuth = RequireAndVerifyClientCert rejects the
	//    empty client Certificate message.
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caBytes) {
		t.Fatal("failed to parse node CA PEM")
	}
	noClientCert := &tls.Config{
		ServerName: "127.0.0.1",
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS13,
	}
	ccBad, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(noClientCert)))
	if err != nil {
		t.Fatalf("grpc.NewClient (no client cert): %v", err)
	}
	defer ccBad.Close()
	badCtx, badCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer badCancel()
	if _, err := adminv1.NewAdminClient(ccBad).ListNodes(badCtx, &adminv1.ListNodesRequest{}); err == nil {
		t.Fatal("expected ListNodes to fail when client presents no cert; got nil")
	}

	// Sanity: with the proper operator cert + node CA, dial succeeds.
	operatorCreds, err := creds.Build(creds.Spec{
		Driver: creds.DriverTLS,
		TLS: &creds.TLSSpec{
			CAFile:      caFile,
			CertFile:    opCert,
			KeyFile:     opKey,
			TrustDomain: testTrustDomain,
			ServerName:  "127.0.0.1",
		},
	}, nil)
	if err != nil {
		t.Fatalf("creds.Build (operator): %v", err)
	}
	defer func() {
		if operatorCreds.Close != nil {
			_ = operatorCreds.Close()
		}
	}()
	cc, err := grpc.NewClient(ln.Addr().String(), operatorCreds.ClientDial...)
	if err != nil {
		t.Fatalf("grpc.NewClient with operator creds: %v", err)
	}
	defer cc.Close()
	cli := adminv1.NewAdminClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.ListNodes(ctx, &adminv1.ListNodesRequest{}); err != nil {
		t.Fatalf("operator-authenticated ListNodes failed: %v", err)
	}
}

// TestSnapshot_PartitionExportAndArchive triggers an exported
// snapshot through the engine.Host helper, archives it via the fs
// repository, and confirms a non-empty Fetch round-trip.
func TestSnapshot_PartitionExportAndArchive(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	// Find a leader for partition shard 1.
	deadline := time.Now().Add(15 * time.Second)
	var owner *nodeRig
	for time.Now().Before(deadline) && owner == nil {
		owner = findPartitionLeader(rigs, 1)
		if owner == nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if owner == nil {
		t.Fatal("no leader for shard 1")
	}

	// Snapshot to a fresh export dir.
	exportDir := filepath.Join(t.TempDir(), "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	idx, err := owner.Host.SnapshotPartitionToDir(ctx, 1, exportDir)
	if err != nil {
		t.Fatalf("SnapshotPartitionToDir: %v", err)
	}
	if idx == 0 {
		t.Fatal("expected non-zero snapshot index")
	}

	// dragonboat writes a subdirectory under exportDir.
	entries, err := os.ReadDir(exportDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("export dir is empty")
	}
	src := exportDir
	if len(entries) == 1 && entries[0].IsDir() {
		src = filepath.Join(exportDir, entries[0].Name())
	}

	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()
	repo := &enginesnap.BlobRepository{Bucket: bucket}
	if err := enginesnap.SaveDir(ctx, repo, 1, idx, src); err != nil {
		t.Fatalf("SaveDir: %v", err)
	}
	refs, err := repo.List(ctx, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 1 || refs[0].Index != idx {
		t.Fatalf("List = %+v; want one entry at index %d", refs, idx)
	}
	dst := filepath.Join(t.TempDir(), "restored")
	if err := enginesnap.RestoreDir(ctx, repo, 1, idx, dst); err != nil {
		t.Fatalf("RestoreDir: %v", err)
	}
	// Restored dir is non-empty.
	rentries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(rentries) == 0 {
		t.Fatal("restored snapshot dir is empty")
	}
}

// TestAdminDeleteSnapshot puts two archives, deletes one via the
// admin RPC, and verifies List returns only the survivor.
func TestAdminDeleteSnapshot(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	bucket, err := blob.OpenBucket(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()
	repo := &enginesnap.BlobRepository{Bucket: bucket}

	// Seed two archives. SaveDir tars+gzips a source dir; reuse a tiny
	// scratch dir so we don't need to drive dragonboat for this test.
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := enginesnap.SaveDir(ctx, repo, 1, 100, src); err != nil {
		t.Fatalf("SaveDir 100: %v", err)
	}
	if err := enginesnap.SaveDir(ctx, repo, 1, 200, src); err != nil {
		t.Fatalf("SaveDir 200: %v", err)
	}

	srv, err := admin.NewServer(admin.Config{
		Host:   leader.Host,
		Runner: leader.Host.MetadataRunner(),
		Repo:   repo,
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", freeLocalAddr(t))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.GracefulStop(); _ = ln.Close() })

	cli, done := dialInsecureAdmin(t, ln.Addr().String())
	defer done()

	if _, err := cli.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		ShardId: 1, Index: 100,
	}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	resp, err := cli.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{ShardId: 1})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	got := resp.GetSnapshots()
	if len(got) != 1 || got[0].GetIndex() != 200 {
		t.Fatalf("after delete: %+v; want only index=200", got)
	}

	// Idempotent: deleting the same key again succeeds.
	if _, err := cli.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		ShardId: 1, Index: 100,
	}); err != nil {
		t.Fatalf("second DeleteSnapshot: %v", err)
	}
}

// writeAdminTLSFixtures builds an ephemeral single-CA PKI with a node
// leaf (for the server) + an operator leaf (for the client), each
// carrying its SPIFFE URI SAN. Returns a server-side TLSSpec, the
// operator cert+key paths, and the shared CA path so callers can build
// the client-side spec or raw tls.Config as needed.
func writeAdminTLSFixtures(t *testing.T, dir string) (creds.TLSSpec, string, string, string) {
	t.Helper()
	ca, err := pki.NewCA("phase4_2-ca")
	if err != nil {
		t.Fatal(err)
	}
	caCrt, _, err := ca.WriteSingle(dir)
	if err != nil {
		t.Fatal(err)
	}
	nodeURI, err := pki.BuildSPIFFEID(testTrustDomain, "node", "1")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:  pki.LeafNode,
		Name:  "node-1",
		Hosts: []string{"127.0.0.1", "localhost"},
		URIs:  []*url.URL{nodeURI},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeCrt, nodeKey, err := pki.WriteMaterial(dir, "node-1", leaf)
	if err != nil {
		t.Fatal(err)
	}
	opURI, err := pki.BuildSPIFFEID(testTrustDomain, "operator", "test-op")
	if err != nil {
		t.Fatal(err)
	}
	opLeaf, err := ca.Issue(pki.LeafOptions{
		Kind: pki.LeafOperator,
		Name: "test-op",
		URIs: []*url.URL{opURI},
	})
	if err != nil {
		t.Fatal(err)
	}
	opCrt, opKey, err := pki.WriteMaterial(dir, "operator-test-op", opLeaf)
	if err != nil {
		t.Fatal(err)
	}
	return creds.TLSSpec{
		CAFile:      caCrt,
		CertFile:    nodeCrt,
		KeyFile:     nodeKey,
		TrustDomain: testTrustDomain,
	}, opCrt, opKey, caCrt
}

const testTrustDomain = "reflow.local"

// awaitable just keeps the `engine` import used when not all code paths
// hit it; placate vet on Linux builds.
var _ = engine.HostConfig{}

// silence unused-import diagnostic when grpc isn't referenced through
// the test (it is, via grpc.NewServer above, but keep this for static
// analyzers that don't follow that path).
var _ = fmt.Sprintf
