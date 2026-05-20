package engine_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine"
	enginesnap "github.com/twinfer/reflow/internal/engine/snapshot"

	"github.com/twinfer/reflow/internal/pki"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	"gocloud.dev/blob"
)

// adminRig augments a nodeRig with an admin Connect server. Admin runs
// without the auth middleware in tests; the dedicated TLS-rejection
// tests rebuild the rig with TLS + middleware enabled.
type adminRig struct {
	nodeRig *nodeRig
	srv     *connectserver.Server
}

func (a *adminRig) addr() string { return a.srv.Addr() }

func (a *adminRig) close() {
	if a.srv != nil {
		_ = a.srv.Close()
	}
	if a.nodeRig != nil {
		a.nodeRig.Close()
	}
}

// startAdminInsecure wires an admin Connect server on the local Host
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
	path, h := srv.NewHandler()
	cs, err := connectserver.New(context.Background(), connectserver.Config{
		Addr: freeLocalAddr(t),
	}, connectserver.Route{Path: path, Handler: h})
	if err != nil {
		t.Fatalf("connectserver.New: %v", err)
	}
	return &adminRig{nodeRig: r, srv: cs}
}

// dialInsecureAdmin opens an h2c Connect client to addr and returns the
// typed client + a cleanup.
func dialInsecureAdmin(t *testing.T, addr string) (adminv1connect.AdminClient, func()) {
	t.Helper()
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	hc := &http.Client{Transport: tr}
	cli := adminv1connect.NewAdminClient(hc, "http://"+addr)
	return cli, func() { tr.CloseIdleConnections() }
}

// awaitMetadataLeaderRig polls until a metadata leader is found and
// returns the rig that leads.
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

// TestAdminListNodes confirms the leader's admin surface returns every
// registered node.
func TestAdminListNodes(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	ar := startAdminInsecure(t, leader)
	defer ar.close()

	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ListNodes(ctx, connect.NewRequest(&adminv1.ListNodesRequest{}))
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(resp.Msg.GetNodes()) != 3 {
		t.Fatalf("ListNodes returned %d; want 3", len(resp.Msg.GetNodes()))
	}
	seen := map[uint64]bool{}
	for _, m := range resp.Msg.GetNodes() {
		seen[m.GetNodeId()] = true
	}
	for id := uint64(1); id <= 3; id++ {
		if !seen[id] {
			t.Errorf("ListNodes missing node %d: %v", id, seen)
		}
	}
}

// TestAdminListPartitions returns the bootstrap partition table.
func TestAdminListPartitions(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	ar := startAdminInsecure(t, leader)
	defer ar.close()
	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.ListPartitions(ctx, connect.NewRequest(&adminv1.ListPartitionsRequest{}))
		if err == nil && resp.Msg.GetTable() != nil && len(resp.Msg.GetTable().GetShards()) == 3 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("ListPartitions never returned a 3-shard table")
}

// TestAdminRemoveNode_LogicallyEvicts proposes EvictNode and asserts
// the apply path marks last_seen_ms=0 and enqueues DELETE_REPLICA steps.
func TestAdminRemoveNode_LogicallyEvicts(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

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
	if _, err := cli.RemoveNode(ctx, connect.NewRequest(&adminv1.RemoveNodeRequest{NodeId: victim})); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

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

// TestAdminMutualTLS_RejectsUnsignedClient builds an admin Connect
// listener wired with operator-CA mTLS and asserts that a client
// without any cert cannot complete the handshake.
func TestAdminMutualTLS_RejectsUnsignedClient(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)
	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)

	dir := t.TempDir()
	tlsSpec, opCert, opKey, caFile := writeAdminTLSFixtures(t, dir)

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

	mw, authCloser, err := auth.HTTPMiddleware(auth.Config{TrustDomain: testTrustDomain}, nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}
	defer func() {
		if authCloser != nil {
			_ = authCloser()
		}
	}()

	path, h := srv.NewHandler()
	cs, err := connectserver.New(context.Background(), connectserver.Config{
		Addr: freeLocalAddr(t),
		TLS:  serverCreds.ServerTLSConfig,
	}, connectserver.Route{Path: path, Handler: mw(h)})
	if err != nil {
		t.Fatalf("connectserver.New: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// 1) creds.Build refuses a TLS spec without a leaf keypair.
	if _, err := creds.Build(creds.Spec{
		Driver: creds.DriverTLS,
		TLS:    &creds.TLSSpec{CAFile: caFile, TrustDomain: testTrustDomain},
	}, nil); err == nil {
		t.Fatal("expected creds.Build to reject empty operator cert; got nil err")
	}

	// 2) Dial via HTTPS+HTTP/2 that trusts the server CA but presents no
	// client cert. The handshake must fail because the server's
	// ClientAuth = RequireAndVerifyClientCert rejects the empty client
	// Certificate message.
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caBytes) {
		t.Fatal("failed to parse node CA PEM")
	}
	noCertCfg := &tls.Config{
		ServerName: "127.0.0.1",
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS13,
	}
	badTr := &http.Transport{Protocols: new(http.Protocols), TLSClientConfig: noCertCfg}
	badTr.Protocols.SetHTTP2(true)
	badTr.Protocols.SetHTTP1(false)
	defer badTr.CloseIdleConnections()
	badCli := adminv1connect.NewAdminClient(&http.Client{Transport: badTr}, "https://"+cs.Addr())
	badCtx, badCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer badCancel()
	if _, err := badCli.ListNodes(badCtx, connect.NewRequest(&adminv1.ListNodesRequest{})); err == nil {
		t.Fatal("expected ListNodes to fail when client presents no cert; got nil")
	}

	// 3) With the proper operator cert + node CA, dial succeeds.
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
	tr := &http.Transport{Protocols: new(http.Protocols), TLSClientConfig: operatorCreds.ClientTLSConfig}
	tr.Protocols.SetHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	defer tr.CloseIdleConnections()
	cli := adminv1connect.NewAdminClient(&http.Client{Transport: tr}, "https://"+cs.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.ListNodes(ctx, connect.NewRequest(&adminv1.ListNodesRequest{})); err != nil {
		t.Fatalf("operator-authenticated ListNodes failed: %v", err)
	}
}

// TestSnapshot_PartitionExportAndArchive triggers an exported snapshot
// through the engine.Host helper, archives it via the fs repository,
// and confirms a non-empty Fetch round-trip.
func TestSnapshot_PartitionExportAndArchive(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

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
	rentries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(rentries) == 0 {
		t.Fatal("restored snapshot dir is empty")
	}
}

// TestAdminDeleteSnapshot puts two archives, deletes one via the admin
// RPC, and verifies List returns only the survivor.
func TestAdminDeleteSnapshot(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	bucket, err := blob.OpenBucket(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()
	repo := &enginesnap.BlobRepository{Bucket: bucket}

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
	path, h := srv.NewHandler()
	cs, err := connectserver.New(context.Background(), connectserver.Config{
		Addr: freeLocalAddr(t),
	}, connectserver.Route{Path: path, Handler: h})
	if err != nil {
		t.Fatalf("connectserver.New: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	cli, done := dialInsecureAdmin(t, cs.Addr())
	defer done()

	if _, err := cli.DeleteSnapshot(ctx, connect.NewRequest(&adminv1.DeleteSnapshotRequest{
		ShardId: 1, Index: 100,
	})); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	resp, err := cli.ListSnapshots(ctx, connect.NewRequest(&adminv1.ListSnapshotsRequest{ShardId: 1}))
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	got := resp.Msg.GetSnapshots()
	if len(got) != 1 || got[0].GetIndex() != 200 {
		t.Fatalf("after delete: %+v; want only index=200", got)
	}

	if _, err := cli.DeleteSnapshot(ctx, connect.NewRequest(&adminv1.DeleteSnapshotRequest{
		ShardId: 1, Index: 100,
	})); err != nil {
		t.Fatalf("second DeleteSnapshot: %v", err)
	}
}

// TestAdminSnapshotRPCs_RejectFollower verifies CreateSnapshot and
// DeleteSnapshot reject a non-leader caller with CodeUnavailable so
// pkg/adminclient.CallWithLeaderRedirect can chase the leader the same
// way it does for the other mutating RPCs. The LeaderHint *detail* is
// gossip-driven and best-effort; the test rig doesn't publish admin
// endpoints over gossip, so we only assert the code here.
func TestAdminSnapshotRPCs_RejectFollower(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	leader := awaitMetadataLeaderRig(t, rigs, 15*time.Second)
	awaitMembership(t, leader, 3, 10*time.Second)

	var follower *nodeRig
	for _, r := range rigs {
		if r != leader {
			follower = r
			break
		}
	}
	if follower == nil {
		t.Fatal("could not pick a non-leader rig")
	}

	ar := startAdminInsecure(t, follower)
	defer func() {
		if ar.srv != nil {
			_ = ar.srv.Close()
		}
	}()
	cli, done := dialInsecureAdmin(t, ar.addr())
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	expectUnavailable := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s on follower returned nil; want CodeUnavailable", name)
		}
		if got := connect.CodeOf(err); got != connect.CodeUnavailable {
			t.Fatalf("%s on follower: code=%s; want Unavailable; err=%v", name, got, err)
		}
	}

	_, cErr := cli.CreateSnapshot(ctx, connect.NewRequest(&adminv1.CreateSnapshotRequest{ShardId: 1}))
	expectUnavailable("CreateSnapshot", cErr)

	_, dErr := cli.DeleteSnapshot(ctx, connect.NewRequest(&adminv1.DeleteSnapshotRequest{
		ShardId: 1, Index: 1,
	}))
	expectUnavailable("DeleteSnapshot", dErr)
}

// writeAdminTLSFixtures builds an ephemeral single-CA PKI with a node
// leaf (for the server) + an operator leaf (for the client).
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

var _ = engine.HostConfig{}
var _ = fmt.Sprintf
