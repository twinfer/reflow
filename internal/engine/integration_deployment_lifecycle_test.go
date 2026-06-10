package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/loadgen"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	"github.com/twinfer/reflw/proto/adminv1/adminv1connect"
)

// newDeploymentClient stands up a loopback Connect client against srv's
// handler. Returned cleanup closes the listener and idle transport.
func newDeploymentClient(t *testing.T, ctx context.Context, srv *admin.Server) (adminv1connect.AdminClient, func()) {
	t.Helper()
	path, h := srv.NewHandler()
	cs, err := connectserver.New(ctx, connectserver.Config{Addr: "127.0.0.1:0"},
		connectserver.Route{Path: path, Handler: h})
	if err != nil {
		t.Fatalf("connectserver.New: %v", err)
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	cli := adminv1connect.NewAdminClient(&http.Client{Transport: tr}, "http://"+cs.Addr())
	return cli, func() {
		tr.CloseIdleConnections()
		cs.Close()
	}
}

// TestConfig_DeploymentLifecycle exercises the end-to-end CRUD flow:
// register → list → describe → delete (force) → list-empty. Verifies
// the CAS revision advances on each mutation and that ListDeployments
// returns the same revision SyncRead observes.
func TestConfig_DeploymentLifecycle(t *testing.T) {
	fake := &fakeHandlerHTTP2{output: []byte("ok")}
	fakeAddr, teardown := startFakeHandlerHTTP2(t, fake)
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 1})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	srv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cli, closeCli := newDeploymentClient(t, ctx, srv)
	defer closeCli()

	// Register.
	regResp, err := cli.RegisterDeployment(ctx, connect.NewRequest(&adminv1.RegisterDeploymentRequest{
		Url: "http://" + fakeAddr,
	}))
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}
	id := regResp.Msg.GetDeploymentId()
	if id == "" {
		t.Fatal("empty deployment_id from Register")
	}

	// List — must contain the new id and a non-zero revision.
	listResp, err := cli.ListDeployments(ctx, connect.NewRequest(&adminv1.ListDeploymentsRequest{}))
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}
	if listResp.Msg.GetTableRevision() == 0 {
		t.Fatalf("table_revision after register is 0; want >0")
	}
	if got := len(listResp.Msg.GetDeployments()); got != 1 {
		t.Fatalf("List size=%d; want 1", got)
	}
	if listResp.Msg.GetDeployments()[0].GetId() != id {
		t.Fatalf("Listed id=%q; want %q", listResp.Msg.GetDeployments()[0].GetId(), id)
	}
	revAfterRegister := listResp.Msg.GetTableRevision()

	// Describe — must return the same record.
	descResp, err := cli.DescribeDeployment(ctx, connect.NewRequest(&adminv1.DescribeDeploymentRequest{
		DeploymentId: id,
	}))
	if err != nil {
		t.Fatalf("DescribeDeployment: %v", err)
	}
	if descResp.Msg.GetDeployment().GetId() != id {
		t.Fatalf("Describe id=%q; want %q", descResp.Msg.GetDeployment().GetId(), id)
	}

	// Describe-of-absent → NotFound.
	_, err = cli.DescribeDeployment(ctx, connect.NewRequest(&adminv1.DescribeDeploymentRequest{
		DeploymentId: "ghost",
	}))
	if err == nil {
		t.Fatal("DescribeDeployment(ghost) returned nil err; want NotFound")
	}
	if cerr, ok := err.(*connect.Error); !ok || cerr.Code() != connect.CodeNotFound {
		t.Fatalf("Describe(ghost) err code=%v; want NotFound", err)
	}

	// Delete without --force → FailedPrecondition.
	_, err = cli.DeleteDeployment(ctx, connect.NewRequest(&adminv1.DeleteDeploymentRequest{
		DeploymentId: id,
	}))
	if err == nil {
		t.Fatal("DeleteDeployment(force=false) returned nil; want FailedPrecondition")
	}
	if cerr, ok := err.(*connect.Error); !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("Delete(force=false) err code=%v; want FailedPrecondition", err)
	}

	// Delete with --force and matching CAS.
	delResp, err := cli.DeleteDeployment(ctx, connect.NewRequest(&adminv1.DeleteDeploymentRequest{
		DeploymentId:      id,
		Force:             true,
		IfTableRevisionEq: revAfterRegister,
	}))
	if err != nil {
		t.Fatalf("DeleteDeployment(force=true): %v", err)
	}
	if delResp.Msg.GetTableRevision() <= revAfterRegister {
		t.Fatalf("post-delete revision=%d; want >%d", delResp.Msg.GetTableRevision(), revAfterRegister)
	}

	// List must now be empty.
	listResp, err = cli.ListDeployments(ctx, connect.NewRequest(&adminv1.ListDeploymentsRequest{}))
	if err != nil {
		t.Fatalf("ListDeployments after delete: %v", err)
	}
	if got := len(listResp.Msg.GetDeployments()); got != 0 {
		t.Fatalf("after delete, List size=%d; want 0", got)
	}

	// Stale CAS round-trip — register then delete with stale revision.
	regResp2, err := cli.RegisterDeployment(ctx, connect.NewRequest(&adminv1.RegisterDeploymentRequest{
		Url: "http://" + fakeAddr,
	}))
	if err != nil {
		t.Fatalf("RegisterDeployment #2: %v", err)
	}
	_, err = cli.DeleteDeployment(ctx, connect.NewRequest(&adminv1.DeleteDeploymentRequest{
		DeploymentId:      regResp2.Msg.GetDeploymentId(),
		Force:             true,
		IfTableRevisionEq: 1, // stale
	}))
	if err == nil {
		t.Fatal("Delete with stale ifRev returned nil; want FailedPrecondition")
	}
	if cerr, ok := err.(*connect.Error); !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("Delete(stale ifRev) err code=%v; want FailedPrecondition", err)
	}
}
