package loadgen

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/pkg/handler"
)

// StartEmbeddedHandlers spins up a pkg/handler.NewServer endpoint on a
// free local port hosting reg and registers it as a deployment with the
// cluster's metadata leader. Returns a teardown function the caller
// defers; the function stops the server and closes the listener.
//
// Blocks until the cluster has a metadata leader and the
// RegisterDeployment proposal commits. Tests should use this in place
// of the old loadgen.ClusterOptions.Handlers field.
//
// reg with zero handlers is a no-op (returns a noop teardown).
func StartEmbeddedHandlers(t testing.TB, cluster *Cluster, reg *handler.Registry) func() {
	t.Helper()
	if reg == nil || reg.Len() == 0 {
		return func() {}
	}

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("loadgen: handler.NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("loadgen: listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	teardown := func() {
		_ = srv.Shutdown()
		_ = ln.Close()
	}

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		teardown()
		t.Fatalf("loadgen: AwaitAnyMetadataLeader: %v", err)
	}

	leader := findMetadataLeaderHost(cluster)
	if leader == nil {
		teardown()
		t.Fatal("loadgen: no metadata leader after AwaitAnyMetadataLeader")
	}

	asrv, err := admin.NewServer(admin.Config{Host: leader, Runner: leader.MetadataRunner()})
	if err != nil {
		teardown()
		t.Fatalf("loadgen: admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
		teardown()
		t.Fatalf("loadgen: AutoSeed: %v", err)
	}
	return teardown
}

// findMetadataLeaderHost walks cluster.Nodes looking for an in-process
// node whose MetadataRunner reports leadership. Returns nil if no
// in-process leader is found (subprocess clusters don't expose
// MetadataRunner — those tests must use the admin gRPC surface).
func findMetadataLeaderHost(cluster *Cluster) *engine.Host {
	if cluster == nil {
		return nil
	}
	for _, n := range cluster.Nodes {
		ip, ok := n.(*InProcessNode)
		if !ok {
			continue
		}
		if mr := ip.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
			return ip.Host
		}
	}
	return nil
}
