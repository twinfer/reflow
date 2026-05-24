//go:build e2e

// Package eventsource_test exercises every registered event-source
// backend factory (kafka, nats, sqs) against a real broker container
// brought up via testcontainers-go. The pattern in each *_test.go is:
//
//  1. Bring up a single-shard reflow Host + ingress runtime + handler
//     server (shared via bringUpEventSourceHost).
//  2. Spin up the backend container.
//  3. Configure an eventsource.Source pointed at the broker, start the
//     Manager.
//  4. Publish one (or more) messages, assert the handler runs.
//
// The harness is deliberately a single-node host (NumPartitionShards=1).
// What's under test is the backend factory + the dispatcher path
// (broker → Watermill subscriber → SubmitInvocation → handler), not
// the multi-shard / multi-node cluster mechanics — those have their
// own coverage in internal/e2e/chaos.
package eventsource_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/authz"
	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
)

// eventSourceHost is the cluster + ingress + handler-server fixture
// every backend test reuses. Owns the lifetime of its members; teardown
// is via t.Cleanup so subtests can't leak state into each other.
type eventSourceHost struct {
	h  *engine.Host
	rt *ingress.Runtime
}

// bringUpEventSourceHost stands up a single-node, single-shard cluster
// with one registered handler whose body invokes hf. The returned
// fixture is the seam every backend test plugs its Manager into.
func bringUpEventSourceHost(t *testing.T, svc, hname string, hf handler.Handler) *eventSourceHost {
	t.Helper()

	reg := handler.NewRegistry()
	if err := reg.RegisterService(svc, hname, hf); err != nil {
		t.Fatalf("register: %v", err)
	}

	dir := t.TempDir()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	hsrv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("handler.NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen sdk: %v", err)
	}
	go func() { _ = hsrv.Serve(ln) }()
	t.Cleanup(func() { _ = hsrv.Shutdown(); _ = ln.Close() })

	asrv, err := config.NewServer(config.Config{Host: h, Runner: h.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
		t.Fatalf("AutoSeed: %v", err)
	}

	mw, _, _, err := auth.HTTPMiddleware(auth.Config{}, nil)
	if err != nil {
		t.Fatalf("auth middleware: %v", err)
	}
	authzIc, err := authz.NewFoundationalInterceptor(nil, false)
	if err != nil {
		t.Fatalf("authz interceptor: %v", err)
	}
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:             "127.0.0.1:0",
		Middleware:       mw,
		AuthzInterceptor: authzIc,
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	return &eventSourceHost{h: h, rt: rt}
}

// startManager wires the eventsource.Manager against the host's ingress
// Server, starts its Run goroutine, and returns a settle window each
// caller can extend per backend (Kafka rebalance, JetStream consumer
// provision, SQS GetQueueUrl + ReceiveMessage).
func startManager(t *testing.T, h *eventSourceHost, cfg eventsource.Config, settle time.Duration) {
	t.Helper()
	mgr, err := eventsource.NewManager(cfg, h.rt.Server(), prometheus.NewRegistry(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr == nil {
		t.Fatal("NewManager returned nil; expected at least one source")
	}
	done := make(chan struct{})
	go func() {
		mgr.Run(context.Background())
		close(done)
	}()
	t.Cleanup(func() {
		_ = mgr.Close()
		<-done
	})
	if settle > 0 {
		time.Sleep(settle)
	}
}

// freeLocalAddr returns a free 127.0.0.1:N. The listener is closed
// immediately so the caller can bind. Races with concurrent allocators
// are possible but vanishingly rare in tests.
func freeLocalAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
