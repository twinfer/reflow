package eventsource_test

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/authz"
	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// gochannelTest brings up a single-shard host with one registered
// handler whose payload the test can observe. Cleanup tears down the
// shared GoChannel registry so cases don't leak into each other.
type gochannelTest struct {
	h        *engine.Host
	rt       *ingress.Runtime
	received chan recordedInvocation
	flake    *atomic.Int32
}

type recordedInvocation struct {
	input []byte
}

func newGochannelTest(t *testing.T, svc, hname string, hf handler.Handler) *gochannelTest {
	t.Helper()
	reg := handler.NewRegistry()
	if err := reg.RegisterService(svc, hname, hf); err != nil {
		t.Fatalf("register: %v", err)
	}

	dir := t.TempDir()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
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

	return &gochannelTest{h: h, rt: rt}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startManager wraps eventsource.NewManager + Run for one source and
// returns a cleanup. Each test gets a fresh GoChannel instance keyed by
// its name so DLQ subscribers don't collide.
func startManager(t *testing.T, rt *ingress.Runtime, cfg eventsource.Config) *eventsource.Manager {
	t.Helper()
	mgr, err := eventsource.NewManager(cfg, rt.Server(), prometheus.NewRegistry(), nil)
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
	// Give the dispatcher goroutine a tick to actually subscribe before
	// tests publish — gochannel discards messages with no subscriber.
	time.Sleep(150 * time.Millisecond)
	return mgr
}

// TestEventSource_GoChannel_HappyPath publishes one message to an
// in-process GoChannel topic and asserts the bound handler runs with
// the message's payload.
func TestEventSource_GoChannel_HappyPath(t *testing.T) {
	t.Cleanup(eventsource.ResetGoChannelInstances)

	received := make(chan recordedInvocation, 1)
	tcase := newGochannelTest(t, "Echo", "ingest", func(_ handler.Context, in []byte) ([]byte, error) {
		received <- recordedInvocation{input: in}
		return []byte("ok"), nil
	})

	const instance = "happy-path"
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "test-source",
		Type:    "gochannel",
		Topic:   "orders.created",
		Service: "Echo",
		Handler: "ingest",
		ObjectKey: eventsource.ExtractorConfig{
			From: "header", Value: "X-User-Id",
		},
		Backend: eventsource.BackendConfig{Settings: map[string]string{"instance": instance}},
	}}}
	startManager(t, tcase.rt, cfg)

	gc := eventsource.GoChannelInstance(instance)
	msg := message.NewMessage(uuid.NewString(), []byte("hello-payload"))
	msg.Metadata.Set("X-User-Id", "user-42")
	if err := gc.Publish("orders.created", msg); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case rec := <-received:
		if string(rec.input) != "hello-payload" {
			t.Errorf("input = %q; want hello-payload", rec.input)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("handler never received the published message")
	}
}

// TestEventSource_GoChannel_IdempotencyOnUUID publishes the same message
// twice; the engine's idempotency cache should fold the second into the
// first so the handler only runs once.
func TestEventSource_GoChannel_IdempotencyOnUUID(t *testing.T) {
	t.Cleanup(eventsource.ResetGoChannelInstances)

	var count atomic.Int32
	tcase := newGochannelTest(t, "Echo", "once", func(_ handler.Context, in []byte) ([]byte, error) {
		count.Add(1)
		return in, nil
	})

	const instance = "idem"
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "test-source",
		Type:    "gochannel",
		Topic:   "ev",
		Service: "Echo",
		Handler: "once",
		Backend: eventsource.BackendConfig{Settings: map[string]string{"instance": instance}},
	}}}
	startManager(t, tcase.rt, cfg)

	gc := eventsource.GoChannelInstance(instance)
	id := uuid.NewString()
	payload := []byte("dup")
	if err := gc.Publish("ev", message.NewMessage(id, payload)); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := gc.Publish("ev", message.NewMessage(id, payload)); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Allow a brief settling window for a potential second delivery.
	time.Sleep(500 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("handler runs = %d; want 1 (idempotency key should dedup)", got)
	}
}

// TestEventSource_GoChannel_TerminalDivertsToDLQ exercises the
// PoisonQueue middleware: a missing required header (terminal
// classification at the extractor) diverts the message to the DLQ
// topic and the handler is never invoked.
func TestEventSource_GoChannel_TerminalDivertsToDLQ(t *testing.T) {
	t.Cleanup(eventsource.ResetGoChannelInstances)

	const instance = "dlq"
	const dlqTopic = "ev.dlq"

	gc := eventsource.GoChannelInstance(instance)
	dlqCtx, dlqCancel := context.WithCancel(context.Background())
	t.Cleanup(dlqCancel)
	dlqCh, err := gc.Subscribe(dlqCtx, dlqTopic)
	if err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}

	var invoked atomic.Bool
	tcase := newGochannelTest(t, "Echo", "never", func(_ handler.Context, in []byte) ([]byte, error) {
		invoked.Store(true)
		return in, nil
	})

	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:      "needs-header",
		Type:      "gochannel",
		Topic:     "ev",
		Service:   "Echo",
		Handler:   "never",
		ObjectKey: eventsource.ExtractorConfig{From: "header", Value: "X-Required"},
		Backend:   eventsource.BackendConfig{Settings: map[string]string{"instance": instance}},
		Retry:     eventsource.RetryConfig{MaxRetries: 1, InitialInterval: 10 * time.Millisecond},
		DLQ:       eventsource.DLQConfig{Topic: dlqTopic},
	}}}
	startManager(t, tcase.rt, cfg)

	// Publish without the X-Required header → terminal extractor failure.
	msg := message.NewMessage(uuid.NewString(), []byte("bad"))
	if err := gc.Publish("ev", msg); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case poisoned, ok := <-dlqCh:
		if !ok {
			t.Fatal("dlq channel closed before message arrived")
		}
		poisoned.Ack()
		if got := string(poisoned.Payload); got != "bad" {
			t.Errorf("dlq payload = %q; want bad", got)
		}
		if invoked.Load() {
			t.Error("handler should not have run for a poisoned message")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("terminal message never reached DLQ")
	}
}

// TestEventSource_NoSources_ManagerNil confirms that an empty config
// leaves NewManager returning (nil, nil) — no goroutines, no metrics
// registered.
func TestEventSource_NoSources_ManagerNil(t *testing.T) {
	mgr, err := eventsource.NewManager(eventsource.Config{}, dummySubmitter{}, prometheus.NewRegistry(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr != nil {
		t.Fatalf("expected nil manager, got %v", mgr)
	}
}

type dummySubmitter struct{}

func (dummySubmitter) SubmitInvocation(_ context.Context, _ *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error) {
	return connect.NewResponse(&ingressv1.SubmitInvocationResponse{}), nil
}

// keep imports referenced even when only a subset of tests run.
var _ sync.Mutex
