//go:build kafka_integration

// Run with: go test -tags=kafka_integration -timeout=5m -v ./internal/ingress/eventsource/...
//
// Requires a working Docker daemon — testcontainers-go spins up a single
// confluent-local broker per test and tears it down via t.Cleanup.

package eventsource_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
)

// startKafkaContainer spins up a confluent-local broker for one test and
// returns its bootstrap address. Container is terminated via t.Cleanup.
func startKafkaContainer(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("reflow-eventsource-test"),
	)
	if err != nil {
		t.Skipf("kafka container unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	if len(brokers) == 0 {
		t.Fatal("kafka container returned no brokers")
	}
	return brokers[0]
}

// publishToKafka builds an ephemeral watermill-kafka publisher, sends one
// message, and closes the publisher. Used to inject messages into the
// dispatcher under test.
func publishToKafka(t *testing.T, broker, topic string, msg *message.Message) {
	t.Helper()
	pub, err := wmkafka.NewPublisher(wmkafka.PublisherConfig{
		Brokers:   []string{broker},
		Marshaler: wmkafka.DefaultMarshaler{},
	}, watermill.NopLogger{})
	if err != nil {
		t.Fatalf("kafka publisher: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish(topic, msg); err != nil {
		t.Fatalf("kafka publish: %v", err)
	}
}

// bringUpKafkaTest mirrors newGochannelTest but binds the dispatcher to
// a real Kafka container.
type kafkaTest struct {
	h      *engine.Host
	rt     *ingress.Runtime
	broker string
}

func bringUpKafkaTest(t *testing.T, svc, hname string, hf handler.Handler) *kafkaTest {
	t.Helper()
	broker := startKafkaContainer(t)

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

	asrv, err := admin.NewServer(admin.Config{Host: h, Runner: h.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
		t.Fatalf("AutoSeed: %v", err)
	}

	mw, _, err := auth.HTTPMiddleware(auth.Config{TrustDomain: "reflow.local"}, nil)
	if err != nil {
		t.Fatalf("auth middleware: %v", err)
	}
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:       "127.0.0.1:0",
		Middleware: mw,
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	return &kafkaTest{h: h, rt: rt, broker: broker}
}

// startManagerKafka constructs the event-source manager for the kafka
// test and starts it. Mirrors startManager from the gochannel suite but
// with an isolated registry per test.
func startManagerKafka(t *testing.T, rt *ingress.Runtime, cfg eventsource.Config) {
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
	// Kafka consumer-group join takes a few seconds; give it room before
	// the test publishes. Without this delay the first message goes to a
	// partition with no assigned consumer and waits a rebalance round.
	time.Sleep(5 * time.Second)
}

// uniqueTopic returns a topic name unique per test invocation. Each test
// uses a fresh broker container, so collisions are unlikely — but
// duplicate topic names within one container leak consumer-group state
// across subtests.
func uniqueTopic(prefix string) string {
	return prefix + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

// TestEventSource_Kafka_HappyPath exercises the full kafka backend
// end-to-end: container → factory → dispatcher → SubmitInvocation →
// handler. This is the canonical "do real brokers work?" regression.
func TestEventSource_Kafka_HappyPath(t *testing.T) {
	received := make(chan []byte, 1)
	tcase := bringUpKafkaTest(t, "Echo", "ingest", func(_ handler.Context, in []byte) ([]byte, error) {
		received <- in
		return []byte("ok"), nil
	})

	topic := uniqueTopic("orders")
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "kafka-source",
		Type:    "kafka",
		Topic:   topic,
		Service: "Echo",
		Handler: "ingest",
		ObjectKey: eventsource.ExtractorConfig{
			From: "header", Value: "X-User-Id",
		},
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"brokers":        tcase.broker,
			"consumer_group": "reflow-test-" + uuid.NewString(),
		}},
	}}}
	startManagerKafka(t, tcase.rt, cfg)

	msg := message.NewMessage(uuid.NewString(), []byte("hello-kafka"))
	msg.Metadata.Set("X-User-Id", "user-99")
	publishToKafka(t, tcase.broker, topic, msg)

	select {
	case in := <-received:
		if string(in) != "hello-kafka" {
			t.Errorf("payload = %q; want hello-kafka", in)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("handler never received the kafka message")
	}
}

// TestEventSource_Kafka_IdempotencyOnUUID publishes the same message
// twice (same Watermill UUID); the engine's idempotency cache should
// dedup so the handler only runs once.
func TestEventSource_Kafka_IdempotencyOnUUID(t *testing.T) {
	var count atomic.Int32
	tcase := bringUpKafkaTest(t, "Echo", "once", func(_ handler.Context, in []byte) ([]byte, error) {
		count.Add(1)
		return in, nil
	})

	topic := uniqueTopic("idem")
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "kafka-source",
		Type:    "kafka",
		Topic:   topic,
		Service: "Echo",
		Handler: "once",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"brokers":        tcase.broker,
			"consumer_group": "reflow-test-" + uuid.NewString(),
		}},
	}}}
	startManagerKafka(t, tcase.rt, cfg)

	id := uuid.NewString()
	payload := []byte("dup")
	publishToKafka(t, tcase.broker, topic, message.NewMessage(id, payload))
	publishToKafka(t, tcase.broker, topic, message.NewMessage(id, payload))

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Allow a brief settling window for a potential second invocation.
	time.Sleep(2 * time.Second)
	if got := count.Load(); got != 1 {
		t.Fatalf("handler runs = %d; want 1 (idempotency key should dedup)", got)
	}
}
