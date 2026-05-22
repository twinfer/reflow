//go:build e2e

package eventsource_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
)

// kafkaSettle is the post-Manager-start delay before the test publishes.
// Kafka consumer-group join takes a few seconds; without this delay the
// first message goes to a partition with no assigned consumer and waits
// a rebalance round (visible as a flake on slower hosts).
const kafkaSettle = 5 * time.Second

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
	es := bringUpEventSourceHost(t, "Echo", "ingest", func(_ handler.Context, in []byte) ([]byte, error) {
		received <- in
		return []byte("ok"), nil
	})
	broker := startKafkaContainer(t)

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
			"brokers":        broker,
			"consumer_group": "reflow-test-" + uuid.NewString(),
		}},
	}}}
	startManager(t, es, cfg, kafkaSettle)

	msg := message.NewMessage(uuid.NewString(), []byte("hello-kafka"))
	msg.Metadata.Set("X-User-Id", "user-99")
	publishToKafka(t, broker, topic, msg)

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
	es := bringUpEventSourceHost(t, "Echo", "once", func(_ handler.Context, in []byte) ([]byte, error) {
		count.Add(1)
		return in, nil
	})
	broker := startKafkaContainer(t)

	topic := uniqueTopic("idem")
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "kafka-source",
		Type:    "kafka",
		Topic:   topic,
		Service: "Echo",
		Handler: "once",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"brokers":        broker,
			"consumer_group": "reflow-test-" + uuid.NewString(),
		}},
	}}}
	startManager(t, es, cfg, kafkaSettle)

	id := uuid.NewString()
	payload := []byte("dup")
	publishToKafka(t, broker, topic, message.NewMessage(id, payload))
	publishToKafka(t, broker, topic, message.NewMessage(id, payload))

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
