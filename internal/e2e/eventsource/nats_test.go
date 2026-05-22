//go:build e2e

package eventsource_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
)

// natsSettle is the post-Manager-start delay before publish. JetStream
// has to provision the durable consumer the first time the subscriber
// binds; this delay covers that round-trip. Tightening makes the test
// flake on cold-cache CI runs.
const natsSettle = 3 * time.Second

// natsJetstreamImage is the official NATS image started with -js. JS is
// what the reflow factory binds to (factory_nats.go requires
// `durable_prefix` — JetStream-only).
const natsJetstreamImage = "nats:2.10-alpine"

// startNATSContainer spins up a JetStream-enabled NATS server, returns
// its client URL (nats://host:port). Cleanup is via t.Cleanup.
func startNATSContainer(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        natsJetstreamImage,
		Cmd:          []string{"-js"},
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Server is ready").WithStartupTimeout(45 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("nats container unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("nats host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatalf("nats mapped port: %v", err)
	}
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}

// publishToNATSJetstream sends one message into the JetStream subject
// `subj` so the watermill subscriber under test (started by the
// Manager) receives it. AutoProvision on the subscriber side creates
// the stream; on the publish side we use a plain Nats Publish, which
// JetStream picks up when a matching stream + subject filter exists.
func publishToNATSJetstream(t *testing.T, url, subj string, body []byte, hdrs nats.Header) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nats JetStream(): %v", err)
	}
	m := &nats.Msg{Subject: subj, Data: body, Header: hdrs}
	if _, err := js.PublishMsg(m); err != nil {
		t.Fatalf("nats publish: %v", err)
	}
}

// TestEventSource_NATS_HappyPath validates the NATS (JetStream) backend
// end-to-end: container → factory → durable consumer → SubmitInvocation
// → handler. Mirrors the Kafka happy-path shape so a regression in
// either factory shows up as a single-test failure.
func TestEventSource_NATS_HappyPath(t *testing.T) {
	received := make(chan []byte, 1)
	es := bringUpEventSourceHost(t, "Echo", "ingest", func(_ handler.Context, in []byte) ([]byte, error) {
		received <- in
		return []byte("ok"), nil
	})
	url := startNATSContainer(t)

	// NATS JetStream stream names disallow dots. watermill-nats with
	// AutoProvision uses the topic verbatim as the stream name, so the
	// topic must be a single dotless token. The published subject is
	// the same string; JetStream binds the stream to the subject filter.
	subject := uniqueTopic("reflow-orders")
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "nats-source",
		Type:    "nats",
		Topic:   subject,
		Service: "Echo",
		Handler: "ingest",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"url":            url,
			"durable_prefix": "reflow-test-" + uuid.NewString()[:8],
		}},
	}}}
	startManager(t, es, cfg, natsSettle)

	hdr := nats.Header{}
	hdr.Set("Nats-Msg-Id", uuid.NewString())
	publishToNATSJetstream(t, url, subject, []byte("hello-nats"), hdr)

	select {
	case in := <-received:
		if string(in) != "hello-nats" {
			t.Errorf("payload = %q; want hello-nats", in)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("handler never received the nats message")
	}
}

// TestEventSource_NATS_IdempotencyOnMsgID publishes the same Nats-Msg-Id
// twice. JetStream's server-side duplicate window dedups by Nats-Msg-Id,
// so even before reflow's invocation-id cache trips, only one delivery
// reaches the subscriber. Either dedup layer is sufficient for the
// invariant the test asserts: handler runs exactly once.
func TestEventSource_NATS_IdempotencyOnMsgID(t *testing.T) {
	var count atomic.Int32
	es := bringUpEventSourceHost(t, "Echo", "once", func(_ handler.Context, in []byte) ([]byte, error) {
		count.Add(1)
		return in, nil
	})
	url := startNATSContainer(t)

	subject := uniqueTopic("reflow-idem")
	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "nats-source",
		Type:    "nats",
		Topic:   subject,
		Service: "Echo",
		Handler: "once",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"url":            url,
			"durable_prefix": "reflow-test-" + uuid.NewString()[:8],
		}},
	}}}
	startManager(t, es, cfg, natsSettle)

	id := uuid.NewString()
	hdr := nats.Header{}
	hdr.Set("Nats-Msg-Id", id)
	publishToNATSJetstream(t, url, subject, []byte("dup"), hdr)
	publishToNATSJetstream(t, url, subject, []byte("dup"), hdr)

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
		t.Fatalf("handler runs = %d; want 1 (Nats-Msg-Id should dedup)", got)
	}
}
