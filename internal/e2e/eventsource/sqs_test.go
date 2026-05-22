//go:build e2e

package eventsource_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/handler"
)

// sqsSettle is the post-Manager-start delay before publishing. SQS
// long-poll + GetQueueUrl resolution adds noticeable startup latency
// the first time the subscriber binds.
const sqsSettle = 3 * time.Second

// localstackImage pins the LocalStack tag. Override via
// REFLOW_E2E_LOCALSTACK_IMAGE; not exposed in the e2e API since this
// is a test-tier dependency only.
const localstackImage = "localstack/localstack:3.7.0"

// localstackRegion is the fake region the LocalStack SQS service is
// addressed under. Matches what AWS_DEFAULT_REGION inside the container
// expects when the test sets it on the SDK client.
const localstackRegion = "us-east-1"

// startLocalStackSQS spins up a LocalStack container with the SQS
// service enabled and returns its docker-host-visible endpoint
// (e.g. http://127.0.0.1:55020). Cleanup via t.Cleanup.
func startLocalStackSQS(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        localstackImage,
		ExposedPorts: []string{"4566/tcp"},
		Env:          map[string]string{"SERVICES": "sqs"},
		// LocalStack's /_localstack/health emits a `"sqs": "running"`
		// line once SQS is fully initialized; matching that is sturdier
		// than waiting on the bare port.
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("localstack container unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("localstack host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4566/tcp")
	if err != nil {
		t.Fatalf("localstack mapped port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// sqsClient returns an aws-sdk-go-v2 SQS client wired to talk to
// `endpoint` with fake LocalStack credentials. Tests use it both to
// create the queue up front and to publish into it during the test.
func sqsClient(t *testing.T, endpoint string) *sqs.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(localstackRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSQSQueue creates a fresh queue per test and returns its URL.
// The watermill subscriber resolves the URL itself via GetQueueUrl
// against the LocalStack endpoint, so the test only uses the URL to
// publish into the queue.
func createSQSQueue(t *testing.T, cli *sqs.Client, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("CreateQueue %q: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// publishToSQS sends one message to `queueURL`. SQS doesn't have
// per-message headers the way Kafka/NATS do; the body is the entire
// payload reflow sees.
func publishToSQS(t *testing.T, cli *sqs.Client, queueURL, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := cli.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
}

// withSQSEndpoint installs AWS_ENDPOINT_URL_SQS (read by the AWS SDK v2
// default config loader inside factory_sqs.go) for the lifetime of the
// test, restoring the prior value on Cleanup. This is the seam that
// routes the production factory through LocalStack without a config
// schema change.
//
// AWS_CONFIG_FILE and AWS_SHARED_CREDENTIALS_FILE are pinned to
// /dev/null so the SDK's default loader does NOT try to read any
// developer's ~/.aws/* state — that's bitten this suite when the
// path resolves to a directory or a credential-helper that can't run
// inside CI. We supply static credentials via env vars, which the SDK
// resolves before shared-config fallbacks anyway.
func withSQSEndpoint(t *testing.T, endpoint string) {
	t.Helper()
	t.Setenv("AWS_ENDPOINT_URL_SQS", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", localstackRegion)
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
}

// TestEventSource_SQS_HappyPath exercises the SQS backend end-to-end
// against LocalStack: container → factory → SQS subscriber polling →
// SubmitInvocation → handler. Mirrors the Kafka/NATS happy-path shape.
func TestEventSource_SQS_HappyPath(t *testing.T) {
	received := make(chan []byte, 1)
	es := bringUpEventSourceHost(t, "Echo", "ingest", func(_ handler.Context, in []byte) ([]byte, error) {
		received <- in
		return []byte("ok"), nil
	})
	endpoint := startLocalStackSQS(t)
	withSQSEndpoint(t, endpoint)

	cli := sqsClient(t, endpoint)
	queueName := "reflow-test-" + uniqueTopic("orders")
	queueURL := createSQSQueue(t, cli, queueName)

	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "sqs-source",
		Type:    "sqs",
		Topic:   queueName,
		Service: "Echo",
		Handler: "ingest",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"region": localstackRegion,
		}},
	}}}
	startManager(t, es, cfg, sqsSettle)

	publishToSQS(t, cli, queueURL, "hello-sqs")

	select {
	case in := <-received:
		if string(in) != "hello-sqs" {
			t.Errorf("payload = %q; want hello-sqs", in)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("handler never received the sqs message")
	}
}

// TestEventSource_SQS_TwoMessages publishes two distinct payloads and
// asserts both reach the handler. SQS doesn't carry an upstream-dedup
// hint the way Kafka offsets / Nats-Msg-Id do, so the corresponding
// "exactly-once" idempotency assertion only makes sense for those
// backends. Here we just verify the subscriber drains the queue.
func TestEventSource_SQS_TwoMessages(t *testing.T) {
	var count atomic.Int32
	got := make(chan struct{}, 4)
	es := bringUpEventSourceHost(t, "Echo", "drain", func(_ handler.Context, in []byte) ([]byte, error) {
		count.Add(1)
		got <- struct{}{}
		return in, nil
	})
	endpoint := startLocalStackSQS(t)
	withSQSEndpoint(t, endpoint)

	cli := sqsClient(t, endpoint)
	queueName := "reflow-test-" + uniqueTopic("drain")
	queueURL := createSQSQueue(t, cli, queueName)

	cfg := eventsource.Config{Sources: []eventsource.SourceConfig{{
		Name:    "sqs-source",
		Type:    "sqs",
		Topic:   queueName,
		Service: "Echo",
		Handler: "drain",
		Backend: eventsource.BackendConfig{Settings: map[string]string{
			"region": localstackRegion,
		}},
	}}}
	startManager(t, es, cfg, sqsSettle)

	publishToSQS(t, cli, queueURL, "a")
	publishToSQS(t, cli, queueURL, "b")

	deadline := time.After(60 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-got:
		case <-deadline:
			t.Fatalf("only %d/2 sqs messages reached the handler", count.Load())
		}
	}
}
