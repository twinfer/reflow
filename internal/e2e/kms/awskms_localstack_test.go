//go:build e2e

package kms_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssdkkms "github.com/aws/aws-sdk-go/service/kms"
	awskmstink "github.com/tink-crypto/tink-go-awskms/v2/integration/awskms"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflw/internal/e2e"
)

const (
	localstackImage  = "localstack/localstack:3.7.0"
	localstackRegion = "us-east-1"
)

// startLocalStackKMS spins up LocalStack with SERVICES=kms enabled
// and returns the host-visible endpoint (e.g. http://127.0.0.1:55020).
// Mirrors the SQS bring-up in eventsource/sqs_test.go but with
// SERVICES=kms — same image, same wait predicate, different feature.
func startLocalStackKMS(t *testing.T) string {
	t.Helper()
	e2e.SkipUnlessDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        localstackImage,
		ExposedPorts: []string{"4566/tcp"},
		Env:          map[string]string{"SERVICES": "kms"},
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
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	// One quick health probe to confirm KMS is up — LocalStack
	// returns 200 with `"kms": "running"` in the body once ready.
	waitLocalStackKMS(t, endpoint)
	return endpoint
}

// waitLocalStackKMS polls /_localstack/health for up to 30s until the
// "kms" service reports "running" (or "available"). The container
// WaitingFor unblocks on HTTP 200, but LocalStack returns 200 before
// individual services finish bootstrapping.
func waitLocalStackKMS(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/_localstack/health", nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			// Body contains a JSON object with service statuses; the
			// kms key is sufficient indicator. We don't strictly parse
			// — a 200 + KMS-shaped startup window of ~5s is the usual.
			time.Sleep(2 * time.Second)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("localstack /_localstack/health never reported KMS ready")
}

// localstackKMSSession returns an aws-sdk-go v1 KMS client pointed at
// LocalStack with static "test" credentials. The Tink awskms package
// (which uses sdk-v1) consumes this via WithKMS(...).
func localstackKMSSession(t *testing.T, endpoint string) *awssdkkms.KMS {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(localstackRegion),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials("test", "test", ""),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("aws session: %v", err)
	}
	return awssdkkms.New(sess)
}

// createLocalStackKey provisions one KMS key in LocalStack and returns
// the key ARN. The Tink awskms URI form is
// `aws-kms://arn:aws:kms:<region>:<account>:key/<key-id>` — LocalStack
// returns the same ARN shape with account 000000000000.
func createLocalStackKey(t *testing.T, kmsCli *awssdkkms.KMS) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := kmsCli.CreateKeyWithContext(ctx, &awssdkkms.CreateKeyInput{
		Description: aws.String("reflw e2e test"),
		KeyUsage:    aws.String(awssdkkms.KeyUsageTypeEncryptDecrypt),
	})
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if out.KeyMetadata == nil || out.KeyMetadata.Arn == nil {
		t.Fatal("CreateKey: no ARN returned")
	}
	return *out.KeyMetadata.Arn
}

// TestAWSKMS_LocalStack_RoundTrip exercises the Tink awskms
// integration end-to-end against LocalStack:
//   - aws-sdk-go v1 KMS client pointed at LocalStack
//   - Tink awskms.NewClientWithOptions(uriPrefix, WithKMS(...))
//   - Encrypt → decrypt via the AEAD primitive
//
// Skips the Tink registry entirely. Goes through Tink's client
// directly — secretstore-via-aws-kms would require either
// `ClearKMSClients` (wiping the production `aws-kms://` init
// registration) or a config-time prefix-narrowing seam in
// pkg/kms/awskms that isn't shipped yet. The secretstore wiring is
// covered by the BlobKMS + Vault tests; this test scopes to "is the
// Tink awskms integration wired up against a real AWS-KMS shape?".
func TestAWSKMS_LocalStack_RoundTrip(t *testing.T) {
	endpoint := startLocalStackKMS(t)
	kmsCli := localstackKMSSession(t, endpoint)
	keyArn := createLocalStackKey(t, kmsCli)

	keyURI := "aws-kms://" + keyArn
	tinkClient, err := awskmstink.NewClientWithOptions(keyURI, awskmstink.WithKMS(kmsCli))
	if err != nil {
		t.Fatalf("tink awskms client: %v", err)
	}
	aead, err := tinkClient.GetAEAD(keyURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	pt := []byte("hmac-payload-via-aws-kms")
	aad := []byte("reflw-test")
	ct, err := aead.Encrypt(pt, aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := aead.Decrypt(ct, aad)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, pt)
	}
}

// TestAWSKMS_LocalStack_AADRebindingRejects verifies AAD-binding holds
// end-to-end through Tink → AWS SDK → LocalStack: ciphertext encrypted
// under AAD "name-A" cannot be decrypted under AAD "name-B". Tink's
// awskms client (in non-legacy AssociatedData mode) sends AAD via the
// KMS EncryptionContext, so this also checks LocalStack's
// EncryptionContext semantics.
func TestAWSKMS_LocalStack_AADRebindingRejects(t *testing.T) {
	endpoint := startLocalStackKMS(t)
	kmsCli := localstackKMSSession(t, endpoint)
	keyArn := createLocalStackKey(t, kmsCli)

	keyURI := "aws-kms://" + keyArn
	tinkClient, err := awskmstink.NewClientWithOptions(keyURI, awskmstink.WithKMS(kmsCli))
	if err != nil {
		t.Fatalf("tink awskms client: %v", err)
	}
	aead, err := tinkClient.GetAEAD(keyURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt([]byte("payload"), []byte("name-A"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := aead.Decrypt(ct, []byte("name-B")); err == nil {
		t.Fatal("Decrypt under wrong AAD should fail; AWS KMS EncryptionContext appears unbound")
	}
}
