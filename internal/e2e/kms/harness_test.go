//go:build e2e

// Package kms_test exercises the KMS provider tiers (BlobKMS, Vault
// Transit, AWS KMS) against real backend containers. Each test is a
// thin slice:
//
//  1. Bring up the backend (minio / vault dev / localstack).
//  2. Stage / configure the KEK material.
//  3. Round-trip encrypt/decrypt via Tink — either directly through
//     a constructed client or through Tink's process-global registry.
//  4. Where the prod-package init can be reused (blob, vault), also
//     drive secretstore.Resolver end-to-end against a SecretRecord
//     pointing at a real KEK.
//
// GCP KMS is intentionally absent — no good local emulator exists for
// it. Coverage there relies on unit tests in pkg/kms/gcpkms.
package kms_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/twinfer/reflow/internal/e2e"
)

const (
	minioImage    = "minio/minio:RELEASE.2024-09-13T20-26-02Z"
	minioRootUser = "minioadmin"
	minioRootPass = "minioadmin"
	minioRegion   = "us-east-1"
)

// minioBackend bundles everything tests need to talk to a running
// minio container: the host-visible endpoint and a bucket name
// created for this test.
type minioBackend struct {
	endpoint  string                // e.g. http://127.0.0.1:55020
	bucket    string                // pre-created bucket
	container testcontainers.Container
}

// startMinio spins up a minio S3 server in single-node mode and
// creates one bucket via `mc mb` inside the container. `mc` is
// pre-installed in the minio image, so we avoid a host-side SigV4
// implementation just for bucket setup.
func startMinio(t *testing.T) *minioBackend {
	t.Helper()
	e2e.SkipUnlessDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        minioImage,
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioRootUser,
			"MINIO_ROOT_PASSWORD": minioRootPass,
		},
		Cmd: []string{"server", "/data"},
		// /minio/health/ready emits 200 once the S3 listeners attach.
		// The bare-port wait would unblock before SigV4 is ready.
		WaitingFor: wait.ForHTTP("/minio/health/ready").
			WithPort("9000/tcp").
			WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("minio container unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("minio host: %v", err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("minio mapped port: %v", err)
	}
	mb := &minioBackend{
		endpoint:  fmt.Sprintf("http://%s:%s", host, port.Port()),
		bucket:    "reflow-kms-" + randHex(t, 8),
		container: c,
	}
	mb.createBucketViaExec(t)
	return mb
}

// withEnv pins AWS env vars so gocloud.dev/blob's s3blob driver
// (and any aws-sdk consumer) addresses this container's endpoint.
// Test teardown restores prior state via t.Setenv.
//
// AWS_CONFIG_FILE / AWS_SHARED_CREDENTIALS_FILE are pinned to
// /dev/null for the same reason as the SQS suite: developer-machine
// ~/.aws state is unpredictable; a directory at that path breaks
// LoadDefaultConfig.
func (mb *minioBackend) withEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", minioRootUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioRootPass)
	t.Setenv("AWS_REGION", minioRegion)
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
}

// gocloudBucketURI returns the s3:// URI that gocloud.dev/blob.OpenBucket
// uses to open the test bucket. The endpoint, path-style, and disable-ssl
// query parameters are what route the bucket call to minio instead of
// the public AWS S3 endpoints.
func (mb *minioBackend) gocloudBucketURI() string {
	q := url.Values{}
	q.Set("endpoint", mb.endpoint)
	q.Set("region", minioRegion)
	q.Set("s3ForcePathStyle", "true")
	q.Set("hostname_immutable", "true")
	return "s3://" + mb.bucket + "?" + q.Encode()
}

// gocloudObjectURI returns the full object URI for `key` in the test
// bucket — what gets stored in a SecretRecord.blob_uri.
func (mb *minioBackend) gocloudObjectURI(key string) string {
	q := url.Values{}
	q.Set("endpoint", mb.endpoint)
	q.Set("region", minioRegion)
	q.Set("s3ForcePathStyle", "true")
	q.Set("hostname_immutable", "true")
	return "s3://" + mb.bucket + "/" + key + "?" + q.Encode()
}

// putObject uploads `data` at `key` inside the test bucket via the
// gocloud s3 driver. Used to stage KEK blobs + ciphertext for tests.
func (mb *minioBackend) putObject(t *testing.T, key string, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bkt, err := blob.OpenBucket(ctx, mb.gocloudBucketURI())
	if err != nil {
		t.Fatalf("open minio bucket: %v", err)
	}
	defer bkt.Close()
	if err := bkt.WriteAll(ctx, key, data, nil); err != nil {
		t.Fatalf("write %q: %v", key, err)
	}
}

// createBucketViaExec drives `mc mb` inside the running minio
// container. `mc` ships pre-installed in the minio image and accepts
// the local server with root creds.
func (mb *minioBackend) createBucketViaExec(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// mc alias set + mc mb. `--insecure` is a safety net in case mc
	// resolves the local TLS cert through a different code path; minio
	// runs http in our test config, so it's just noise.
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf(
			"mc alias set local http://127.0.0.1:9000 %s %s && mc mb --ignore-existing local/%s",
			minioRootUser, minioRootPass, mb.bucket,
		),
	}
	rc, reader, err := mb.container.Exec(ctx, cmd)
	if err != nil {
		t.Fatalf("minio mc exec: %v", err)
	}
	out, _ := io.ReadAll(reader)
	if rc != 0 {
		t.Fatalf("minio mc mb %q rc=%d out=%s", mb.bucket, rc, out)
	}
}

func randHex(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand: %v", err)
	}
	const hex = "0123456789abcdef"
	out := make([]byte, n*2)
	for i, b := range buf {
		out[i*2] = hex[b>>4]
		out[i*2+1] = hex[b&0x0f]
	}
	return string(out)
}
