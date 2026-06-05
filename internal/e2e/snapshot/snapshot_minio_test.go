//go:build e2e

// Package snapshot_test exercises the BlobRepository against a real
// S3-shaped backend (minio). The unit tests in
// internal/engine/snapshot/blob_test.go cover fileblob and memblob;
// this suite covers s3blob with a containerized minio so the SigV4
// + path-style query-param plumbing is regression-checked too.
package snapshot_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gocloud.dev/gcerrors"
	_ "gocloud.dev/blob/s3blob"

	"github.com/twinfer/reflw/internal/e2e"
	enginesnapshot "github.com/twinfer/reflw/internal/engine/snapshot"
)

const (
	minioImage    = "minio/minio:RELEASE.2024-09-13T20-26-02Z"
	minioRootUser = "minioadmin"
	minioRootPass = "minioadmin"
	minioRegion   = "us-east-1"
)

// minioBackend is a copy of the kms_test helper, scoped to this
// package. Extracting to a shared internal package would be cleaner
// but isn't worth the indirection for one extra suite.
type minioBackend struct {
	endpoint  string
	bucket    string
	container testcontainers.Container
}

// startMinio spins up minio in single-node mode, creates one bucket
// via `mc mb` inside the container, and returns the backend handle.
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
		bucket:    "reflw-snapshots-" + randHex(t, 8),
		container: c,
	}
	mb.createBucket(t)
	mb.setAWSEnv(t)
	return mb
}

// setAWSEnv pins the SDK env vars to point gocloud.dev/blob's s3blob
// driver at minio. /dev/null on the config + credentials file paths
// guards against unpredictable ~/.aws state on the dev machine.
func (mb *minioBackend) setAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", minioRootUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioRootPass)
	t.Setenv("AWS_REGION", minioRegion)
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
}

// gocloudBucketURI returns the s3:// URI that resolves the test bucket
// against minio via gocloud.dev/blob.OpenBucket.
func (mb *minioBackend) gocloudBucketURI() string {
	q := url.Values{}
	q.Set("endpoint", mb.endpoint)
	q.Set("region", minioRegion)
	q.Set("s3ForcePathStyle", "true")
	q.Set("hostname_immutable", "true")
	return "s3://" + mb.bucket + "?" + q.Encode()
}

func (mb *minioBackend) createBucket(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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

// openRepo opens a BlobRepository pointed at the test bucket. Caller
// owns the Close.
func (mb *minioBackend) openRepo(t *testing.T) *enginesnapshot.BlobRepository {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bkt, err := enginesnapshot.OpenBucket(ctx, mb.gocloudBucketURI())
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	return &enginesnapshot.BlobRepository{Bucket: bkt}
}

// TestSnapshot_Minio_RoundTrip drives one snapshot archive through
// the BlobRepository: write → list → read back → delete. Verifies the
// s3:// path-style + minio combination handles the gzip + sidecar
// shape end-to-end.
func TestSnapshot_Minio_RoundTrip(t *testing.T) {
	mb := startMinio(t)
	repo := mb.openRepo(t)
	t.Cleanup(func() { _ = repo.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		shardID   uint64 = 7
		raftIndex uint64 = 42
	)
	payload := []byte("snapshot-payload-via-minio")

	w, err := repo.NewWriter(ctx, shardID, raftIndex)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	refs, err := repo.List(ctx, shardID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("List len = %d; want 1", len(refs))
	}
	if refs[0].ShardID != shardID || refs[0].Index != raftIndex {
		t.Fatalf("List ref = %+v; want shard=%d index=%d", refs[0], shardID, raftIndex)
	}
	if refs[0].SizeBytes <= 0 {
		t.Errorf("List ref size = %d; want > 0", refs[0].SizeBytes)
	}

	r, err := repo.NewReader(ctx, shardID, raftIndex)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read = %q; want %q", got, payload)
	}

	if err := repo.Delete(ctx, shardID, raftIndex); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	refs, err = repo.List(ctx, shardID)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("List after delete len = %d; want 0", len(refs))
	}
}

// TestSnapshot_Minio_RetainEvictsOldest writes more snapshots than
// the Retain budget and asserts the inline retention path runs against
// minio without leaking the oldest. Mirrors the file-backed retention
// test, but against a real S3 backend so List+Delete-via-SigV4 is
// exercised under load.
func TestSnapshot_Minio_RetainEvictsOldest(t *testing.T) {
	mb := startMinio(t)
	bkt, err := enginesnapshot.OpenBucket(context.Background(), mb.gocloudBucketURI())
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	repo := &enginesnapshot.BlobRepository{Bucket: bkt, Retain: 2}
	t.Cleanup(func() { _ = repo.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const shardID uint64 = 11
	for _, idx := range []uint64{10, 20, 30} {
		w, err := repo.NewWriter(ctx, shardID, idx)
		if err != nil {
			t.Fatalf("NewWriter(%d): %v", idx, err)
		}
		if _, err := w.Write([]byte(fmt.Sprintf("idx=%d", idx))); err != nil {
			t.Fatalf("Write(%d): %v", idx, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%d): %v", idx, err)
		}
	}

	refs, err := repo.List(ctx, shardID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("List len = %d; want 2 (Retain=2 should have evicted idx=10)", len(refs))
	}
	if refs[0].Index != 20 || refs[1].Index != 30 {
		t.Fatalf("retained indices = [%d,%d]; want [20,30]", refs[0].Index, refs[1].Index)
	}
}

// TestSnapshot_Minio_ReaderNotFoundSurfacesGcerr asserts the typed
// gcerrors.NotFound contract holds against s3blob. Callers that branch
// on `gcerrors.Code(err) == gcerrors.NotFound` for "snapshot missing"
// must keep working when the backend is real S3.
func TestSnapshot_Minio_ReaderNotFoundSurfacesGcerr(t *testing.T) {
	mb := startMinio(t)
	repo := mb.openRepo(t)
	t.Cleanup(func() { _ = repo.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := repo.NewReader(ctx, 99, 99)
	if err == nil {
		t.Fatal("NewReader on missing snapshot returned no error")
	}
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Fatalf("err = %v (gcerrors.Code=%v); want gcerrors.NotFound", err, gcerrors.Code(err))
	}
}

