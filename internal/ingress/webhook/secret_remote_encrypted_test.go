package webhook_test

import (
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tinkreg "github.com/tink-crypto/tink-go/v2/core/registry"

	tinkkmsblob "github.com/twinfer/reflow/pkg/tinkkms/blob"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// blobkmsOnce registers BlobKMS exactly once across the webhook_test
// package: tink-go's registry.RegisterKMSClient appends without dedup
// (Tink registers globally per-process), so repeated calls from
// parallel tests would stack duplicate clients.
var blobkmsOnce sync.Once

func ensureBlobKMS(t *testing.T) {
	t.Helper()
	blobkmsOnce.Do(func() {
		tinkreg.RegisterKMSClient(tinkkmsblob.New())
	})
}

// stageMasterAndCiphertext writes a fresh 32-byte master key and the
// ciphertext for `plaintext` (AAD = name) to a temp dir, returns the
// blobkms+ KEK URI, the ciphertext blob URI, and the ciphertext path
// (so the test can rewrite it for rotation).
func stageMasterAndCiphertext(t *testing.T, name string, plaintext []byte) (kekURI, blobURI, ctPath string) {
	t.Helper()
	dir := t.TempDir()

	mk := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, mk); err != nil {
		t.Fatalf("rand: %v", err)
	}
	masterPath := filepath.Join(dir, "master.key")
	if err := os.WriteFile(masterPath, mk, 0o600); err != nil {
		t.Fatalf("write master: %v", err)
	}
	kekURI = tinkkmsblob.URIPrefix + "file://" + dir + "/master.key"

	// Use BlobKMS to encrypt; the registered client will be used in
	// production via registry.GetKMSClient, but here we exercise it
	// directly so the test doesn't depend on registration order.
	aead, err := tinkkmsblob.New().GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt(plaintext, []byte(name))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	ctPath = filepath.Join(dir, "secret.bin")
	if err := os.WriteFile(ctPath, ct, 0o600); err != nil {
		t.Fatalf("write ciphertext: %v", err)
	}
	blobURI = "file://" + dir + "/secret.bin"
	return kekURI, blobURI, ctPath
}

// reEncryptToFile replaces the ciphertext at ctPath with a fresh
// encryption of `plaintext` under kekURI / AAD=name. Used to simulate
// operator key rotation.
func reEncryptToFile(t *testing.T, kekURI, ctPath, name string, plaintext []byte) {
	t.Helper()
	aead, err := tinkkmsblob.New().GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt(plaintext, []byte(name))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if err := os.WriteFile(ctPath, ct, 0o600); err != nil {
		t.Fatalf("rewrite ciphertext: %v", err)
	}
}

func TestRunReconciler_RemoteEncrypted_Roundtrip(t *testing.T) {
	t.Parallel()
	ensureBlobKMS(t)
	name := "github-prod"
	secret := []byte("hmac-secret-v1")
	kekURI, blobURI, _ := stageMasterAndCiphertext(t, name, secret)

	sub := &recSubmitter{}
	m := newMgr(t, sub)
	rec := &enginev1.WebhookSourceRecord{
		Name:     name,
		Path:     "/webhooks/github",
		Verifier: "github",
		SecretRef: &enginev1.SecretRef{Source: &enginev1.SecretRef_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: blobURI,
				KekUri:  kekURI,
			},
		}},
		Service: "svc",
		Handler: "on",
	}
	reader := &fakeReader{}
	reader.set(1, rec)
	wakes := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.RunReconciler(ctx, wakes, reader) }()

	if !waitForCount(t, sub, m, "/webhooks/github", secret, 1, 2*time.Second) {
		t.Fatal("initial dispatch never landed")
	}
}

func TestRunReconciler_RemoteEncrypted_Rotation(t *testing.T) {
	t.Parallel()
	ensureBlobKMS(t)
	name := "github-rotate"
	secretV1 := []byte("hmac-v1")
	secretV2 := []byte("hmac-v2")
	kekURI, blobURI, ctPath := stageMasterAndCiphertext(t, name, secretV1)

	sub := &recSubmitter{}
	m := newMgr(t, sub)
	rec := &enginev1.WebhookSourceRecord{
		Name:     name,
		Path:     "/webhooks/rotate",
		Verifier: "github",
		SecretRef: &enginev1.SecretRef{Source: &enginev1.SecretRef_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: blobURI,
				KekUri:  kekURI,
			},
		}},
		Service: "svc",
		Handler: "on",
	}
	reader := &fakeReader{}
	reader.set(1, rec)
	wakes := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.RunReconciler(ctx, wakes, reader) }()

	if !waitForCount(t, sub, m, "/webhooks/rotate", secretV1, 1, 2*time.Second) {
		t.Fatal("initial dispatch never landed")
	}

	// Rotate: overwrite the ciphertext with a new secret under the
	// same KEK + AAD. Trigger a wake; the reconciler should re-fetch
	// and re-decrypt.
	reEncryptToFile(t, kekURI, ctPath, name, secretV2)
	wakes <- struct{}{}

	if !waitForReject(t, m, "/webhooks/rotate", secretV1, 2*time.Second) {
		t.Fatal("rotation didn't take effect; v1 signature still accepted")
	}
	before := sub.count()
	if resp := postSigned(t, m, "/webhooks/rotate", secretV2, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("v2 signature: status=%d", resp.StatusCode)
	}
	if sub.count() != before+1 {
		t.Fatal("submitter not called for v2 signature")
	}
}

func TestRunReconciler_RemoteEncrypted_WrongAADPreservesPrev(t *testing.T) {
	t.Parallel()
	ensureBlobKMS(t)
	name := "github-aad"
	secretV1 := []byte("hmac-aad-v1")
	kekURI, blobURI, ctPath := stageMasterAndCiphertext(t, name, secretV1)

	sub := &recSubmitter{}
	m := newMgr(t, sub)
	rec := &enginev1.WebhookSourceRecord{
		Name:     name,
		Path:     "/webhooks/aad",
		Verifier: "github",
		SecretRef: &enginev1.SecretRef{Source: &enginev1.SecretRef_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: blobURI,
				KekUri:  kekURI,
			},
		}},
		Service: "svc",
		Handler: "on",
	}
	reader := &fakeReader{}
	reader.set(1, rec)
	wakes := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.RunReconciler(ctx, wakes, reader) }()
	if !waitForCount(t, sub, m, "/webhooks/aad", secretV1, 1, 2*time.Second) {
		t.Fatal("initial dispatch never landed")
	}

	// Re-encrypt under a DIFFERENT AAD; this simulates a tampered or
	// mis-targeted blob. Next reconcile should fail Decrypt and
	// preserve the previous resolved secret.
	reEncryptToFile(t, kekURI, ctPath, "wrong-aad", []byte("hmac-aad-v2"))
	wakes <- struct{}{}
	time.Sleep(120 * time.Millisecond) // let reconcile run

	before := sub.count()
	if resp := postSigned(t, m, "/webhooks/aad", secretV1, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("prev secret should still verify after wrong-AAD blob: status=%d", resp.StatusCode)
	}
	if sub.count() != before+1 {
		t.Fatal("submitter not called; prev secret didn't carry through wrong-AAD failure")
	}
}

func TestRunReconciler_RemoteEncrypted_MissingBlobPreservesPrev(t *testing.T) {
	t.Parallel()
	ensureBlobKMS(t)
	name := "github-missing"
	secret := []byte("hmac-missing")
	kekURI, blobURI, ctPath := stageMasterAndCiphertext(t, name, secret)

	sub := &recSubmitter{}
	m := newMgr(t, sub)
	rec := &enginev1.WebhookSourceRecord{
		Name:     name,
		Path:     "/webhooks/missing",
		Verifier: "github",
		SecretRef: &enginev1.SecretRef{Source: &enginev1.SecretRef_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: blobURI,
				KekUri:  kekURI,
			},
		}},
		Service: "svc",
		Handler: "on",
	}
	reader := &fakeReader{}
	reader.set(1, rec)
	wakes := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.RunReconciler(ctx, wakes, reader) }()
	if !waitForCount(t, sub, m, "/webhooks/missing", secret, 1, 2*time.Second) {
		t.Fatal("initial dispatch never landed")
	}

	// Delete the ciphertext blob — next reconcile should hit
	// blob_fetch failure and preserve the previously-resolved bytes.
	if err := os.Remove(ctPath); err != nil {
		t.Fatalf("remove ciphertext: %v", err)
	}
	wakes <- struct{}{}
	time.Sleep(120 * time.Millisecond)

	before := sub.count()
	if resp := postSigned(t, m, "/webhooks/missing", secret, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("prev secret should still verify after blob delete: status=%d", resp.StatusCode)
	}
	if sub.count() != before+1 {
		t.Fatal("submitter not called; prev secret didn't carry through missing-blob failure")
	}
}
