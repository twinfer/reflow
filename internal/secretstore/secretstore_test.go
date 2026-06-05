package secretstore_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tink-crypto/tink-go/v2/core/registry"

	"github.com/twinfer/reflw/internal/secretstore"
	tinkkmsblob "github.com/twinfer/reflw/pkg/kms/blob"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// stageSecret produces a SecretRecord pointing at a freshly-encrypted
// ciphertext blob backed by a fresh BlobKMS KEK. Returns the record
// plus the on-disk paths so the test can rotate / delete to exercise
// preserve-prev semantics.
func stageSecret(t *testing.T, name string, plaintext []byte) (*enginev1.SecretRecord, string, string) {
	t.Helper()
	dir := t.TempDir()
	kekPath := filepath.Join(dir, "kek.bin")
	ctPath := filepath.Join(dir, "secret.bin")

	kek, err := tinkkmsblob.InitKEK()
	if err != nil {
		t.Fatalf("InitKEK: %v", err)
	}
	if err := os.WriteFile(kekPath, kek, 0o600); err != nil {
		t.Fatalf("write kek: %v", err)
	}
	kekURI := tinkkmsblob.URIPrefix + "file://" + kekPath
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("GetKMSClient: %v", err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt(plaintext, []byte(name))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if err := os.WriteFile(ctPath, ct, 0o600); err != nil {
		t.Fatalf("write ciphertext: %v", err)
	}
	rec := &enginev1.SecretRecord{
		Name: name,
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: "file://" + ctPath,
				KekUri:  kekURI,
			},
		},
	}
	return rec, kekURI, ctPath
}

func TestResolver_Roundtrip(t *testing.T) {
	t.Parallel()
	rec, _, _ := stageSecret(t, "github-hmac", []byte("ghs_payload_v1"))
	r := secretstore.New(nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, ok := r.Lookup("github-hmac")
	if !ok {
		t.Fatal("Lookup returned false; expected resolved bytes")
	}
	if string(got) != "ghs_payload_v1" {
		t.Fatalf("Lookup = %q; want ghs_payload_v1", got)
	}
}

func TestResolver_PreservePrevOnError(t *testing.T) {
	t.Parallel()
	rec, _, ctPath := stageSecret(t, "rotate-me", []byte("original"))
	r := secretstore.New(nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if got, _ := r.Lookup("rotate-me"); string(got) != "original" {
		t.Fatalf("first Lookup = %q; want original", got)
	}
	// Delete the ciphertext blob; next reconcile must fail to fetch
	// but preserve the previously-resolved bytes.
	if err := os.Remove(ctPath); err != nil {
		t.Fatalf("remove ciphertext: %v", err)
	}
	if err := r.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got, _ := r.Lookup("rotate-me"); string(got) != "original" {
		t.Fatalf("Lookup after blob delete = %q; want preserved 'original'", got)
	}
}

func TestResolver_LookupUnknownReturnsFalse(t *testing.T) {
	t.Parallel()
	r := secretstore.New(nil, nil)
	if _, ok := r.Lookup("never-existed"); ok {
		t.Fatal("Lookup of unknown name returned true")
	}
}

func TestResolver_RemovedRecordDropsFromSnapshot(t *testing.T) {
	t.Parallel()
	rec, _, _ := stageSecret(t, "doomed", []byte("temp"))
	r := secretstore.New(nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := r.Lookup("doomed"); !ok {
		t.Fatal("expected doomed to be resolved")
	}
	// Next reconcile with empty desired list — removed rows drop.
	if err := r.Reconcile(context.Background(), nil); err != nil {
		t.Fatalf("empty Reconcile: %v", err)
	}
	if _, ok := r.Lookup("doomed"); ok {
		t.Fatal("doomed should be gone after empty reconcile")
	}
}
