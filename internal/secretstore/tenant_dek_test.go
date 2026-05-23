package secretstore_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"

	"github.com/twinfer/reflow/internal/secretstore"
	tinkkmsblob "github.com/twinfer/reflow/pkg/kms/blob"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// stageTenantDEK produces a TenantDEKRecord pointing at a freshly-
// generated AES-256-GCM keyset, serialized as cleartext, then
// KEK-encrypted (AAD = []byte(name)) into a fresh BlobKMS-backed
// ciphertext blob. Returns the record plus the ciphertext path so
// tests can rotate / delete to exercise preserve-prev semantics.
func stageTenantDEK(t *testing.T, tenantID uint32, name string) *enginev1.TenantDEKRecord {
	t.Helper()
	dir := t.TempDir()
	kekPath := filepath.Join(dir, "kek.bin")
	ctPath := filepath.Join(dir, "tenant.dek")

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
	wrap, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}

	// Fresh AES-256-GCM tenant DEK keyset, serialized as cleartext.
	handle, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		t.Fatalf("NewHandle: %v", err)
	}
	var ksBuf bytes.Buffer
	if err := insecurecleartextkeyset.Write(handle, keyset.NewBinaryWriter(&ksBuf)); err != nil {
		t.Fatalf("Write cleartext keyset: %v", err)
	}

	ct, err := wrap.Encrypt(ksBuf.Bytes(), []byte(name))
	if err != nil {
		t.Fatalf("KEK Encrypt: %v", err)
	}
	if err := os.WriteFile(ctPath, ct, 0o600); err != nil {
		t.Fatalf("write ciphertext: %v", err)
	}
	return &enginev1.TenantDEKRecord{
		TenantId: tenantID,
		Name:     name,
		RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
			BlobUri: "file://" + ctPath,
			KekUri:  kekURI,
		},
	}
}

func TestTenantDEKResolver_Roundtrip(t *testing.T) {
	t.Parallel()
	rec := stageTenantDEK(t, 42, "dek-acme-v1")
	r := secretstore.NewTenantDEKResolver(nil, nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.TenantDEKRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	a, ok := r.Lookup(42)
	if !ok {
		t.Fatal("Lookup(42) returned false; expected resolved AEAD")
	}
	// Round-trip encrypt+decrypt through the resolved primitive.
	ct, err := a.Encrypt([]byte("hello"), []byte("aad"))
	if err != nil {
		t.Fatalf("Encrypt via resolved AEAD: %v", err)
	}
	pt, err := a.Decrypt(ct, []byte("aad"))
	if err != nil {
		t.Fatalf("Decrypt via resolved AEAD: %v", err)
	}
	if string(pt) != "hello" {
		t.Fatalf("round-trip = %q; want hello", pt)
	}
}

func TestTenantDEKResolver_PerTenantIsolation(t *testing.T) {
	t.Parallel()
	recA := stageTenantDEK(t, 11, "dek-alpha")
	recB := stageTenantDEK(t, 22, "dek-beta")
	r := secretstore.NewTenantDEKResolver(nil, nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.TenantDEKRecord{recA, recB}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	aeadA, ok := r.Lookup(11)
	if !ok {
		t.Fatal("Lookup(11) returned false")
	}
	aeadB, ok := r.Lookup(22)
	if !ok {
		t.Fatal("Lookup(22) returned false")
	}
	// Ciphertext from A's AEAD must not decrypt under B's AEAD.
	ct, err := aeadA.Encrypt([]byte("for-alpha"), []byte("shared-aad"))
	if err != nil {
		t.Fatalf("Encrypt under A: %v", err)
	}
	if _, err := aeadB.Decrypt(ct, []byte("shared-aad")); err == nil {
		t.Fatal("B decrypted A's ciphertext; per-tenant isolation broken")
	}
}

func TestTenantDEKResolver_DefaultAEAD(t *testing.T) {
	t.Parallel()
	// nil default → Lookup(0) is (nil, false).
	r := secretstore.NewTenantDEKResolver(nil, nil, nil)
	if _, ok := r.Lookup(0); ok {
		t.Fatal("Lookup(0) with nil default returned true; expected passthrough sentinel")
	}
	// non-nil default → Lookup(0) returns it.
	handle, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		t.Fatalf("NewHandle: %v", err)
	}
	def, err := aead.New(handle)
	if err != nil {
		t.Fatalf("aead.New: %v", err)
	}
	r2 := secretstore.NewTenantDEKResolver(nil, nil, def)
	got, ok := r2.Lookup(0)
	if !ok {
		t.Fatal("Lookup(0) with default returned false")
	}
	if got != def {
		t.Fatal("Lookup(0) returned a different AEAD than the configured default")
	}
}

func TestTenantDEKResolver_AADMismatchFails(t *testing.T) {
	t.Parallel()
	rec := stageTenantDEK(t, 7, "dek-aad-binding")
	// Mutate the AAD on the record (the resolver passes record.name as
	// AAD into KEK Decrypt) — decryption must fail.
	rec.Name = "dek-different-name"
	r := secretstore.NewTenantDEKResolver(nil, nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.TenantDEKRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := r.Lookup(7); ok {
		t.Fatal("Lookup(7) returned true after AAD mismatch; resolver accepted a tampered record")
	}
}

func TestTenantDEKResolver_RemovedRecordDropsFromSnapshot(t *testing.T) {
	t.Parallel()
	rec := stageTenantDEK(t, 99, "doomed")
	r := secretstore.NewTenantDEKResolver(nil, nil, nil)
	if err := r.Reconcile(context.Background(), []*enginev1.TenantDEKRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := r.Lookup(99); !ok {
		t.Fatal("expected tenant 99 resolved")
	}
	if err := r.Reconcile(context.Background(), nil); err != nil {
		t.Fatalf("empty Reconcile: %v", err)
	}
	if _, ok := r.Lookup(99); ok {
		t.Fatal("tenant 99 should be gone after empty reconcile")
	}
}
