//go:build e2e

package kms_test

import (
	"context"
	"testing"

	"github.com/tink-crypto/tink-go/v2/core/registry"

	"github.com/twinfer/reflw/internal/secretstore"
	tinkkmsblob "github.com/twinfer/reflw/pkg/kms/blob"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestBlobKMS_Minio_RoundTrip exercises the production BlobKMS path
// end-to-end against minio: KEK lives in the bucket, secret ciphertext
// lives in the same bucket, secretstore.Resolver does a real Reconcile
// pass that fetches both via gocloud.dev/blob and decrypts via Tink.
//
// This is the canonical "BYO blob store works against a real S3-shaped
// backend" smoke. The unit path (tinkkmsblob.TestRoundtrip) covers
// fileblob; this one covers s3blob.
func TestBlobKMS_Minio_RoundTrip(t *testing.T) {
	mb := startMinio(t)
	mb.withEnv(t)

	// Stage a fresh KEK in the bucket.
	rawKEK, err := tinkkmsblob.InitKEK()
	if err != nil {
		t.Fatalf("InitKEK: %v", err)
	}
	const kekKey = "kek.bin"
	mb.putObject(t, kekKey, rawKEK)

	// Resolve the AEAD via Tink's registry the same way secretstore
	// does on the hot path. The pkg/kms/blob init has already
	// registered the blobkms+ KMSClient at package load time.
	kekURI := tinkkmsblob.URIPrefix + mb.gocloudObjectURI(kekKey)
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("GetKMSClient(%s): %v", kekURI, err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	const secretName = "github-hmac"
	pt := []byte("ghs_payload_v1")
	ct, err := aead.Encrypt(pt, []byte(secretName))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Stage the ciphertext in the SAME bucket. Keep the key flat
	// (no `secrets/` prefix) — s3blob's URLOpener does not treat
	// the bucket-URI path as a key prefix, so a key at the bucket
	// root is the simplest shape that round-trips.
	const ctKey = "secret-github-hmac.ct"
	mb.putObject(t, ctKey, ct)

	rec := &enginev1.SecretRecord{
		Name: secretName,
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: mb.gocloudObjectURI(ctKey),
				KekUri:  kekURI,
			},
		},
	}

	resolver := secretstore.New(nil, nil)
	if err := resolver.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, ok := resolver.Lookup(secretName)
	if !ok {
		t.Fatal("Lookup returned false; expected resolved bytes")
	}
	if string(got) != string(pt) {
		t.Fatalf("Lookup = %q; want %q", got, pt)
	}
}

// TestBlobKMS_Minio_WrongAADRejects is the AAD-binding regression
// check against the real backend. AAD = []byte(secret.name) is the
// guarantee that a ciphertext for secret A cannot be replayed onto a
// SecretRecord named B; this test verifies it holds when both the
// KEK and the ciphertext travel through s3blob's request path (in
// case any of that path subtly mangles bytes).
func TestBlobKMS_Minio_WrongAADRejects(t *testing.T) {
	mb := startMinio(t)
	mb.withEnv(t)

	rawKEK, err := tinkkmsblob.InitKEK()
	if err != nil {
		t.Fatalf("InitKEK: %v", err)
	}
	mb.putObject(t, "kek.bin", rawKEK)
	kekURI := tinkkmsblob.URIPrefix + mb.gocloudObjectURI("kek.bin")
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("GetKMSClient: %v", err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	// Encrypt under name-A.
	ct, err := aead.Encrypt([]byte("payload"), []byte("name-A"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	mb.putObject(t, "secret-misnamed.ct", ct)

	// SecretRecord claims name-B; secretstore Resolver should call
	// AEAD.Decrypt with AAD=name-B and fail.
	rec := &enginev1.SecretRecord{
		Name: "name-B",
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: mb.gocloudObjectURI("secret-misnamed.ct"),
				KekUri:  kekURI,
			},
		},
	}
	resolver := secretstore.New(nil, nil)
	if err := resolver.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, ok := resolver.Lookup("name-B"); ok {
		t.Fatalf("Lookup(name-B) succeeded with bytes %q; expected AAD mismatch to surface as unresolved", got)
	}
}

