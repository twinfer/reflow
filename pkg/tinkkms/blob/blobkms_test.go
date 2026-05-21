package blob

import (
	"crypto/rand"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "gocloud.dev/blob/fileblob"
)

func TestSplitURI(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in           string
		wantBucket   string
		wantKey      string
		wantErrMatch string
	}{
		{"blobkms+s3://bucket/master.key", "s3://bucket", "master.key", ""},
		{"blobkms+s3://bucket/path/master.key", "s3://bucket/path", "master.key", ""},
		{"blobkms+file:///etc/reflow/master.key", "file:///etc/reflow", "master.key", ""},
		{"blobkms+mem://test/k", "mem://test", "k", ""},
		{"blobkms+gs://b/x/y/z.bin", "gs://b/x/y", "z.bin", ""},
		// errors
		{"blobkms+s3://bucket/", "", "", "empty object key"},
		{"blobkms+nopath", "", "", "missing scheme"},
		{"blobkms+s3://onlyauthority", "", "", "missing object key"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			b, k, err := SplitURI(tc.in)
			if tc.wantErrMatch != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrMatch) {
					t.Fatalf("want err containing %q, got %v", tc.wantErrMatch, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if b != tc.wantBucket || k != tc.wantKey {
				t.Fatalf("got (%q, %q) want (%q, %q)", b, k, tc.wantBucket, tc.wantKey)
			}
		})
	}
}

func TestSupported(t *testing.T) {
	t.Parallel()
	kc := New()
	if !kc.Supported("blobkms+mem://x/k") {
		t.Error("Supported(blobkms+...) = false; want true")
	}
	if kc.Supported("aws-kms://foo") {
		t.Error("Supported(aws-kms://...) = true; want false")
	}
}

// stageMaster writes a 32-byte master key into a fresh temp directory
// and returns the blobkms+ URI that addresses it.
func stageMaster(t *testing.T, bytes []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "master.key")
	if err := os.WriteFile(path, bytes, 0o600); err != nil {
		t.Fatalf("write master: %v", err)
	}
	// fileblob accepts file:///abs/path — split at last '/'.
	return URIPrefix + "file://" + (&url.URL{Path: dir}).String() + "/master.key"
}

func TestRoundtrip(t *testing.T) {
	t.Parallel()
	mk := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, mk); err != nil {
		t.Fatalf("rand: %v", err)
	}
	aead, err := New().GetAEAD(stageMaster(t, mk))
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	plaintext := []byte("hmac-secret-payload")
	aad := []byte("github-prod")
	ct, err := aead.Encrypt(plaintext, aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := aead.Decrypt(ct, aad)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("Decrypt = %q; want %q", got, plaintext)
	}
}

func TestDecrypt_WrongAAD(t *testing.T) {
	t.Parallel()
	mk := make([]byte, 32)
	_, _ = io.ReadFull(rand.Reader, mk)
	aead, err := New().GetAEAD(stageMaster(t, mk))
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt([]byte("payload"), []byte("name-A"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := aead.Decrypt(ct, []byte("name-B")); err == nil {
		t.Fatal("Decrypt with wrong AAD should fail")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	t.Parallel()
	mk := make([]byte, 32)
	_, _ = io.ReadFull(rand.Reader, mk)
	aead, err := New().GetAEAD(stageMaster(t, mk))
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	ct, err := aead.Encrypt([]byte("payload"), nil)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	ct[len(ct)-1] ^= 0xff
	if _, err := aead.Decrypt(ct, nil); err == nil {
		t.Fatal("Decrypt of tampered ciphertext should fail")
	}
}

func TestGetAEAD_WrongLengthMaster(t *testing.T) {
	t.Parallel()
	uri := stageMaster(t, []byte("only sixteen by5")) // 16 bytes, not 32
	if _, err := New().GetAEAD(uri); err == nil {
		t.Fatal("want error for non-32-byte master")
	}
}

func TestGetAEAD_MissingBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	uri := URIPrefix + "file://" + dir + "/missing.key"
	if _, err := New().GetAEAD(uri); err == nil {
		t.Fatal("want error for missing blob")
	}
}

func TestGetAEAD_UnsupportedURI(t *testing.T) {
	t.Parallel()
	if _, err := New().GetAEAD("aws-kms://foo"); err == nil {
		t.Fatal("want error for unsupported URI")
	}
}

// sanity check that the package's exported constants haven't drifted.
func TestConstants(t *testing.T) {
	t.Parallel()
	if URIPrefix != "blobkms+" {
		t.Errorf("URIPrefix = %q; want blobkms+", URIPrefix)
	}
	if MasterKeySize != 32 {
		t.Errorf("MasterKeySize = %d; want 32", MasterKeySize)
	}
	// silence the import lint if anything below grows.
	_ = errors.New
}
