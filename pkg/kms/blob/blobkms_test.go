package blob

import (
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
		{"blobkms+s3://bucket/kek.bin", "s3://bucket", "kek.bin", ""},
		{"blobkms+s3://bucket/path/kek.bin", "s3://bucket/path", "kek.bin", ""},
		{"blobkms+file:///etc/reflw/kek.bin", "file:///etc/reflw", "kek.bin", ""},
		{"blobkms+mem://test/k", "mem://test", "k", ""},
		{"blobkms+gs://b/x/y/z.bin", "gs://b/x/y", "z.bin", ""},
		// Query-string-carrying URI (endpoint overrides, etc.). The
		// query attaches to the bucket URI; the slash-split happens
		// against the path portion only.
		{
			"blobkms+s3://bucket/kek.bin?endpoint=http%3A%2F%2Fminio%3A9000&region=us-east-1",
			"s3://bucket?endpoint=http%3A%2F%2Fminio%3A9000&region=us-east-1",
			"kek.bin",
			"",
		},
		{
			"blobkms+s3://bucket/path/kek.bin?endpoint=http%3A%2F%2Flocalhost%3A9000",
			"s3://bucket/path?endpoint=http%3A%2F%2Flocalhost%3A9000",
			"kek.bin",
			"",
		},
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

// stageKEK writes a fresh KEK blob via InitKEK to a temp directory and
// returns the blobkms+ URI addressing it.
func stageKEK(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kek.bin")
	raw, err := InitKEK()
	if err != nil {
		t.Fatalf("InitKEK: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write kek: %v", err)
	}
	return URIPrefix + "file://" + dir + "/kek.bin"
}

func TestRoundtrip(t *testing.T) {
	t.Parallel()
	aead, err := New().GetAEAD(stageKEK(t))
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
	aead, err := New().GetAEAD(stageKEK(t))
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
	aead, err := New().GetAEAD(stageKEK(t))
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

func TestGetAEAD_TooShortBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Write a 16-byte blob — well under BootKeySize + an encrypted keyset.
	if err := os.WriteFile(filepath.Join(dir, "kek.bin"), []byte("only sixteen by5"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	uri := URIPrefix + "file://" + dir + "/kek.bin"
	if _, err := New().GetAEAD(uri); err == nil {
		t.Fatal("want error for too-short KEK blob")
	}
}

func TestGetAEAD_BadKeyset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Write a blob with a valid 32-byte boot key but garbage where the
	// encrypted keyset bytes should be.
	junk := make([]byte, 32+64)
	for i := range junk {
		junk[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(dir, "kek.bin"), junk, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	uri := URIPrefix + "file://" + dir + "/kek.bin"
	if _, err := New().GetAEAD(uri); err == nil {
		t.Fatal("want error for malformed keyset bytes")
	}
}

func TestGetAEAD_MissingBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	uri := URIPrefix + "file://" + dir + "/missing.bin"
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
	if BootKeySize != 32 {
		t.Errorf("BootKeySize = %d; want 32", BootKeySize)
	}
}
