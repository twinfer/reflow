package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tinkkmsblob "github.com/twinfer/reflw/pkg/kms/blob"
)

// captureStdout returns whatever fn wrote to os.Stdout.
func captureStdout(t *testing.T, fn func()) []byte {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	prev := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = prev }()
	done := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- b
	}()
	fn()
	_ = w.Close()
	return <-done
}

func TestInitKEK_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	kekPath := filepath.Join(dir, "kek.bin")
	kekURI := "file://" + kekPath

	if err := cmdInitKEK(context.Background(), []string{"--blob-uri=" + kekURI}); err != nil {
		t.Fatalf("init-kek: %v", err)
	}
	info, err := os.Stat(kekPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() < tinkkmsblob.BootKeySize {
		t.Fatalf("KEK blob is %d bytes; want > %d", info.Size(), tinkkmsblob.BootKeySize)
	}

	// init-kek without --force should refuse to overwrite.
	if err := cmdInitKEK(context.Background(), []string{"--blob-uri=" + kekURI}); err == nil {
		t.Fatal("init-kek should refuse to overwrite existing blob without --force")
	}
	if err := cmdInitKEK(context.Background(), []string{"--blob-uri=" + kekURI, "--force"}); err != nil {
		t.Fatalf("init-kek --force: %v", err)
	}
}

// TestEncryptDecrypt_Roundtrip exercises the file-ops half of the
// create/decrypt-secret pair without involving the admin RPC (no live
// cluster needed). The helper encryptToBlob is what cmdCreateSecret
// calls before issuing UpsertSecret; tests target it directly.
func TestEncryptDecrypt_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	kekURI := "file://" + filepath.Join(dir, "kek.bin")
	ctURI := "file://" + filepath.Join(dir, "secret.bin")
	kmsURI := tinkkmsblob.URIPrefix + kekURI

	if err := cmdInitKEK(context.Background(), []string{"--blob-uri=" + kekURI}); err != nil {
		t.Fatalf("init-kek: %v", err)
	}

	plaintext := []byte("ghs_test_secret_payload")
	if err := encryptToBlob(context.Background(), "github-prod", kmsURI, ctURI, plaintext); err != nil {
		t.Fatalf("encryptToBlob: %v", err)
	}

	out := captureStdout(t, func() {
		err := cmdDecryptSecret(context.Background(), []string{
			"--name=github-prod",
			"--kek-uri=" + kmsURI,
			"--blob-uri=" + ctURI,
		})
		if err != nil {
			t.Fatalf("decrypt-secret: %v", err)
		}
	})
	if !bytes.Equal(out, plaintext) {
		t.Fatalf("roundtrip mismatch: got %q, want %q", out, plaintext)
	}
}

func TestDecryptSecret_WrongAAD(t *testing.T) {
	dir := t.TempDir()
	kekURI := "file://" + filepath.Join(dir, "kek.bin")
	ctURI := "file://" + filepath.Join(dir, "secret.bin")
	kmsURI := tinkkmsblob.URIPrefix + kekURI

	if err := cmdInitKEK(context.Background(), []string{"--blob-uri=" + kekURI}); err != nil {
		t.Fatalf("init-kek: %v", err)
	}
	if err := encryptToBlob(context.Background(), "name-A", kmsURI, ctURI, []byte("payload")); err != nil {
		t.Fatalf("encryptToBlob: %v", err)
	}
	err := cmdDecryptSecret(context.Background(), []string{
		"--name=name-B",
		"--kek-uri=" + kmsURI,
		"--blob-uri=" + ctURI,
	})
	if err == nil || !strings.Contains(err.Error(), "Decrypt") {
		t.Fatalf("expected decrypt error with wrong AAD; got %v", err)
	}
}

func TestCreateSecret_MissingFlags(t *testing.T) {
	cases := [][]string{
		{}, // all missing
		{"--name=foo"},
		{"--name=foo", "--kek-uri=blobkms+file:///tmp/k"},
	}
	for _, args := range cases {
		err := cmdCreateSecret(context.Background(), args)
		if err == nil {
			t.Errorf("args=%v: want missing-flag error", args)
		}
	}
}
