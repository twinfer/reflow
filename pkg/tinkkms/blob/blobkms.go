// Package blob implements a Tink KMSClient backed by gocloud.dev/blob.
//
// URI form: blobkms+<gocloud-blob-uri>. The bucket portion is what
// gocloud.dev/blob.OpenBucket accepts; the key portion is the object name
// inside that bucket. Examples:
//
//	blobkms+s3://my-bucket/master.key
//	blobkms+file:///etc/reflow/master.key
//	blobkms+gs://my-bucket/keys/master.key
//	blobkms+mem://test/master.key   (tests only)
//
// The master key is read as raw bytes; it must be exactly 32 bytes
// (AES-256-GCM). Bytes are wrapped in a tink.AEAD that nonce-prepends
// AES-GCM ciphertext, matching the format used by Tink's own
// kmsenvelopeaead helpers — so a payload produced via `tinkey encrypt`
// against the same blob round-trips.
//
// Registration: callers (typically pkg/reflow.Run) must call
// registry.RegisterKMSClient(blob.New()) once at process start. The
// Tink registry is process-global; sync.Once at the call site avoids
// duplicate entries under repeated reflow.Run invocations (tests).
//
// Security note: the entire boundary is the access control on the
// master-key blob. A reader who can fetch the blob can decrypt every
// secret encrypted with this KEK. Operators MUST scope the storage
// ACL (IAM, file perms) accordingly.
package blob

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/tink"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

// URIPrefix is the scheme prefix BlobKMS responds to. Every URI passed
// to GetAEAD must start with this prefix.
const URIPrefix = "blobkms+"

// MasterKeySize is the required length in bytes of the underlying
// master key (AES-256-GCM).
const MasterKeySize = 32

// New returns a Tink KMSClient that reads 32-byte AES-256-GCM master
// keys from any gocloud.dev/blob backend.
func New() registry.KMSClient {
	return kmsClient{}
}

type kmsClient struct{}

func (kmsClient) Supported(uri string) bool {
	return strings.HasPrefix(uri, URIPrefix)
}

// GetAEAD opens the master-key blob and returns a tink.AEAD that
// encrypts/decrypts with AES-256-GCM. The bucket is closed before
// GetAEAD returns; the AEAD itself holds only the derived cipher
// state.
func (c kmsClient) GetAEAD(uri string) (tink.AEAD, error) {
	if !c.Supported(uri) {
		return nil, fmt.Errorf("blobkms: unsupported URI %q (expected prefix %q)", uri, URIPrefix)
	}
	bucketURI, key, err := SplitURI(uri)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return nil, fmt.Errorf("blobkms: open bucket %q: %w", bucketURI, err)
	}
	defer bkt.Close()
	mk, err := bkt.ReadAll(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("blobkms: read master key %q: %w", key, err)
	}
	if len(mk) != MasterKeySize {
		return nil, fmt.Errorf("blobkms: master key %q is %d bytes; want %d", key, len(mk), MasterKeySize)
	}
	block, err := aes.NewCipher(mk)
	if err != nil {
		return nil, fmt.Errorf("blobkms: aes.NewCipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("blobkms: cipher.NewGCM: %w", err)
	}
	return &gcmAEAD{gcm: gcm}, nil
}

// SplitURI parses a blobkms+ URI into the bucket URL and object key.
// It splits at the last '/' on the part after the scheme://authority.
//
// Examples:
//
//	blobkms+s3://bucket/path/master.key      -> s3://bucket/path, master.key
//	blobkms+file:///etc/reflow/master.key    -> file:///etc/reflow, master.key
//	blobkms+mem://test/k                     -> mem://test, k
//
// Exported so callers (admin validation) can reuse the parser without
// instantiating a KMSClient.
func SplitURI(uri string) (bucketURI, key string, err error) {
	rest := strings.TrimPrefix(uri, URIPrefix)
	// Locate the scheme delimiter "://" so we don't split inside it.
	schemeEnd := strings.Index(rest, "://")
	if schemeEnd < 0 {
		return "", "", fmt.Errorf("blobkms: URI %q missing scheme://", uri)
	}
	pathStart := schemeEnd + len("://")
	slash := strings.LastIndex(rest[pathStart:], "/")
	if slash < 0 {
		return "", "", fmt.Errorf("blobkms: URI %q missing object key (no '/' after authority)", uri)
	}
	cut := pathStart + slash
	bucketURI = rest[:cut]
	key = rest[cut+1:]
	if key == "" {
		return "", "", fmt.Errorf("blobkms: URI %q has empty object key", uri)
	}
	return bucketURI, key, nil
}

// gcmAEAD wraps a single AES-GCM cipher.AEAD with the tink.AEAD shape.
// Ciphertext layout is nonce || gcm.Seal(...); 12-byte nonce prefix.
type gcmAEAD struct {
	gcm cipher.AEAD
}

func (g *gcmAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	nonce := make([]byte, g.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("blobkms: read nonce: %w", err)
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+g.gcm.Overhead())
	out = append(out, nonce...)
	out = g.gcm.Seal(out, nonce, plaintext, associatedData)
	return out, nil
}

func (g *gcmAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < g.gcm.NonceSize() {
		return nil, errors.New("blobkms: ciphertext shorter than nonce")
	}
	nonce, body := ciphertext[:g.gcm.NonceSize()], ciphertext[g.gcm.NonceSize():]
	return g.gcm.Open(nil, nonce, body, associatedData)
}
