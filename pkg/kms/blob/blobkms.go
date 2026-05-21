// Package blob implements a Tink KMSClient backed by gocloud.dev/blob.
//
// URI form: blobkms+<gocloud-blob-uri>. The bucket portion is what
// gocloud.dev/blob.OpenBucket accepts; the key portion is the object name
// inside that bucket. Examples:
//
//	blobkms+s3://my-bucket/kek.bin
//	blobkms+file:///etc/reflow/kek.bin
//	blobkms+gs://my-bucket/keys/kek.bin
//	blobkms+mem://test/kek.bin   (tests only)
//
// On-disk shape — boot key concatenated with an encrypted Tink keyset:
//
//	bytes[0:32]  = boot key (AES-256-GCM key encryption key)
//	bytes[32:]   = serialized tinkpb.EncryptedKeyset (the data keyset,
//	               encrypted by the boot key via Tink's standard
//	               keyset.Handle.Write path)
//
// GetAEAD reads both halves, builds a boot AEAD over the 32-byte prefix,
// decrypts the keyset, and returns the keyset's primary AEAD. Rotation
// works by editing the keyset (add new key, mark primary, re-write) while
// the boot key stays put — old ciphertexts still decrypt via the keyset's
// non-primary keys.
//
// Registration: callers (typically pkg/reflow.Run via a blank import)
// trigger the package init() to call registry.RegisterKMSClient once.
// The Tink registry is process-global; sync.Once at the init site avoids
// duplicate entries under repeated reflow.Run invocations (tests).
//
// Security note: the entire boundary is the access control on the KEK
// blob. A reader who can fetch the blob recovers the boot key, decrypts
// the keyset, and decrypts every secret. Operators MUST scope the
// storage ACL (IAM, file perms) accordingly.
package blob

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/keyset"
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

// BootKeySize is the required length in bytes of the boot key prefix
// stored at the head of the KEK blob.
const BootKeySize = 32

func init() {
	registry.RegisterKMSClient(New())
}

// New returns a Tink KMSClient that resolves blobkms+ URIs against any
// gocloud.dev/blob backend. Callers normally don't construct this
// directly — the package's init() registers the client; depend on
// registry.GetKMSClient instead.
func New() registry.KMSClient {
	return kmsClient{}
}

type kmsClient struct{}

func (kmsClient) Supported(uri string) bool {
	return strings.HasPrefix(uri, URIPrefix)
}

// GetAEAD reads the KEK blob, splits boot-key from encrypted-keyset,
// decrypts the keyset, and returns its primary AEAD. The bucket is
// closed before GetAEAD returns; the returned AEAD owns only the
// derived primitive state.
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
	raw, err := bkt.ReadAll(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("blobkms: read KEK blob %q: %w", key, err)
	}
	return aeadFromBytes(raw)
}

func aeadFromBytes(raw []byte) (tink.AEAD, error) {
	if len(raw) <= BootKeySize {
		return nil, fmt.Errorf("blobkms: KEK blob is %d bytes; want > %d (boot key + encrypted keyset)", len(raw), BootKeySize)
	}
	bootKey, ksBytes := raw[:BootKeySize], raw[BootKeySize:]
	bootAEAD, err := newBootAEAD(bootKey)
	if err != nil {
		return nil, err
	}
	handle, err := keyset.Read(keyset.NewBinaryReader(bytes.NewReader(ksBytes)), bootAEAD)
	if err != nil {
		return nil, fmt.Errorf("blobkms: decrypt keyset: %w", err)
	}
	a, err := aead.New(handle)
	if err != nil {
		return nil, fmt.Errorf("blobkms: keyset AEAD primitive: %w", err)
	}
	return a, nil
}

// InitKEK builds a fresh KEK blob: a random boot key concatenated with
// a freshly-generated AES-256-GCM keyset encrypted by that boot key.
// Operators call this once per cluster via `reflowd cluster init-kek`.
func InitKEK() ([]byte, error) {
	bootKey := make([]byte, BootKeySize)
	if _, err := io.ReadFull(rand.Reader, bootKey); err != nil {
		return nil, fmt.Errorf("blobkms: read boot key: %w", err)
	}
	bootAEAD, err := newBootAEAD(bootKey)
	if err != nil {
		return nil, err
	}
	handle, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		return nil, fmt.Errorf("blobkms: new keyset handle: %w", err)
	}
	var ksBuf bytes.Buffer
	if err := handle.Write(keyset.NewBinaryWriter(&ksBuf), bootAEAD); err != nil {
		return nil, fmt.Errorf("blobkms: encrypt keyset: %w", err)
	}
	out := make([]byte, 0, BootKeySize+ksBuf.Len())
	out = append(out, bootKey...)
	out = append(out, ksBuf.Bytes()...)
	return out, nil
}

// newBootAEAD wraps a 32-byte AES-256 key as a tink.AEAD suitable for
// keyset.Read / keyset.Handle.Write. Boot AEAD ciphertext layout is
// nonce || cipher.AEAD.Seal(...); the same shape Tink's kmsenvelopeaead
// uses, so the boot-key blob is interoperable with `tinkey` if an
// operator ever needs to reach for it.
func newBootAEAD(bootKey []byte) (tink.AEAD, error) {
	if len(bootKey) != BootKeySize {
		return nil, fmt.Errorf("blobkms: boot key is %d bytes; want %d", len(bootKey), BootKeySize)
	}
	block, err := aes.NewCipher(bootKey)
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
//	blobkms+s3://bucket/path/kek.bin   -> s3://bucket/path, kek.bin
//	blobkms+file:///etc/reflow/kek.bin -> file:///etc/reflow, kek.bin
//	blobkms+mem://test/k               -> mem://test, k
//
// Exported so admin validation and CLI code can reuse the parser
// without instantiating a KMSClient.
func SplitURI(uri string) (bucketURI, key string, err error) {
	return ParseGocloudURI(strings.TrimPrefix(uri, URIPrefix))
}

// ParseGocloudURI splits a bare gocloud.dev/blob URI (no blobkms+
// prefix) into bucket URL and object key using the same last-slash
// rule as SplitURI. Callers that hold blob_uri values from secret
// records or CLI flags use this directly instead of round-tripping
// through SplitURI.
//
// Examples:
//
//	s3://bucket/path/kek.bin   -> s3://bucket/path, kek.bin
//	file:///etc/reflow/kek.bin -> file:///etc/reflow, kek.bin
//	mem://test/k               -> mem://test, k
func ParseGocloudURI(uri string) (bucketURI, key string, err error) {
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd < 0 {
		return "", "", fmt.Errorf("blob URI %q missing scheme://", uri)
	}
	pathStart := schemeEnd + len("://")
	slash := strings.LastIndex(uri[pathStart:], "/")
	if slash < 0 {
		return "", "", fmt.Errorf("blob URI %q missing object key (no '/' after authority)", uri)
	}
	cut := pathStart + slash
	bucketURI = uri[:cut]
	key = uri[cut+1:]
	if key == "" {
		return "", "", fmt.Errorf("blob URI %q has empty object key", uri)
	}
	return bucketURI, key, nil
}

// gcmAEAD wraps cipher.AEAD with the tink.AEAD interface. Ciphertext
// layout is nonce || gcm.Seal(...). Used as the boot AEAD wrapping the
// 32-byte key; not the primitive callers see — that's the keyset's
// primary AEAD.
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
