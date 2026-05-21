package webhook

// resolveSecret turns a SecretRef into the live secret bytes. Three
// sources are supported:
//   - env_var_name:     os.Getenv on the local node
//   - file_path:        os.ReadFile, trailing-newline trimmed
//   - remote_encrypted: ciphertext fetched via gocloud.dev/blob,
//     decrypted via Tink registry.GetKMSClient(kek_uri).
//     AAD = []byte(name) so a leaked ciphertext for row A cannot
//     be replayed as the secret for row B.
//
// On error, callers should log + bump
// reflow_webhook_secret_resolve_errors_total{name,source} and KEEP
// the previously-resolved bytes (passed in via prev) — a transient
// NFS hiccup, KMS unavailability, or blob fetch failure should not
// knock the live source offline. Trade-off: an unresolvable secret
// stays "stuck" at the last-known value until either the row is
// deleted or the source becomes resolvable again with new value.
// Matches consul-template / vault-agent semantics.
//
// env_var_name is effectively static within a process (reflowd
// never re-execs); rotation through env requires a restart.
// file_path is re-read on every reconcile (5s ticker + notifier
// wake), so hot-rotation works by atomic file replacement.
// remote_encrypted is also re-fetched + re-decrypted every reconcile;
// hot-rotation works by overwriting the blob fleet-wide.
//
// remote_encrypted errors are also stamped on
// reflow_webhook_kms_decrypt_errors_total with stage-precise labels
// (parse, blob_open, blob_fetch, kms_lookup, kms_get_aead, decrypt)
// so operators can distinguish "KMS slow" from "wrong AAD" from
// "ciphertext tampered" without log-scraping.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tink-crypto/tink-go/v2/core/registry"
	"gocloud.dev/blob"

	tinkkmsblob "github.com/twinfer/reflow/pkg/tinkkms/blob"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Source labels emitted on
// reflow_webhook_secret_resolve_errors_total so dashboards can
// distinguish where a resolution failed without scraping the
// secret name.
const (
	SecretSourceEnv             = "env"
	SecretSourceFile            = "file"
	SecretSourceRemoteEncrypted = "remote_encrypted"
)

// Stage labels emitted on reflow_webhook_kms_decrypt_errors_total.
// Bounded set — six values — safe to use as a Prometheus label.
const (
	kmsStageParse      = "parse"
	kmsStageBlobOpen   = "blob_open"
	kmsStageBlobFetch  = "blob_fetch"
	kmsStageKMSLookup  = "kms_lookup"
	kmsStageKMSGetAEAD = "kms_get_aead"
	kmsStageDecrypt    = "decrypt"
)

// resolveSecret returns (bytes, sourceLabel, err). On any error
// sourceLabel is still set so the caller's metric increment carries
// useful detail. Returns an error for unset env vars, empty file
// bodies, or any failure along the remote_encrypted pipeline.
//
// For remote_encrypted, this function also stamps
// reflow_webhook_kms_decrypt_errors_total and observes
// reflow_webhook_kms_decrypt_seconds — the caller is expected to
// stamp reflow_webhook_secret_resolve_errors_total separately on
// the return error so both the high-level counter and the
// fine-grained one fire.
func resolveSecret(ctx context.Context, ref *enginev1.SecretRef, name string, m *Metrics) ([]byte, string, error) {
	if ref == nil {
		return nil, "", fmt.Errorf("secret_ref is nil")
	}
	switch src := ref.GetSource().(type) {
	case *enginev1.SecretRef_EnvVarName:
		envName := src.EnvVarName
		if envName == "" {
			return nil, SecretSourceEnv, fmt.Errorf("secret_ref.env_var_name is empty")
		}
		v := os.Getenv(envName)
		if v == "" {
			return nil, SecretSourceEnv, fmt.Errorf("env var %q is unset or empty", envName)
		}
		return []byte(v), SecretSourceEnv, nil
	case *enginev1.SecretRef_FilePath:
		path := src.FilePath
		if path == "" {
			return nil, SecretSourceFile, fmt.Errorf("secret_ref.file_path is empty")
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, SecretSourceFile, fmt.Errorf("read %s: %w", path, err)
		}
		b = bytes.TrimRight(b, "\n\r")
		if len(b) == 0 {
			return nil, SecretSourceFile, fmt.Errorf("secret file %s is empty", path)
		}
		return b, SecretSourceFile, nil
	case *enginev1.SecretRef_RemoteEncrypted:
		return resolveRemoteEncrypted(ctx, src.RemoteEncrypted, name, m)
	case nil:
		return nil, "", fmt.Errorf("secret_ref has no source set")
	default:
		return nil, "", fmt.Errorf("secret_ref: unsupported source type %T", src)
	}
}

// resolveRemoteEncrypted does the blob fetch + Tink decrypt with
// AAD = []byte(name). All error paths stamp KMSDecryptErrors with a
// stage label; the happy path stamps KMSDecryptTotal. Latency is
// observed on every call regardless of outcome.
func resolveRemoteEncrypted(ctx context.Context, re *enginev1.RemoteEncryptedSecret, name string, m *Metrics) ([]byte, string, error) {
	kekScheme := schemeOf(re.GetKekUri())
	start := time.Now()
	defer func() {
		if m != nil {
			m.KMSDecryptSeconds.WithLabelValues(kekScheme).Observe(time.Since(start).Seconds())
		}
	}()
	bumpErr := func(stage string, err error) error {
		if m != nil {
			m.KMSDecryptErrors.WithLabelValues(name, kekScheme, stage).Inc()
		}
		return err
	}

	if re.GetBlobUri() == "" {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageParse, fmt.Errorf("remote_encrypted.blob_uri is empty"))
	}
	if re.GetKekUri() == "" {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageParse, fmt.Errorf("remote_encrypted.kek_uri is empty"))
	}
	bucketURI, key, err := splitBlobURI(re.GetBlobUri())
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageParse, err)
	}
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageBlobOpen, fmt.Errorf("open bucket %q: %w", bucketURI, err))
	}
	defer bkt.Close()
	ct, err := bkt.ReadAll(ctx, key)
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageBlobFetch, fmt.Errorf("read %q from %q: %w", key, bucketURI, err))
	}

	kc, err := registry.GetKMSClient(re.GetKekUri())
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageKMSLookup, fmt.Errorf("GetKMSClient(%q): %w", re.GetKekUri(), err))
	}
	aead, err := kc.GetAEAD(re.GetKekUri())
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageKMSGetAEAD, fmt.Errorf("GetAEAD(%q): %w", re.GetKekUri(), err))
	}
	pt, err := aead.Decrypt(ct, []byte(name))
	if err != nil {
		return nil, SecretSourceRemoteEncrypted, bumpErr(kmsStageDecrypt, fmt.Errorf("AEAD.Decrypt: %w", err))
	}
	if m != nil {
		m.KMSDecryptTotal.WithLabelValues(kekScheme).Inc()
	}
	return pt, SecretSourceRemoteEncrypted, nil
}

// splitBlobURI delegates to the BlobKMS parser so blob_uri and kek_uri
// parse with identical semantics. Exported through the package because
// the gocloud.dev/blob URI shape is the same for both fields.
func splitBlobURI(uri string) (bucketURI, key string, err error) {
	// blob_uri carries no "blobkms+" prefix — re-add then strip, so we
	// reuse a single parser implementation.
	bucketURI, key, err = tinkkmsblob.SplitURI(tinkkmsblob.URIPrefix + uri)
	return
}

// schemeOf returns the URI prefix used as the kek_scheme metric label.
// Bounded by the set of registered KMS clients (≤ a handful in
// practice). Falls back to "unknown" for malformed input so the metric
// stays cardinality-safe under operator typos.
func schemeOf(uri string) string {
	i := strings.Index(uri, ":")
	if i <= 0 {
		return "unknown"
	}
	scheme := uri[:i]
	// Tink convention: scheme often carries the suffix "://"; tolerate
	// "blobkms+s3" by trimming after the first '+' too so the label
	// stays the operator-visible KMS identity.
	if plus := strings.Index(scheme, "+"); plus > 0 {
		scheme = scheme[:plus]
	}
	return scheme
}
