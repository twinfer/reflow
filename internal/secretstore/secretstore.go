// Package secretstore is the per-node Resolver for shard 0's
// SecretTable. Consumers (the webhook ingress Manager today;
// event-source vendor creds, outbound HMAC signing keys, OIDC client
// secrets tomorrow) hold a *Resolver and call Lookup(name) on the hot
// path to read the most-recently-resolved plaintext for a named secret.
//
// The Resolver owns an atomic.Pointer[map[string][]byte] that the
// reconciler swaps each pass. Hot-path readers see whichever map was
// live when their Load() ran — no per-call locking, no per-call KMS
// trip.
//
// Resolve semantics: each pass iterates SecretRecord rows from shard 0;
// for each row, fetches ciphertext via gocloud.dev/blob, dispatches the
// KEK URI through Tink's process-global KMSClient registry, and decrypts
// with AAD = []byte(secret.name). On error, the previously-resolved
// bytes for that name are preserved so a transient blob-fetch or KMS
// hiccup doesn't knock the consumer offline.
//
// AAD binds the ciphertext to the secret row's identity, not to the
// consumer that uses it. Multiple webhooks (or future consumers) can
// share a single named secret and a single ciphertext+KEK; renaming
// the secret is a re-encrypt operation, by design.
package secretstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"gocloud.dev/blob"

	tinkkmsblob "github.com/twinfer/reflow/pkg/kms/blob"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// reconcileInterval is the backstop tick for the reconciler loop.
const reconcileInterval = 5 * time.Second

// Source label for resolve-error counters. Bounded to one value today;
// retained as a constant so future SecretRecord source variants (e.g.
// inline-hybrid, vault-kv-path) can extend the set.
const sourceRemoteEncrypted = "remote_encrypted"

// Stage labels emitted on Metrics.DecryptErrors. Bounded set — six
// values — safe to use as a Prometheus label.
const (
	stageParse      = "parse"
	stageBlobOpen   = "blob_open"
	stageBlobFetch  = "blob_fetch"
	stageKMSLookup  = "kms_lookup"
	stageKMSGetAEAD = "kms_get_aead"
	stageDecrypt    = "decrypt"
)

// Reader is the seam RunReconciler uses to fetch desired state.
// Production wiring is a thin adapter over engine.Host.Secrets; tests
// hand in a fake.
type Reader interface {
	ListSecrets(ctx context.Context) ([]*enginev1.SecretRecord, uint64 /*tableRev*/, error)
}

// Resolver owns the live name→bytes snapshot. Reconcile swaps a fresh
// map atomically; Lookup is a single atomic.Pointer.Load() on the hot
// path with no per-call work.
type Resolver struct {
	metrics *Metrics
	log     *slog.Logger
	live    atomic.Pointer[map[string][]byte]

	// reconcileMu serializes concurrent Reconcile calls so the
	// "previous snapshot" carry-over stays consistent under overlapping
	// callers (which only happen in tests; RunReconciler is
	// single-flighted).
	reconcileMu sync.Mutex
}

// New constructs an empty Resolver. Use Reconcile to populate the
// snapshot; use RunReconciler for the production wake-on-notifier loop.
func New(reg prometheus.Registerer, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	r := &Resolver{
		metrics: NewMetrics(reg),
		log:     log,
	}
	empty := map[string][]byte{}
	r.live.Store(&empty)
	return r
}

// Lookup returns the most-recently-resolved bytes for name (and true).
// Returns (nil, false) when the name doesn't exist or has never been
// successfully resolved. Hot-path safe: single atomic.Pointer.Load().
func (r *Resolver) Lookup(name string) ([]byte, bool) {
	if r == nil {
		return nil, false
	}
	snap := r.live.Load()
	if snap == nil {
		return nil, false
	}
	b, ok := (*snap)[name]
	return b, ok
}

// LookupForCASigning is Lookup specialised for the cluster CA signing
// key resolved via the same Reconcile pipeline. Each call increments
// reflow_pki_ca_sign_total{name} on hit, or
// reflow_pki_ca_sign_errors_total{name, reason} on miss; the caller
// (certmgr.ClusterIssuer) is expected to emit one audit log entry per
// signing operation alongside this metric. Per-name labelling is safe
// here because the row set is bounded (typically one "active" row;
// ≤handful with rotation history).
func (r *Resolver) LookupForCASigning(name string) ([]byte, error) {
	if r == nil {
		return nil, errors.New("secretstore: nil resolver")
	}
	b, ok := r.Lookup(name)
	if !ok {
		if r.metrics != nil {
			r.metrics.CASignErrors.WithLabelValues(name, "missing").Inc()
		}
		return nil, fmt.Errorf("secretstore: ca signing key %q not resolved", name)
	}
	if r.metrics != nil {
		r.metrics.CASignTotal.WithLabelValues(name).Inc()
	}
	return b, nil
}

// RunReconciler is the production-mode reconcile loop. Wakes on the
// notifier (FSM post-commit Bump) or a 5s ticker, SyncRead's the
// desired state, resolves each record, and atomically swaps the
// snapshot. Errors are logged + counted; the loop keeps running until
// ctx is cancelled.
//
// Goroutine affinity: own dedicated goroutine. Never runs on the FSM
// apply path — the notifier wake just signals; blob fetch + KMS
// decrypt happen off-loop.
func (r *Resolver) RunReconciler(ctx context.Context, sub <-chan struct{}, reader Reader) error {
	if r == nil {
		return nil
	}
	if reader == nil {
		return errors.New("secretstore: reader is required for reconcile loop")
	}
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	r.reconcileFromReader(ctx, reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			r.reconcileFromReader(ctx, reader)
		case <-ticker.C:
			r.reconcileFromReader(ctx, reader)
		}
	}
}

// reconcileFromReader does one ListSecrets + per-record resolve +
// snapshot swap pass. Errors are logged + counted, never propagated.
func (r *Resolver) reconcileFromReader(ctx context.Context, reader Reader) {
	records, rev, err := reader.ListSecrets(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Warn("secretstore: read desired state", "err", err)
			r.metrics.ReconcileErrors.WithLabelValues("*").Inc()
		}
		return
	}
	if err := r.Reconcile(ctx, records); err != nil {
		r.log.Warn("secretstore: reconcile failed", "err", err)
	}
	r.metrics.TableRevision.Set(float64(rev))
}

// Reconcile rebuilds the live snapshot from `desired`. Each record is
// resolved fresh; on resolve error the previously-resolved bytes for
// that name are carried into the new snapshot so a transient blob /
// KMS hiccup doesn't knock consumers offline.
func (r *Resolver) Reconcile(ctx context.Context, desired []*enginev1.SecretRecord) error {
	if r == nil {
		return nil
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	prev := r.live.Load()
	next := make(map[string][]byte, len(desired))
	for _, rec := range desired {
		name := rec.GetName()
		if name == "" {
			continue
		}
		bytes, err := ResolveRemoteEncrypted(ctx, rec.GetRemoteEncrypted(), []byte(name), r.metrics)
		if err != nil {
			r.log.Warn("secretstore: resolve secret",
				"name", name, "err", err)
			r.metrics.ResolveErrors.WithLabelValues(sourceRemoteEncrypted).Inc()
			if prev != nil {
				if prevBytes, ok := (*prev)[name]; ok {
					next[name] = prevBytes
				}
			}
			continue
		}
		next[name] = bytes
	}
	r.live.Store(&next)
	return nil
}

// ResolveRemoteEncrypted is the per-record fetch + decrypt path used
// by both Resolver (for shard 0 SecretTable rows) and TenantDEKResolver
// (for shard 0 TenantDEKTable rows). Fetches ciphertext from
// gocloud.dev/blob, dispatches the KEK URI through Tink's KMSClient
// registry, and AEAD-decrypts with the caller-supplied aad. Returns
// plaintext bytes on success; emits stage-labeled metrics on any
// failure (the metrics argument may be nil if the caller doesn't
// participate in the shared instrumentation).
//
// aad binds the ciphertext to the row identity. SecretRecord passes
// []byte(record.name); TenantDEKRecord likewise passes
// []byte(record.name) — both rely on rename-is-re-encrypt to prevent
// cross-row ciphertext replay.
func ResolveRemoteEncrypted(ctx context.Context, re *enginev1.RemoteEncryptedSecret, aad []byte, m *Metrics) ([]byte, error) {
	if re == nil {
		return nil, errors.New("source.remote_encrypted is nil")
	}
	kekScheme := schemeOf(re.GetKekUri())
	start := time.Now()
	defer func() {
		if m != nil {
			m.DecryptSeconds.WithLabelValues(kekScheme).Observe(time.Since(start).Seconds())
		}
	}()
	bumpErr := func(stage string, err error) error {
		if m != nil {
			m.DecryptErrors.WithLabelValues(kekScheme, stage).Inc()
		}
		return err
	}

	if re.GetBlobUri() == "" {
		return nil, bumpErr(stageParse, errors.New("remote_encrypted.blob_uri is empty"))
	}
	if re.GetKekUri() == "" {
		return nil, bumpErr(stageParse, errors.New("remote_encrypted.kek_uri is empty"))
	}
	bucketURI, key, err := tinkkmsblob.ParseGocloudURI(re.GetBlobUri())
	if err != nil {
		return nil, bumpErr(stageParse, err)
	}
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return nil, bumpErr(stageBlobOpen, fmt.Errorf("open bucket %q: %w", bucketURI, err))
	}
	defer bkt.Close()
	ct, err := bkt.ReadAll(ctx, key)
	if err != nil {
		return nil, bumpErr(stageBlobFetch, fmt.Errorf("read %q from %q: %w", key, bucketURI, err))
	}

	kc, err := registry.GetKMSClient(re.GetKekUri())
	if err != nil {
		return nil, bumpErr(stageKMSLookup, fmt.Errorf("GetKMSClient(%q): %w", re.GetKekUri(), err))
	}
	aead, err := kc.GetAEAD(re.GetKekUri())
	if err != nil {
		return nil, bumpErr(stageKMSGetAEAD, fmt.Errorf("GetAEAD(%q): %w", re.GetKekUri(), err))
	}
	pt, err := aead.Decrypt(ct, aad)
	if err != nil {
		return nil, bumpErr(stageDecrypt, fmt.Errorf("AEAD.Decrypt: %w", err))
	}
	if m != nil {
		m.DecryptTotal.WithLabelValues(kekScheme).Inc()
	}
	return pt, nil
}

// schemeOf extracts the operator-visible KMS scheme from a kek_uri
// for use as a Prometheus label. Bounded by the set of registered KMS
// clients. "unknown" on malformed input keeps cardinality safe.
func schemeOf(uri string) string {
	i := strings.Index(uri, ":")
	if i <= 0 {
		return "unknown"
	}
	scheme := uri[:i]
	if plus := strings.Index(scheme, "+"); plus > 0 {
		scheme = scheme[:plus]
	}
	return scheme
}
