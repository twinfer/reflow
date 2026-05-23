package secretstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TenantDEKReader is the seam TenantDEKResolver.RunReconciler uses to
// fetch desired state. Production wiring is a thin adapter over
// engine.Host.TenantDEKs; tests hand in a fake.
type TenantDEKReader interface {
	ListTenantDEKs(ctx context.Context) ([]*enginev1.TenantDEKRecord, uint64 /*tableRev*/, error)
}

// TenantDEKResolver owns the live tenant_id→tink.AEAD snapshot.
// Reconcile swaps a fresh map atomically; Lookup is a single
// atomic.Pointer.Load() on the hot path with no per-call work or KMS
// trip.
//
// Per-record resolution shares secretstore.ResolveRemoteEncrypted with
// the SecretTable Resolver. The post-decrypt step is the only
// difference: SecretTable returns plaintext bytes; TenantDEKTable
// parses those bytes as a Tink keyset and materializes an AEAD
// primitive via aead.New(handle). Plaintext keyset bytes never leave
// this package's stack frame.
//
// The optional defaultAEAD is used for Lookup(0): tenant_id==0 is the
// default-tenant sentinel and never persists in TenantDEKTable. When
// defaultAEAD is nil, Lookup(0) returns (nil, false) — the
// encstore wrapper interprets that as "passthrough; tenant 0 data is
// plaintext at rest". Operators who want at-rest encryption for
// anonymous traffic wire a bootstrap AEAD at Host startup.
type TenantDEKResolver struct {
	metrics     *TenantDEKMetrics
	log         *slog.Logger
	live        atomic.Pointer[map[uint32]tink.AEAD]
	defaultAEAD tink.AEAD

	// reconcileMu serializes concurrent Reconcile calls so the
	// "previous snapshot" carry-over stays consistent under overlapping
	// callers (only relevant in tests; RunReconciler is
	// single-flighted).
	reconcileMu sync.Mutex
}

// NewTenantDEKResolver constructs an empty resolver. defaultAEAD may
// be nil — see the TenantDEKResolver doc for the implications. Use
// Reconcile to populate the snapshot or RunReconciler for the
// production wake-on-notifier loop.
func NewTenantDEKResolver(reg prometheus.Registerer, log *slog.Logger, defaultAEAD tink.AEAD) *TenantDEKResolver {
	if log == nil {
		log = slog.Default()
	}
	r := &TenantDEKResolver{
		metrics:     NewTenantDEKMetrics(reg),
		log:         log,
		defaultAEAD: defaultAEAD,
	}
	empty := map[uint32]tink.AEAD{}
	r.live.Store(&empty)
	return r
}

// Lookup returns the most-recently-resolved AEAD primitive for
// tenantID (and true). Returns the constructor-supplied defaultAEAD
// for tenantID==0 when one was provided; otherwise (nil, false).
// Hot-path safe: single atomic.Pointer.Load().
func (r *TenantDEKResolver) Lookup(tenantID uint32) (tink.AEAD, bool) {
	if r == nil {
		return nil, false
	}
	if tenantID == 0 {
		if r.defaultAEAD != nil {
			return r.defaultAEAD, true
		}
		return nil, false
	}
	snap := r.live.Load()
	if snap == nil {
		return nil, false
	}
	a, ok := (*snap)[tenantID]
	return a, ok
}

// RunReconciler is the production-mode reconcile loop. Wakes on the
// notifier (FSM post-commit Bump on TenantDEKTable) or a 5s ticker,
// SyncRead's the desired state, resolves each record, and atomically
// swaps the snapshot. Errors are logged + counted; the loop keeps
// running until ctx is cancelled.
//
// Goroutine affinity: own dedicated goroutine. Never runs on the FSM
// apply path — the notifier wake just signals; blob fetch + KMS
// decrypt + keyset parse happen off-loop.
func (r *TenantDEKResolver) RunReconciler(ctx context.Context, sub <-chan struct{}, reader TenantDEKReader) error {
	if r == nil {
		return nil
	}
	if reader == nil {
		return errors.New("secretstore: TenantDEKReader is required for reconcile loop")
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

func (r *TenantDEKResolver) reconcileFromReader(ctx context.Context, reader TenantDEKReader) {
	records, rev, err := reader.ListTenantDEKs(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Warn("tenant_dek: read desired state", "err", err)
			r.metrics.ReconcileErrors.WithLabelValues("*").Inc()
		}
		return
	}
	if err := r.Reconcile(ctx, records); err != nil {
		r.log.Warn("tenant_dek: reconcile failed", "err", err)
	}
	r.metrics.TableRevision.Set(float64(rev))
}

// Reconcile rebuilds the live snapshot from desired. Each record is
// resolved fresh; on resolve error the previously-resolved AEAD for
// that tenant_id is carried into the new snapshot so a transient blob
// / KMS / keyset-parse hiccup doesn't knock the encstore wrapper
// offline for in-flight traffic.
func (r *TenantDEKResolver) Reconcile(ctx context.Context, desired []*enginev1.TenantDEKRecord) error {
	if r == nil {
		return nil
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	prev := r.live.Load()
	next := make(map[uint32]tink.AEAD, len(desired))
	for _, rec := range desired {
		id := rec.GetTenantId()
		if id == 0 {
			// The FSM rejects tenant_id==0 rows, but defend in depth.
			r.log.Warn("tenant_dek: skipping row with zero tenant_id")
			continue
		}
		a, err := r.resolveDEK(ctx, rec)
		if err != nil {
			r.log.Warn("tenant_dek: resolve",
				"tenant_id", id, "name", rec.GetName(), "err", err)
			r.metrics.ResolveErrors.WithLabelValues(sourceRemoteEncrypted).Inc()
			if prev != nil {
				if prevAEAD, ok := (*prev)[id]; ok {
					next[id] = prevAEAD
				}
			}
			continue
		}
		next[id] = a
	}
	r.live.Store(&next)
	return nil
}

// resolveDEK does the per-record KEK→plaintext-keyset→AEAD-primitive
// pipeline. KEK→plaintext is the shared ResolveRemoteEncrypted helper;
// AAD = []byte(record.name) so renaming = re-encrypt (a leaked
// ciphertext under "dek-acme-v1" can't be replayed under
// "dek-acme-v2"). plaintext → tink.AEAD uses
// insecurecleartextkeyset.Read because the bytes are already plaintext
// at this point — the "insecure" name reflects that the keyset is no
// longer protected, which is intentional: we just decrypted it.
func (r *TenantDEKResolver) resolveDEK(ctx context.Context, rec *enginev1.TenantDEKRecord) (tink.AEAD, error) {
	// Bridge into the shared decrypt-stage metrics so operators see one
	// instrumentation surface for KEK trips (whether the consumer is
	// SecretTable or TenantDEKTable). The shared *Metrics struct is
	// secretstore's; ours is the TenantDEK-flavored sibling. The shared
	// metrics rely on schemeOf for kek_scheme label cardinality
	// (bounded by registered KMS clients).
	plaintext, err := ResolveRemoteEncrypted(ctx, rec.GetRemoteEncrypted(), []byte(rec.GetName()), r.metrics.shared())
	if err != nil {
		return nil, err
	}
	handle, err := insecurecleartextkeyset.Read(keyset.NewBinaryReader(bytes.NewReader(plaintext)))
	if err != nil {
		return nil, fmt.Errorf("tenant_dek: parse keyset: %w", err)
	}
	a, err := aead.New(handle)
	if err != nil {
		return nil, fmt.Errorf("tenant_dek: AEAD primitive: %w", err)
	}
	return a, nil
}
