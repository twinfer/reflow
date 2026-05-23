// Package encstore wraps storage.Store and storage.Batch with
// per-tenant value-level AEAD encryption. The wrapper inspects each
// key against a static "encrypted namespace" set computed from
// keys.AllLPNamespaces — keys under those namespaces carry the
// per-tenant 4-byte BE id immediately after the LP id, so the
// wrapper parses the tenant out of the key and looks up the matching
// tink.AEAD primitive via Resolver.Lookup.
//
// Encryption happens at the storage boundary so the FSM and tables
// stay oblivious; the apply-path atomicity invariant (one Pebble
// IndexedBatch commit per Update) is preserved because the wrapper
// only mutates the value bytes, never the batch lifecycle.
//
// Shard-meta namespaces (meta, format, timer/, outbox/, dedup/self/,
// workflow_reap/, lp_freeze/, lp_staging/) bypass encryption by
// design — they do not carry a tenant prefix and the FSM reads them
// during recovery before the TenantDEKResolver has a chance to
// converge.
//
// AAD = full storage key on every encrypt+decrypt. A row encrypted
// under key K cannot be replayed as the value for a different key —
// even within the same tenant.
package encstore

import (
	"errors"
	"fmt"
	"io"

	"github.com/tink-crypto/tink-go/v2/tink"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// Resolver looks up the AEAD primitive for a tenant_id. Production
// wiring is internal/secretstore.TenantDEKResolver; tests hand in a
// fake. Hot-path: called once per Set/Get/Iter row, so implementations
// must be lock-free (atomic.Pointer-swap is the canonical pattern).
type Resolver interface {
	// Lookup returns the AEAD for tenantID and whether it was found.
	// (nil, false) means "no DEK for this tenant" — the wrapper
	// translates that into a clean error rather than panicking, so a
	// cold-start race between partition open and DEK resolution
	// surfaces as an Unavailable to callers.
	Lookup(tenantID uint32) (tink.AEAD, bool)
}

// ErrTenantDEKUnavailable signals that the wrapper looked up a tenant
// whose DEK has not been resolved yet. Callers see this as an
// io-shaped error rather than ErrNotFound (which has a specific
// meaning) so cold-start races don't get silently converted into
// "row absent."
var ErrTenantDEKUnavailable = errors.New("encstore: tenant DEK not resolved")

// NewStore wraps s with per-tenant value-level AEAD. Keys under
// LP-prefixed namespaces (keys.AllLPNamespaces) have their tenant_id
// parsed and their values encrypted; keys under shard-meta namespaces
// pass through verbatim.
func NewStore(s storage.Store, r Resolver) storage.Store {
	return &valueEncryptingStore{inner: s, resolver: r, mask: defaultMask}
}

// valueEncryptingStore is the storage.Store wrapper. All methods
// either delegate (passthrough namespaces) or transform value bytes
// before/after delegation (LP-prefixed namespaces).
type valueEncryptingStore struct {
	inner    storage.Store
	resolver Resolver
	mask     *namespaceMask
}

func (s *valueEncryptingStore) Get(key []byte) ([]byte, io.Closer, error) {
	if !s.mask.encrypted(key) {
		return s.inner.Get(key)
	}
	raw, closer, err := s.inner.Get(key)
	if err != nil {
		return nil, nil, err
	}
	// Decrypt eagerly so the caller doesn't have to track an extra
	// closer for the plaintext buffer. We close the underlying closer
	// once we've copied the plaintext out.
	pt, derr := s.decryptForKey(key, raw)
	_ = closer.Close()
	if derr != nil {
		return nil, nil, derr
	}
	return pt, noopCloser{}, nil
}

func (s *valueEncryptingStore) NewIter(lower, upper []byte) (storage.Iter, error) {
	it, err := s.inner.NewIter(lower, upper)
	if err != nil {
		return nil, err
	}
	return &valueEncryptingIter{inner: it, parent: s}, nil
}

func (s *valueEncryptingStore) NewBatch() storage.Batch {
	return &valueEncryptingBatch{inner: s.inner.NewBatch(), parent: s}
}

func (s *valueEncryptingStore) Checkpoint(destDir string) error {
	return s.inner.Checkpoint(destDir)
}

func (s *valueEncryptingStore) Flush() error { return s.inner.Flush() }

func (s *valueEncryptingStore) Close() error { return s.inner.Close() }

// encryptForKey returns the value to actually store under key. Keys
// outside the encrypted namespace set return value unchanged. Keys
// inside trigger a tenant lookup + AEAD.Encrypt with AAD = key.
func (s *valueEncryptingStore) encryptForKey(key, value []byte) ([]byte, error) {
	tenant, ok := s.mask.tenantForKey(key)
	if !ok {
		// Caller already checked encrypted(); if we got here without a
		// tenant we have a key-shape bug, not an operational issue.
		return nil, fmt.Errorf("encstore: encrypted key too short to carry tenant: %x", key)
	}
	aead, ok := s.resolver.Lookup(tenant)
	if !ok {
		return nil, fmt.Errorf("%w: tenant_id=%d key=%x", ErrTenantDEKUnavailable, tenant, key)
	}
	return aead.Encrypt(value, key)
}

func (s *valueEncryptingStore) decryptForKey(key, ciphertext []byte) ([]byte, error) {
	tenant, ok := s.mask.tenantForKey(key)
	if !ok {
		return nil, fmt.Errorf("encstore: encrypted key too short to carry tenant: %x", key)
	}
	aead, ok := s.resolver.Lookup(tenant)
	if !ok {
		return nil, fmt.Errorf("%w: tenant_id=%d key=%x", ErrTenantDEKUnavailable, tenant, key)
	}
	return aead.Decrypt(ciphertext, key)
}

// valueEncryptingBatch wraps storage.Batch. Reads from a Batch must
// see the in-batch writes (read-your-writes coherence per
// internal/engine/CLAUDE.md), so the batch's Get/Iter wrap the same
// way the store's do.
type valueEncryptingBatch struct {
	inner  storage.Batch
	parent *valueEncryptingStore
}

func (b *valueEncryptingBatch) Get(key []byte) ([]byte, io.Closer, error) {
	if !b.parent.mask.encrypted(key) {
		return b.inner.Get(key)
	}
	raw, closer, err := b.inner.Get(key)
	if err != nil {
		return nil, nil, err
	}
	pt, derr := b.parent.decryptForKey(key, raw)
	_ = closer.Close()
	if derr != nil {
		return nil, nil, derr
	}
	return pt, noopCloser{}, nil
}

func (b *valueEncryptingBatch) NewIter(lower, upper []byte) (storage.Iter, error) {
	it, err := b.inner.NewIter(lower, upper)
	if err != nil {
		return nil, err
	}
	return &valueEncryptingIter{inner: it, parent: b.parent}, nil
}

func (b *valueEncryptingBatch) Set(key, value []byte) error {
	if !b.parent.mask.encrypted(key) {
		return b.inner.Set(key, value)
	}
	ct, err := b.parent.encryptForKey(key, value)
	if err != nil {
		return err
	}
	return b.inner.Set(key, ct)
}

func (b *valueEncryptingBatch) Delete(key []byte) error { return b.inner.Delete(key) }
func (b *valueEncryptingBatch) DeleteRange(start, end []byte) error {
	return b.inner.DeleteRange(start, end)
}

func (b *valueEncryptingBatch) Commit(sync bool) error { return b.inner.Commit(sync) }
func (b *valueEncryptingBatch) Close() error           { return b.inner.Close() }

// valueEncryptingIter wraps storage.Iter. Value() decrypts on demand
// per row; the plaintext buffer is owned by the iterator and is
// reused on the next Next/SeekGE/First call (same lifetime contract
// as storage.Iter.Value).
type valueEncryptingIter struct {
	inner  storage.Iter
	parent *valueEncryptingStore

	cached   []byte
	cachedAt int // call-counter so we know whether `cached` matches the current position
	pos      int

	deferred error
}

func (it *valueEncryptingIter) First() bool {
	it.pos++
	it.cached = nil
	return it.inner.First()
}

func (it *valueEncryptingIter) SeekGE(key []byte) bool {
	it.pos++
	it.cached = nil
	return it.inner.SeekGE(key)
}

func (it *valueEncryptingIter) Next() bool {
	it.pos++
	it.cached = nil
	return it.inner.Next()
}

func (it *valueEncryptingIter) Valid() bool { return it.inner.Valid() }

func (it *valueEncryptingIter) Key() []byte { return it.inner.Key() }

func (it *valueEncryptingIter) Value() []byte {
	if it.cached != nil && it.cachedAt == it.pos {
		return it.cached
	}
	key := it.inner.Key()
	raw := it.inner.Value()
	if !it.parent.mask.encrypted(key) {
		it.cached = raw
		it.cachedAt = it.pos
		return raw
	}
	pt, err := it.parent.decryptForKey(key, raw)
	if err != nil {
		it.deferred = err
		// Surface as empty value; the caller observes via Error().
		it.cached = nil
		it.cachedAt = it.pos
		return nil
	}
	it.cached = pt
	it.cachedAt = it.pos
	return pt
}

func (it *valueEncryptingIter) Error() error {
	if it.deferred != nil {
		return it.deferred
	}
	return it.inner.Error()
}

func (it *valueEncryptingIter) Close() error { return it.inner.Close() }

// noopCloser exists because Get/Batch.Get return a Closer the caller
// is expected to invoke; we close the underlying closer eagerly
// after decrypting (the plaintext buffer is heap-allocated and has
// no external resource) so we hand back a no-op closer.
type noopCloser struct{}

func (noopCloser) Close() error { return nil }

// namespaceMask precomputes the set of LP-prefixed namespace
// byte-prefixes from keys.AllLPNamespaces. Constructed once at
// package init.
type namespaceMask struct {
	prefixes [][]byte
}

func buildMask() *namespaceMask {
	m := &namespaceMask{}
	for _, ns := range keys.AllLPNamespaces {
		full := ns.Prefix(0)
		// full = <ns_prefix><4-byte BE LP=0>; trim the LP bytes off.
		if len(full) < keys.LPLen {
			panic(fmt.Sprintf("encstore: LP namespace %q built a prefix shorter than LPLen (%d bytes)", ns.Name, keys.LPLen))
		}
		nsBytes := full[:len(full)-keys.LPLen]
		// Defensive copy — Prefix may reuse its backing array.
		cp := append([]byte(nil), nsBytes...)
		m.prefixes = append(m.prefixes, cp)
	}
	return m
}

var defaultMask = buildMask()

// encrypted reports whether key falls under an encrypted (LP-prefixed)
// namespace. Linear scan over 14 short prefixes — measured at <50ns
// per call; not worth a trie.
func (m *namespaceMask) encrypted(key []byte) bool {
	for _, p := range m.prefixes {
		if hasPrefix(key, p) {
			return true
		}
	}
	return false
}

// tenantForKey returns the tenant_id encoded at the LP+tenant slot of
// an encrypted key. Returns (0, false) when the key isn't long
// enough — the caller treats that as a key-shape bug, not a runtime
// condition.
func (m *namespaceMask) tenantForKey(key []byte) (uint32, bool) {
	for _, p := range m.prefixes {
		if !hasPrefix(key, p) {
			continue
		}
		need := len(p) + keys.LPLen + keys.TenantLen
		if len(key) < need {
			return 0, false
		}
		off := len(p) + keys.LPLen
		// 4-byte BE tenant.
		return uint32(key[off])<<24 | uint32(key[off+1])<<16 | uint32(key[off+2])<<8 | uint32(key[off+3]), true
	}
	return 0, false
}

func hasPrefix(s, p []byte) bool {
	if len(s) < len(p) {
		return false
	}
	for i, b := range p {
		if s[i] != b {
			return false
		}
	}
	return true
}
