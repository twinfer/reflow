package encstore_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/encstore"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// fakeResolver is a tenant_id → tink.AEAD lookup table.
type fakeResolver map[uint32]tink.AEAD

func (f fakeResolver) Lookup(tenantID uint32) (tink.AEAD, bool) {
	a, ok := f[tenantID]
	return a, ok
}

func newAEAD(t *testing.T) tink.AEAD {
	t.Helper()
	h, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		t.Fatalf("NewHandle: %v", err)
	}
	a, err := aead.New(h)
	if err != nil {
		t.Fatalf("aead.New: %v", err)
	}
	return a
}

// invocationKey hand-builds an `inv/<lp:4><tenant:4><suffix>` key
// without depending on enginev1.InvocationId. The encstore wrapper
// only needs to (a) detect the LP-prefixed namespace, (b) parse the
// tenant out of the right byte offset. Real callers go through
// keys.InvocationKey, but that adds proto dependencies the encstore
// tests don't otherwise need.
func invocationKey(lp uint32, tenant uint32, suffix []byte) []byte {
	out := make([]byte, 0, 4+keys.LPLen+keys.TenantLen+len(suffix))
	out = append(out, []byte("inv/")...)
	var lpBuf [4]byte
	binary.BigEndian.PutUint32(lpBuf[:], lp)
	out = append(out, lpBuf[:]...)
	var tBuf [4]byte
	binary.BigEndian.PutUint32(tBuf[:], tenant)
	out = append(out, tBuf[:]...)
	return append(out, suffix...)
}

func TestEncstore_LPKey_RoundTrip(t *testing.T) {
	t.Parallel()
	tenant := uint32(7)
	res := fakeResolver{tenant: newAEAD(t)}
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	key := invocationKey(1, tenant, []byte("uuid-aaaaaaaaaaaaaaaa"))
	b := s.NewBatch()
	if err := b.Set(key, []byte("plaintext-payload")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	got, closer, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer closer.Close()
	if string(got) != "plaintext-payload" {
		t.Fatalf("Get = %q; want plaintext-payload", got)
	}

	// Sanity-check the raw bytes on the underlying store are NOT
	// plaintext — confirms encryption actually happened.
	raw, rcloser, err := mem.Get(key)
	if err != nil {
		t.Fatalf("inner Get: %v", err)
	}
	defer rcloser.Close()
	if bytes.Equal(raw, []byte("plaintext-payload")) {
		t.Fatal("inner store holds plaintext; encryption did not happen")
	}
}

func TestEncstore_ShardMetaKey_Passthrough(t *testing.T) {
	t.Parallel()
	res := fakeResolver{} // resolver is empty — should never be consulted
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	key := keys.MetaKey()
	b := s.NewBatch()
	if err := b.Set(key, []byte("meta-bytes")); err != nil {
		t.Fatalf("Set meta: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	// Raw inner bytes must equal what we passed (no encryption).
	raw, closer, err := mem.Get(key)
	if err != nil {
		t.Fatalf("inner Get: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(raw, []byte("meta-bytes")) {
		t.Fatalf("inner raw = %q; want meta-bytes (passthrough)", raw)
	}
}

func TestEncstore_AADBoundToKey(t *testing.T) {
	t.Parallel()
	tenant := uint32(11)
	res := fakeResolver{tenant: newAEAD(t)}
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	keyA := invocationKey(1, tenant, []byte("uuid-aaaaaaaaaaaaaaaa"))
	keyB := invocationKey(1, tenant, []byte("uuid-bbbbbbbbbbbbbbbb"))

	b := s.NewBatch()
	if err := b.Set(keyA, []byte("payload-A")); err != nil {
		t.Fatalf("Set A: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	// Plant A's raw ciphertext into B's slot. Decrypt under B's key
	// (AAD = keyB) must fail — AAD-bound by storage key.
	raw, closer, err := mem.Get(keyA)
	if err != nil {
		t.Fatalf("inner Get A: %v", err)
	}
	rawCopy := append([]byte(nil), raw...)
	closer.Close()

	b2 := mem.NewBatch()
	if err := b2.Set(keyB, rawCopy); err != nil {
		t.Fatalf("planted B: %v", err)
	}
	if err := b2.Commit(true); err != nil {
		t.Fatalf("planted B commit: %v", err)
	}
	b2.Close()

	_, c2, err := s.Get(keyB)
	if err == nil {
		c2.Close()
		t.Fatal("Get B succeeded; AAD binding broken (ciphertext for key A decrypted under key B)")
	}
}

func TestEncstore_PerTenantIsolation(t *testing.T) {
	t.Parallel()
	res := fakeResolver{11: newAEAD(t), 22: newAEAD(t)}
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	keyA := invocationKey(1, 11, []byte("uuid-aaaaaaaaaaaaaaaa"))
	keyB := invocationKey(1, 22, []byte("uuid-bbbbbbbbbbbbbbbb"))

	b := s.NewBatch()
	if err := b.Set(keyA, []byte("tenantA-payload")); err != nil {
		t.Fatalf("Set A: %v", err)
	}
	if err := b.Set(keyB, []byte("tenantB-payload")); err != nil {
		t.Fatalf("Set B: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	// Confirm both decrypt independently.
	gotA, cA, err := s.Get(keyA)
	if err != nil {
		t.Fatalf("Get A: %v", err)
	}
	defer cA.Close()
	if string(gotA) != "tenantA-payload" {
		t.Fatalf("A = %q; want tenantA-payload", gotA)
	}
	gotB, cB, err := s.Get(keyB)
	if err != nil {
		t.Fatalf("Get B: %v", err)
	}
	defer cB.Close()
	if string(gotB) != "tenantB-payload" {
		t.Fatalf("B = %q; want tenantB-payload", gotB)
	}

	// Plant A's ciphertext at a key shape that looks like tenant B.
	// Since AAD includes the full key (including tenant bytes), B-side
	// decrypt must fail.
	raw, rc, err := mem.Get(keyA)
	if err != nil {
		t.Fatalf("inner Get A: %v", err)
	}
	rawCopy := append([]byte(nil), raw...)
	rc.Close()

	keyA2 := invocationKey(1, 22, []byte("uuid-aaaaaaaaaaaaaaaa")) // same uuid as A but tenant=22
	bp := mem.NewBatch()
	if err := bp.Set(keyA2, rawCopy); err != nil {
		t.Fatalf("plant: %v", err)
	}
	if err := bp.Commit(true); err != nil {
		t.Fatalf("plant commit: %v", err)
	}
	bp.Close()

	if _, c, err := s.Get(keyA2); err == nil {
		c.Close()
		t.Fatal("B decrypted A's planted ciphertext; per-tenant + AAD-binding isolation broken")
	}
}

func TestEncstore_ColdStart_NoDEK(t *testing.T) {
	t.Parallel()
	res := fakeResolver{} // tenant 7 never resolves
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	key := invocationKey(1, 7, []byte("uuid-cccccccccccccccc"))
	b := s.NewBatch()
	err := b.Set(key, []byte("payload"))
	if err == nil {
		t.Fatal("Set succeeded with no DEK; expected ErrTenantDEKUnavailable")
	}
	if !errors.Is(err, encstore.ErrTenantDEKUnavailable) {
		t.Fatalf("Set err = %v; want ErrTenantDEKUnavailable", err)
	}
	b.Close()
}

func TestEncstore_Iter_DecryptsEachRow(t *testing.T) {
	t.Parallel()
	res := fakeResolver{11: newAEAD(t), 22: newAEAD(t)}
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	want := map[string]string{}
	b := s.NewBatch()
	for _, tenant := range []uint32{11, 22} {
		for _, payload := range []string{"first", "second"} {
			suffix := append([]byte("u-"), byte(tenant), byte(payload[0]))
			k := invocationKey(1, tenant, suffix)
			if err := b.Set(k, []byte(payload)); err != nil {
				t.Fatalf("Set: %v", err)
			}
			want[string(k)] = payload
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	lower := []byte("inv/")
	upper := append([]byte(nil), lower...)
	upper[len(upper)-1]++
	it, err := s.NewIter(lower, upper)
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer it.Close()
	got := map[string]string{}
	for ok := it.First(); ok; ok = it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("iter rows = %d; want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("row %x = %q; want %q", k, got[k], v)
		}
	}
}

func TestEncstore_BatchReadYourWrites(t *testing.T) {
	t.Parallel()
	tenant := uint32(5)
	res := fakeResolver{tenant: newAEAD(t)}
	mem := storage.NewMemStore()
	defer mem.Close()
	s := encstore.NewStore(mem, res)

	key := invocationKey(1, tenant, []byte("uuid-rrrrrrrrrrrrrrrr"))
	b := s.NewBatch()
	defer b.Close()

	if err := b.Set(key, []byte("ryw-payload")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Read against the in-flight batch — must see the just-written
	// plaintext (read-your-writes via wrapper Decrypt of the
	// just-encrypted bytes).
	got, closer, err := b.Get(key)
	if err != nil {
		t.Fatalf("batch.Get: %v", err)
	}
	defer closer.Close()
	if string(got) != "ryw-payload" {
		t.Fatalf("batch.Get = %q; want ryw-payload", got)
	}
}
