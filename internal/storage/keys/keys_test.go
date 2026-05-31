package keys

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math/rand/v2"
	"sort"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func testID(t *testing.T, pk uint64, uuid string) *enginev1.InvocationId {
	t.Helper()
	if len(uuid) != 16 {
		t.Fatalf("test uuid must be 16 bytes, got %d", len(uuid))
	}
	return &enginev1.InvocationId{
		PartitionKey: pk,
		Uuid:         []byte(uuid),
	}
}

func TestEncodeDecodeInvocationID(t *testing.T) {
	id := testID(t, 0xDEADBEEFCAFEBABE, "0123456789abcdef")
	raw, err := EncodeInvocationID(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 28 {
		t.Fatalf("raw length = %d; want 28", len(raw))
	}
	if binary.BigEndian.Uint32(raw[:4]) != 0 {
		t.Errorf("default tenant not at raw[:4]")
	}
	if binary.BigEndian.Uint64(raw[4:12]) != 0xDEADBEEFCAFEBABE {
		t.Errorf("partition_key not big-endian at raw[4:12]")
	}
	decoded, err := DecodeInvocationID(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GetPartitionKey() != id.GetPartitionKey() {
		t.Errorf("partition_key roundtrip failed")
	}
	if !bytes.Equal(decoded.GetUuid(), id.GetUuid()) {
		t.Errorf("uuid roundtrip failed")
	}
}

func TestEncodeInvocationID_InvalidUUID(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("short")}
	if _, err := EncodeInvocationID(id); err == nil {
		t.Fatal("expected error for short uuid")
	}
}

func TestLPFromPartitionKey_Bounds(t *testing.T) {
	cases := []struct {
		pk     uint64
		wantLP uint32
	}{
		{0, 0},
		{uint64(LPCount - 1), LPCount - 1},
		{uint64(LPCount), 0},                         // wraps
		{uint64(LPCount) + 1, 1},                     // wraps + 1
		{^uint64(0), LPCount - 1},                    // max uint64
		{0x123456789ABCDEF0, uint32(0xDEF0) & 0xFFF}, // arbitrary
	}
	for _, c := range cases {
		got := LPFromPartitionKey(c.pk)
		if got != c.wantLP {
			t.Errorf("LPFromPartitionKey(0x%X) = %d; want %d", c.pk, got, c.wantLP)
		}
		if got >= LPCount {
			t.Errorf("LPFromPartitionKey(0x%X) = %d; out of range [0, %d)", c.pk, got, LPCount)
		}
	}
}

func TestInvocationKey_LPPrefix(t *testing.T) {
	id := testID(t, 0x123, "0123456789abcdef")
	k, err := InvocationKey(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(k, []byte("inv/")) {
		t.Errorf("bad prefix: %q", k)
	}
	if len(k) != len("inv/")+LPLen+invocationIDLen {
		t.Errorf("len = %d; want %d", len(k), len("inv/")+LPLen+invocationIDLen)
	}
	// LP encoded directly after the namespace prefix.
	wantLP := LPFromPartitionKey(0x123)
	gotLP := binary.BigEndian.Uint32(k[len("inv/") : len("inv/")+LPLen])
	if gotLP != wantLP {
		t.Errorf("encoded lp = %d; want %d", gotLP, wantLP)
	}
	// Tenant id immediately after the LP.
	gotTenant := binary.BigEndian.Uint32(k[len("inv/")+LPLen : len("inv/")+LPLen+TenantLen])
	if gotTenant != TenantDefault {
		t.Errorf("encoded tenant = %d; want %d", gotTenant, TenantDefault)
	}
	// The 28-byte invocation id (whose leading 4 bytes are the tenant)
	// follows the LP.
	decoded, err := DecodeInvocationID(k[len("inv/")+LPLen:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GetPartitionKey() != 0x123 {
		t.Errorf("decoded pk = 0x%X; want 0x123", decoded.GetPartitionKey())
	}
}

func TestInvocationKey_TenantIsolation(t *testing.T) {
	// Same id, distinct tenant — must produce distinct keys (no aliasing
	// across tenants on the same LP). Load-bearing invariant for the
	// value-encryption layer in PR 4 and for the per-tenant data
	// isolation the multi-tenancy refactor is built on.
	idA := &enginev1.InvocationId{TenantId: 1, PartitionKey: 0x42, Uuid: []byte("0123456789abcdef")}
	idB := &enginev1.InvocationId{TenantId: 2, PartitionKey: 0x42, Uuid: []byte("0123456789abcdef")}
	kA, _ := InvocationKey(idA)
	kB, _ := InvocationKey(idB)
	if bytes.Equal(kA, kB) {
		t.Fatalf("tenant 1 and tenant 2 produced the same key for the same id: %x", kA)
	}
	// Both keys live under the LP-wide prefix (LP-transfer captures all
	// tenants on the LP with a single scan).
	pfx := InvocationLPPrefix(LPFromPartitionKey(0x42))
	if !bytes.HasPrefix(kA, pfx) || !bytes.HasPrefix(kB, pfx) {
		t.Errorf("tenant-prefixed keys must still live under the LP-wide prefix")
	}
}

func TestInvocationLPPrefix_ScanBoundary(t *testing.T) {
	// Two ids with different partition_keys that hash to different LPs
	// must produce disjoint per-LP prefixes; a per-LP scan must isolate
	// each LP's rows.
	idA := testID(t, 0x100, "aaaaaaaaaaaaaaaa")
	idB := testID(t, 0x101, "bbbbbbbbbbbbbbbb")
	lpA := LPFromPartitionKey(0x100)
	lpB := LPFromPartitionKey(0x101)
	if lpA == lpB {
		// 0x100 vs 0x101 differ by 1 in the low bits, so they should differ
		// with any reasonable LPCount. Defensive: just skip if they collide.
		t.Skipf("test partition_keys collided on LP: %d", lpA)
	}
	kA, _ := InvocationKey(idA)
	kB, _ := InvocationKey(idB)
	pfxA := InvocationLPPrefix(lpA)
	pfxB := InvocationLPPrefix(lpB)
	if !bytes.HasPrefix(kA, pfxA) {
		t.Errorf("kA not under its LP prefix")
	}
	if !bytes.HasPrefix(kB, pfxB) {
		t.Errorf("kB not under its LP prefix")
	}
	if bytes.HasPrefix(kA, pfxB) || bytes.HasPrefix(kB, pfxA) {
		t.Errorf("LP prefixes overlap unexpectedly")
	}
}

func TestInvocationLPPrefix_CapturesAllTenantsOnLP(t *testing.T) {
	// The LP-transfer scan range `[ns/<lp>, ns/<lp+1>)` must capture
	// every tenant's rows on that LP — the LP-prefix builders return
	// `<ns>/<lp:4>` (not `<ns>/<lp:4><tenant:4>`) precisely so the
	// transfer scan is tenant-agnostic.
	id := testID(t, 0x42, "0123456789abcdef")
	lp := LPFromPartitionKey(0x42)
	pfx := InvocationLPPrefix(lp)
	for _, tenant := range []uint32{0, 1, 42, ^uint32(0)} {
		id.TenantId = tenant
		k, _ := InvocationKey(id)
		if !bytes.HasPrefix(k, pfx) {
			t.Errorf("tenant=%d key not under InvocationLPPrefix(%d): %x", tenant, lp, k)
		}
	}
}

func TestJournalKeyOrdering(t *testing.T) {
	// Same id (so same LP), increasing index — must sort by index.
	id := testID(t, 1, "0123456789abcdef")
	k0, _ := JournalKey(id, 0)
	k1, _ := JournalKey(id, 1)
	k2, _ := JournalKey(id, 256)
	if bytes.Compare(k0, k1) >= 0 || bytes.Compare(k1, k2) >= 0 {
		t.Errorf("journal keys not ordered: %x %x %x", k0, k1, k2)
	}
}

func TestJournalKey_TenantIsolation(t *testing.T) {
	idA := &enginev1.InvocationId{TenantId: 1, PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	idB := &enginev1.InvocationId{TenantId: 2, PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	kA, _ := JournalKey(idA, 0)
	kB, _ := JournalKey(idB, 0)
	if bytes.Equal(kA, kB) {
		t.Fatalf("distinct tenants must produce distinct journal keys")
	}
}

func TestTimerKey_OrderByFireAtThenID(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	type timer struct {
		FireAt uint64
		Raw    []byte
		Key    []byte
	}
	timers := make([]timer, 1000)
	for i := range timers {
		var uuidBuf [16]byte
		for j := range uuidBuf {
			uuidBuf[j] = byte(rng.Uint32())
		}
		id := &enginev1.InvocationId{
			PartitionKey: rng.Uint64(),
			Uuid:         uuidBuf[:],
		}
		fireAt := rng.Uint64() % 1_000_000
		k, err := TimerKey(fireAt, id) // primary timer key is LP+tenant-agnostic
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := EncodeInvocationID(id)
		timers[i] = timer{FireAt: fireAt, Raw: raw, Key: k}
	}
	sort.Slice(timers, func(i, j int) bool {
		return bytes.Compare(timers[i].Key, timers[j].Key) < 0
	})
	for i := 1; i < len(timers); i++ {
		a, b := timers[i-1], timers[i]
		switch {
		case a.FireAt > b.FireAt:
			t.Fatalf("out-of-order fire times at %d: %d > %d", i, a.FireAt, b.FireAt)
		case a.FireAt == b.FireAt:
			if bytes.Compare(a.Raw, b.Raw) > 0 {
				t.Fatalf("out-of-order id at %d (same fireAt)", i)
			}
		}
	}
}

func TestTimerKeyRoundtrip(t *testing.T) {
	id := testID(t, 0x42, "abcdefghijklmnop")
	k, err := TimerKey(1234567, id)
	if err != nil {
		t.Fatal(err)
	}
	fireAt, decoded, err := DecodeTimerKey(k)
	if err != nil {
		t.Fatal(err)
	}
	if fireAt != 1234567 {
		t.Errorf("fireAt = %d; want 1234567", fireAt)
	}
	if decoded.GetPartitionKey() != 0x42 {
		t.Errorf("partition_key roundtrip failed")
	}
	if !bytes.Equal(decoded.GetUuid(), []byte("abcdefghijklmnop")) {
		t.Errorf("uuid roundtrip failed")
	}
}

func TestTimerLPKey_RoundtripAndPerLPScan(t *testing.T) {
	lp := uint32(42)
	const tenant uint32 = 7
	id := &enginev1.InvocationId{TenantId: tenant, PartitionKey: 0x100, Uuid: []byte("abcdefghijklmnop")}
	k, err := TimerLPKey(lp, 9999, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(k, TimerLPPrefix()) {
		t.Errorf("missing timer_lp/ prefix: %q", k)
	}
	gotLP, gotTenant, gotFire, gotID, err := DecodeTimerLPKey(k)
	if err != nil {
		t.Fatal(err)
	}
	if gotLP != lp || gotTenant != tenant || gotFire != 9999 || gotID.GetPartitionKey() != 0x100 {
		t.Errorf("roundtrip mismatch: lp=%d tenant=%d fire=%d pk=0x%X", gotLP, gotTenant, gotFire, gotID.GetPartitionKey())
	}
	// Per-LP prefix isolates rows across LPs and captures all tenants on
	// the LP (LP-transfer scan invariant).
	pfx := TimerLPPrefixForLP(lp)
	if !bytes.HasPrefix(k, pfx) {
		t.Errorf("key not under per-LP prefix")
	}
	other := TimerLPPrefixForLP(lp + 1)
	if bytes.HasPrefix(k, other) {
		t.Errorf("key falls under neighbor LP prefix")
	}
	// Distinct tenant under the same LP+id+fire must yield a distinct
	// key (no cross-tenant aliasing).
	id2 := &enginev1.InvocationId{TenantId: tenant + 1, PartitionKey: 0x100, Uuid: []byte("abcdefghijklmnop")}
	k2, _ := TimerLPKey(lp, 9999, id2)
	if bytes.Equal(k, k2) {
		t.Fatalf("distinct tenants must produce distinct timer_lp keys")
	}
}

func TestTimerIdxKey_RoundtripAndPrefix(t *testing.T) {
	idA := testID(t, 0x42, "abcdefghijklmnop")
	idB := testID(t, 0x42, "zyxwvutsrqponmlk")

	k, err := TimerIdxKey(idA, 1234567)
	if err != nil {
		t.Fatal(err)
	}
	gotTenant, gotID, fireAt, err := DecodeTimerIdxKey(k)
	if err != nil {
		t.Fatal(err)
	}
	if fireAt != 1234567 {
		t.Errorf("fireAt = %d; want 1234567", fireAt)
	}
	if gotTenant != TenantDefault {
		t.Errorf("tenant roundtrip: got %d, want %d", gotTenant, TenantDefault)
	}
	if gotID.GetPartitionKey() != 0x42 || !bytes.Equal(gotID.GetUuid(), idA.GetUuid()) {
		t.Errorf("id roundtrip failed: %+v", gotID)
	}

	// Two ids with the same partition_key must produce disjoint prefixes;
	// a prefix scan over idA's range must not see idB's rows.
	pa, _ := TimerIdxPrefixForID(idA)
	pb, _ := TimerIdxPrefixForID(idB)
	if bytes.Equal(pa, pb) {
		t.Fatalf("prefix collision between distinct ids")
	}
	kB, _ := TimerIdxKey(idB, 999)
	if bytes.HasPrefix(kB, pa) {
		t.Errorf("idB key falls within idA prefix scan range")
	}
	// Distinct tenants must produce disjoint scan ranges for the same id.
	idAt1 := &enginev1.InvocationId{TenantId: 1, PartitionKey: 0x42, Uuid: idA.GetUuid()}
	idAt2 := &enginev1.InvocationId{TenantId: 2, PartitionKey: 0x42, Uuid: idA.GetUuid()}
	tenantAPfx, _ := TimerIdxPrefixForID(idAt1)
	tenantBPfx, _ := TimerIdxPrefixForID(idAt2)
	if bytes.Equal(tenantAPfx, tenantBPfx) {
		t.Fatalf("distinct tenants must produce distinct timer_idx prefixes")
	}
}

func TestPrefixUpperBound(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"empty", []byte{}, nil},
		{"single", []byte{0x01}, []byte{0x02}},
		{"trailing nonFF", []byte("ab"), []byte("ac")},
		{"trailing FF", []byte{0x61, 0xFF}, []byte{0x62}},
		{"all FF", []byte{0xFF, 0xFF, 0xFF}, nil},
		{"middle increment", []byte{0xFE, 0xFF, 0xFF}, []byte{0xFF}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := PrefixUpperBound(c.in)
			if !bytes.Equal(got, c.want) {
				t.Errorf("PrefixUpperBound(%x) = %x; want %x", c.in, got, c.want)
			}
		})
	}
}

func TestPrefixUpperBound_NoAliasing(t *testing.T) {
	in := []byte{0x01, 0x02}
	_ = PrefixUpperBound(in)
	if in[0] != 0x01 || in[1] != 0x02 {
		t.Errorf("input was mutated: %x", in)
	}
}

func TestDedupKeys(t *testing.T) {
	selfA := DedupSelfKey(1, 0)
	selfB := DedupSelfKey(2, 0)
	if bytes.Compare(selfA, selfB) >= 0 {
		t.Errorf("dedup self keys not in epoch order")
	}
	selfA0 := DedupSelfKey(1, 0)
	selfA1 := DedupSelfKey(1, 1)
	if bytes.Compare(selfA0, selfA1) >= 0 {
		t.Errorf("dedup self keys not in seq order within an epoch")
	}
	if bytes.Equal(selfA0, selfA1) {
		t.Errorf("dedup self keys at different seq must differ")
	}
	arb := DedupArbitraryKey(7, TenantDefault, "client-x", 0)
	if !bytes.HasPrefix(arb, []byte("dedup/arbitrary/")) {
		t.Errorf("bad arbitrary prefix: %q", arb)
	}
	// LP is encoded as 4 BE bytes immediately after dedup/arbitrary/.
	wantLPPrefix := append([]byte("dedup/arbitrary/"), 0, 0, 0, 7)
	if !bytes.HasPrefix(arb, wantLPPrefix) {
		t.Errorf("arbitrary key %q missing 4-byte BE LP=7 prefix", arb)
	}
	arb1 := DedupArbitraryKey(7, TenantDefault, "client-x", 1)
	if bytes.Equal(arb, arb1) {
		t.Errorf("dedup arbitrary keys at different seq must differ")
	}
	// Same (producer, seq) under a different LP is a distinct key — this
	// is the load-bearing invariant for the LP-prefix refactor (PR 4).
	arbLP8 := DedupArbitraryKey(8, TenantDefault, "client-x", 0)
	if bytes.Equal(arb, arbLP8) {
		t.Errorf("arbitrary keys at different lp must differ")
	}
	// Same (lp, producer, seq) under different tenants is also distinct
	// (per-tenant prefix isolates dedup state — load-bearing for the
	// multi-tenancy LP-transfer flip).
	arbTenantB := DedupArbitraryKey(7, 1, "client-x", 0)
	if bytes.Equal(arb, arbTenantB) {
		t.Errorf("arbitrary keys at different tenants must differ")
	}
	// The per-LP scan prefix is dedup/arbitrary/<lp:4> (tenant-agnostic).
	lpPrefix := DedupArbitraryLPPrefix(7)
	if !bytes.HasPrefix(arb, lpPrefix) {
		t.Errorf("arb key %q not under DedupArbitraryLPPrefix(7) %q", arb, lpPrefix)
	}
	if !bytes.HasPrefix(arbTenantB, lpPrefix) {
		t.Errorf("LP-7 tenant-1 arb key must still be under LP-7 prefix")
	}
	if bytes.HasPrefix(arbLP8, lpPrefix) {
		t.Errorf("LP-8 key must not be under LP-7 prefix")
	}
	// Self and arbitrary share the dedup/ prefix; ensure they remain in
	// distinct ranges (no key in one can be a prefix of a key in the other).
	if bytes.HasPrefix(selfA, arb) || bytes.HasPrefix(arb, selfA) {
		t.Errorf("self and arbitrary key spaces overlap")
	}
}

func TestNamespacesDistinct(t *testing.T) {
	id := testID(t, 1, "0123456789abcdef")
	const lp uint32 = 7
	invK, _ := InvocationKey(id)
	jouK, _ := JournalKey(id, 0)
	timK, _ := TimerKey(0, id)
	stateK := StateKey(lp, TenantDefault, "Svc", "obj", "key")
	outK := OutboxKey(1)
	awkK := AwakeableKey(lp, TenantDefault, "awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ0")
	leaseK := KeyLeaseKey(lp, TenantDefault, "Svc", "obj")
	idemK := IdempotencyKey(lp, TenantDefault, "Svc", "h", "obj", "ikey")
	timLP, _ := TimerLPKey(lp, 0, id)
	all := [][]byte{invK, jouK, timK, stateK, outK, awkK, leaseK, idemK, timLP}
	for i := range all {
		for j := i + 1; j < len(all); j++ {
			a, b := all[i], all[j]
			if bytes.HasPrefix(a, b) || bytes.HasPrefix(b, a) {
				t.Errorf("namespaces collide: %q vs %q", a, b)
			}
		}
	}
}

func TestStateKey_RoundtripAndPrefix(t *testing.T) {
	const lp uint32 = 13
	k := StateKey(lp, TenantDefault, "Greeter", "alice", "counter")
	pfx := StatePrefixForObject(lp, TenantDefault, "Greeter", "alice")
	if !bytes.HasPrefix(k, pfx) {
		t.Errorf("key %q not under prefix %q", k, pfx)
	}
	// Suffix after prefix should be the state key itself.
	if got := string(k[len(pfx):]); got != "counter" {
		t.Errorf("state key suffix = %q; want %q", got, "counter")
	}
	// Prefix shape: "state/" + 4-byte LP + 4-byte tenant + "Greeter/alice/".
	if !bytes.HasPrefix(pfx, StatePrefix()) {
		t.Errorf("prefix missing state/ namespace")
	}
	if gotLP := binary.BigEndian.Uint32(pfx[len(StatePrefix()) : len(StatePrefix())+LPLen]); gotLP != lp {
		t.Errorf("encoded LP = %d; want %d", gotLP, lp)
	}
	if gotTenant := binary.BigEndian.Uint32(pfx[len(StatePrefix())+LPLen : len(StatePrefix())+LPLen+TenantLen]); gotTenant != TenantDefault {
		t.Errorf("encoded tenant = %d; want %d", gotTenant, TenantDefault)
	}
	// Unkeyed services: object_key = "".
	uk := StateKey(lp, TenantDefault, "Unkeyed", "", "config")
	upfx := StatePrefixForObject(lp, TenantDefault, "Unkeyed", "")
	if !bytes.HasPrefix(uk, upfx) {
		t.Errorf("unkeyed key %q not under prefix %q", uk, upfx)
	}
	// Same (lp, service, obj, state_key) under different tenants must
	// produce distinct keys — multi-tenant isolation.
	kB := StateKey(lp, 1, "Greeter", "alice", "counter")
	if bytes.Equal(k, kB) {
		t.Fatalf("distinct tenants must produce distinct state keys")
	}
}

func TestStateKey_PerObjectScanIsolation(t *testing.T) {
	// Within one logical partition + one tenant, a per-object scan must
	// isolate that object's rows from other objects in the same service.
	const lp uint32 = 99
	keys := [][]byte{
		StateKey(lp, TenantDefault, "Svc", "alice", "balance"),
		StateKey(lp, TenantDefault, "Svc", "alice", "name"),
		StateKey(lp, TenantDefault, "Svc", "bob", "balance"),
		StateKey(lp, TenantDefault, "Svc", "bob", "name"),
		StateKey(lp, TenantDefault, "Svc", "carol", "name"),
	}
	aliceLo := StatePrefixForObject(lp, TenantDefault, "Svc", "alice")
	aliceHi := PrefixUpperBound(aliceLo)
	for i, k := range keys {
		inRange := bytes.Compare(k, aliceLo) >= 0 && bytes.Compare(k, aliceHi) < 0
		wantInRange := i < 2 // first two are alice's
		if inRange != wantInRange {
			t.Errorf("key %q in alice range = %v; want %v", k, inRange, wantInRange)
		}
	}
	// Within an object, state keys sort by state_key.
	if bytes.Compare(keys[0], keys[1]) >= 0 {
		t.Errorf("balance should sort before name within alice")
	}
}

func TestOutboxKey_LexNumericAgreement(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	type sample struct {
		Seq uint64
		Key []byte
	}
	samples := make([]sample, 1000)
	for i := range samples {
		seq := rng.Uint64()
		samples[i] = sample{Seq: seq, Key: OutboxKey(seq)}
	}
	sort.Slice(samples, func(i, j int) bool {
		return bytes.Compare(samples[i].Key, samples[j].Key) < 0
	})
	for i := 1; i < len(samples); i++ {
		if samples[i-1].Seq > samples[i].Seq {
			t.Fatalf("lex/numeric mismatch at %d: %d > %d",
				i, samples[i-1].Seq, samples[i].Seq)
		}
	}
}

func TestOutboxKey_Roundtrip(t *testing.T) {
	for _, seq := range []uint64{0, 1, 1 << 32, ^uint64(0)} {
		k := OutboxKey(seq)
		got, err := DecodeOutboxKey(k)
		if err != nil {
			t.Fatalf("seq=%d: %v", seq, err)
		}
		if got != seq {
			t.Errorf("seq roundtrip: got %d, want %d", got, seq)
		}
	}
}

func TestAwakeableKey_RoundtripAndPrefix(t *testing.T) {
	const lp uint32 = 21
	id := "awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ0" // 31 chars, all valid
	if err := ValidateAwakeableID(id); err != nil {
		t.Fatalf("validate: %v", err)
	}
	k := AwakeableKey(lp, TenantDefault, id)
	if !bytes.HasPrefix(k, AwakeablePrefix()) {
		t.Errorf("bad prefix: %q", k)
	}
	if len(k) != len("awakeable/")+LPLen+TenantLen+awakeableIDLen {
		t.Errorf("len=%d want %d", len(k), len("awakeable/")+LPLen+TenantLen+awakeableIDLen)
	}
	// LP is BE-encoded right after namespace; tenant immediately after.
	if gotLP := binary.BigEndian.Uint32(k[len("awakeable/") : len("awakeable/")+LPLen]); gotLP != lp {
		t.Errorf("encoded lp = %d; want %d", gotLP, lp)
	}
	if gotTenant := binary.BigEndian.Uint32(k[len("awakeable/")+LPLen : len("awakeable/")+LPLen+TenantLen]); gotTenant != TenantDefault {
		t.Errorf("encoded tenant = %d; want %d", gotTenant, TenantDefault)
	}
	// Distinct tenants must produce distinct awakeable keys.
	kB := AwakeableKey(lp, 1, id)
	if bytes.Equal(k, kB) {
		t.Fatalf("distinct tenants must produce distinct awakeable keys")
	}
}

func TestIdempotencyKey_DeterministicAndSensitive(t *testing.T) {
	const lp uint32 = 5
	// Same tuple → same key.
	a := IdempotencyKey(lp, TenantDefault, "Counter", "incr", "user-1", "req-7")
	b := IdempotencyKey(lp, TenantDefault, "Counter", "incr", "user-1", "req-7")
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic key: %x vs %x", a, b)
	}
	// Length: prefix + LP + tenant + 32-byte sha256.
	if len(a) != len("idempotency/")+LPLen+TenantLen+32 {
		t.Errorf("len = %d, want %d", len(a), len("idempotency/")+LPLen+TenantLen+32)
	}
	// Adjacent components must not alias. ("ab","c") vs ("a","bc"). Same lp
	// for both so the hash difference isn't masked by an lp difference.
	k1 := IdempotencyKey(lp, TenantDefault, "ab", "c", "", "k")
	k2 := IdempotencyKey(lp, TenantDefault, "a", "bc", "", "k")
	if bytes.Equal(k1, k2) {
		t.Errorf("adjacent-field aliasing: %x", k1)
	}
	// Distinct idempotency_keys differ.
	if bytes.Equal(
		IdempotencyKey(lp, TenantDefault, "S", "h", "o", "k1"),
		IdempotencyKey(lp, TenantDefault, "S", "h", "o", "k2"),
	) {
		t.Errorf("distinct idempotency keys collided")
	}
	// Distinct LPs differ (same tuple, different LP → different key).
	if bytes.Equal(
		IdempotencyKey(0, TenantDefault, "S", "h", "o", "k"),
		IdempotencyKey(1, TenantDefault, "S", "h", "o", "k"),
	) {
		t.Errorf("different LPs produced the same key")
	}
	// Distinct tenants differ (same tuple, different tenant → different key).
	if bytes.Equal(
		IdempotencyKey(lp, 1, "S", "h", "o", "k"),
		IdempotencyKey(lp, 2, "S", "h", "o", "k"),
	) {
		t.Errorf("different tenants produced the same key")
	}
}

func TestAwakeableOwnerPartitionKey_Roundtrip(t *testing.T) {
	for _, pk := range []uint64{0, 1, 42, 1 << 31, 1<<63 + 17, ^uint64(0)} {
		const tenant uint32 = 7
		var body [20]byte
		binary.BigEndian.PutUint32(body[:4], tenant)
		binary.BigEndian.PutUint64(body[4:12], pk)
		// Last 8 bytes are random in production; the decoder ignores them.
		body[12], body[19] = 0xDE, 0xAD
		id := "awk_" + base64.RawURLEncoding.EncodeToString(body[:])
		got, err := AwakeableOwnerPartitionKey(id)
		if err != nil {
			t.Fatalf("pk=%d: %v", pk, err)
		}
		if got != pk {
			t.Errorf("pk roundtrip: got %d, want %d", got, pk)
		}
		gotTenant, err := AwakeableOwnerTenant(id)
		if err != nil {
			t.Fatalf("pk=%d: tenant decode: %v", pk, err)
		}
		if gotTenant != tenant {
			t.Errorf("tenant roundtrip: got %d, want %d", gotTenant, tenant)
		}
	}
}

func TestAwakeableOwnerPartitionKey_RejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"awk_short",
		"bad_ABCDEFGHIJKLMNOPQRSTUV",
		"awk_ABCDEFGHIJKLMNOPQRSTU!", // bad charset
	}
	for _, id := range cases {
		if _, err := AwakeableOwnerPartitionKey(id); err == nil {
			t.Errorf("AwakeableOwnerPartitionKey(%q): want error, got nil", id)
		}
	}
}

func TestValidateAwakeableID(t *testing.T) {
	cases := []struct {
		id    string
		valid bool
	}{
		{"awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ0", true},
		{"awk_0123456789_-abcdefghijklmno", true},
		{"awk_aaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"", false},
		{"awk_short", false},
		{"bad_ABCDEFGHIJKLMNOPQRSTUVWXYZ0", false},
		{"awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ/", false}, // '/' is illegal
		{"awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ!", false},
		{"awk_ABCDEFGHIJKLMNOPQRSTUVWXYZ0 ", false}, // trailing space → wrong len
	}
	for _, c := range cases {
		err := ValidateAwakeableID(c.id)
		got := err == nil
		if got != c.valid {
			t.Errorf("ValidateAwakeableID(%q): got valid=%v, want %v (err=%v)",
				c.id, got, c.valid, err)
		}
	}
}
