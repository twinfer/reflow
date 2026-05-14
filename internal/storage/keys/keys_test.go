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
	if len(raw) != 24 {
		t.Fatalf("raw length = %d; want 24", len(raw))
	}
	if binary.BigEndian.Uint64(raw[:8]) != 0xDEADBEEFCAFEBABE {
		t.Errorf("partition_key not big-endian")
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

func TestInvocationKey(t *testing.T) {
	id := testID(t, 1, "0123456789abcdef")
	k, err := InvocationKey(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(k, []byte("inv/")) {
		t.Errorf("bad prefix: %q", k)
	}
	if len(k) != 4+24 {
		t.Errorf("len = %d; want 28", len(k))
	}
}

func TestJournalKeyOrdering(t *testing.T) {
	id := testID(t, 1, "0123456789abcdef")
	k0, _ := JournalKey(id, 0)
	k1, _ := JournalKey(id, 1)
	k2, _ := JournalKey(id, 256)
	if bytes.Compare(k0, k1) >= 0 || bytes.Compare(k1, k2) >= 0 {
		t.Errorf("journal keys not ordered: %x %x %x", k0, k1, k2)
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
		k, err := TimerKey(fireAt, id)
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
	selfA := DedupSelfKey(1)
	selfB := DedupSelfKey(2)
	if bytes.Compare(selfA, selfB) >= 0 {
		t.Errorf("dedup self keys not in epoch order")
	}
	arb := DedupArbitraryKey("client-x")
	if !bytes.HasPrefix(arb, []byte("dedup/arbitrary/")) {
		t.Errorf("bad arbitrary prefix: %q", arb)
	}
	// Self and arbitrary share the dedup/ prefix; ensure they remain in
	// distinct ranges (no key in one can be a prefix of a key in the other).
	if bytes.HasPrefix(selfA, arb) || bytes.HasPrefix(arb, selfA) {
		t.Errorf("self and arbitrary key spaces overlap")
	}
}

func TestNamespacesDistinct(t *testing.T) {
	id := testID(t, 1, "0123456789abcdef")
	invK, _ := InvocationKey(id)
	jouK, _ := JournalKey(id, 0)
	timK, _ := TimerKey(0, id)
	stateK := StateKey("Svc", "obj", "key")
	outK := OutboxKey(1)
	awkK := AwakeableKey("awk_AAAAAAAAAAAAAAAAAAAAAA")
	leaseK := KeyLeaseKey("Svc", "obj")
	idemK := IdempotencyKey("Svc", "h", "obj", "ikey")
	all := [][]byte{invK, jouK, timK, stateK, outK, awkK, leaseK, idemK}
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
	k := StateKey("Greeter", "alice", "counter")
	if string(k) != "state/Greeter/alice/counter" {
		t.Errorf("unexpected key: %q", k)
	}
	pfx := StatePrefixForObject("Greeter", "alice")
	if string(pfx) != "state/Greeter/alice/" {
		t.Errorf("unexpected prefix: %q", pfx)
	}
	if !bytes.HasPrefix(k, pfx) {
		t.Errorf("key %q not under prefix %q", k, pfx)
	}
	// Unkeyed services: object_key = "".
	uk := StateKey("Unkeyed", "", "config")
	if string(uk) != "state/Unkeyed//config" {
		t.Errorf("unkeyed state key: %q", uk)
	}
	upfx := StatePrefixForObject("Unkeyed", "")
	if !bytes.HasPrefix(uk, upfx) {
		t.Errorf("unkeyed key %q not under prefix %q", uk, upfx)
	}
}

func TestStateKey_OrderingAcrossObjects(t *testing.T) {
	// Within a service the (object_key, state_key) lex order should match
	// natural string ordering. A scan from StatePrefixForObject("Svc", X)
	// should only return keys for that exact object — never spill into a
	// neighbouring object_key.
	keys := [][]byte{
		StateKey("Svc", "alice", "balance"),
		StateKey("Svc", "alice", "name"),
		StateKey("Svc", "bob", "balance"),
		StateKey("Svc", "bob", "name"),
		StateKey("Svc", "carol", "name"),
		StateKey("Tvc", "alice", "x"), // different service comes after
	}
	// Confirm pre-sorted; if so, in-place sort is a no-op.
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Fatalf("keys not sorted at %d: %q vs %q", i, keys[i-1], keys[i])
		}
	}
	// Verify per-object scan bounds isolate alice's rows from bob's.
	aliceLo := StatePrefixForObject("Svc", "alice")
	aliceHi := PrefixUpperBound(aliceLo)
	for i, k := range keys {
		inRange := bytes.Compare(k, aliceLo) >= 0 && bytes.Compare(k, aliceHi) < 0
		wantInRange := i < 2 // first two are alice's
		if inRange != wantInRange {
			t.Errorf("key %q in alice range = %v; want %v", k, inRange, wantInRange)
		}
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
	id := "awk_ABCDEFGHIJKLMNOPQRSTUV" // 26 chars, all valid
	if err := ValidateAwakeableID(id); err != nil {
		t.Fatalf("validate: %v", err)
	}
	k := AwakeableKey(id)
	if !bytes.HasPrefix(k, AwakeablePrefix()) {
		t.Errorf("bad prefix: %q", k)
	}
	if len(k) != len("awakeable/")+26 {
		t.Errorf("len=%d want %d", len(k), len("awakeable/")+26)
	}
}

func TestIdempotencyKey_DeterministicAndSensitive(t *testing.T) {
	// Same tuple → same key.
	a := IdempotencyKey("Counter", "incr", "user-1", "req-7")
	b := IdempotencyKey("Counter", "incr", "user-1", "req-7")
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic key: %x vs %x", a, b)
	}
	// Length: prefix + 32-byte sha256.
	if len(a) != len("idempotency/")+32 {
		t.Errorf("len = %d, want %d", len(a), len("idempotency/")+32)
	}
	// Adjacent components must not alias. ("ab","c") vs ("a","bc").
	k1 := IdempotencyKey("ab", "c", "", "k")
	k2 := IdempotencyKey("a", "bc", "", "k")
	if bytes.Equal(k1, k2) {
		t.Errorf("adjacent-field aliasing: %x", k1)
	}
	// Empty object_key vs absent are the same (Phase 3 has only one form).
	// Distinct idempotency_keys differ.
	if bytes.Equal(IdempotencyKey("S", "h", "o", "k1"), IdempotencyKey("S", "h", "o", "k2")) {
		t.Errorf("distinct idempotency keys collided")
	}
}

func TestAwakeableOwnerPartitionKey_Roundtrip(t *testing.T) {
	for _, pk := range []uint64{0, 1, 42, 1 << 31, 1<<63 + 17, ^uint64(0)} {
		var body [16]byte
		binary.BigEndian.PutUint64(body[:8], pk)
		// Last 8 bytes are random in production; the decoder ignores them.
		body[8], body[15] = 0xDE, 0xAD
		id := "awk_" + base64.RawURLEncoding.EncodeToString(body[:])
		got, err := AwakeableOwnerPartitionKey(id)
		if err != nil {
			t.Fatalf("pk=%d: %v", pk, err)
		}
		if got != pk {
			t.Errorf("pk roundtrip: got %d, want %d", got, pk)
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
		{"awk_ABCDEFGHIJKLMNOPQRSTUV", true},
		{"awk_0123456789_-abcdefghij", true},
		{"awk_aaaaaaaaaaaaaaaaaaaaaa", true},
		{"", false},
		{"awk_short", false},
		{"bad_ABCDEFGHIJKLMNOPQRSTUV", false},
		{"awk_ABCDEFGHIJKLMNOPQRSTU/", false}, // '/' is illegal
		{"awk_ABCDEFGHIJKLMNOPQRSTU!", false},
		{"awk_ABCDEFGHIJKLMNOPQRSTUV ", false}, // trailing space → wrong len
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
