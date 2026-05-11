package keys

import (
	"bytes"
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
	// Each pair must NOT share a prefix relationship — different namespaces.
	if bytes.HasPrefix(invK, jouK) || bytes.HasPrefix(jouK, invK) {
		t.Errorf("inv and journal namespaces collide")
	}
	if bytes.HasPrefix(invK, timK) || bytes.HasPrefix(timK, invK) {
		t.Errorf("inv and timer namespaces collide")
	}
	if bytes.HasPrefix(jouK, timK) || bytes.HasPrefix(timK, jouK) {
		t.Errorf("journal and timer namespaces collide")
	}
}
