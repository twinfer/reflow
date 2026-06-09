package keys

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestResumeToken_RoundTrip(t *testing.T) {
	cases := []ResumeTarget{
		{PartitionKey: 0, Service: "", InstanceKey: "", NodeID: ""},
		{PartitionKey: 1, Service: "approval", InstanceKey: "order-42", NodeID: "u1"},
		{PartitionKey: 0xFFFF_FFFF_FFFF_FFFF, Service: "Case_X", InstanceKey: "k", NodeID: "pi1"},
		// CMMN replicaKey for a repeating plan item — the chars that motivated
		// length-prefixed-base64url framing over a delimiter scheme.
		{PartitionKey: 7, Service: "incident-case", InstanceKey: "inc/2026/06", NodeID: "pi1#cfi[0]"},
		// Unicode + delimiters in the instance key must survive verbatim.
		{PartitionKey: 99, Service: "café-process", InstanceKey: "клиент/2026", NodeID: "task-✓"},
	}
	for _, want := range cases {
		tok, err := MintResumeToken(want.PartitionKey, want.Service, want.InstanceKey, want.NodeID)
		if err != nil {
			t.Fatalf("MintResumeToken(%+v): %v", want, err)
		}
		if !strings.HasPrefix(tok, resumeTokenPrefix) {
			t.Errorf("token %q missing %q prefix", tok, resumeTokenPrefix)
		}
		got, err := DecodeResumeToken(tok)
		if err != nil {
			t.Fatalf("DecodeResumeToken(%q): %v", tok, err)
		}
		if got != want {
			t.Errorf("round-trip mismatch:\n got  %+v\n want %+v\n (token %q)", got, want, tok)
		}
	}
}

// Determinism is the whole point: the same parked task must always mint the same
// token, so a re-GET hands back an identical handle and DeliverProcessEvent retries
// dedup naturally. (Contrast the awakeable id, which embeds 8 random bytes.)
func TestResumeToken_Deterministic(t *testing.T) {
	a, err := MintResumeToken(42, "approval", "order-42", "u1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := MintResumeToken(42, "approval", "order-42", "u1")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("mint not deterministic: %q != %q", a, b)
	}
}

func TestResumeToken_DistinctInputsDistinctTokens(t *testing.T) {
	base, _ := MintResumeToken(42, "approval", "order-42", "u1")
	variants := map[string]string{}
	mint := func(pk uint64, s, ik, n string) string {
		tok, err := MintResumeToken(pk, s, ik, n)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	variants["pk"] = mint(43, "approval", "order-42", "u1")
	variants["service"] = mint(42, "approvalX", "order-42", "u1")
	variants["instance"] = mint(42, "approval", "order-43", "u1")
	variants["node"] = mint(42, "approval", "order-42", "u2")
	for which, v := range variants {
		if v == base {
			t.Errorf("changing %s did not change the token (%q)", which, v)
		}
	}
	// A field-boundary shift (service+instance vs instance prefix) must not alias:
	// length-prefixing, not concatenation, is what guarantees this.
	x := mint(42, "ab", "c", "")
	y := mint(42, "a", "bc", "")
	if x == y {
		t.Errorf("field-boundary collision: (ab,c) aliased (a,bc): %q", x)
	}
}

func TestResumeToken_URLSafe(t *testing.T) {
	tok, err := MintResumeToken(0xDEAD_BEEF, "café-process", "клиент/2026", "pi1#cfi[0]")
	if err != nil {
		t.Fatal(err)
	}
	// Safe to drop verbatim into the /v1/tasks/{token} path segment: prefix plus
	// RawURLEncoding alphabet only, no padding, no '+' or '/'.
	for i, c := range tok[len(resumeTokenPrefix):] {
		ok := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-'
		if !ok {
			t.Errorf("token char %d (%q) outside base64url alphabet: %q", i, c, tok)
		}
	}
	if strings.ContainsAny(tok, "=+/") {
		t.Errorf("token %q contains a non-URL-safe char", tok)
	}
}

func TestDecodeResumeToken_Errors(t *testing.T) {
	good, err := MintResumeToken(42, "approval", "order-42", "u1")
	if err != nil {
		t.Fatal(err)
	}
	goodBody, err := base64.RawURLEncoding.DecodeString(good[len(resumeTokenPrefix):])
	if err != nil {
		t.Fatal(err)
	}
	reencode := func(b []byte) string {
		return resumeTokenPrefix + base64.RawURLEncoding.EncodeToString(b)
	}

	tests := []struct {
		name string
		tok  string
	}{
		{"empty", ""},
		{"prefix only", resumeTokenPrefix},
		{"wrong prefix", "awk_" + good[len(resumeTokenPrefix):]},
		{"bad base64", resumeTokenPrefix + "!!!not-base64!!!"},
		{"too short", reencode([]byte{1, 0, 0, 0})},
		{"wrong version", func() string {
			b := append([]byte(nil), goodBody...)
			b[0] = 2
			return reencode(b)
		}()},
		{"trailing bytes", reencode(append(append([]byte(nil), goodBody...), 0xAB))},
		{"truncated field", func() string {
			// ver | pk(8) | svc len=0 | ik len=0 | node len=10 but no bytes follow.
			b := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10}
			return reencode(b)
		}()},
	}
	for _, tc := range tests {
		if _, err := DecodeResumeToken(tc.tok); err == nil {
			t.Errorf("%s: DecodeResumeToken(%q) = nil error, want error", tc.name, tc.tok)
		}
	}
}

func TestMintResumeToken_FieldTooLong(t *testing.T) {
	huge := strings.Repeat("x", 0x1_0000) // 65536 > u16 ceiling
	if _, err := MintResumeToken(1, huge, "k", "n"); err == nil {
		t.Error("oversize service: want error, got nil")
	}
	// The u16 maximum itself must encode fine.
	max := strings.Repeat("y", 0xFFFF)
	tok, err := MintResumeToken(1, "s", "k", max)
	if err != nil {
		t.Fatalf("max-length node_id: %v", err)
	}
	got, err := DecodeResumeToken(tok)
	if err != nil {
		t.Fatalf("decode max-length node_id: %v", err)
	}
	if got.NodeID != max {
		t.Errorf("max-length node_id not preserved (len got %d want %d)", len(got.NodeID), len(max))
	}
}
