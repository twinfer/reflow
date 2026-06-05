package bootstrap

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func TestFindToken_PicksMatchingHash(t *testing.T) {
	want := []byte{0xAA, 0xBB, 0xCC}
	other := []byte{0x01, 0x02}
	records := []*enginev1.JoinTokenRecord{
		{TokenHash: other},
		{TokenHash: want, RequestedName: "auto"},
		{TokenHash: []byte{0xDE, 0xAD}},
	}
	got := findToken(records, want)
	if got == nil || got.GetRequestedName() != "auto" {
		t.Fatalf("findToken returned %+v; want hash match", got)
	}
	if findToken(records, []byte{0xFF}) != nil {
		t.Fatal("unexpected match for unknown hash")
	}
}

func TestParseCSRCommonName(t *testing.T) {
	cases := []struct {
		in       string
		wantKind string
		wantName string
		wantOK   bool
	}{
		{"node/7", "node", "7", true},
		{"operator/alice", "operator", "alice", true},
		{"node/auto", "node", "auto", true},
		{"noslash", "", "", false},
		{"/leading", "", "", false},
		{"trailing/", "", "", false},
	}
	for _, c := range cases {
		k, n, ok := parseCSRCommonName(c.in)
		if ok != c.wantOK || k != c.wantKind || n != c.wantName {
			t.Errorf("parseCSRCommonName(%q) = (%q, %q, %v); want (%q, %q, %v)",
				c.in, k, n, ok, c.wantKind, c.wantName, c.wantOK)
		}
	}
}

func TestPrincipalKindFromToken(t *testing.T) {
	got, err := principalKindFromToken(enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE)
	if err != nil || got != "node" {
		t.Errorf("node mapping = (%q, %v)", got, err)
	}
	got, err = principalKindFromToken(enginev1.JoinTokenKind_JOIN_TOKEN_KIND_OPERATOR)
	if err != nil || got != "operator" {
		t.Errorf("operator mapping = (%q, %v)", got, err)
	}
	if _, err := principalKindFromToken(enginev1.JoinTokenKind_JOIN_TOKEN_KIND_UNSPECIFIED); err == nil {
		t.Error("unspecified mapping should error")
	}
}

func TestCAFingerprint_DeterministicAndPrefixed(t *testing.T) {
	caPEM := mintSelfSignedECDSACert(t)
	first := caFingerprint(caPEM)
	second := caFingerprint(caPEM)
	if first == "" || first != second {
		t.Fatalf("fingerprint mismatch: %q vs %q", first, second)
	}
	if len(first) != len("sha256:")+64 {
		t.Errorf("fingerprint %q wrong length", first)
	}
	block, _ := pem.Decode(caPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	const hextab = "0123456789abcdef"
	wantHex := make([]byte, 0, 2*len(want))
	for _, c := range want {
		wantHex = append(wantHex, hextab[c>>4], hextab[c&0x0F])
	}
	if string(wantHex) != first[len("sha256:"):] {
		t.Errorf("fingerprint != sha256(RawSubjectPublicKeyInfo); got %q", first)
	}
}

// mintSelfSignedECDSACert returns a PEM-encoded self-signed ECDSA-P256
// cert. Used only by the fingerprint round-trip test; production CA
// minting lives in internal/certmgr's clusterissuer path.
func mintSelfSignedECDSACert(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
