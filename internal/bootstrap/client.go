package bootstrap

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	connect "connectrpc.com/connect"
	"golang.org/x/net/http2"

	bootstrapv1 "github.com/twinfer/reflw/proto/bootstrapv1"
	"github.com/twinfer/reflw/proto/bootstrapv1/bootstrapv1connect"
)

// JoinResult bundles the outputs of a successful MeshSign exchange.
type JoinResult struct {
	// CertPEM is the signed leaf, PEM-encoded.
	CertPEM []byte
	// KeyPEM is the locally-generated private key, PEM-encoded.
	KeyPEM []byte
	// CAChainPEM is the active CA chain returned by the server.
	CAChainPEM []byte
	// AssignedNodeID is the node_id allocated by the server when the
	// token's requested_name was "auto". Zero for operator tokens or
	// when the joiner requested a fixed name.
	AssignedNodeID uint64
	// CAFingerprint is the SPKI fingerprint of the active CA at signing
	// time (sha256:<hex>), as reported by the server.
	CAFingerprint string
}

// JoinOptions configures Join.
type JoinOptions struct {
	// Addr is the bootstrap listener address (e.g. "node1.example.com:8443").
	Addr string
	// Token is the plaintext join token printed by
	// `reflowd config create-join-token`.
	Token string
	// Kind is "node" or "operator" — determines the CSR CN prefix and
	// (for operator tokens) the requested name.
	Kind string
	// RequestedName is the name segment. For node tokens this is
	// typically "auto" (server allocates a node_id); for operator
	// tokens it's the operator's identifier (e.g. "alice").
	RequestedName string
	// RootCertPin, when non-empty, is the expected SPKI fingerprint
	// (sha256:<hex>) of the bootstrap server's leaf. Defends against
	// bootstrap MITM by short-circuiting the handshake when the leaf
	// doesn't match. Matches kubeadm's --discovery-token-ca-cert-hash.
	RootCertPin string
	// ExtraHosts are DNS / IP SANs to embed in the CSR (and so in the
	// signed leaf). The joiner's own hostname / IP is the canonical
	// entry.
	ExtraHosts []string
}

// Join generates a fresh ECDSA-P256 keypair, builds a CSR with
// CN=<kind>/<requested_name>, dials the bootstrap listener at opts.Addr,
// presents the token, and returns the signed leaf bundle. The dial is
// TLS-but-skip-verify because the joiner has no mesh-CA pool yet; the
// optional --root-cert-pin SPKI fingerprint check is the only
// authentication of the server's identity.
func Join(ctx context.Context, opts JoinOptions) (*JoinResult, error) {
	if opts.Addr == "" || opts.Token == "" || opts.Kind == "" || opts.RequestedName == "" {
		return nil, errors.New("bootstrap: Addr, Token, Kind, and RequestedName are required")
	}
	if opts.Kind != "node" && opts.Kind != "operator" {
		return nil, fmt.Errorf("bootstrap: unsupported kind %q", opts.Kind)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: generate key: %w", err)
	}
	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: opts.Kind + "/" + opts.RequestedName},
	}
	for _, h := range opts.ExtraHosts {
		appendSAN(tmpl, h)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, tmpl, key)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: build CSR: %w", err)
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		VerifyConnection:   newPinVerifier(opts.RootCertPin),
	}
	httpClient := &http.Client{
		Transport: &http2.Transport{TLSClientConfig: tlsCfg},
	}
	cli := bootstrapv1connect.NewMeshSignClient(httpClient, "https://"+opts.Addr)
	resp, err := cli.SignCSR(ctx, connect.NewRequest(&bootstrapv1.SignCSRRequest{
		CsrDer:    csrDER,
		JoinToken: opts.Token,
	}))
	if err != nil {
		return nil, fmt.Errorf("bootstrap: MeshSign.SignCSR: %w", err)
	}

	leafBlock, _ := pem.Decode(resp.Msg.GetCertPem())
	if leafBlock == nil {
		return nil, errors.New("bootstrap: returned cert is not PEM")
	}
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: parse returned leaf: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(resp.Msg.GetCaChainPem()) {
		return nil, errors.New("bootstrap: returned ca_chain_pem unparseable")
	}
	if _, verr := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); verr != nil {
		return nil, fmt.Errorf("bootstrap: returned leaf does not chain to returned CA: %w", verr)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: marshal key: %w", err)
	}
	return &JoinResult{
		CertPEM:        resp.Msg.GetCertPem(),
		KeyPEM:         pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		CAChainPEM:     resp.Msg.GetCaChainPem(),
		AssignedNodeID: resp.Msg.GetAssignedNodeId(),
		CAFingerprint:  resp.Msg.GetCaFingerprint(),
	}, nil
}

// newPinVerifier returns a VerifyConnection callback that requires the
// peer leaf's SPKI to match pin. Empty pin disables the check (the
// caller is trusting the network — kubeadm calls this "unsafe skip-
// verify" and warns operators in the same place). When the pin doesn't
// match, the handshake fails with a descriptive error.
func newPinVerifier(pin string) func(tls.ConnectionState) error {
	if pin == "" {
		return func(tls.ConnectionState) error { return nil }
	}
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("bootstrap: peer presented no certificates")
		}
		got := spkiPinFromCert(state.PeerCertificates[0])
		if !pinEqual(got, pin) {
			return fmt.Errorf("bootstrap: server SPKI %s does not match pin %s", got, pin)
		}
		return nil
	}
}

func pinEqual(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func spkiPinFromCert(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	const hextab = "0123456789abcdef"
	out := make([]byte, 0, 7+2*len(sum))
	out = append(out, "sha256:"...)
	for _, b := range sum {
		out = append(out, hextab[b>>4], hextab[b&0x0F])
	}
	return string(out)
}

func appendSAN(req *x509.CertificateRequest, host string) {
	if ip := net.ParseIP(host); ip != nil {
		req.IPAddresses = append(req.IPAddresses, ip)
		return
	}
	req.DNSNames = append(req.DNSNames, host)
}
