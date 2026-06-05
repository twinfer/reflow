package creds

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// LeafPrincipal returns the principal Raw form ("<kind>/<name>") read
// from leaf.Subject.CommonName. Returns ("", error) if the CN is empty
// or does not match the shape — exactly one `/` with non-empty kind and
// non-empty name. Mesh leaves issued by internal/pki encode the
// principal Raw form in the CN; this is the canonical extraction
// helper used by the Signer (for iss), Verifier (for the leaf-identity
// check), and the auth middleware (for principal materialization).
func LeafPrincipal(leaf *x509.Certificate) (string, error) {
	if leaf == nil {
		return "", errors.New("reflw/creds: nil leaf certificate")
	}
	cn := leaf.Subject.CommonName
	i := strings.IndexByte(cn, '/')
	if i <= 0 || i == len(cn)-1 || strings.IndexByte(cn[i+1:], '/') >= 0 {
		return "", fmt.Errorf("reflw/creds: leaf CN %q does not match <kind>/<name>", cn)
	}
	return cn, nil
}

// SPKIFingerprint returns the "sha256:<hex>" digest over
// cert.RawSubjectPublicKeyInfo. Used as the mesh-CA trust-anchor
// identifier (the CA's public key, not the cert as a whole, is what
// stays stable across re-issues with the same key material).
func SPKIFingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	h := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(h[:])
}
