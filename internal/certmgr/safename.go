package certmgr

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/net/idna"
)

// safeMeshName maps an arbitrary principal Raw form ("<kind>/<name>") to
// a deterministic IDNA-clean DNS-style name certmagic can carry as the
// identifier through ManageSync. CertMagic runs every identifier
// through idna.ToASCII before CSR generation (config.go:1052) which
// rejects '/' (and other DNS-unsafe characters), so the principal
// cannot be passed verbatim.
//
// First-pass: substitute '/' with '-' and normalize via the strict
// IDNA Lookup profile. That keeps "node/7" → "node-7" and
// "operator/alice" → "operator-alice", which read clearly in logs and
// in CertMagic's on-disk storage layout. When the principal contains
// IDN-invalid characters that even '-' substitution can't rescue, fall
// back to a SHA-256 prefix so cert issuance still works — the leaf's
// identity (verified via creds.LeafPrincipal) lives in the CN and is
// independent of this safe name.
func safeMeshName(principal string) string {
	candidate := strings.ReplaceAll(principal, "/", "-")
	if got, err := idna.Lookup.ToASCII(candidate); err == nil && got != "" {
		return got
	}
	sum := sha256.Sum256([]byte(principal))
	return "reflw-" + hex.EncodeToString(sum[:8]) + ".mesh"
}
