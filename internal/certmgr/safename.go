package certmgr

import (
	"crypto/sha256"
	"encoding/hex"
)

// safeMeshName maps an arbitrary principal Raw form ("<kind>/<name>") to
// a deterministic IDNA-clean single-label domain certmagic can carry as
// a "domain name" through ManageSync. CertMagic runs every identifier
// through idna.ToASCII before CSR generation (config.go:1052) which
// rejects '/', so the principal cannot be passed verbatim. A SHA-256
// prefix is enough to identify one node's leaf across restarts; the
// real identity lives in the issued leaf's CN, which the Issuer sets
// from its construction-time principal independent of the safe name.
func safeMeshName(principal string) string {
	sum := sha256.Sum256([]byte(principal))
	return "reflow-" + hex.EncodeToString(sum[:8]) + ".mesh"
}
