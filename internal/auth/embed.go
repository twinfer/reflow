package auth

import _ "embed"

// StarterPolicyJSON is the embedded fallback authz policy applied when
// Config.AuthzPolicyFile is empty. It encodes the three reflow surface
// rules (ingress allow-all, delivery node-only, admin operator-only)
// keyed off the x-reflow-principal metadata header stamped by the
// principal-extractor shim.
//
//go:embed starter_policy.json
var StarterPolicyJSON string
