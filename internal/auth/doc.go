// Package auth is reflow's authentication + authorization HTTP
// middleware layer for the Connect-based ingress, admin, and delivery
// listeners. The model is:
//
//   - An authn.AuthFunc turns each inbound *http.Request into a
//     Principal. Today there are two authenticators: SPIFFE from the
//     verified mTLS leaf (spiffe_authfunc.go) and Bearer JWT against
//     one or more configured OIDC issuers (jwt_authfunc.go). mTLS
//     wins when both are presented.
//
//   - The policy handler stamps Principal.Raw into the server-
//     controlled X-Reflow-Principal header (any inbound copy is
//     stripped first, so a client cannot forge identity) and then
//     matches request URL.Path against a JSON allow-list policy.
//     Denial emits a connect-coded error (CodeUnauthenticated for
//     anonymous, CodePermissionDenied for known-but-rejected) so
//     clients see the right error across Connect / gRPC / gRPC-Web /
//     HTTP-JSON.
//
//   - The embedded starter policy lives in starter_policy.json;
//     operators override via Config.PolicyFile, which is polled for
//     mtime changes every FileWatcherReload (30s).
//
// Adding a new authentication source (e.g. PASETO, AWS SigV4) means
// writing one more authn.AuthFunc and composing it ahead of the
// bearer step in composeAuthFunc.
package auth
