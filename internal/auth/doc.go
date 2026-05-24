// Package auth is reflow's authentication HTTP middleware layer for the
// Connect-based ingress, admin, and delivery listeners. Authorization is a
// separate concern, enforced downstream by the Cedar Connect interceptor in
// internal/authz. The model is:
//
//   - An authn.AuthFunc turns each inbound *http.Request into a
//     Principal. Today there are two authenticators: mesh-leaf-CN
//     from the verified mTLS leaf (mesh_authfunc.go) and Bearer JWT
//     against one or more configured OIDC issuers (jwt_authfunc.go).
//     mTLS wins when both are presented.
//
//   - The stamp handler (policy_handler.go) stamps Principal.Raw into the
//     server-controlled X-Reflow-Principal header (any inbound copy is
//     stripped first, so a client cannot forge identity) and attaches the
//     Principal to the request context. It never denies — authentication
//     failures are emitted by the authn middleware, and authorization is
//     decided by the Cedar interceptor, which sees the decoded request body.
//
// Adding a new authentication source (e.g. PASETO, AWS SigV4) means
// writing one more authn.AuthFunc and composing it ahead of the
// bearer step in composeAuthFunc.
package auth
