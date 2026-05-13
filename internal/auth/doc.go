// Package auth is reflow's authentication and authorization layer for
// gRPC servers. Both Admin and Delivery share this stack.
//
// The model is Temporal-shaped:
//
//   - ClaimMapper turns raw transport material (mTLS state today, JWT
//     tomorrow) into trusted Claims.
//   - Authorizer takes Claims + CallTarget and returns Allow/Deny.
//   - The unary and stream interceptors chain those two calls per RPC.
//
// The default ClaimMapper (CertClaimMapper) reads the SPIFFE URI SAN
// from a verified mTLS leaf and parses the two-segment path into
// {Kind, Subject}. The default Authorizer (ProtoPolicyAuthorizer)
// consults a method->role map built from proto annotations
// (proto/optionsv1/options.proto). Adding JWT/OIDC support later means
// implementing one more ClaimMapper and chaining it ahead of the cert
// mapper; no changes to the Authorizer or interceptors are required.
//
// Identity well-formedness is enforced at the TLS layer
// (pkg/reflow/tls.go); role enforcement lives here.
package auth
