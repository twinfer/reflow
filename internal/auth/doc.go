// Package auth is reflow's authentication + authorization interceptor
// layer for gRPC servers (Admin, Delivery — and Ingress in the
// future). The model is:
//
//   - Extractor turns a server context (TLS peer info today; JWT
//     metadata later) into a Principal.
//   - The interceptor stamps Principal.Raw into a server-controlled
//     metadata header (x-reflow-principal). Any inbound copy of that
//     header is stripped first, so a client cannot forge identity.
//   - grpc-go's authz package then matches the stamped header against
//     a JSON policy. The embedded starter policy lives in
//     starter_policy.json; operators override via Config.PolicyFile,
//     which gets hot-reloaded by authz.FileWatcher.
//
// Adding a new authentication source (JWT, OIDC) means writing one
// more Extractor and chaining it ahead of SPIFFEExtractor. The
// Authorizer side stays unchanged because every principal funnels
// through the same metadata header.
package auth
