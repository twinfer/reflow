# 11. Delivery History

This document chronicles the historical progress and development phases of the Reflw engine.

---

## Single Node Foundation
- Per-partition Pebble DB + typed key codec (`internal/storage/keys`,
  `internal/storage`).
- Typed storage tables (`internal/storage/tables`) for invocations,
  journal, timers, dedup, meta.
- dragonboat single-node Raft group setup with `IOnDiskStateMachine`.
- Envelope/Command/Dedup/InvocationStatus proto schema mirrored from
  restate's wal-protocol v2 and journal v2 (`proto/enginev1`).
- Action collector + leader/follower state split.
- AnnounceLeader handshake with leader-epoch seeding across restarts.
- Self-propose + ingress-propose pathways (RaftProposer).
- Leader-only TimerService with restart rebuild.
- Snapshotter with close → swap → reopen lifecycle.
- Prometheus metrics + structured logging + `cmd/reflwd` single binary.
- Integration tests: replay across restart, dedup blocking, timer survives
  restart.

---

## In-process Go SDK + Invoker

The first-class developer experience: write a Go function, register it
with `reflwd`, have it become a durable goroutine.

- **`handler.Context`** Go API in `pkg/handler/` (the durable-execution
  handle exposed to handler authors) and the handler-side Connect
  HTTP/2 runtime in the same package (`pkg/handler/server.go`) that
  translates between the wire and the Context methods.
- **Per-partition Invoker** (`internal/engine/invoker/`) — session
  bookkeeping per active invocation, journal reader for replay,
  `InvokerEffect` proposals via `Proposer.ProposeSelf`. The actual
  handler runs in a separate process; the Invoker drives it over a
  Connect bidi stream (see §6.10).
- **Ingress** — Connect RPC in `internal/ingress/`, content-negotiating
  Connect / gRPC / gRPC-Web / HTTP-JSON on one HTTP/2 listener. Awakeable
  resolution rides the same surface. REST facade at `/v1/*` mounted on
  the same listener via `ingress.Config.ExtraRoutes` (see §6.1).
- **Journal entry types**: `JERun`, `JEGetState` / `JESetState` /
  `JEClearState` / `JEClearAllState`, `JEGetStateKeys` /
  `JEGetEagerStateKeys`, `JEAwakeable` / `JEAwakeableResult`, `JESignal`.
  Eager-state preload at session start (no per-read journaling — see §6.10.3).
- **Outbox** (`internal/engine/outbox.go`, `internal/storage/tables/outbox.go`)
  for parent-invocation notifications and cross-partition call results.
- **Exactly-once side-effect replay** via the journal — verified by
  property-style integration tests under
  `internal/engine/integration_invoker_wiring_test.go`.
- **Wire protocol** — `proto/protocolv1` is the engine ↔ handler
  contract; `internal/engine/handlerclient` is the engine-side client.

---

## Virtual Objects

- Per-key lease + FIFO queue (`KeyLeaseStatus`); FSM via
  `qmuntal/stateless` (`internal/engine/object_fsm.go`).
- Object K/V state in Pebble (`state/` namespace via
  `tables.StateTable`), with eager-state preload on session start
  (`StartMessage.state_map`, served locally by `wireContext.stateCache`;
  see §6.10.3).
- Single-writer gate: only `KeyLeaseStatus.CurrentInvocation` may run;
  new arrivals for an `ACTIVE` lease queue and stay `Scheduled`.
- **Idempotency keys** (`tables.IdempotencyTable`): first
  `(service, handler, object_key, idempotency_key)` wins; later
  submissions are dropped pre-status-write.
- **Retry policy** for `Run` blocks: per-entry backoff schedule
  persisted in the journal; the FSM re-arms timers on each failure
  until exhaustion.
- **Attach RPCs**: ingress `Attach` / `GetOutput` resolve an existing
  invocation's terminal output without re-driving it.
- Integration coverage: `integration_virtual_object_test.go`,
  `integration_combinators_test.go`, plus the rapid PBT tests.

---

## Combinator Futures

`Promise.all` / `Promise.race` over awakeable / call / signal
completions, persisted as a single journal entry whose pending-set
shrinks as completions land. Lets handlers fan out durable work and
join on the first-N / all-N without bespoke bookkeeping in user code.

---

## Multi-Node Replication

Target: a 3–10 node cluster. No external coordination service introduced
(see §6.2).

**Embedded metadata Raft + static bootstrap.** Shard 0 hosts node
membership, partition table, assignment epoch; founder/joiner bootstrap via
`--bootstrap-cluster` / `--join`.

**Dynamic membership + failure detection + DR snapshots + mTLS admin.**
Dragonboat gossip (memberlist/SWIM) drives K-of-N liveness; the metadata
leader's `metadataRebalancer.failureLoop` turns missed gossip observations
into `EvictNode` proposals to shard 0. The cluster admin CLI lives in
`reflwd cluster` (`add-node`, `remove-node`, `nodes list`,
`partitions list`, `snapshot {create,list,delete}`).
`SnapshotRepository` filesystem driver wired. Admin Connect surface is
two services on one mTLS-protected listener: `reflw.clusterctl.v1.ClusterCtl`
(fleet ops) and `reflw.config.v1.Config` (app config — deployments,
event sources, webhooks, secrets). The split mirrors Restate's
`cluster-ctrl` vs `admin` naming, with `admin` flipped to `config` to
avoid the overloaded word.

**Storage format version marker.** Per-Pebble-DB `uint32` marker
(`internal/storage/format.go`). Refuses to open a DB written by a binary
with a different `StorageFormatVersion`. Replaced the earlier
"command-stream VersionBarrier" sketch.

**Single CA + SPIFFE URI SAN identity.** Collapsed operator-CA + node-CA
into one cluster CA; role moved into the SPIFFE URI SAN
(`spiffe://<td>/<kind>/<name>`). TLS verifier checks chain + URI prefix.

**Path-glob authz at the HTTP/Connect layer.** Auth runs as
`internal/auth.HTTPMiddleware` chained below Connect's protocol dispatch
(works uniformly for unary and streaming RPCs across Connect / gRPC /
gRPC-Web / HTTP-JSON, unlike the older `connect.UnaryInterceptorFunc`
which silently skipped streaming). Two authenticators chain in
`composeAuthFunc`: SPIFFE URI SAN extraction from the verified mTLS leaf
and Bearer-JWT verification against one or more OIDC issuers; mTLS wins
when both are presented. The embedded starter policy
(`internal/auth/starter_policy.json`, hot-reloaded from
`cfg.Auth.PolicyFile`) is path-glob: `clusterctl` and `config` rules
gate `/reflw.clusterctl.v1.ClusterCtl/*` and
`/reflw.config.v1.Config/*` to `operator/*`; `delivery` gates
`/reflw.delivery.v1.Delivery/*` to `node/*`; ingress paths default to
anonymous. See §6.13.

- **Embedded metadata Raft group** (`shardID = 0`) hosted by the same
  `NodeHost` as partition shards. Holds node list, partition table,
  partition assignment epoch.
- **Static peer bootstrap.** `--bootstrap-cluster` for the founder,
  `--join=<addr>` for joiners. No discovery service required.
- **`reflwd cluster` admin subcommands** for `add-node`, `remove-node`,
  `partitions list`, `partition move`.
- **Multi-node partition shards** with dragonboat membership operations
  (`RequestAddNonVoting` → catch-up → `RequestAddReplica` →
  `RequestRemoveReplica`).
- **Partition rebalancing** orchestrated by the metadata leader: failure
  detection via missed-heartbeat eviction proposals, reassignment proposed
  to shard `0`, per-partition membership changes driven by the metadata
  leader.
- **Failure detection via dragonboat gossip.** Enable
  `NodeHostConfig.AddressByNodeHostID = true` + `GossipConfig{Seed: ...}`
  so every NodeHost runs memberlist/SWIM probes. Observers turn `K`
  consecutive failed probes into a `RemoveNode` proposal to shard `0`;
  eviction itself remains a Raft decision. No additional dependency
  (memberlist is already vendored inside `lni/dragonboat/v4`).
- **Endpoint resolution + leader hint cache via gossip.** Every node
  publishes its reflw gRPC endpoint via the gossip `Meta` blob and reads
  `NodeHostRegistry.GetShardInfo` for `ShardView{LeaderID, Replicas,
  Term}`. Cross-partition delivery dials by `NodeHostID` without re-reading
  shard 0 on the hot path; `NOT_LEADER` triggers a fallback re-read.
- **`SnapshotRepository` abstraction.** New replicas joining a partition
  try the repository before falling back to dragonboat snapshot transfer.
  See §6.12.

**Exit criteria:** A 3-node cluster sustains invocation progress through
single-node failures with no data loss, recovers when the failed node
returns, and tolerates a planned `remove-node` of any single member.
Chaos tests cover network partitions, leader oscillation, and concurrent
add/remove operations.

---

## REST ingress + caller metadata (done)

- **REST facade at `/v1/*`** (`internal/ingress/http/`). Mounted on the
  Connect ingress listener via `ingress.Config.ExtraRoutes` — same port,
  same TLS, same auth middleware. Verbs: `call` (submit + long-poll),
  `send` (submit-only), `attach`, `output`, `cancel`, `awakeables/.../resolve`,
  `promises/.../resolve`, `state`. Each handler builds a typed
  `*connect.Request[T]` and delegates to the in-process `*ingress.Server`
  — no business-logic duplication. Long-poll capped at 30s (vs Connect's
  60s) to stay under common LB idle timeouts. `ensureDeadline` middleware
  instains a default request timeout because `*ingress.Server` calls
  dragonboat `SyncRead` under the hood (rejects deadlineless contexts).
- **Caller metadata end-to-end.** `SubmitInvocationRequest.metadata`
  (`map<string,string>`) flows through `InvokeCommand.metadata` →
  `Scheduled.metadata` (slot-0 transient) → `JEInput.metadata` (durable
  in the journal) → `InputCommandMessage.headers` (sorted-by-key for
  deterministic replay bytes) → `wireContext.metadata` →
  `handler.Context.Metadata()`. REST surface convention: HTTP headers
  prefixed `Reflw-Meta-*` are lifted (lowercased + stripped) into the
  proto field, so operator HMAC-verifier middleware can stamp facts
  the durable handler reads without re-verifying.
- **`pkg/hostmux` operator primitive.** Trust-aware host dispatcher with
  atomic-swappable table for runtime reconfig. Lives in `pkg/` because
  it is operator infrastructure, not engine machinery — Reflw itself
  does not import it. Enables multi-tenant SaaS via per-host
  routing without Reflw owning tenant state.

---

## Cluster-managed app config + webhook KMS (done)

Application config that used to require a koanf file + cluster
restart is now hot-reconfigurable Raft state on shard 0. Webhook
secrets — the most sensitive of the bunch — are encrypted at rest
via Tink, with the ciphertext stored in `gocloud.dev/blob` and the
KEK delivered through Tink's `KMSClient` registry. See §6.14.

- **Shard 0 typed tables.** `DeploymentTable`, `EventSourceTable`,
  `WebhookSourceTable`. Mutations go through `Command_Upsert*` /
  `Command_Delete*` with `Envelope.precondition.if_table_revision_eq`
  for CAS; the FSM signals CAS failure via `Result.Value =
  ResultValueFailedPrecondition` (returning an error halts the shard,
  per `internal/engine/CLAUDE.md`). Per-table singleton `__rev` row
  encoded as `TableRevision{revision, updated_at_ms}`.
- **Reconciler pattern.** Per-node subsystems subscribe to a
  `cluster.TableNotifier` (buffered-1 non-blocking send from the
  FSM apply goroutine, post-commit) and pull a fresh snapshot on
  wake; 5s ticker backstop. Event-source `Manager` reconciles
  dispatcher goroutines (per-source `sync.WaitGroup` for ≤5s graceful
  drain on remove); webhook `Manager` reconciles a path→source map
  via `atomic.Pointer` swap.
- **kubectl-shaped CLI.** `reflwd config {eventsources,webhooks}
  {list,delete}` plus top-level `config apply -f <file>` and
  `config export [--kind=…]`. Multi-doc YAML with
  `kind`/`metadata.name`/`spec`. `sigs.k8s.io/yaml` for
  YAML→JSON→protojson so proto field additions auto-flow.
- **Bootstrap-koanf seed path (event sources only).**
  `cfg.EventSources.Sources` proposes with `if_table_revision_eq=0`
  — only succeeds against an empty table. Once operator-managed,
  the file is ignored. Webhooks have no koanf-seed path; secrets
  would be required first and a plaintext-secret seed file is not
  a default we want to ship.
- **Webhook secrets via shard-0 SecretTable indirection.**
  `WebhookSourceRecord.secret_name` references a row in shard 0's
  `SecretTable`. The webhook record carries no ciphertext or KMS
  material; the SecretTable is the single source of truth for all
  named secrets (used by webhooks today; event-source vendor
  credentials, outbound HMAC signing keys, OIDC client secrets
  tomorrow — define-once, reference-many). `SecretRecord.source`
  is a oneof with `remote_encrypted{blob_uri, kek_uri}` as its only
  variant today, leaving room for future shapes (inline-hybrid,
  vault-kv-path) without disturbing existing rows. Secrets never
  traverse Raft. Per-node `internal/secretstore` Reconcilers
  reconcile the SecretTable on the same notifier/ticker pattern as
  every other shard-0 table, fetch ciphertext via
  `gocloud.dev/blob.ReadAll`, dispatch the KEK URI through Tink's
  process-global `KMSClient` registry, decrypt with
  `AAD = []byte(secret.name)` (binds ciphertext to row identity, so
  renaming a secret is a re-encrypt operation by design), and
  atomically swap a fresh name→bytes map. Webhook Manager and
  future consumers call `Resolver.Lookup(name)` on each reconcile
  pass — single `atomic.Pointer.Load`, no per-call KMS trip.
  Resolve failure preserves the previously-resolved bytes so a
  transient blob/KMS hiccup doesn't knock dependent consumers
  offline.

---

## Unified secret management (done)

One resolve path, one provider interface, one on-disk shape — the
three threads landed together so the cluster has a uniform
"ciphertext + operator-managed KEK" posture regardless of which
consumer (webhook today; event-source creds, outbound HMAC keys,
OIDC client secrets tomorrow) references the secret.

- **Shard-0 `SecretTable` indirection.** Secrets are first-class
  records in shard 0 (CAS-checked Upsert/Delete via
  `Envelope.precondition.if_table_revision_eq`, per-table notifier,
  TableRevision singleton — same shape as `EventSourceTable` and
  `WebhookSourceTable`). `WebhookSourceRecord.secret_name`
  references a `SecretRecord.name`; the webhook row carries no
  ciphertext or KMS material. An earlier inline `SecretRef` shape
  (`env_var_name`, `file_path`, `remote_encrypted`) was rejected
  before production: env vars and file paths leaked per-node
  deployment state that varied across the fleet; the indirection
  table forces one uniform posture.
- **`SecretRecord.source` oneof — `remote_encrypted` only today.**
  `RemoteEncryptedSecret{blob_uri, kek_uri}` is the single variant;
  the oneof shape leaves room for future shapes (inline-hybrid,
  vault-kv-path) without disturbing existing rows.
- **`pkg/kms/{awskms,gcpkms,hcvault,blob}/` — always-linked,
  config-gated.** Matches the event-source backends' pattern in
  `internal/ingress/eventsource/factory_*.go`. BlobKMS / AWS / GCP
  self-register at `init()` and read the standard credential chain
  (env, instance metadata, workload identity); Vault registers via
  `cfg.KMS.Vault{Address, TokenFile}` because it needs explicit
  address + token. Shipped binary grows ~20MB (mostly AWS SDK v2),
  same trade-off event-source backends made.
- **BlobKMS Tink-keyset shape.** On-disk: `boot_key(32B) ||
  serialized_encrypted_keyset` — the boot key encrypts a Tink AEAD
  keyset that is the operational KEK. Gives multi-key rotation
  (add new key to the keyset, mark primary; old ciphertexts still
  decrypt via non-primary entries), crypto-agility (swap AEAD type
  via key template change, no proto change), and primitive-swap
  (`aead.New(handle)` today; `hybrid.NewHybridEncrypt(handle)` or
  `keyderivation.New(handle)` tomorrow) without touching storage.
- **Per-node `internal/secretstore` Resolver.** Holds
  `atomic.Pointer[map[string][]byte]` (name → resolved plaintext
  bytes), swapped each reconcile pass. Consumers call
  `Lookup(name)` on the hot path with no per-call work. AAD
  binds ciphertext to row identity (`[]byte(secret.name)`), not to
  the consumer; multiple consumers may share one secret name.
  Resolve failure preserves the previously-resolved bytes.
  Hand-instrumented Prometheus
  (`reflw_secretstore_decrypt_total{kek_scheme}` /
  `_errors_total{name,kek_scheme,stage}` / `_seconds`) because
  Tink's `monitoring.RegisterMonitoringClient` lives in
  `tink-go/v2/internal/internalregistry` (blocked from external
  import in v2.6).
- **`reflwd config {init-kek, create-secret, delete-secret,
  list-secrets, decrypt-secret, upsert-webhook}`.** `init-kek`
  generates the keyset + boot key at a `gocloud.dev/blob` URI.
  `create-secret` reads plaintext from stdin / `--input`, encrypts
  with the named KEK, writes ciphertext to `--blob-uri`, and proposes
  `Config.UpsertSecret` so the row lands in shard 0 in one command.
  `upsert-webhook` references an existing secret by `--secret=NAME`.
  `decrypt-secret` is operator self-verification.

Sketched, not scheduled — journal/state encryption-at-rest via
`tink-go/v2/keyderivation`. Per-`object_key` derived AEADs with an
LRU cache for the FSM apply hot path, riding on the same keyset
shape. Migration story is the harder half (read-handles-both-formats
phase + background sweep) and warrants its own delivery cycle.

---

## Two-layer routing (LP → shard) (done)

Decouples the routing modulus from the replication unit so hot-spot
relief can happen online without ever changing the number of partition
shards. Five PRs landed in lockstep — each kept routing correct end-to-
so the rollout could be paused at any commit. See §6.2.1 for the
full design.

- **PR 1 — table-driven Partitioner + LPOwnersTable (commit `71d7797`).**
  Replaced the implicit `hash % N` partition map with an explicit
  shard-0 `LPOwnersTable` (4096 rows, one per LP) read through a
  per-Host `atomic.Pointer` snapshot. Identity-seeded so the rollout
  was wire-compatible at this checkpoint.
- **PR 2 — consistent-hash planner (commit `6f83ad4`).** Swapped the
  identity seed for `buraksezer/consistent` + `xxhash` in
  `internal/engine/routing/planner.go`. Deterministic across replicas
  (xxhash is platform-neutral; constructor sorts shard ids). Exposes
  `Diff(current, desired) []LPMove` as the seam PR 3 + 5 consume.
- **PR 3 — cross-shard LP transfer protocol (commit `1f81c1d`).**
  Six-phase saga (INIT → SHIPPING → STAGED → FLIPPED → CLEANED, abort
  branch → ABORTING → ABORTED) coordinated by the `lpMover` goroutine
  on the metadata leader. Source-side `LPTransferSourceService` scans
  LP-prefixed namespaces + the `timer_lp/` secondary index, ships ~256
  KiB chunks via `CrossShardSender` to the destination's apply path.
  Apply-path freeze gate (`partition.go:checkLPFreeze`) gates every
  LP-touching arm; monotonic phase check on shard 0 absorbs duplicate
  acks; `FLIPPED → ABORTING` is rejected because the `LPOwnersTable`
  flip is the point of no return.
- **PR 4 — LP-prefix dedup/arbitrary (commit `164543d`).**
  `dedup/arbitrary/*` became `dedup/arbitrary/<lp:4>/<producer>/<seq>`
  and rides the LP-transfer scan as
  `TRANSFER_NS_DEDUP_ARBITRARY=15`. Closes the residual hazard from
  PR 3: external producer retries (or slow third-shard outbox retries)
  after an LP flip now find their dedup row on the new owner. The
  LP is derived from the command kind via `partition.lpFromCommand`;
  LP-agnostic commands (today only `OutboxAck`) key under
  `keys.LPNoLP = 0xFFFF_FFFF`, a sentinel that's never a real LP
  (real LPs are < 4096) and therefore never range-deleted by
  `FinishLPTransfer`.
- **PR 5.0 — autonomous LP rebalancer (commit `6d22e87`).**
  Leader-only goroutine in `internal/engine/rebalance/` plugged into
  `MetadataRunner.onBecomeLeader` alongside the membership-handling
  `metadataRebalancer` and the saga-executing `lpMover`. Three modes:
  `off` (default), `advisory` (observes + emits metrics + logs
  `would_transfer`, never proposes), `auto` (proposes
  `Command_InitiateLPTransfer` — the same path
  `reflwd cluster transfer-lp` takes — so autonomous transfers
  appear in `ListLPTransfers` with no extra plumbing). Triggers in
  PR 5.0: membership change + operator-requested drain. Drained
  shards live in shard 0's new `RebalanceDrainTable`
  (CAS-revisioned, cluster-managed, runtime-mutable via
  `ClusterCtl/RebalanceDrain`); the advisor subtracts them from the
  planner's input set. Skew metric is mis-placement fraction
  (`len(routing.Diff(current, desired)) / total_LPs`); hysteresis
  engage 15% / disengage 8%; defaults are conservative (1 concurrent
  transfer, 60s cooldown). Eight new metrics under
  `reflw_rebalance_*`. CLI: `reflwd cluster rebalance-advise`
  (read-only) + `reflwd cluster rebalance-drain --shard=N [--stop]`.

**Deferred to PR 5.1+:** capacity circuit breakers (Pebble L0,
write-amp — should *gate destinations*, not *trigger* moves),
load-based triggers (QPS, p99 — bursty workloads make rolling-window
load a noisy signal that fires mid-burst), soft drain with grace
seconds, `RebalancePause/Resume` RPCs (`Mode=off` already covers
operator pause). (SST shipping via Pebble Ingest landed —
`internal/engine/lp_transfer_sst.go`, `feat(engine/lp-transfer):
wire SST upload RPC + dest Ingest end-to-end`.)

---

## Production Hardening (in progress)

- **Cloud-backed `SnapshotRepository` drivers (done).** Single
  `BlobRepository` over `gocloud.dev/blob` covers S3, GCS, Azure Blob,
  filesystem, and in-memory. `.meta.json` sidecar per archive. Count,
  age, and GFS tiered retention via a per-shard reaper goroutine.
  Admin `DeleteSnapshot` RPC + `reflwd cluster snapshot delete` CLI.
  Server-side encryption flows through gocloud URL parameters. Restore
  RPC and DR/migration runbooks remain future work. See §6.12.
- Pebble snapshot tuning (compaction, log retention, checkpoint cadence).
- Load testing + chaos testing harness (jepsen-style at small scale).
- Admin API surface: partition status, invocation inspection, replay
  debugger, `purge_journal` / `kill_invocation` operations.
- Operational docs: deployment recipes, backup/restore, upgrade
  procedure (using the per-DB storage format marker from §6.2).
- **Non-Go SDKs (community-driven).** TypeScript / Python / Java / Kotlin
  / Rust SDKs talk to reflw via the same `protocolv1` HTTP/2 wire as the
  Go SDK (§6.10). These ride on whatever effort the community
  contributes; reflw itself guarantees the wire-protocol surface, not
  the SDK quality across languages.

---

## Strip to core: mesh-only auth + Cedar, battery removal, in-cluster tenancy (done)

A deliberate narrowing of the surface area. The auth model collapsed to
mesh-only mTLS + Cedar, several "batteries" were removed wholesale (no
proto service, no record type, no package left behind), the routing
modulus widened, and multi-tenancy was re-grounded on LP-banding instead
of the old logical-tenant model. Entries above that record SPIFFE-URI
identity, the OIDC/Bearer authenticator, path-glob authz, event sources,
and webhooks are accurate history of what those phases delivered; this
entry records their removal.

- **Auth → mesh-only mTLS leaf-CN + Cedar.** The `Principal` is now the
  verified mTLS leaf's Common Name, a bare `<kind>/<name>` with
  `kind ∈ {node, operator, tenant}` (`internal/auth/mesh_authfunc.go` →
  `creds.LeafPrincipal`). Removed: SPIFFE URI SAN identity (no
  trust-domain segment, no `--trust-domain` config), the Bearer-JWT /
  OIDC-issuer authenticator (`jwt_authfunc.go` deleted), and the
  path-glob `starter_policy.json`. A request with no client cert is
  anonymous; the per-procedure decision is now Cedar's.
- **Authz → Cedar.** `internal/authz` evaluates `cedar-policy/cedar-go`
  policies, replacing the old path-glob / proto-annotation authz. The
  foundational policy gates `/reflw.clusterctl.v1.ClusterCtl/*` and
  `/reflw.config.v1.Config/*` to `operator/*` (one carve-out:
  `SelfJoin` for `node/*`), and enforces full tenant isolation on the
  ingress data plane.
- **Batteries removed.** Event sources, webhooks, OIDC ingress, quota,
  encstore, audit, and the old logical-tenant model are gone — no proto
  service, no record type, no package. The offline CA path went with
  them: the `reflwd pki` subcommand and `internal/pki` package were
  deleted (in-cluster issuance via the `MeshSign` / `Config` services
  is the only path now). Live proto services are exactly: `ClusterCtl`,
  `Config`, `Delivery`, `DiscoveryService`, `HandlerService`, `Ingress`,
  `MeshSign`.
- **Routing modulus widened.** `keys.LPCount` went 4096 → 16384, giving
  finer LP granularity for hot-spot relief and tenant banding.
- **In-cluster multi-tenancy via tenant LP-banding.** A tenant folds
  into `partition_key`'s LP band rather than being a key or id field:
  the LP's high `TenantBandBits` (8) select the band, the low
  `IntraLPBits` (6) carry the intra-tenant hash → 256 bands × 64 LPs
  (`keys.BandLP` / `keys.TenantFromPartitionKey`). A per-tenant range is
  just that band's contiguous LP prefix, so existing LP machinery
  (transfer, dedup, range-delete) isolates tenants for free. Tenant
  identity is a verified `tenant/<n>` mTLS leaf, issued via
  `reflwd config issue-tenant` (CSR with `CN=tenant/<id>` →
  `Config.IssueTenant`); Cedar enforces tenant isolation on the ingress
  data plane.
