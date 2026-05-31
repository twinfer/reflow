# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Reflow is a **single-binary durable-execution engine for Go**, inspired by Restate (Rust). One engine binary, one data directory; handlers run as separate Go processes the engine reaches over HTTP/2. Built on `dragonboat` (multi-group Raft) + `cockroachdb/pebble` (embedded K/V). The design doc lives at `durable-execution-go-sad.md` — read it before non-trivial work on cluster, FSM, or storage. Restate wire-protocol concepts are mirrored where applicable; comments cite the source as `crates/.../file.rs:line` (Restate v1.6.2).


## Common commands

```bash
make build        # go build ./...
make test         # gotestsum -race ./... — one line per package on PASS,
                  # focused failure output on FAIL, plus a `DONE N tests`
                  # summary line. Auto-installs gotestsum to $GOBIN on
                  # first run. Unit + integration, no loadtest.
make test-verbose # go test -race -v ./... — escape hatch for full logs.
make vet
make tidy
make proto        # buf generate (regenerates proto/*/*.pb.go)
```

Run a single test or scope to one package:

```bash
make test-pkg PKG=./internal/engine/... RUN=TestSingleNodeReplayAcrossRestart
# or, without make:
go test -race -run TestSingleNodeReplayAcrossRestart ./internal/engine/...
```

Two parallel test tiers run on top of the unit + engine-integration baseline:

```bash
# In-proc perf baseline (loadtest tag). One test: TestLoad_SteadyState
# in internal/loadgen/. Use this for percentile-regression checks
# against the reference numbers in this file.
go test -tags=loadtest -timeout=10m -run=TestLoad_SteadyState -v ./internal/loadgen/...

# Containerized chaos suite (e2e tag) in internal/e2e/chaos/.
# Requires Docker. Lifecycle chaos via Docker ContainerKill / Start;
# network chaos via per-source Toxiproxy sidecars. Replaces the
# legacy in-proc internal/chaos/ + cmd/loadnode/ subprocess harness.
make test-e2e
# or scoped:
go test -tags=e2e -timeout=10m -run=TestChaos_LeaderSIGKILL -v ./internal/e2e/chaos/...
```

Scope: the symbol/reference tools accept `scope` (`workspace`, `workspace+direct`
(default), or `all`). The default catches calls inside your module and its
direct `require`s. Pass `scope: "all"` to reach into indirect deps and stdlib
when the server has been started with `-deps all` (or `dep_index.stdlib: true`).


`cmd/loadhandler` is a minimal HTTP/2 echo handler used as a sidecar by the containerized chaos suite — registered against the cluster via `Config.RegisterDeployment` over the admin Connect RPC. **Production deployments use `cmd/reflowd`, not `cmd/loadhandler`.**

## Architecture map

The dependency direction is `cmd → pkg → internal → proto`. Internal packages must not depend on `pkg/*`, with one deliberate exception: `pkg/handler/wire` holds the shared engine↔handler protocol vocabulary (Codec, frame helpers, Type* constants, Route) and is imported by both sides — keeping it in pkg/ makes it part of the public SDK contract; bending the rule for this one focused package is preferable to duplicating the wire format.

- **`cmd/reflowd`** — the production binary with three subcommand groups:
  - `reflowd run` — start the engine. Loads layered koanf config (defaults → optional file from `$REFLOW_CONFIG` → `REFLOW_*` env vars; later overrides earlier), then calls `reflow.Run`. Handlers run in separate Go processes registered with the engine as HTTP/2 deployments; `examples/embedded/` shows a single-`main` dev setup that runs both.
  - `reflowd cluster {add-node|remove-node|nodes list|partitions list|snapshot {create|list|delete}|transfer-lp|list-lp-transfers}` — mTLS-authenticated RPCs against the admin Connect port, dispatched to the `reflow.clusterctl.v1.ClusterCtl` service (fleet ops: membership, partitions, snapshots, LP transfers).
  - `reflowd config {register-deployment|list-deployments|describe-deployment|delete-deployment|init-kek|create-secret|delete-secret|list-secrets|decrypt-secret|ca {…}|create-join-token|list-join-tokens|delete-join-token|issue-operator|issue-tenant|upsert-cluster-authz-policy|get-cluster-authz-policy}` — same admin listener, dispatched to the `reflow.config.v1.Config` service (app config: deployments, secrets, CA roots, join tokens, cluster authz policy). `issue-operator` / `issue-tenant` mint `operator/<name>` / `tenant/<n>` client leaves against the active cluster CA (the offline `reflowd pki` flow was removed; CA bootstrap is `config ca`). DeploymentTable carries a CAS revision (`RevisionTableDeployment`); `delete-deployment` requires `--force` because in-flight invocations resolve their pinned deployment per stream-open and a delete can break them.
  - A single mTLS listener hosts both services. `--admin` may point at any cluster node — mutating commands follow `LeaderHint` connect.Error details (attached by each server's `requireLeader`) to redirect to the metadata leader. The redirect helper lives at `pkg/reflowclient.CallWithLeaderRedirect`. Naming mirrors Restate (`cluster-ctrl` = cluster admin; `admin` is the developer/app-config surface), with `admin` flipped to `config` to avoid the overloaded word.
- **`cmd/loadhandler`** — test-only HTTP/2 echo handler sidecar used by `internal/e2e/chaos/` to give the cluster a deployment that survives `ContainerKill` of any reflowd node.

- **`pkg/reflow`** — the **stable public API**. `reflow.Run(ctx, cfg) → *Host`. `Config` uses koanf tags (snake_case so the env provider maps `REFLOW_INGRESS_GRPC_ADDR → ingress.grpc_addr`). `pkg/reflow/config` builds `Config` from any `koanf.Provider` — secret backends plug in as additional providers, no inline templating.
- **`pkg/handler`** — the handler-facing surface: `Handler`, `Registry`, `Context`, `Future`, `Target`, `Failure`. `Context` is the durable-execution handle: every method (`Sleep`, `Run`, `Call`, `OneWayCall`, awakeables, state, …) is journaled. `Context.Metadata() map[string]string` returns caller-stamped metadata captured at submit time (lifted from inbound `Reflow-Meta-*` headers). Determinism rule (non-negotiable): handlers must reach non-determinism (time, RNG, I/O) only through `Context`, never via `time.Now`/`rand`/`net/http` directly. `*Failure` returned from a handler is terminal and persisted; any other error is transient and retried.

- **`internal/engine`** — the FSM, Raft proposer, leadership, action dispatch, invoker, snapshotter. Two shard kinds on one `NodeHost`: shard 0 (metadata, `engine/cluster`) holds `PartitionTable` + `NodeRegistry`; shards 1..N (partition, `partition.go`) hold invocation state. **Before editing inside `internal/engine`, read `internal/engine/CLAUDE.md` — it covers the goroutine model, the Action pattern, leadership two-phase, the leader-service lifecycle, and the don'ts.**
- **`internal/engine/invoker`** — per-partition in-process session runner. `Start`/`Stop` are leader-gain/loss callbacks; per-action methods run on the apply-path dispatch loop.
- **`internal/engine/delivery`** — cross-shard outbox dispatcher (`Client`/`Server`) wired into `PartitionRunner` as `CrossShardSender`. Single-node deployments leave it `nil`.
- **`internal/engine/snapshot`** — DR snapshot producer/repository/reaper backed by `gocloud.dev/blob` (file/s3/gs/azblob/mem URLs).
- **`internal/storage`** — `Store` interface (Pebble + in-memory); `keys` defines the byte-level key layout (no partition_id prefix — each partition has its own DB); `tables` is the typed view over keys.
- **`internal/ingress`** — Connect RPC entrypoints (`SubmitInvocation`, `AwaitInvocation`, `AttachInvocation`, `GetInvocationOutput`, `CancelInvocation`, `ResolveAwakeable`, `ResolveWorkflowPromise`, `GetObjectState`, admin reads) over HTTP/2 with content-negotiated Connect / gRPC / gRPC-Web / HTTP-JSON on one listener. Routes via `Host.Partitioner` (hash of `service` + `object_key`). `ingress.Config.ExtraRoutes func(*Server) []connectserver.Route` is the seam for mounting additional HTTP handlers on the same listener without a second port/cert. Inbound HTTP headers prefixed `Reflow-Meta-` are lifted (lowercased + stripped) into `SubmitInvocationRequest.metadata` so operator middleware can pass facts to the durable handler via `ctx.Metadata()`.
- **`internal/secretstore`** — per-node Resolver for shard 0's `SecretTable`. Holds an `atomic.Pointer[map[string][]byte]` (name → resolved plaintext bytes) that the reconciler swaps each pass; consumers call `Lookup(name)` (and `LookupForCASigning` for the cluster CA signing key — see `internal/certmgr`) on the hot path with no per-call work. Each reconcile pass iterates `SecretRecord` rows: fetch ciphertext via `gocloud.dev/blob`, dispatch the KEK URI through Tink's process-global `KMSClient` registry, decrypt with `AAD = []byte(secret.name)` so a leaked ciphertext for row A can't be replayed onto row B. On resolve error the previously-resolved bytes are preserved so a transient blob/KMS hiccup doesn't knock dependent consumers offline. KMS providers ship always-linked at `pkg/kms/{blob,awskms,gcpkms,hcvault}/`: BlobKMS, AWS, GCP self-register at package `init()` under `sync.Once`; Vault registers via `hcvaultkms.Register` when `cfg.KMS.Vault.TokenFile` is set. BlobKMS' on-disk shape is `boot_key(32B) || serialized_encrypted_keyset` — the boot key encrypts a Tink AEAD keyset, enabling multi-key rotation without a wire change.
- **`internal/engine/handlerclient`** — engine-side wire client for handler deployments. Single transport is Connect RPC (`connectclient/`) over HTTP/2; the handler-side server lives at `pkg/handler` (see `pkg/handler/server.go`).
- **`internal/auth`** — inbound authentication, HTTP-level middleware built on `connectrpc.com/authn` so failures emit proper Connect-coded errors (`CodeUnauthenticated` / `CodePermissionDenied`) across Connect, gRPC, gRPC-Web, and HTTP-JSON, and works uniformly for unary and streaming RPCs. Authentication is mesh-only: `composeAuthFunc` resolves the `Principal` from the SPIFFE URI SAN on the verified mTLS leaf — `spiffe://<trust-domain>/node/<id>` vs `/operator/<name>` vs `/tenant/<n>` — the last is the LP-band tenancy identity, issued by `config issue-tenant` (`mesh_authfunc.go`). A request with no client cert is anonymous; the Cedar authz interceptor (`internal/authz`) decides whether anonymous is acceptable per procedure. The stamp handler (`policy_handler.go`) strips any forged `X-Reflow-Principal` header and stamps the server-verified value for the interceptor. Per-listener transport security is driven by `cfg.Delivery.Creds` / `cfg.Admin.Creds` via `pkg/reflow/creds`; multi-node insecure delivery emits a startup warning in `pkg/reflow/run.go`. Authorization is Cedar (`internal/authz`): the foundational policy restricts `/reflow.clusterctl.v1.ClusterCtl/*` and `/reflow.config.v1.Config/*` to `operator/*` with one carve-out — `SelfJoin` is allowed for `node/*` so a freshly-provisioned joiner can register itself (the handler then enforces SPIFFE-equals-`req.node_id`). The ingress data plane is **tenant-isolated**: the interceptor builds a tenant-scoped `Invocation` resource per ingress RPC, and the foundational policy permits a `tenant/<n>` principal only on its own band (`resource.tenant_id == principal.tenant_id`), anonymous / non-tenant callers only on band 0 (`internal/authz/policies.go`, `interceptor.go`; the by-id resource resolver is `pkg/reflow.ingressResourceTenant`).
- **`internal/observability`** — `*Metrics` is a single Prometheus collector struct passed down into the partition apply path + timer service. The engine never constructs its own registry; wiring lives in `pkg/reflow`.

- **`proto/`** — `buf generate` output lives next to the `.proto`. Don't hand-edit the `*.pb.go`. `enginev1` is the on-disk + Raft wire format; pre-prod so retired fields are deleted (no `reserved` noise), but tag grouping by feature area is documented inline in `proto/enginev1/engine.proto` so new variants land in the right block.

## Tests

- **Unit tests** sit next to the code under test (`xxx_test.go`, package `xxx`).
- **Engine integration tests** live in `internal/engine/integration_*_test.go` under `package engine_test`. They use the `internal/loadgen` cluster bootstrap (so `loadgen` is imported from non-loadtest builds — keep its non-`//go:build loadtest` files free of test-only dependencies the production import path can't satisfy).
- **`internal/engine/pbt_test.go`** is property-based with `pgregory.net/rapid`.
- **Perf baseline** is `TestLoad_SteadyState` in `internal/loadgen/`, gated by `//go:build loadtest`. In-proc cluster, 50qps for 20s; reference numbers in §Performance baseline below.
- **Chaos tests** live in `internal/e2e/chaos/` behind `//go:build e2e`. The harness brings up a real `reflow/reflowd:e2e` image cluster via testcontainers, registers a `cmd/loadhandler` sidecar via the admin RPC, and injects faults two ways: lifecycle (Docker `ContainerKill` / `ContainerStart` — `ContainerNode.Kill` / `.Restart`) for kill+restart scenarios, and network (per-source Toxiproxy sidecars exposing per-pair `Cut`/`Heal` — `ContainerCluster.Tx`) for partition scenarios. `TestChaos_LeaderSIGKILL` is the canonical real-SIGKILL Pebble torn-WAL recovery test that the in-proc harness structurally cannot exercise.

## Performance baseline

`TestLoad_SteadyState` (3-node in-proc cluster, 50 qps target, 16 concurrency, 20s workload) is the canonical "did we regress" smoke. The summary.md content is dumped into the test log; the most recent reference run lives in this section so future runs have something to diff against.

Run:

```bash
go test -tags=loadtest -timeout=10m -count=1 -run=TestLoad_SteadyState -v ./internal/loadgen/...
```

Reference (commit `d849283`, 2026-05-16, Darwin/arm64 laptop):

```
- Issued: 440        # rate-limit + concurrency-cap interplay; not a deterministic target
- Completed: 440
- Failed: 1          # within the 1% cancelled-propose tolerance
- InFlightAtEnd: 0
- Duration: 20s

Latency (end-to-end, µs)
- p50:  108_735
- p90:  200_831
- p99:  308_223
- p999: 403_199
- max:  403_199

Pebble
- peak L0 files (any shard, any node): 1
- mean write-amp across samples:       1.025

Invariants: all passed.
```

Numbers vary by machine (IO/CPU). Order-of-magnitude shifts (10× latency, peak L0 in the dozens, write-amp > 5) are the actual regression signal; a 20% drift on percentiles is noise. Bump the reference block on any major refactor that touches the apply or invoker paths.

### Containerized perf baseline

`TestE2EPerf_SteadyState` in `internal/e2e/perf/` is the e2e-tier counterpart — same workload shape (50qps × 20s, concurrency 16) against a real 3-node `reflow/reflowd:e2e` cluster + a `cmd/loadhandler` sidecar over a docker user-defined network. Exercises engine→handler RPC, ingress/admin Connect, real TCP raft. No Pebble metrics (the sampler reaches into `*loadgen.InProcessNode` internals).

Run:

```bash
go test -tags=e2e -timeout=10m -count=1 -run=TestE2EPerf_SteadyState -v ./internal/e2e/perf/...
```

Reference (commit `bfddbdf+1`, 2026-05-22, Darwin/arm64 laptop, Docker Desktop 29.1.2):

```
- Issued: 1050
- Completed: 1050
- Failed: 0
- InFlightAtEnd: 0
- Duration: 20s

Latency (end-to-end, µs)
- p50:  28_271
- p90:  48_031
- p99:  49_855
- p999: 60_543
- max:  79_167

Invariants: all passed.
```

Note: the containerized run is faster end-to-end than the in-proc reference above on this machine — that's a stale in-proc reference taken under heavier laptop load, not a "containers are faster than in-proc" claim. Recalibrate both baselines together when one drifts so the comparison stays meaningful.

## Conventions worth knowing before editing

- **`NumPartitionShards` is the routing modulus** and is independent of peer count and replication factor. A 3-node deployment can host any number of shards; don't equate the two.
- **Tenancy is LP-banding, not a key field.** The tenant folds into `partition_key`'s LP region: `lp = (band << keys.IntraLPBits) | (hash & intraMask)`, so the `<lp>` prefix on every key isolates tenants. `keys.LPCount`=16384, 8 band bits × 6 intra bits → 256 bands × 64 LPs each; band 0 is the default/untenanted band. The tenant enters once, at ingress, from a verified `tenant/<n>` mTLS leaf (`ingress.principalTenant`), and rides `partition_key` everywhere — recover it with `keys.TenantFromPartitionKey(pk)`, never a separate field. Construct a banded key via `routing.PartitionKey(tenant, svc, key)`; in the apply path use `partition.bandedEntityPK(id, svc, key)` for entity rows. LP→shard routing (`LPFromPartitionKey` → `LPOwnersTable`) is orthogonal and unchanged.
- **`Host.Close` is idempotent and tears down in a specific order** (snapshot cancel → admin → delivery → engine → metrics). Don't reorder without checking listeners' close semantics.
- **Single-node vs multi-node bootstrap** lives in `internal/engine/host.go` (`applyMultiNodeConfig`, `JoinExisting`). Three modes:
  - `Peers` empty → single-node static bootstrap (gossip off, `applyMultiNodeConfig` not called).
  - `Peers` non-empty + `JoinExisting=false` → static multi-node bootstrap; every shard seeds with the full `Peers` set.
  - `Peers` non-empty + `JoinExisting=true` → dynamic join; `pkg/reflow/run.go:callSelfJoin` is invoked from `finishStartup` *before* `StartMetadataShard` and dials the gossip-resolved metadata leader's `Admin/SelfJoin`. The operator-driven `reflowd cluster add-node` flow is the alternative path.
  Both paths share `admin/server.go:addNodeInternal` (the FSM-driving body). The split is intentional: `Admin/AddNode` requires an `operator/*` SPIFFE principal; `Admin/SelfJoin` accepts a `node/*` principal but only when the SPIFFE NodeID equals `req.node_id` (the gate is `checkSelfJoinPrincipal`). If you refactor that shared helper, preserve the per-RPC auth policy.
- **Gossip publishes admin endpoints** — `NodeHostMeta.admin_endpoint` is filled from `HostConfig.AdminEndpoint` (set to `cfg.Admin.Addr` by `pkg/reflow/run.go`). `Host.NodeAdminEndpoint(nodeID)` resolves it via gossip; the joiner uses `Host.PartitionLeaderHint(0) + NodeAdminEndpoint` to dial the metadata leader without preconfiguration. The admin server's `requireLeader` attaches the same address as a `LeaderHint` detail on `Unavailable` so non-NodeHost clients (the CLI) can redirect too.
- **`PebbleOptions` and `RaftTransportFactory` hooks on `engine.HostConfig`** exist so the chaos harness can inject fault-injecting VFS / bufconn transport without forking the engine. Keep them at this seam; don't bury new harness knobs deeper.
- **Cluster-managed config is on shard 0.** Deployments, secrets, CA roots, join tokens, and the cluster authz policy live in shard 0's typed tables (`DeploymentTable`, `SecretTable`, `CARootTable`, `JoinTokenTable`, `PlatformConfigTable`). Mutations propose `Command_Upsert*/Delete*` with `Envelope.precondition.if_table_revision_eq` for CAS; the FSM signals CAS failure via `Result.Value = ResultValueFailedPrecondition` (returning an error halts the shard, per `internal/engine/CLAUDE.md`). Per-node subsystems subscribe to a `cluster.TableNotifier` and pull a fresh snapshot on wake (5s ticker backstop). **Secrets do not traverse Raft**: `SecretRecord` carries pointer fields (`remote_encrypted { blob_uri, kek_uri }`) only; per-node `internal/secretstore` Resolvers fetch via `gocloud.dev/blob`, dispatch KEK through Tink's `KMSClient` registry, and decrypt with `AAD = []byte(secret.name)`. The cluster CA signing key is the primary secret consumer (the in-cluster CA at `internal/certmgr` resolves it via `LookupForCASigning`). Four KMS providers ship in-binary at `pkg/kms/{blob,awskms,gcpkms,hcvault}/` (always-linked, config-gated).
- **Arbitrary dedup is LP-keyed; self dedup is shard-scoped.** `dedup/arbitrary/<lp:4>/<producer>/<seq>` rows ride the LP-transfer scan so a producer retry that hits the LP's new owner after a transfer flip finds its dedup row already present. The LP is derived from the command kind via `partition.lpFromCommand`; LP-agnostic kinds (today only `OutboxAck`) key under `keys.LPNoLP = 0xFFFF_FFFF`, a sentinel that is never a real LP (real LPs are < `keys.LPCount` = 16384) and is therefore never range-deleted by `FinishLPTransfer`. `dedup/self/*` stays shard-scoped per leader epoch (`GCSelfBelowEpoch` reclaims old epochs on leader gain).
- **Invocation metadata is durable end-to-end.** `SubmitInvocationRequest.metadata` (`map<string,string>`) flows through `InvokeCommand.metadata` → `Scheduled.metadata` (slot-0 transient) → `JEInput.metadata` (durable, in the journal) → `InputCommandMessage.headers` (sorted-by-key for deterministic replay bytes — critical because the engine hashes replay frames for divergence detection) → `wireContext.metadata` → `handler.Context.Metadata()`. Reflow never interprets values. The canonical HTTP carrier on the REST surface is the `Reflow-Meta-*` header prefix; keys are lowercased on entry. Operator middleware mounted via `ingress.Config.ExtraRoutes` can stamp verified facts here so the durable handler routes without re-verifying.

## Style

- Don't add Phase-marker comments. Don't write multi-paragraph docstrings on internal types — one-line `// Foo does X.` is the norm.
- Match Restate semantics where comments cite a `crates/...:line` source; if you intentionally diverge, say so in the comment (there are several examples of "narrower than Restate" notes already).
- **Connect interceptors must implement the full `connect.Interceptor` interface** (`WrapUnary` + `WrapStreamingClient` + `WrapStreamingHandler`), not `connect.UnaryInterceptorFunc` — the latter silently skips streaming RPCs per https://connectrpc.com/docs/go/streaming/. `internal/ingress/interceptor.go`'s `withDefaultDeadline` is the template.
- Don't add a `Co-Authored-By` trailer to git commits.
- Reflow still in preproduction no backwards-compt is'nt needed dont bloot the codebase
