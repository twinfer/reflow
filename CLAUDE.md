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

Chaos / load tests are gated by the `loadtest` build tag and live in `internal/chaos/` and `internal/loadgen/`. They are not part of `make test`:

```bash
go test -tags=loadtest -timeout=10m -run=TestChaos_LeaderSIGKILL -v ./internal/chaos/...
go test -tags=loadtest -timeout=10m -run=TestLoad_SteadyState   -v ./internal/loadgen/...
```

For Go code in this reflow, prefer the gopher-mcp MCP tools over textual search:

| Goal                              | Use                                            | Not                          |
| --------------------------------- | ---------------------------------------------- | ---------------------------- |
| Find where a symbol is declared   | `mcp__repo__find_symbol`                       | `grep "func Foo"`            |
| Jump from a use-site to its decl  | `mcp__repo__definition`                        | reading the file             |
| Find every caller of a function   | `mcp__repo__references` / `callers`            | `grep -r "Foo("`             |
| List types implementing an iface  | `mcp__repo__implementations`                   | grep + guessing              |
| Match Go syntax (calls, asserts)  | `mcp__repo__ast_grep`                          | `grep`                       |
| Trace which entry reaches code X  | `mcp__repo__reverse_trace`                     | reading call sites manually  |
| Find readers/writers of a proto   | `mcp__repo__proto_field_xref`                  | `grep "FieldName"`           |
| Resolve a `crates/...:42` comment | `mcp__repo__cite_resolve`                      | walking vendor by hand       |

Grep is still the right tool for: comments, log strings, config files,
non-Go files, and anything outside the indexed module(s).

Scope: the symbol/reference tools accept `scope` (`workspace`, `workspace+direct`
(default), or `all`). The default catches calls inside your module and its
direct `require`s. Pass `scope: "all"` to reach into indirect deps and stdlib
when the server has been started with `-deps all` (or `dep_index.stdlib: true`).


`cmd/loadnode` is a subprocess harness used by chaos tests so SIGKILL exercises Pebble WAL torn-write recovery. **Production deployments use `cmd/reflowd`, not loadnode.**

## Architecture map

The dependency direction is `cmd → pkg → internal → proto`. Internal packages must not depend on `pkg/*`, with one deliberate exception: `pkg/handler/wire` holds the shared engine↔handler protocol vocabulary (Codec, frame helpers, Type* constants, Route) and is imported by both sides — keeping it in pkg/ makes it part of the public SDK contract; bending the rule for this one focused package is preferable to duplicating the wire format.

- **`cmd/reflowd`** — the production binary with three subcommand groups:
  - `reflowd run` — start the engine. Loads layered koanf config (defaults → optional file from `$REFLOW_CONFIG` → `REFLOW_*` env vars; later overrides earlier), then calls `reflow.Run`. Handlers run in separate Go processes registered with the engine as HTTP/2 deployments; `examples/embedded/` shows a single-`main` dev setup that runs both.
  - `reflowd pki {init-ca|issue-cert|issue-operator}` — offline CA + leaf issuance.
  - `reflowd cluster {add-node|remove-node|nodes list|partitions list|snapshot create|list|delete|register-deployment}` — mTLS-authenticated admin RPCs against the Admin Connect port. `--admin` may point at any cluster node — mutating commands follow `LeaderHint` connect.Error details (attached by the server's `requireLeader`) to redirect to the metadata leader. The redirect helper lives at `pkg/adminclient.CallWithLeaderRedirect`.
- **`cmd/loadnode`** — test-only subprocess wrapper for chaos.

- **`pkg/reflow`** — the **stable public API**. `reflow.Run(ctx, cfg) → *Host`. `Config` uses koanf tags (snake_case so the env provider maps `REFLOW_INGRESS_GRPC_ADDR → ingress.grpc_addr`). `pkg/reflow/config` builds `Config` from any `koanf.Provider` — secret backends plug in as additional providers, no inline templating.
- **`pkg/sdk`** — the handler-facing surface: `Handler`, `Registry`, `Context`, `Future`, `Target`, `Failure`. `Context` is the durable-execution handle: every method (`Sleep`, `Run`, `Call`, `OneWayCall`, awakeables, state, …) is journaled. `Context.Metadata() map[string]string` returns caller-stamped metadata captured at submit time (see the metadata path in §`internal/ingress/http`). Determinism rule (non-negotiable): handlers must reach non-determinism (time, RNG, I/O) only through `Context`, never via `time.Now`/`rand`/`net/http` directly. `*Failure` returned from a handler is terminal and persisted; any other error is transient and retried.
- **`pkg/hostmux`** — operator-owned HTTP host dispatcher. Trust-aware host resolution (X-Forwarded-Host / RFC 7239 honored only when peer IP is in `TrustPolicy.Proxies`), exact + wildcard host match, atomic-swappable table for runtime reconfig (`Mux.Set`). Reflow's runtime does not import this package; it exists so operators can mount per-tenant or per-vendor handler trees in front of (or alongside) the Connect/REST ingress. Multi-tenant SaaS pattern: operator's tenant manager reacts to add/remove/rotate by calling `Mux.Set` — Reflow stores nothing about tenants, no admin RPC owns it, secrets live in the operator's secret store.

- **`internal/engine`** — the FSM, Raft proposer, leadership, action dispatch, invoker, snapshotter. Two shard kinds on one `NodeHost`: shard 0 (metadata, `engine/cluster`) holds `PartitionTable` + `NodeRegistry`; shards 1..N (partition, `partition.go`) hold invocation state. **Before editing inside `internal/engine`, read `internal/engine/CLAUDE.md` — it covers the goroutine model, the Action pattern, leadership two-phase, the leader-service lifecycle, and the don'ts.**
- **`internal/engine/invoker`** — per-partition in-process session runner. `Start`/`Stop` are leader-gain/loss callbacks; per-action methods run on the apply-path dispatch loop.
- **`internal/engine/delivery`** — cross-shard outbox dispatcher (`Client`/`Server`) wired into `PartitionRunner` as `CrossShardSender`. Single-node deployments leave it `nil`.
- **`internal/engine/snapshot`** — DR snapshot producer/repository/reaper backed by `gocloud.dev/blob` (file/s3/gs/azblob/mem URLs).
- **`internal/storage`** — `Store` interface (Pebble + in-memory); `keys` defines the byte-level key layout (no partition_id prefix — each partition has its own DB); `tables` is the typed view over keys.
- **`internal/ingress`** — Connect RPC entrypoints (`SubmitInvocation`, `AwaitInvocation`, `AttachInvocation`, `GetInvocationOutput`, `CancelInvocation`, `ResolveAwakeable`, `ResolveWorkflowPromise`, `GetObjectState`, admin reads) over HTTP/2 with content-negotiated Connect / gRPC / gRPC-Web / HTTP-JSON on one listener. Routes via `Host.Partitioner` (hash of `service` + `object_key`). `ingress.Config.ExtraRoutes func(*Server) []connectserver.Route` is the seam for mounting additional HTTP handlers on the same listener — `internal/ingress/http` is the canonical caller, but operator code can ride it too without a second port/cert.
- **`internal/ingress/http`** — REST-style facade at `/v1/*` (chi-based) mounted on the Connect ingress listener via `ExtraRoutes`. Each handler builds a typed `*connect.Request` from URL params + headers + body and delegates to the same in-process `*ingress.Server` Connect handlers — no business-logic duplication. Default request timeout is installed by an `ensureDeadline` middleware because `*ingress.Server` calls dragonboat `SyncRead` under the hood (rejects deadlineless contexts). Long-poll endpoints cap at 30s (vs Connect's 60s) to stay under common LB idle timeouts. Inbound HTTP headers prefixed `Reflow-Meta-` are lifted (lowercased + stripped) into `SubmitInvocationRequest.metadata` so operator middleware can pass verified webhook facts to the durable handler via `ctx.Metadata()`.
- **`internal/ingress/eventsource`** — Watermill-backed broker triggers. One dispatcher goroutine per configured source subscribes to a topic (Kafka / NATS-JetStream / SQS / gochannel), maps each message to a `SubmitInvocationRequest`, and calls the in-process `ingress.Server` directly (no localhost RPC). Wired from `pkg/reflow/run.go` after the ingress listener is up; closed before the engine in `Host.Close`. Empty `cfg.EventSources.Sources` means the manager is never constructed.
- **`internal/engine/handlerclient`** — engine-side wire client for handler deployments. Single transport is Connect RPC (`connectclient/`) over HTTP/2; the handler-side server lives at `pkg/sdk/server`.
- **`internal/auth`** — inbound auth + authz, HTTP-level middleware built on `connectrpc.com/authn` so failures emit proper Connect-coded errors (`CodeUnauthenticated` / `CodePermissionDenied`) across Connect, gRPC, gRPC-Web, and HTTP-JSON, and works uniformly for unary and streaming RPCs. Two authenticators chain in `composeAuthFunc`: (1) SPIFFE URI SAN on the verified mTLS leaf — `spiffe://<trust-domain>/node/<id>` vs `/operator/<name>`; (2) Bearer JWT verified against one or more OIDC issuers configured under `cfg.Auth.OIDC []OIDCIssuer` (lazy discovery with `cenkalti/backoff/v5`; `JWKSFile` for air-gapped). **mTLS wins when both are presented on the same request** — a leaked bearer token cannot forge a peer-verified leaf. Per-listener transport security is driven by `cfg.Delivery.Creds` / `cfg.Admin.Creds` via `pkg/reflow/creds`; multi-node insecure delivery emits a startup warning in `pkg/reflow/run.go`. The starter authz policy (`starter_policy.json`, embedded via `//go:embed`) restricts `/Admin/*` to `operator/*` with one carve-out: `/Admin/SelfJoin` is allowed for `node/*` so a freshly-provisioned joiner can register itself. The handler then enforces SPIFFE-equals-`req.node_id` so a leaked `node/7` cert can only join as node 7. JWT principals get `Kind`/`Subject` from configurable claims (default `sub` mapped to `user/<sub>`); the `/` character in subjects is sanitized to `_` so an IdP-controlled `sub` can't traverse principal-glob matching.
- **`internal/pki`** — offline CA + leaf issuance used by `reflowd pki {init-ca|issue-cert|issue-operator}`.
- **`internal/observability`** — `*Metrics` is a single Prometheus collector struct passed down into the partition apply path + timer service. The engine never constructs its own registry; wiring lives in `pkg/reflow`.

- **`proto/`** — `buf generate` output lives next to the `.proto`. Don't hand-edit the `*.pb.go`. `enginev1` is the on-disk + Raft wire format — **any change is an upgrade-compat decision**; tag allocations and reservations are documented in `proto/enginev1/engine.proto` and in `durable-execution-go-sad.md` §6.2.

## Tests

- **Unit tests** sit next to the code under test (`xxx_test.go`, package `xxx`).
- **Engine integration tests** live in `internal/engine/integration_*_test.go` under `package engine_test`. They use the `internal/loadgen` cluster bootstrap (so `loadgen` is imported from non-loadtest builds — keep its non-`//go:build loadtest` files free of test-only dependencies the production import path can't satisfy).
- **`internal/engine/pbt_test.go`** is property-based with `pgregory.net/rapid`.
- **Chaos / load tests** are `//go:build loadtest` only and excluded from `make test`. The harness uses `loadgen.HelloHandler` and bufconn-backed Raft transport so per-pair links can be `Cut`/`Heal`ed in-test without real ports.

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

## Conventions worth knowing before editing

- **`NumPartitionShards` is the routing modulus** and is independent of peer count and replication factor. A 3-node deployment can host any number of shards; don't equate the two.
- **`Host.Close` is idempotent and tears down in a specific order** (snapshot cancel → admin → delivery → engine → metrics). Don't reorder without checking listeners' close semantics.
- **Single-node vs multi-node bootstrap** lives in `internal/engine/host.go` (`applyMultiNodeConfig`, `JoinExisting`). Three modes:
  - `Peers` empty → single-node static bootstrap (gossip off, `applyMultiNodeConfig` not called).
  - `Peers` non-empty + `JoinExisting=false` → static multi-node bootstrap; every shard seeds with the full `Peers` set.
  - `Peers` non-empty + `JoinExisting=true` → dynamic join; `pkg/reflow/run.go:callSelfJoin` is invoked from `finishStartup` *before* `StartMetadataShard` and dials the gossip-resolved metadata leader's `Admin/SelfJoin`. The operator-driven `reflowd cluster add-node` flow is the alternative path.
  Both paths share `admin/server.go:addNodeInternal` (the FSM-driving body). The split is intentional: `Admin/AddNode` requires an `operator/*` SPIFFE principal; `Admin/SelfJoin` accepts a `node/*` principal but only when the SPIFFE NodeID equals `req.node_id` (the gate is `checkSelfJoinPrincipal`). If you refactor that shared helper, preserve the per-RPC auth policy.
- **Gossip publishes admin endpoints** — `NodeHostMeta.admin_endpoint` is filled from `HostConfig.AdminEndpoint` (set to `cfg.Admin.Addr` by `pkg/reflow/run.go`). `Host.NodeAdminEndpoint(nodeID)` resolves it via gossip; the joiner uses `Host.PartitionLeaderHint(0) + NodeAdminEndpoint` to dial the metadata leader without preconfiguration. The admin server's `requireLeader` attaches the same address as a `LeaderHint` detail on `Unavailable` so non-NodeHost clients (the CLI) can redirect too.
- **`PebbleOptions` and `RaftTransportFactory` hooks on `engine.HostConfig`** exist so the chaos harness can inject fault-injecting VFS / bufconn transport without forking the engine. Keep them at this seam; don't bury new harness knobs deeper.
- **Event-source backends are always-linked, config-gated.** `internal/ingress/eventsource/factory_*.go` register Kafka / NATS / SQS / gochannel factories at package `init()`; backends never start unless a `cfg.EventSources.Sources` entry selects them. The `Source` type is `= message.Subscriber`, so operators can implement raw Subscribers (custom Kafka rebalance hooks, JetStream pull-consumer filters, SQS visibility-extension) and register them via `RegisterFactory` without forking. `pkg/reflow.EventSourcesConfig = eventsource.Config` (type alias) keeps the public surface stable while implementation lives next to consumers — preserves the `pkg → internal → proto` import direction.
- **Invocation metadata is durable end-to-end.** `SubmitInvocationRequest.metadata` (`map<string,string>`) flows through `InvokeCommand.metadata` → `Scheduled.metadata` (slot-0 transient) → `JEInput.metadata` (durable, in the journal) → `InputCommandMessage.headers` (sorted-by-key for deterministic replay bytes — critical because the engine hashes replay frames for divergence detection) → `wireContext.metadata` → `handler.Context.Metadata()`. Reflow never interprets values. Webhook adapters (operator-side HMAC middleware) stamp verified facts here so the durable handler can route without re-verifying. The canonical HTTP carrier on the REST surface is the `Reflow-Meta-*` header prefix; keys are lowercased on entry (Go's `net/http` canonicalizes header names, depending on the canonical form would be brittle). Vendor-specific webhook adapters (Stripe HMAC, GitHub `X-Hub-Signature-256`, …) are **not** Reflow's job — operators write them as thin middleware in their own main, mount via `ingress.Config.ExtraRoutes`, and dispatch to a normal `HandlerService` registered in their handler binary.

## Style

- Don't add Phase-marker comments. Don't write multi-paragraph docstrings on internal types — one-line `// Foo does X.` is the norm.
- Match Restate semantics where comments cite a `crates/...:line` source; if you intentionally diverge, say so in the comment (there are several examples of "narrower than Restate" notes already).
- **Connect interceptors must implement the full `connect.Interceptor` interface** (`WrapUnary` + `WrapStreamingClient` + `WrapStreamingHandler`), not `connect.UnaryInterceptorFunc` — the latter silently skips streaming RPCs per https://connectrpc.com/docs/go/streaming/. `internal/ingress/interceptor.go`'s `withDefaultDeadline` is the template.
- Don't add a `Co-Authored-By` trailer to git commits.
- Reflow still in preproduction no backwards-compt is'nt needed dont bloot the codebase
