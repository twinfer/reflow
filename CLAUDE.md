# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Reflow is a **single-binary durable-execution engine for Go**, inspired by Restate (Rust). One engine binary, one data directory; handlers run as separate Go processes the engine reaches over HTTP/2. Built on `dragonboat` (multi-group Raft) + `cockroachdb/pebble` (embedded K/V). The design doc lives at `durable-execution-go-sad.md` — read it before non-trivial work on cluster, FSM, or storage. Restate wire-protocol concepts are mirrored where applicable; comments cite the source as `crates/.../file.rs:line` (Restate v1.6.2).


## Common commands

```bash
make build        # go build ./...
make test         # go test -race ./...   (unit + integration, no loadtest)
make vet
make tidy
make proto        # buf generate (regenerates proto/*/*.pb.go)
```

Run a single test:

```bash
go test -race -run TestSingleNodeReplayAcrossRestart ./internal/engine/...
```

Chaos / load tests are gated by the `loadtest` build tag and live in `internal/chaos/` and `internal/loadgen/`. They are not part of `make test`:

```bash
go test -tags=loadtest -timeout=10m -run=TestChaos_LeaderSIGKILL -v ./internal/chaos/...
go test -tags=loadtest -timeout=10m -run=TestLoad_SteadyState   -v ./internal/loadgen/...
```

`cmd/loadnode` is a subprocess harness used by chaos tests so SIGKILL exercises Pebble WAL torn-write recovery. **Production deployments use `cmd/reflowd`, not loadnode.**

## Architecture map

The dependency direction is `cmd → pkg → internal → proto`. Internal packages must not depend on `pkg/*`.

- **`cmd/reflowd`** — the production binary with three subcommand groups:
  - `reflowd run` — start the engine. Loads layered koanf config (defaults → optional file from `$REFLOW_CONFIG` → `REFLOW_*` env vars; later overrides earlier), then calls `reflow.Run`. Handlers run in separate Go processes registered with the engine as HTTP/2 deployments; `examples/embedded/` shows a single-`main` dev setup that runs both.
  - `reflowd pki {init-ca|issue-cert|issue-operator}` — offline CA + leaf issuance.
  - `reflowd cluster {add-node|remove-node|nodes list|partitions list|snapshot create|list|delete|register-deployment}` — mTLS-authenticated admin RPCs against the Admin Connect port. `--admin` may point at any cluster node — mutating commands follow `LeaderHint` connect.Error details (attached by the server's `requireLeader`) to redirect to the metadata leader. The redirect helper lives at `pkg/adminclient.CallWithLeaderRedirect`.
- **`cmd/loadnode`** — test-only subprocess wrapper for chaos.

- **`pkg/reflow`** — the **stable public API**. `reflow.Run(ctx, cfg) → *Host`. `Config` uses koanf tags (snake_case so the env provider maps `REFLOW_INGRESS_GRPC_ADDR → ingress.grpc_addr`). `pkg/reflow/config` builds `Config` from any `koanf.Provider` — secret backends plug in as additional providers, no inline templating.
- **`pkg/sdk`** — the handler-facing surface: `Handler`, `Registry`, `Context`, `Future`, `Target`, `Failure`. `Context` is the durable-execution handle: every method (`Sleep`, `Run`, `Call`, `OneWayCall`, awakeables, state, …) is journaled. Determinism rule (non-negotiable): handlers must reach non-determinism (time, RNG, I/O) only through `Context`, never via `time.Now`/`rand`/`net/http` directly. `*Failure` returned from a handler is terminal and persisted; any other error is transient and retried.

- **`internal/engine`** — the FSM, Raft proposer, leadership, action dispatch, invoker, snapshotter. Two shard kinds on one `NodeHost`: shard 0 (metadata, `engine/cluster`) holds `PartitionTable` + `NodeRegistry`; shards 1..N (partition, `partition.go`) hold invocation state. **Before editing inside `internal/engine`, read `internal/engine/CLAUDE.md` — it covers the goroutine model, the Action pattern, leadership two-phase, the leader-service lifecycle, and the don'ts.**
- **`internal/engine/invoker`** — per-partition in-process session runner. `Start`/`Stop` are leader-gain/loss callbacks; per-action methods run on the apply-path dispatch loop.
- **`internal/engine/delivery`** — cross-shard outbox dispatcher (`Client`/`Server`) wired into `PartitionRunner` as `CrossShardSender`. Single-node deployments leave it `nil`.
- **`internal/engine/snapshot`** — DR snapshot producer/repository/reaper backed by `gocloud.dev/blob` (file/s3/gs/azblob/mem URLs).
- **`internal/storage`** — `Store` interface (Pebble + in-memory); `keys` defines the byte-level key layout (no partition_id prefix — each partition has its own DB); `tables` is the typed view over keys.
- **`internal/ingress`** — gRPC + grpc-gateway HTTP/JSON entrypoints (`SubmitInvocation`, `AwaitInvocation`, `AttachInvocation`, awakeables, admin reads). Routes via `Host.Partitioner` (hash of `service` + `object_key`).
- **`internal/engine/handlerclient`** — engine-side wire client for handler deployments. Single transport is Connect RPC (`connectclient/`) over HTTP/2; the handler-side server lives at `pkg/sdk/server`.
- **`internal/auth`** — single-CA mTLS with SPIFFE URI SAN role enforcement (`spiffe://<trust-domain>/node/<id>` vs `/operator/<name>`). Per-listener transport security is driven by `cfg.Delivery.Creds` / `cfg.Admin.Creds` via `pkg/reflow/creds`; multi-node insecure delivery emits a startup warning in `pkg/reflow/run.go`. The starter authz policy (`starter_policy.json`, embedded via `//go:embed`) restricts `/Admin/*` to `operator/*` with one carve-out: `/Admin/SelfJoin` is allowed for `node/*` so a freshly-provisioned joiner can register itself. The handler then enforces SPIFFE-equals-`req.node_id` so a leaked `node/7` cert can only join as node 7.
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
- **Single-node vs multi-node bootstrap** lives in `internal/engine/host.go` (`applyMultiNodeConfig`, `JoinExisting`). `Peers` empty → single-node static bootstrap (gossip off); `Peers` non-empty + `JoinExisting=false` → static multi-node bootstrap; `Peers` non-empty + `JoinExisting=true` → dynamic join. Joiners self-register via `Admin/SelfJoin` (called from `pkg/reflow/run.go:callSelfJoin` before `StartMetadataShard`); the operator-driven `reflowd cluster add-node` flow remains as the alternative. Both paths share `admin/server.go:addNodeInternal` — refactor seam is load-bearing for the auth split.
- **Gossip publishes admin endpoints** — `NodeHostMeta.admin_endpoint` is filled from `HostConfig.AdminEndpoint` (set to `cfg.Admin.Addr` by `pkg/reflow/run.go`). `Host.NodeAdminEndpoint(nodeID)` resolves it via gossip; the joiner uses `Host.PartitionLeaderHint(0) + NodeAdminEndpoint` to dial the metadata leader without preconfiguration. The admin server's `requireLeader` attaches the same address as a `LeaderHint` detail on `Unavailable` so non-NodeHost clients (the CLI) can redirect too.
- **`PebbleOptions` and `RaftTransportFactory` hooks on `engine.HostConfig`** exist so the chaos harness can inject fault-injecting VFS / bufconn transport without forking the engine. Keep them at this seam; don't bury new harness knobs deeper.

## Style

- Don't add Phase-marker comments. Don't write multi-paragraph docstrings on internal types — one-line `// Foo does X.` is the norm.
- Match Restate semantics where comments cite a `crates/...:line` source; if you intentionally diverge, say so in the comment (there are several examples of "narrower than Restate" notes already).
- Don't add a `Co-Authored-By` trailer to git commits.
- Reflow still in preproduction no backwards-compt is'nt needed dont bloot the codebase
