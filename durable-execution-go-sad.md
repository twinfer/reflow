# Solution Architecture Document
# Durable Execution Engine in Go

**Version:** 0.7 (Draft)
**Date:** 2026-05-16
**Status:** Single-node foundation, HTTP/2 wire protocol between engine and
Go SDK (`pkg/sdk/server`), Virtual Objects (single-writer gate, idempotency,
retry policy, eager state, attach RPCs), combinator futures (`Promise.all` /
`Promise.race`), multi-node replication (mTLS admin, dynamic membership, DR
snapshots), and auth consolidation (single-CA SPIFFE identity,
proto-annotation authz, single Authorizer seam) implemented. Cloud snapshot
drivers, retention, and non-Go SDKs outstanding. Open gap: OPEN-1
(joining-node startup) — see §9.

---

## 1. Overview

Reflow is a durable execution engine designed for Go shops that want a single
self-contained engine binary instead of a multi-component topology. Handlers
are independent Go processes that the engine reaches over HTTP/2. It is
inspired by Restate's design and borrows Restate's wire-level concepts where
applicable, but its operational profile is intentionally different:
Restate-grade durability and exactly-once semantics with SQLite-style
deployment ergonomics.

**One-line pitch.** *Single-binary durable-execution engine for Go. One
engine, one data directory, your workflows survive crashes.*

**Positioning.** Reflow is the right choice when:

- Your stack is Go-first and you want durable execution backed by a single
  engine binary rather than a multi-component platform to operate.
- You want a Go-native handler SDK with the same wire protocol as the
  engine itself uses for cross-language handlers (no special "in-process"
  fast path that diverges from the production path).
- You need an Apache-2.0-from-day-one license with no single-vendor
  dependency.

Reflow is **not** the right choice when:

- You need polyglot SDK parity today across TypeScript / Python / Java /
  Kotlin / Rust / Go. Use Restate.
- You're already running Restate. Don't switch.
- You need millions of concurrent invocations at Temporal scale.
- You need today's mature feature surface across virtual objects, awakeables,
  promises, schedulers, etc.

The key technical decision is to replace Restate's custom distributed log
(Bifrost) and its separate metadata-server / log-server topology with a
direct composition of `dragonboat` (multi-group Raft) and `cockroachdb/pebble`
(embedded K/V store), both linked into a single Go binary. The cluster's own
metadata lives in another `dragonboat` Raft group hosted by the same
`NodeHost` as the partition shards (see §6.2). Operationally, `reflowd` is
one process with one data directory.

---

## 2. Goals

- **Durable execution.** Handler execution survives crashes and resumes
  exactly where it stopped.
- **Single self-contained engine binary.** `reflowd` is the only engine
  process the operator runs. No external metadata store, no external log
  servers, no required sidecars, no Kubernetes operator. Static peer
  config is enough to form a multi-node cluster.
- **Go SDK as a first-class path.** A Go handler is a separate process
  that hosts `pkg/sdk/server` (HTTP/2). The engine talks to it via the
  same wire protocol used for any other language. There is one path, not
  two — what works for the Go SDK works for cross-language handlers, and
  vice versa.
- **Virtual Objects.** Stateful entities with single-writer consistency
  and durable K/V state.
- **Workflows.** Long-running, multi-step processes with durable timers
  and suspension.
- **Exactly-once side effects.** External calls are deduplicated via
  journal replay; ingress proposals are dedup'd at the partition boundary.
- **Apache 2.0 from day one.** No commercial-use restriction, no
  single-vendor dependency, no relicensing risk.
- **Horizontal scalability for small-to-medium clusters.** 3–10 nodes is
  the design center. Larger deployments are possible but not the target.

## 3. Non-Goals

- Replacing Kafka or general-purpose message brokers.
- Multi-tenancy.
- WASM-based handler execution.
- **Polyglot SDK parity with Restate.** Reflow's first-class SDK is Go;
  other languages are supported only via the wire protocol, on whatever
  cadence makes sense, with no commitment to feature parity across them.
- **External coordination services.** Reflow will never require etcd,
  Consul, ZooKeeper, Kafka, a separate metadata server, or a separate log
  server to run. Cluster coordination is in-binary via an embedded
  metadata Raft group (see §6.2).
- **Object storage as a hard dependency.** S3/GCS/Azure Blob is an
  **optional** backend for snapshot archival. The default deployment uses
  local filesystem snapshots only and remains zero-external-dep.
- **Temporal-scale concurrency targets.** Designed for thousands of
  in-flight invocations on a small cluster, not millions on a hundred-node
  fleet.

---

## 4. Architecture Overview

```
                         ┌─────────────────────────────────────────┐
   SDK Handlers          │              Ingress Layer               │
   (TypeScript,   ──────▶│   Connect/HTTP-2 + /v1/* REST facade     │
    Python, Go)          │     Invocation routing by partition key  │
                         └────────────────┬────────────────────────┘
                                          │
                         ┌────────────────▼────────────────────────┐
                         │           Cluster Manager                │
                         │   Node discovery, partition assignment   │
                         │   Raft group lifecycle management        │
                         └────────────────┬────────────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
  ┌───────────▼──────────┐   ┌───────────▼──────────┐              ...
  │   Partition 0        │   │   Partition 1        │
  │                      │   │                      │
  │  ┌────────────────┐  │   │  ┌────────────────┐  │
  │  │ Partition      │  │   │  │ Partition      │  │
  │  │ Processor      │  │   │  │ Processor      │  │
  │  │                │  │   │  │                │  │
  │  │ Invocation FSM │  │   │  │ Invocation FSM │  │
  │  │ VObject FSM    │  │   │  │ VObject FSM    │  │
  │  │ Journal Replay │  │   │  │ Journal Replay │  │
  │  │ Timer Service  │  │   │  │ Timer Service  │  │
  │  └───────┬────────┘  │   │  └───────┬────────┘  │
  │          │           │   │          │           │
  │  ┌───────▼────────┐  │   │  ┌───────▼────────┐  │
  │  │  dragonboat    │  │   │  │  dragonboat    │  │
  │  │  Raft Group    │  │   │  │  Raft Group    │  │
  │  └───────┬────────┘  │   │  └───────┬────────┘  │
  │          │           │   │          │           │
  │  ┌───────▼────────┐  │   │  ┌───────▼────────┐  │
  │  │ Pebble (dual)  │  │   │  │ Pebble (dual)  │  │
  │  │  - Raft log    │  │   │  │  - Raft log    │  │
  │  │  - State       │  │   │  │  - State       │  │
  │  └────────────────┘  │   │  └────────────────┘  │
  └──────────────────────┘   └──────────────────────┘
```

Each partition is an independent unit: one dragonboat Raft group, one Pebble instance, one Partition Processor.

---

## 5. Technology Stack

| Concern | Library | License |
|---|---|---|
| Raft consensus + replication | `lni/dragonboat/v4` (pre-release pin) | Apache 2.0 |
| Embedded K/V storage | `cockroachdb/pebble/v2` | Apache 2.0 |
| RPC (ingress, admin, delivery, handler-wire) | `connectrpc.com/connect` over stdlib HTTP/2 | Apache 2.0 |
| REST router (operator-facing `/v1/*` + ExtraRoutes) | `go-chi/chi/v5` | MIT |
| Snapshot archival (filesystem + cloud blob) | `gocloud.dev/blob` | Apache 2.0 |
| Serialization + IDL | `google.golang.org/protobuf` + `buf` v2 | BSD-3 / Apache 2.0 |
| Authn/Authz | stdlib `crypto/tls`, custom SPIFFE URI mapper + proto-annotation authz (`internal/auth`) | — |
| Virtual-Object FSM | `qmuntal/stateless` v1.8.0 | BSD-2 |
| Structured logging | `log/slog` (stdlib) | — |
| Metrics | `prometheus/client_golang` | Apache 2.0 |

The invocation state machine uses a plain switch over the persisted
`InvocationStatus` discriminated union rather than an FSM library — see §6.4.
The per-key Virtual-Object gate uses `qmuntal/stateless` for clarity around
the Active-reentry semantics (queue head promotion) — see §6.5.

`go.mod` pins `cockroachdb/pebble/v2 v2.1.5` (the public Pebble API used
by reflow's `StateStore`) and `lni/dragonboat/v4 v4.0.0-20250723143628-076c7f6497dc`
(the pre-release that internally still uses pebble v1 for its log store).
The two coexist because dragonboat's pebble dependency is package-isolated
behind `cockroachdb/pebble` (no `/v2`) — go's module graph treats them as
distinct packages. Watch dragonboat for an official v4 release that bumps
its internal pebble.

---

## 6. Component Design

### 6.1 Ingress Layer

Accepts invocations from external callers. The ingress is the
client-facing surface; the engine ↔ handler wire (see §6.10) is a
separate code path, not part of ingress.

**Responsibilities:**
- Parse invocation requests (Connect RPC over HTTP/2 — content-negotiates
  Connect / gRPC / gRPC-Web / HTTP-JSON on a single port). A REST-style
  facade at `/v1/*` (chi-based) is mounted on the same listener for
  curl- and webhook-friendly URL shapes.
- Determine the target partition via consistent hashing on
  `(service_name, object_key)` through `Host.Partitioner`.
- Propose invocation commands to the owning partition's leader
  (in-process when co-located; cross-node delivery rides the Delivery
  surface — see §6.6).
- Return invocation ID to caller immediately (async / `/v1/send`) or
  long-poll until completion (`/v1/call`, `/v1/attach`).
- Propagate caller-supplied metadata end-to-end: HTTP headers prefixed
  `Reflow-Meta-*` (REST) or the typed `SubmitInvocationRequest.metadata`
  field (Connect) flow through `InvokeCommand.metadata` → `Scheduled.metadata`
  → `JEInput.metadata` → `InputCommandMessage.headers` → `handler.Context.Metadata()`.
  The engine never interprets the values. Designed for webhook adapters
  that verify a vendor signature (Stripe, GitHub, …) in front of the
  ingress and stash the verified facts here so the durable handler can
  route without re-verifying.

**Routing:**
```
partition_id = hash(service_name + "/" + object_key) % num_partitions
```

**Surfaces hosted by reflowd (all distinct, by design):**

| Surface | Port (default) | Wire | Auth | Purpose |
|--|--|--|--|--|
| Connect ingress (`reflow.ingress.v1`) | 8080 | Connect/HTTP-2 | Anonymous via `ingress_open` policy rule; operator tightens via `cfg.Auth.PolicyFile` | Typed SDK clients submit invocations, await results, resolve awakeables/promises |
| REST ingress (`/v1/*`) | 8080 (same listener) | HTTP/1.1+HTTP/2 + JSON envelope | Anonymous via `ingress_rest_open` policy rule | curl, webhooks, Restate-style URL ergonomics; delegates to the Connect handlers via the in-process `*ingress.Server` |
| Delivery (`reflow.delivery.v1`) | 8081 | Connect/HTTP-2 | mTLS, `spiffe://<td>/node/*` | Cross-partition / cross-node command forwarding |
| Admin (`reflow.admin.v1`) | 8082 | Connect/HTTP-2 | mTLS, `spiffe://<td>/operator/*` (+ `node/*` carve-out on `SelfJoin`) | Cluster ops: add/remove node, list partitions, snapshot mgmt, register-deployment |

**Extension seam.** `ingress.Config.ExtraRoutes func(*Server) []connectserver.Route`
mounts additional HTTP handlers on the Connect ingress listener without
a second port/cert. The REST facade at `/v1/*` is the canonical caller;
operator code (webhook adapters, custom REST endpoints) rides the same
seam. Each `ExtraRoute` is wrapped by the same auth middleware Connect
uses — the SPIFFE policy is the authoritative gate.

**Operator infrastructure: `pkg/hostmux`.** Multi-tenant SaaS deployments
that need per-tenant or per-vendor host-based routing wire `pkg/hostmux.Mux`
as one of `ExtraRoutes`. Reflow never imports the package itself —
runtime mutation of the host table is a Go function call (`Mux.Set`)
driven by the operator's tenant manager (file watcher, control-plane
stream, polling against a tenant DB). Reflow does not durably store
tenant config; secrets stay in the operator's secret store, no admin
RPC owns it.

The engine ↔ handler wire (`proto/protocolv1`) is raw HTTP/2 to the
handler-hosted endpoint, not a service hosted by reflowd. See §6.10.

---

### 6.2 Cluster Manager

The cluster manager is **not a separate process**. Cluster-level state lives
in a dedicated `dragonboat` Raft group hosted by the same `NodeHost` as the
partition shards. By convention this is `shardID = 0` (the "metadata
group"); partition shards use `shardID = 1..N`.

**Principle.** *Raft for safety, gossip for spread.* Every durable cluster
decision goes through the metadata Raft group. Anything advisory (leader
hints for routing, liveness, load hints) rides on dragonboat's built-in
gossip layer (memberlist/SWIM, vendored inside `lni/dragonboat/v4`). The
metadata group is always authoritative; gossip is only ever a hint.

**State held by the metadata Raft group:**

- Node membership: `{node_id, raft_addr, last_seen_ms, version}`.
- Partition table: `{shard_id → [node_id ...]}` with the current replica
  set per partition and a monotonic config-change index.
- Partition assignment epoch: incremented every time the assignment table
  changes; used to fence stale assignment caches.
- Storage format version: a per-pebble-DB `uint32` marker (`internal/storage/format.go`)
  written at every shard's open path. Refuses to open a DB written by a binary
  with a different `StorageFormatVersion`. Replaces the earlier "command-stream
  VersionBarrier" sketch — the latter answered the wrong question (cross-binary
  FSM-logic skew on the live command path) at the cost of a permanent wire-
  format field; the storage marker handles the common case (operator boots a
  binary against an incompatible data dir) without polluting the proto.

**Bootstrap — static peer list, no discovery service required:**

Every node ships with the full `Cluster.Peers` list in its config; there
is no founder/joiner asymmetry in the `reflowd` binary. The cluster
forms once a quorum of `NodeHost`s can reach each other.

```yaml
# reflowd config (identical shape on every node; only node.id differs)
node:
  id: 2
  raft_addr: 10.0.0.2:9091
  gossip_bind_addr: 10.0.0.2:9092
  delivery_addr: 10.0.0.2:8081
storage:
  data_dir: /var/lib/reflow
cluster:
  peers:
    - { node_id: 1, raft_addr: 10.0.0.1:9091, gossip_addr: 10.0.0.1:9092, node_host_id: <uuid> }
    - { node_id: 2, raft_addr: 10.0.0.2:9091, gossip_addr: 10.0.0.2:9092, node_host_id: <uuid> }
    - { node_id: 3, raft_addr: 10.0.0.3:9091, gossip_addr: 10.0.0.3:9092, node_host_id: <uuid> }
```

Each node, in order (`pkg/reflow/run.go:Run`):

1. Validate config (`Node.ID`, `Node.RaftAddr`, `Storage.DataDir`; mTLS
   files required when `Cluster.Peers` is non-empty —
   `cmd/reflowd/main.go:requireTLSWhenMultiNode`).
2. Build the slog logger, register Prometheus collectors, optionally
   start `/metrics` HTTP server.
3. `engine.NewHost` → `dragonboat.NewNodeHost` with
   `DefaultNodeRegistryEnabled = true`, `NodeHostID = self UUID`, and a
   `GossipConfig` whose `Seed` is the *other* peers' gossip addrs and
   whose `Meta` is the proto-marshalled
   `NodeHostMeta{GrpcEndpoint = Node.DeliveryAddr}`
   (`internal/engine/host.go:applyMultiNodeConfig`). The NodeHost is up,
   gossip is running, but no shards have started.
4. Build the Delivery surface (`pkg/reflow/run.go:96-187`): mTLS server
   creds (cluster CA + node leaf), gRPC server with
   `auth.StreamInterceptor`, `delivery.Client` for outbound, and
   `Host.SetCrossShardSender(client)` so partitions started below get a
   Sender.
5. `Host.StartMetadataShard()` opens `${DataDir}/meta/state`, builds the
   cluster FSM + leadership + proposer, and calls
   `nh.StartOnDiskReplica(initialMembers, /*join=*/ false, fsm, raftCfg)`
   (`host.go:321`). `initialMembers` is built from the full peer list as
   `{node_id → NodeHostID}` targets (`host.go:initialMembers`,
   line 613-622) so dragonboat gossip resolves them to live raft addrs.
6. For each `shard_id` in `Cluster.Shards`, `Host.StartPartition`
   repeats the per-partition equivalent: open Pebble at
   `${DataDir}/p{shard_id}/state`, build the Invoker bound to the
   partition's tables, register the leadership callbacks, and call
   `nh.StartOnDiskReplica(initialMembers, false, fsm, raftCfg)`
   (`host.go:508`).
7. Optionally build the snapshot producer (`snapshot.FSRepository` +
   `RunProducer` goroutine per shard).
8. Optionally build the Admin surface: mTLS server creds, gRPC server
   with `auth.UnaryInterceptor`, `adminv1.RegisterAdminServer`.

After `Run` returns, the cluster forms organically. Each NodeHost has
the full member list; once `floor(N/2)+1` of them can reach each other
over the Raft RPC, dragonboat elects a leader. The election fires
`raftEventListener.LeaderUpdated`, which runs
`Leadership.onBecomeLeader` on the elected node — that proposes
`AnnounceLeader`, bumps the leader epoch (seeded from
`MetaTable.latest_announced_epoch` to skip past prior leaders'
proposals), and starts the leader-only services
(`TimerService`, `OutboxService`, the Invoker's leader-side loops).
Followers run the same FSM apply path with leader-only services idle.

**Dynamic membership — partial today (see `OPEN-1` in §9):**

The admin RPC, cluster FSM, and metadata rebalancer that drive
*post-bootstrap* membership changes all exist:

```bash
reflowd cluster add-node    --node-id=4 --raft-addr=10.0.0.4:9091 \
                            --gossip-addr=10.0.0.4:9092 \
                            --grpc-endpoint=10.0.0.4:8081 \
                            --node-host-id=<uuid>
reflowd cluster remove-node --node-id=2
```

- **`add-node`** (`internal/admin/server.go:AddNode`): proposes
  `RegisterNode{Member}` to shard 0, then enqueues a
  `BeginRebalanceStep{Kind: PROMOTE_TO_VOTER, AddNodeId}` for every
  partition the new node should hold. The metadata rebalancer
  (`internal/engine/metadata_rebalancer.go:227-247`) watches the pending
  queue and on the metadata leader executes the dragonboat-side call:
  `SyncRequestAddNonVoting`, then `SyncRequestAddReplica`. On success it
  proposes `CompleteRebalanceStep`, which updates the persisted replica
  set and bumps `assignment_epoch`.
- **`remove-node`**: same path with `EvictNode` →
  `SyncRequestDeleteReplica`. Works end-to-end today because the leaving
  node's `reflowd` already has the live membership in its NodeHost; it
  simply exits when dragonboat removes its replica.

**The gap.** The cluster-side protocol is complete, but the *joining
node's own startup* is not. `Host.StartMetadataShard` and
`Host.StartPartition` both hard-code `nh.StartOnDiskReplica(initial,
join=false, ...)`. Dragonboat's contract for a node joining an existing
Raft group is the opposite: `StartOnDiskReplica(nil, join=true, ...)`
after the existing leader has issued `SyncRequestAddReplica`. Reflow
has no `join=true` code path and no `reflowd --join` flag.

In practice this means: `add-node` against a live 3-node cluster
correctly updates the membership on the existing nodes, but the new
`reflowd` cannot itself come up against an established Raft group. The
missing pieces are minimal — `HostConfig.JoinExisting bool` flipping
both `StartOnDiskReplica` calls to `join=true` + `nil` initial members,
and a corresponding `reflowd` flag / config key. Tracked as **OPEN-1**
in §9 and as a GitHub issue (filed alongside this SAD revision).

**Failure detection.** Dragonboat's built-in gossip (memberlist/SWIM,
enabled via `NodeHostConfig.AddressByNodeHostID = true` + `GossipConfig`)
runs SWIM probes between every NodeHost. Each observer turns `K`
consecutive failed probes against node `X` into a `RemoveNode` proposal
to shard `0`; the metadata leader, seeing reports above the eviction
threshold, commits the membership change. Eviction is a
strongly-consistent decision driven by an eventually-consistent signal.

**Discovery & endpoint resolution.** Two complementary sources:

- *Authoritative (shard 0):* partition table (`shard_id → [node_id ...]`)
  and assignment epoch. All routing decisions that affect correctness
  read from here. (Schema/format-skew protection lives at the storage
  layer via `internal/storage/format.go`, not in shard 0.)
- *Hint cache (dragonboat gossip):* `NodeHostRegistry.GetShardInfo` exposes
  `ShardView{LeaderID, Replicas map[replicaID]raftAddr, Term}` for every
  shard cluster-wide, refreshed by gossip. The per-nodehost `Meta` blob
  carries the reflow gRPC endpoint so cross-partition delivery can dial
  directly by `NodeHostID` without re-reading shard 0 on the hot path.
  On `NOT_LEADER` from the RPC, fall back to shard 0 and retry.
  Gossip is *never* a source of truth — it just makes routing fast and
  decouples node identity from raft addresses (k8s IP churn no longer
  requires a shard-0 proposal).

**Partition count.** Fixed at cluster bootstrap (default 64). Partition
shards are the unit of scalability; rebalancing reassigns shards across
nodes without renaming partition_keys.

*Rationale (see §9 rows 1 & 4).* Constant `N` keeps routing fully
deterministic forever: `shard = PartitionKey % N` agrees across stale
nodes, ingress clients holding old `InvocationId`s, and the metadata-shard
FSM, with no epoch number or `(N, key)` tuple to carry. It also avoids the
split/merge protocol (atomic key-range move across two Pebble DBs + two
dragonboat groups + two leader log positions while in-flight invocations,
timers, and outbox rows are live) — a class of bugs we explicitly opt out
of. The unit of scale-out is moving a shard between nodes via the rebalancer,
not changing `N`. The trade is a hard ceiling on horizontal
scale (~`N` busy leaders) and permanent hot-key skew if a single
`(service, object_key)` becomes dominant; both are acceptable inside the
target envelope. Online resize of `N` is therefore **not supported** —
`Host.Partitioner()` reads `cfg.NumPartitionShards` at boot and never
recomputes the modulus from the metadata shard. *Shard reassignment*
across nodes (the rebalancer's job) is independent of this and remains in
scope: nodes still react to `PartitionTable` updates by starting /
stopping local Raft replicas as ownership shifts.

**Hard boundary the design enforces:** the metadata Raft group is the only
authoritative source of partition ownership. No node ever processes a
command for a partition it does not own according to its locally-observed
copy of the partition table. A stale node will fail the `IsLeader` check
on its `dragonboat` shard before any side effects can escape. Gossip can
never override this — it only feeds advisory signals (liveness reports,
leader hints, endpoint resolution) and Raft proposals are the only path
to authoritative state changes.

---

### 6.3 Partition Processor

The core execution engine. One instance per partition, runs on the Raft leader node.

**Responsibilities:**
- Drive the invocation lifecycle state machine
- Drive the Virtual Object queue state machine
- Replay journals for resuming suspended invocations
- Schedule handler execution against SDK connections
- Commit journal entries to the Raft log
- Apply committed entries to Pebble state

**Internal structure:**

```
Partition Processor
├── Command loop         (reads committed Raft entries, dispatches)
├── Invocation registry  (map[InvocationID → InvocationFSM])
├── VObject registry     (map[ObjectKey → VObjectFSM])
├── Handler scheduler    (assigns running invocations to SDK connections)
├── Journal writer       (proposes entries to Raft)
├── State store          (StateStore interface over Pebble)
├── Timer Service        (manages durable timers, in-memory heap + StateStore persistence)
```

**Threading model:**
- One goroutine per active invocation (blocked on SDK stream)
- One goroutine for the Raft apply loop (sequential, no parallelism)
- Timer goroutine blocked on `timerfd` epoll, wakes on next due entry

---

### 6.4 Invocation State Machine

Implemented as a plain switch over the persisted `InvocationStatus`
discriminated union (proto `oneof status { Free, Scheduled, Invoked,
Suspended, Completed }`), mirroring restate's
`crates/storage-api/src/invocation_status_table/mod.rs:141-155`. Each
command applied to the partition reads the current status, calls a pure
transition function (`transitionOnInvoke`, `transitionOnComplete`,
`transitionOnTimerFired`, etc.), writes the result back inside the same
storage batch, and pushes any side-effect intents onto the leader-only
`ActionCollector`. No FSM library is used; the persistent status row IS the
FSM state. Illegal transitions log a warning and become no-ops — returning
an error from `Update` would halt the shard (dragonboat
`statemachine/disk.go:113`).

The persisted oneof is `{Free, Scheduled, Invoked, Suspended, Completed}`
(`proto/enginev1/engine.proto`). `Free` is the absence-of-record state
returned when no row exists for an `InvocationId`. There is no separate
`Failed` / `Dead` / `Retrying` row — terminal failures land as
`Completed{failure=...}` and retry backoff is encoded as a `Suspended`
waiting on a timer.

```
                    ┌──────────┐
                    │   Free   │  (no row in inv/ table)
                    └────┬─────┘
                         │ InvokeCommand applied
                    ┌────▼─────┐
                    │Scheduled │  (queued for invoker; handler not yet picked up)
                    └────┬─────┘
                         │ handler claims slot
                    ┌────▼─────┐
                    │ Invoked  │◀──────────┐
                    └────┬─────┘           │ resume (timer / awakeable)
                         │                 │
                         │ awaits        ┌─┴──────────┐
                         ├──────────────▶│  Suspended │
                         │               └────────────┘
                         │ EndInvocation
                    ┌────▼─────┐
                    │ Completed│  (terminal — value OR failure)
                    └──────────┘
```

**Apply-arm triggers** (each maps to an `InvokerEffect` or external
command, never an in-FSM action):

- `InvokeCommand` → `Free → Scheduled`.
- `JournalAppended(input)` plus invoker registration → `Scheduled → Invoked`.
- `InvocationSuspended` → `Invoked → Suspended` (records waker IDs).
- `TimerFired` / `AwakeableResolved` / `SignalDelivered` → `Suspended → Invoked`.
- `InvocationCompleted` → `Invoked → Completed` (with value or failure).
- `PurgeInvocation` → row deleted, return to `Free`.

**On-entry actions** (pushed to the leader-only `ActionCollector` after
the storage batch commits):

- `Invoked` → push `ActInvoke` so the Invoker resumes the handler.
- `Suspended` → register pending wakers (timer entries already in
  storage from the same batch; no separate action).
- `Completed` → write output to state, queue outbox envelopes for any
  parent invocations, notify in-process awaits.

#### Differences from Restate's `InvocationStatus`

Restate's enum (`restate/crates/storage-api/src/invocation_status_table/mod.rs:142-155`)
has seven variants: `Scheduled, Inboxed, Invoked, Suspended, Paused,
Completed, Free`. Reflow has five: the two missing variants are
`Inboxed` and `Paused`. Both omissions are deliberate.

**No `Inboxed`.** Restate's `Inboxed(InboxedInvocation)`
(line 144, struct at line 551) carries `inbox_sequence_number` plus
pre-flight metadata and is used when a virtual-object key is busy:
the invocation sits in a per-key inbox waiting for the current holder
to release. The inbox position lives *inside the invocation status
row*.

Reflow factors this gating out into `KeyLeaseStatus`
(`proto/enginev1/engine.proto:436`) — a separate row per
`(service, object_key)` carrying the active invocation id and a FIFO
queue. The invocation itself stays in `Scheduled` until the lease
frees. The trade-off: one extra table lookup on apply, in exchange
for a single FSM shape that's identical for ordinary invocations and
queued VObject calls. The Restate model is denser; the reflow model
is simpler to test.

**No `Paused`.** Restate's `Paused(InFlightInvocationMetadata)`
(line 150) is an *operator-initiated* pause distinct from `Suspended`
(which awaits an internal completion). Triggered by `OnPausedCommand`
(`restate/crates/worker/src/partition/state_machine/lifecycle/paused.rs:30`);
resumed via `manual_resume.rs:49`. Restate keeps the two apart so the
"paused" condition survives crashes and so the resume command is
distinct from an automatic wake.

Reflow doesn't expose an operator-pause primitive today. If it lands
later, the natural shape is a `Suspended` row whose `awaiting_on`
includes a `pause:<reason>` waker, resumable via an Admin RPC that
proposes the matching wake — no new status variant needed.

---

### 6.5 Virtual Object State Machine

Each `(service, object_key)` has a `KeyLeaseStatus` row in the
`keylease/` namespace carrying the active invocation id and a FIFO queue.
Unlike the invocation FSM, the gate is implemented with `qmuntal/stateless`
(`internal/engine/object_fsm.go`) — the Active-reentry semantics
(transitioning Active → Active on `Complete` to promote the queue head)
were awkward to express as a plain switch and read clearly with
`stateless`'s `OnEntryFrom` / `PermitDynamic` primitives.

```
                      enqueue (queue head promoted via OnEntryFrom)
                              ┌─────────────────┐
                              ▼                 │
┌──────┐  enqueue   ┌──────────────────┐  complete (queue non-empty)
│ Idle │──────────▶│      Active      │─────────┘
└──────┘            │ current + queue  │
   ▲                └─────────┬────────┘
   │                          │ complete (queue empty)
   └──────────────────────────┘
```

States: `IDLE`, `ACTIVE`. Triggers: `vobjEnqueue(InvocationId)`,
`vobjComplete`. The FSM is constructed fresh inside each partition
apply call, fired against a working copy of `KeyLeaseStatus`, and
written back via `tables.KeyLeaseTable.Put` in the same Pebble batch
as the invocation status transition that triggered it
(`partition.go:onInvoke`, `releaseKeyLease`).

**Single-writer guarantee:** Only the row's `current_invocation` may
run. New invocations for an `ACTIVE` lease are appended to the queue
and stay in `Scheduled` status until promoted on the prior holder's
`Complete`.

---

### 6.6 Replicated Log (dragonboat + Pebble)

This layer replaces Bifrost entirely.

**dragonboat configuration:**
- One Raft group per partition
- `IOnDiskStateMachine` interface (state persists in Pebble, survives restarts without full log replay)
- Pebble as the Raft log store (via dragonboat's `tan` log backend or custom `ILogDB`)

**Write path:**
```
Partition Processor
  └── journal_writer.Propose(entry)
        └── dragonboat.Propose(groupID, entry)
              └── Raft consensus (replicated to peers)
                    └── IOnDiskStateMachine.Update(entries)
                          └── Pebble batch write
```

**Read path:**
```
Partition Processor
  └── state_reader.Lookup(query)
        └── IOnDiskStateMachine.Lookup(query)
              └── Pebble point read or prefix scan
```

**Snapshots:**

dragonboat triggers `IOnDiskStateMachine.SaveSnapshot` when the Raft log
grows beyond a threshold. Each snapshot is a Pebble Checkpoint
(`pebble v1.1.5 checkpoint.go:135`) — atomic, online, hard-link-backed
when on a real filesystem — tarred into the writer dragonboat hands us
(`internal/engine/snapshotter.go`).

Snapshots transit between replicas over dragonboat's own snapshot-transfer
protocol; the only on-disk artifact is the in-flight Checkpoint dir, deleted
after upload. Log is truncated past the snapshot index. Disk usage stays
bounded regardless of uptime.

The `SnapshotRepository` abstraction (see §6.12) is an optional archival
layer on top:

- Snapshots are still produced by the same Pebble Checkpoint path.
- When a repository is configured, `SaveSnapshot` tees the tar stream
  to both dragonboat's writer and the repository. A joining replica
  attempts a repository download before falling back to dragonboat
  snapshot transfer.
- When no repository is configured (default), only the dragonboat
  transfer path is active.

Cloud-backed repository drivers (S3, GCS, Azure Blob), retention policy,
and operator-facing `reflow snapshot` commands are implemented via a single
`BlobRepository` over `gocloud.dev/blob`.

The metadata shard (`shardID=0`) participates in the same mechanism;
its snapshots are small but include the partition table and are
therefore critical to back up.

---

### 6.7 Pebble Key Schema

Each partition owns its own Pebble DB at `${DataDir}/p{shardID}/state/`, so
keys do NOT carry a partition_id prefix — isolation is at the DB level.
Dragonboat keeps its own state under `${DataDir}/raft/`.

```
Namespace       Key structure                                    Value
─────────────────────────────────────────────────────────────────────────────
meta            meta                                             PartitionMeta (proto)

inv/            inv/<24-byte invocation_id>                      InvocationStatus (proto)

journal/        journal/<24-byte invocation_id>/<4-byte BE idx>  JournalEntry (proto)

timer/          timer/<8-byte BE fire_at_ms>/<24-byte id>        uint32 sleep_index

state/          state/<service>/<obj_key>/<state_key>            Virtual Object K/V state

dedup/self/     dedup/self/<8-byte BE leader_epoch>              DedupEntry (proto)
dedup/arb/      dedup/arbitrary/<producer_id>                    DedupEntry (proto)
```

`InvocationId` is the canonical 24-byte raw form (8-byte BE partition_key +
16-byte uuid), which mirrors restate `types/src/identifiers.rs:456-461`.
All multi-byte integers in keys are big-endian so lexicographic byte order
equals numeric order.

**Key design decisions:**
- One Pebble DB per partition removes the need for a partition_id prefix and
  simplifies snapshot / checkpoint isolation.
- Timer keys sort by `(fire_at_ms, invocation_id)`, so `TimerTable.ScanDue`
  is a bounded prefix scan.
- Journal entries use a monotonic `command_index` per invocation, distinct
  from the Raft log index, so log truncation doesn't leave gaps in the
  journal index space.
- The dedup table is namespaced by producer kind (`self` vs `arbitrary`) so
  the two sequence spaces never collide.

---

### 6.8 Journal and Replay

The journal is the mechanism that makes execution durable. Every side effect an invocation performs is recorded as a journal entry before being executed.

**Journal entry types:**

```protobuf
enum JournalEntryType {
  INPUT            = 0;  // initial input to handler
  CALL             = 1;  // outgoing service call
  CALL_RESULT      = 2;  // result of CALL
  ONE_WAY_CALL     = 3;  // fire-and-forget call
  SLEEP            = 4;  // timer/delay
  SLEEP_RESULT     = 5;  // timer fired
  GET_STATE        = 6;  // read object K/V
  SET_STATE        = 7;  // write object K/V
  CLEAR_STATE      = 8;  // delete object K/V
  AWAKEABLE        = 9;  // external completion handle
  COMPLETE_AWAKEABLE = 10;
  OUTPUT           = 11; // handler return value
}
```

**Replay logic:**
When an invocation resumes after a crash or suspension, the journal is replayed:
- If the next entry has a result already stored → return the stored result to the SDK (no re-execution)
- If the next entry has no result → execute the side effect, store the result, continue

This ensures exactly-once semantics for all external calls.

---

### 6.9 Timer Service

Durable timers are persisted in the partition's `timer/` table and driven
by a leader-only Go service.

**Implementation:** A single goroutine using `time.Timer` for wakeups.
Under heavy memory pressure `time.Timer` may deliver slightly late because
the Go scheduler is itself paused — durable timers don't lose entries, they
may just fire late. A `timerfd`+`epoll` upgrade (or migration to a
JIT-friendly clock) is deferred until a measured latency requirement
justifies the complexity.

**Architecture:** The TimerService owns an in-memory min-heap of timers
sorted by `(fire_at_ms, invocation_id)`. It is constructed for every
partition but only `Run`s on the leader; followers' service is idle.

**Write path:**
1. The partition's FSM, on applying a `Sleep` journal entry, writes a row
   to `timer/<fire_at><id>` in the storage batch and pushes
   `ActRegisterTimer` onto the leader's `ActionCollector`.
2. After the batch commits, the runner forwards the action to the
   TimerService, which heap-pushes the entry.

**Fire path:**
1. The Run loop waits for the earliest heap entry's `fire_at_ms`.
2. When due, the entry is popped and the leader proposes a `TimerFired`
   command via `RaftProposer.ProposeSelf`.
3. On commit, the FSM deletes the timer row, appends a `SleepResult`
   journal entry, and transitions the invocation status from Suspended →
   Invoked (pushing `ActInvoke` so the leader can resume execution).

**On leader gain:** `TimerService.Rebuild` scans the persistent `timer/`
prefix and rebuilds the heap. No timers are lost.

**Backpressure & error handling:** A failed propose re-pushes the timer
onto the heap (it is retried on the next fire-tick). The service NEVER
holds its mutex across the propose call.

**Raft tick loop:** dragonboat owns its own tick loop; reflow does not
intercept it. The original SAD claim that "the same timerfd drives Raft
heartbeats" was incorrect and is removed.

---

### 6.10 SDK Protocol

Handlers run as separate Go processes that host `pkg/sdk/server`, an
HTTP/2 server speaking the wire protocol defined in
`proto/protocolv1/protocol.proto`. The engine dials the handler endpoint
per invocation and POSTs a chunked, framed body to
`/invoke/<service>/<handler>`; the response body carries the handler →
engine frame stream. Polyglot SDKs (TS/Python/Java/Rust/...) ride the
same wire — there is no Go-specific fast path.

#### 6.10.1 Wire shape

Every frame is a 64-bit big-endian header (16-bit type code | 16-bit
flags | 32-bit payload length) followed by the protobuf payload. Type
codes are namespaced:

- `0x0000..0x00FF` — core lifecycle (StartMessage, SuspensionMessage,
  EndMessage, ErrorMessage)
- `0x0400..0x04FF` — command messages (SDK→engine: GetState, SetState,
  Sleep, Run, Call, Awakeable, ...)
- `0x8000..0x80FF` — notification messages (engine→SDK: completion +
  signal results)

The lifecycle is:

1. Engine opens a request stream and sends `StartMessage` with
   `known_entries`, the eager-state snapshot (`state_map` /
   `partial_state`), the deterministic `random_seed`, and the handler
   addressing tuple `(service, handler, key, kind)`.
2. Engine replays journaled entries as command + notification frames in
   journal order, in lockstep with handler progress.
3. SDK runs the handler function. Each `ctx.*` call either (a) returns
   a cached result if the journal already records the step, or (b)
   sends a command frame, blocks on the matching notification, and the
   engine journals the entry through a Raft propose.
4. Handler returns. SDK sends `OutputCommandMessage` + `EndMessage`.
   Engine flips the invocation to Completed.

Suspensions are represented as the SDK sending `SuspensionMessage` and
closing the stream. The engine parks the invocation; when a waker fires
(timer, awakeable resolved, signal delivered), the partition runner
re-issues the invocation from the top of replay.

Inspired by Restate's service-protocol v7 / journal-v2; field
numbering, package, and message set are reflow's. Discovery is a
separate one-shot probe defined in `proto/discoveryv1/discovery.proto`
— issued at RegisterDeployment time, not per invocation.

Compatibility: reflow tracks restate's wire format as a *best-effort*
target so existing TS/Python/Java/Rust SDK semantics translate with
minimal adaptation. We do not commit to bug-for-bug parity, nor to
keeping pace with every Restate release.

#### 6.10.2 Engine-side dispatch

`internal/engine/handlerclient` owns the engine → handler dial: a
keep-alive HTTP/2 client per registered deployment, one stream per
invocation. `internal/engine/invoker/wireSession` translates between
the HTTP/2 frame stream and the partition's `InvokerEffect` propose
path: every command frame becomes a journal-entry propose; every
notification frame is delivered as a completion.

There is no in-process fast path. `examples/embedded/main.go` shows
running the engine and a Go handler in one binary for local dev — the
engine still reaches the handler over a loopback HTTP/2 connection,
identical to the production path.

#### 6.10.3 State read journaling — deliberately partial

Restate's protocol distinguishes four state-read commands:
`GetEagerState` / `GetEagerStateKeys` (single-slot, value carried inline
on the journal entry — SDK already has the answer locally from
`StartMessage.state_map`) and `GetLazyState` / `GetLazyStateKeys`
(two-slot, command + completion — engine resolves on apply). Reflow
wires only **three** of the four:

| Command             | Reflow wire path | Slot cost            |
|---------------------|------------------|----------------------|
| `GetLazyState`      | wired            | 2 (cmd + result)     |
| `GetLazyStateKeys`  | wired            | 2 (cmd + result)     |
| `GetEagerStateKeys` | wired            | 1 (cmd, keys inline) |
| `GetEagerState`     | **dropped**      | n/a — see below      |

Individual cache-hit `GetState` reads are not journaled. The SDK reads
from `wireContext.stateCache` (populated from `StartMessage.state_map`
plus session writes) and returns the value silently — slot cost 0.

**Why we skip per-read eager journaling.** Reflow's apply model gets
eager-read determinism for free that Restate has to recover by journaling:

- Virtual objects serialize. Only one session per `(service, object_key)`
  runs at a time; the next session starts only after the prior one's
  writes have committed to durable state.
- `state_map` is recomputed from durable state at every session start
  via `preloadEagerState` (`internal/engine/invoker/common.go`). So a
  session-2 snapshot reflects every session-1 write.
- Read-after-write inside the session is served by `stateCache` (mutated
  inline by `SetState` / `ClearState` / `ClearAllState`).

The only divergence risk that survives is "eager cap caused a key that
fit on session 1 to fall out on session 2" — and that's already covered
by the lazy-fetch fallback (`partial_state=true` triggers a 2-slot
journaled `GetLazyState`). The marginal win from also journaling
cache-hit reads is "catch a `preloadEagerState` bug at the read site
instead of at the next `SetState`," which is small in exchange for **one
journal slot per state read** (a state-heavy handler doing 100 reads
goes from 0 to 100 extra slots).

The proto type `GetEagerStateCommandMessage` (was wire type 0x0407) and
the journal-entry variant `JEGetEagerState` (was oneof tag 15) were
deleted to avoid hinting at a path the engine doesn't implement.
Tag 15 in `JournalEntry.entry` is free; tag 0x0407 in the protocolv1
type-code space is free. Reflow is preproduction so we are not
preserving them.

**Why we keep `GetEagerStateKeys`.** `GetStateKeys` is a single bulk
read — it's not on the hot path the way `GetState` is, and going
2-slot-lazy even when the snapshot is complete was a clear
slot-cost regression. The eager-keys variant lets the SDK ship the
sorted key list (derived locally from `stateCache`) in a single inline
journal frame when `partial_state=false`. On replay the slot's frame
type code (`TypeCmdGetEagerStateKeys` vs `TypeCmdGetLazyStateKeys`)
decides the path so a `partial_state` flip between sessions doesn't
misalign slot counts — the journal stays the source of truth.

If we ever do need explicit per-read eager journaling (e.g. as a
defense-in-depth gate against a `preloadEagerState` regression, or for
cross-implementation wire-protocol parity with a Restate handler
binary), the work is: re-add `GetEagerStateCommandMessage` at wire type
0x0407, re-add `JEGetEagerState` at tag 15 in `JournalEntry.entry`,
wire an emit branch on cache-hit in `pkg/handler/wirecontext.go:GetState`,
and add the apply-path + replay-translation cases that mirror the
existing `GetEagerStateKeys` path.

---

### 6.11 Storage Pluggability

Bifrost's provider abstraction (local vs. replicated loglet) is replaced by three independent pluggability points already present in the stack. No custom provider layer is required.

#### `vfs.FS` — Pebble filesystem backend

Pebble abstracts the filesystem via `vfs.FS`. Swapping the implementation changes the storage medium with no other code changes:

```go
// production
pebble.Open(dir, &pebble.Options{FS: vfs.Default})

// development / CI — fully in-memory, zero disk I/O
pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
```

`vfs.MemFS` replaces Bifrost's `local-loglet` for development and test environments. There is no special code path — the same Partition Processor runs against either backend.

#### `ILogDB` — dragonboat Raft log storage

dragonboat exposes the Raft log store behind an interface:

```go
type ILogDB interface {
    SaveRaftState(updates []pb.Update, shardID uint64) error
    IterateEntries(ents []pb.Entry, size uint64, shardID uint64, ...) ([]pb.Entry, uint64, error)
    // ...
}
```

The default implementation (`tan`) uses Pebble internally. A custom `ILogDB` can back the Raft log with any durable store — remote object storage, an alternative embedded DB, or a test double — without touching the engine.

#### `StateStore` — state machine storage interface

All Virtual Object K/V state, invocation records, and journal entries are accessed through a `StateStore` interface. Pebble is the default implementation:

```go
type StateStore interface {
    Get(key []byte) ([]byte, error)
    Set(key, value []byte) error
    Delete(key []byte) error
    Scan(prefix []byte) Iterator
    Batch() BatchWriter
}
```

This keeps the state machine logic decoupled from Pebble, enables unit testing with an in-memory map implementation, and leaves the door open for alternative backends.

#### Pluggability map

| Bifrost concept | Go equivalent | Pluggable via |
|---|---|---|
| `local-loglet` (dev/test) | `vfs.MemFS` | Pebble `vfs.FS` |
| `local-loglet` (production) | `vfs.Default` | Pebble `vfs.FS` |
| `replicated-loglet` | dragonboat Raft group | `ILogDB` interface |
| Alternative Raft log backends | Custom `ILogDB` impl | `ILogDB` interface |
| State machine storage | `StateStore` interface | Custom impl |

---

### 6.12 Snapshot Repository

Object storage is reflow's snapshot **archival** layer. It is optional: the
default deployment uses only the local filesystem and remains
zero-external-dep. When configured, it enables fast multi-node replica
catch-up and operator-facing backup / restore / migration workflows.

**Boundary (preserved across all phases):**

- Hot partition state lives in local Pebble. Never in object storage.
- Dedup, applied index, journal entries, timer table — all local.
- Object storage holds *only* snapshot artifacts and their metadata.
- A misconfigured or unreachable repository must not stop reflow from
  running. The local snapshot path is the always-available baseline.

**Interface:**

```go
type Repository interface {
    // NewWriter opens an upload stream for (shardID, raftIndex). The
    // returned WriteCloser frames bytes through gzip + sha256
    // internally; Close finalizes the archive, writes the .meta.json
    // sidecar, and enforces inline retention. Atomic on Close —
    // abandoned writers leave no observable artifact. Refuses
    // overwrite when (shardID, raftIndex) already exists.
    NewWriter(ctx context.Context, shardID, raftIndex uint64) (io.WriteCloser, error)

    // NewReader opens a download stream for (shardID, raftIndex);
    // gzip is stripped automatically. Returns an error satisfying
    // gcerrors.Code(err) == gcerrors.NotFound when absent.
    NewReader(ctx context.Context, shardID, raftIndex uint64) (io.ReadCloser, error)

    // List returns refs sorted by index ascending.
    List(ctx context.Context, shardID uint64) ([]SnapshotRef, error)

    // Delete removes (shardID, raftIndex); no-op when absent.
    Delete(ctx context.Context, shardID, raftIndex uint64) error
}
```

The writer/reader shape (instead of `Save(io.Reader)/Load(io.ReadCloser)`)
mirrors `gocloud.dev/blob.Bucket.NewWriter` directly and lets the
tee path into `Snapshotter.SaveSnapshot` use `io.MultiWriter(dragonboatW, repoW)`
with no `io.Pipe` plumbing. Callers archiving a directory use the
package-level `SaveDir` / `RestoreDir` helpers, which wrap
`TarDir` / `UntarDir` around the stream.

**Library: `gocloud.dev/blob`.** Apache 2.0, single interface over S3 /
GCS / Azure Blob / local filesystem / in-memory. `BlobRepository`
(`internal/engine/snapshot/blob.go`) is the only concrete implementation
and covers every scheme: `s3`, `gs`, `azblob`, `file`, `mem`.

**Object layout:**

```
{prefix}/p{shardID:08d}/snapshot-{raftIndex:020d}.tar.gz
{prefix}/p{shardID:08d}/snapshot-{raftIndex:020d}.meta.json
```

The archive is gzip-compressed tar; DR snapshots are cold and gzip is a
~30–50% size win over raw tar. The `.meta.json` sidecar (protojson-
serialized `enginev1.SnapshotMeta`) carries `{shard_id, raft_index,
leader_epoch, reflow_version, checksum_sha256, created_at_ms}` so an
operator listing a bucket can identify snapshots without unpacking them.

**Configuration:** `Snapshot.URL` selects the bucket; gocloud's native
`?prefix=` URL parameter places the archive under a sub-folder.

```
file:///mnt/reflow-snaps          local fs / NFS / shared volume
s3://my-bucket?prefix=reflow/     AWS S3
gs://my-bucket?prefix=reflow/     Google Cloud Storage
azblob://my-container?prefix=…    Azure Blob Storage
mem://                            in-memory (tests only)
```

Empty `Snapshot.URL` disables archiving; admin snapshot RPCs return
`FailedPrecondition`.

**Wiring into the snapshot path:**

- `Partition.SaveSnapshot` (`internal/engine/partition.go`) invokes a
  non-blocking `OnSnapshotPersisted` hook after dragonboat's snapshot
  write succeeds. The host fans the hook into a per-shard buffered-1
  trigger channel consumed by `snapshot.RunProducer`, which kicks an
  independent archive cycle (fresh Pebble checkpoint via
  `RequestSnapshot{Exported=true}` → `BlobRepository.SaveDir`). This
  is post-hoc rather than a tee: a slow object-store upload can never
  back-pressure dragonboat's inter-replica snapshot transfer, at the
  cost of one extra Pebble checkpoint (hard-link cheap) per archive.
- The producer still runs on `Snapshot.Interval` as a periodic
  fallback, so archives happen even when dragonboat is otherwise
  quiet.
- Cold-bootstrap restore (seeding a brand-new cluster from the
  archive) is an operator-driven workflow that pre-stages the
  archive into dragonboat's snapshot directory before node start. It
  is not threaded into the dragonboat `RecoverFromSnapshot` callback.

**Retention.** Three policies, pruned by a per-shard reaper goroutine:

- `Snapshot.Retain=N` — count: keep last N per shard. Enforced inline
  on `BlobRepository.Put` and re-checked by the reaper.
- `Snapshot.RetentionAge=720h` — age: drop archives whose mod time is
  older than the duration. Enforced by the reaper at hourly cadence.
- `Snapshot.TieredDaily / TieredWeekly / TieredMonthly` — GFS-style:
  keep one ref per recent UTC day / ISO week / calendar month, up to
  the configured slot count. Tiers cascade (a daily ref's week+month
  are "covered" — sibling refs in the same span aren't promoted to a
  coarser tier).

Tiered is exclusive: any non-zero `Tiered*` knob puts the shard's
reaper into tiered mode and disables both count and age policies for
that shard. A typical GFS preset — last 7 daily, 4 weekly, 12 monthly
— is `TieredDaily: 7, TieredWeekly: 4, TieredMonthly: 12`.

The reaper runs on every node (not metadata-leader-only) because
`Repository.Delete` is idempotent against missing keys — duplicate
deletes are benign. Revisit only if Delete RPS to the object store
becomes a billing concern.

**Encryption.** Server-side encryption (S3 SSE-KMS, GCS CMEK, Azure
SSE) is supported by passing the cloud-provider-native flags through;
`gocloud.dev/blob` exposes them as URL parameters. Client-side
encryption is not currently supported.

**Explicit non-features:**

- **Log archival.** Streaming every committed Raft entry to object
  storage for audit/replay is **not** on the roadmap. Snapshots plus
  dragonboat's retained log give point-in-time recovery sufficient for
  the target use cases. Separable feature if a user ever asks.
- **Cross-cluster snapshot sharing as authoritative state.** Snapshots
  are for replica catch-up and DR, not for two clusters to share a live
  view of the same partition.

---

### 6.13 Authentication & Authorization

Reflow's HTTP/2 surfaces (Connect ingress, REST `/v1/*`, internal
Admin/Delivery) share one inbound auth chain built on
`connectrpc.com/authn`. Two identity planes coexist; they serve
different purposes and are not interchangeable.

**Two planes, one principal model:**

| Plane | Identity primitive | Issued by | Runtime dep | Used for |
|---|---|---|---|---|
| SPIFFE / mTLS | `spiffe://<td>/<kind>/<name>` URI SAN on the verified leaf | Reflow's offline CA (`reflowd pki`) | none | cluster mesh + Admin/Delivery |
| OIDC bearer | JWT with claims mapped to `Principal{Kind, Subject}` | Customer's IdP | IdP reachable | ingress + optionally Admin via `kind=operator` |

Both planes produce the same `auth.Principal{Kind, Subject, Raw, …}`
shape, so the downstream policy match (`operator/*`, `node/*`) is
identical regardless of how the principal was established.

**The auth chain (`internal/auth.HTTPMiddleware`):**

1. **`authn.Middleware` runs an `AuthFunc`** that chains two
   authenticators (`internal/auth/connect.go:composeAuthFunc`):
   - `spiffeAuthFunc` inspects `r.TLS.VerifiedChains[0][0].URIs[0]`
     and parses the SPIFFE URI. Verified leaf with one well-formed
     `spiffe://<td>/<kind>/<name>` URI → `Principal{Kind, Subject}`.
     No TLS / zero URIs → fall through. Malformed URI (wrong scheme,
     wrong trust domain, missing segments, multiple URIs) → hard
     `connect.CodeUnauthenticated`.
   - `bearerAuthFunc` (active only when `cfg.Auth.OIDC` is
     non-empty) pulls `Authorization: Bearer …`, dispatches by
     unverified `iss` claim, then re-verifies signature + claims via
     `github.com/coreos/go-oidc/v3`.
   - **mTLS wins when both are present.** A leaked bearer token
     cannot also forge a peer-verified TLS leaf. When mTLS yields a
     non-anonymous principal, the bearer is ignored (debug-logged).

2. **`policyHandler` enforces the path-glob policy** declared in
   `starter_policy.json` (embedded default) or `cfg.Auth.PolicyFile`.
   Denial emits Connect-coded errors via `connect.ErrorWriter`:
   - `CodeUnauthenticated` (HTTP 401) for anonymous denials. The
     response carries `WWW-Authenticate: Bearer` (RFC 7235) when
     OIDC is wired, so non-mTLS clients know which scheme to try.
   - `CodePermissionDenied` (HTTP 403) for known-but-rejected
     principals. No `WWW-Authenticate` — they already authenticated.
   - Bearer-token verification failures from the AuthFunc step add
     `WWW-Authenticate: Bearer error="invalid_token"` (RFC 6750 §3)
     so clients refresh the token rather than retrying with the
     same bad one.

3. **The wrapped handler runs** with the `Principal` attached via
   `auth.ContextWithPrincipal`. The server-controlled
   `X-Reflow-Principal` request header is stamped (any forged inbound
   value is stripped first) so downstream handlers can match on a
   trusted string without re-parsing.

**Per-surface enforcement matrix:**

| Surface | TLS | AuthFunc step | Policy |
|--|--|--|--|
| Admin (`reflow.admin.v1.Admin`) | mTLS (operator), or via OIDC bearer | SPIFFE → operator/node; bearer → claim-mapped principal | `admin` rule allows `operator/*`; `admin-selfjoin` rule allows `node/*` only on `/SelfJoin` (with `checkSelfJoinPrincipal` further requiring URI's `<id>` == `req.node_id`) |
| Delivery (`reflow.delivery.v1.Delivery`) | mTLS (node) | SPIFFE only; bearer ignored (no IdP path for streaming inter-node) | `delivery` rule allows `node/*` |
| Connect ingress (`reflow.ingress.v1`) | Optional (h2c or TLS via `cfg.Ingress.Creds`) | Either; falls through to anonymous when neither present | `ingress_open` rule has no principal constraint by default; operators tighten via `cfg.Auth.PolicyFile` |
| REST ingress (`/v1/*`) | Same listener as Connect ingress | Same as Connect ingress | `ingress_rest_open` rule covers `/v1/*` through `/v1/*/*/*/*/*` |
| Engine → handler (`protocolv1`) | Out of scope here — owned by `pkg/reflow/creds` driver + `pkg/handler.Config` verifying via `RootCAs` / `AllowedSPIFFE` |

**Why both planes:** dropping SPIFFE in favor of OIDC-only would break
load-bearing pieces: (1) `Admin/SelfJoin`'s NodeID-binding gate
requires identity bound to a key in node-X's secret store, which mTLS
provides natively; (2) the dragonboat Raft transport and Delivery RPC
are TCP/streaming surfaces with no header to put a bearer token on;
(3) cluster admin must work when the IdP is unreachable, so the
offline CA is the dependency-free credential path. Dropping OIDC in
favor of SPIFFE-only is fine for closed deployments but limits
ingress to either anonymous-via-policy or mTLS-only (no SaaS-friendly
JWT path).

#### 6.13.1 OIDC as an operator credential

The composed AuthFunc means a single `kind=operator` claim from an
OIDC token produces the same `Principal{Kind: "operator", Subject:
…}` value as an offline-CA `spiffe://td/operator/…` leaf. The
`starter_policy.json` `admin` rule matches on `operator/*` regardless
of how the principal was established, so an OIDC-authenticated CI
pipeline can run `reflowd cluster {add-node, snapshot create, …}`
without ever holding an mTLS cert.

Concrete example — let GitHub Actions or a similar CI run admin RPCs
via OIDC instead of provisioning per-job certs:

```yaml
auth:
  oidc:
    - name: github-actions
      issuer_url: https://token.actions.githubusercontent.com
      audiences: [reflow]
      principal_claim: sub                 # → repo:owner/repo:ref:…
      kind_claim:      reflow_kind         # custom mapper claim
      required_claims:
        reflow_kind: operator              # only "operator"-shaped tokens
      allowed_claims: [repository, workflow, ref]
```

A workflow that mints a GitHub OIDC token with a custom
`reflow_kind: operator` mapper (and audience `reflow`) gets a
`Principal{Kind: "operator", Subject: "repo_org_reflow_main_…"}` —
matches `operator/*`, can hit `/Admin/*`. The `/` in the subject is
sanitized to `_` to keep IdP-controlled values out of
principal-glob traversal.

When operating both planes simultaneously:
- mTLS-presenting clients still win the composition, so a misconfigured
  IdP can't downgrade an operator that already has a cert.
- The 401 response on anonymous denial advertises `WWW-Authenticate:
  Bearer` so a CI tool that has only a token (no cert) discovers the
  scheme on the first attempt.
- Token expiry/rotation is bounded by the IdP; if the IdP is offline,
  `operator/*` mTLS certs still work for cluster recovery.

**Out of scope today (additive later):**

- OPA / fine-grained claim-based authz (the `Claims map[string]string`
  field stays as forward-compat plumbing only).
- Hot-reload of OIDC config (issuer changes require a restart, same as
  TLS material; the policy file is hot-reloaded every 30s).
- Sub-role taxonomy (`operator/readonly`, `operator/admin`); the
  policy-glob model can express this without code changes
  (`operator/readonly/*`), but no policy ships with that split today.
- Per-tenant ingress identity story for multi-tenant SaaS; today
  operators discriminate tenants via `pkg/hostmux` + per-host
  middleware that sets `Reflow-Meta-*` headers.

---

### 6.14 Webhook Ingress

Reflow ships built-in inbound signature verifiers for common vendor
webhooks. Config-driven mounting on the existing ingress listener
means an operator can wire a Stripe / GitHub / Slack webhook by
adding a YAML stanza — no per-vendor middleware to write.

**Built-in verifiers** (`internal/ingress/webhook/factory_*.go`):

| Vendor | Header | Algorithm | Replay window | Metadata stamped |
|---|---|---|---|---|
| Stripe | `Stripe-Signature: t=…,v1=…` | HMAC-SHA256 over `t + "." + body` | 5min (configurable per source) | `webhook_vendor`, `stripe_signed_timestamp` |
| GitHub | `X-Hub-Signature-256: sha256=…` | HMAC-SHA256 over body | none (no signed timestamp; dedup via `X-GitHub-Delivery`) | `webhook_vendor`, `github_event`, `github_delivery`, `github_hook_target_type` |
| Slack | `X-Slack-Signature: v0=…` + `X-Slack-Request-Timestamp` | HMAC-SHA256 over `"v0:" + ts + ":" + body` | 5min | `webhook_vendor`, `slack_signed_timestamp` |

Each verifier registers at package `init()` via
`pkg/webhook.RegisterVerifier`. Operators add custom vendors (Twilio,
PayPal, internal HMAC schemes, …) by implementing the same
`webhook.Verifier` interface and calling `RegisterVerifier` from their
handler binary's `main` before `reflow.Run`.

**Config shape** (`cfg.Webhooks`):

```yaml
webhooks:
  sources:
    - path: /webhooks/stripe
      verifier: stripe
      secret: ${env:STRIPE_WEBHOOK_SECRET}
      invocation:
        service: stripe-events
        handler: receive
        object_key: ""            # optional, for keyed handlers
        metadata:                  # static facts merged into ctx.Metadata()
          environment: prod
    - path: /webhooks/github
      verifier: github
      secret: ${env:GITHUB_WEBHOOK_SECRET}
      invocation:
        service: github-events
        handler: receive
```

**Request flow:**

1. POST arrives on the ingress listener at the configured path.
2. Auth middleware runs first — the starter policy's
   `webhooks_open` rule allows `/webhooks/*` anonymously (the
   signature *is* the auth; tighten via `cfg.Auth.PolicyFile`
   for per-vendor IP allowlists if needed).
3. Manager dispatches to the registered verifier:
   - Verifier reads the body (bounded at 1 MiB via
     `http.MaxBytesReader`), computes HMAC, compares
     constant-time, optionally enforces replay window.
   - On success → `VerifiedEvent{Body, Metadata}`.
   - On failure → `*connect.Error` with `CodeUnauthenticated`,
     surfaced as HTTP 401.
4. Manager merges static `Invocation.Metadata` with
   verifier-stamped facts (verifier wins on collision so an
   operator can't override `stripe_signed_timestamp` with a stale
   literal), builds `SubmitInvocationRequest`, dispatches to the
   in-process `*ingress.Server`.
5. Response: 202 Accepted with the `invocation_id_str` body on
   successful submit; HTTP-coded error on verifier or submit
   failure.

**Why ship verifiers built-in (vs. operator-writes-everything):** the
Stripe / GitHub / Slack schemes have well-defined, stable specs (last
breaking changes 2019, 2020, never). One audited implementation per
vendor is safer than every operator re-implementing HMAC with
`crypto/hmac` and hoping they got the timing-safe compare right. The
3-vendor scope is deliberately small — the long tail (Twilio's
URL+params reconstruction, PayPal's cert chain, AWS SNS) is heavier
and lower-volume, and operators with those needs can write a
`Verifier` impl and call `RegisterVerifier` without forking.

**Out of scope today:**

- Secret backends beyond literal/env: vault, AWS Secrets Manager,
  etc. Operators today resolve secrets via the koanf env provider
  + their own secret-injection infra (CSI driver, init container).
- Hot-reload of `cfg.Webhooks.Sources`: changing sources requires
  a restart, same as the rest of cfg.
- Per-source rate limiting: webhook bursts (Stripe retries,
  GitHub app installs) can saturate ingress; today the engine's
  generic `cfg.Ingress.HTTP.MaxBodyBytes` + tcp-level limits are
  the only knobs.
- Outbound webhook delivery (Reflow → external system): handler
  code can use `net/http` directly; durable retries are the
  handler's responsibility via the SDK's `Run` combinator.

---

## 7. Key Data Flows

### 7.1 New Invocation (Happy Path)

```
1.  Client → ingress: POST /invoke/MyService/myMethod (or grpc Invoke).
2.  Ingress parses request, computes partition_id from
    hash(service + "/" + object_key) % num_partitions.
3.  Ingress dispatches InvokeCommand:
      - same-node partition leader: in-process Proposer.Propose
      - remote leader: gRPC Delivery.Deliver (mTLS, node/* role)
4.  Raft replicates the Envelope, the partition FSM applies it
    inside one storage batch: writes inv/<id>=Scheduled and the
    JEInput journal row, pushes ActInvoke onto ActionCollector.
5.  Runner consumes ActInvoke; ActionCollector flushes to the
    Invoker.
6.  Invoker resolves the deployment URL for (service, handler) via the
    shard-0 lookup, opens an HTTP/2 stream to the deployment, sends
    StartMessage.
7.  Handler streams Propose* frames back. Engine proposes each as a
    JournalEntry; once Raft commits, the engine acks the handler.
8.  Handler streams OutputCommandMessage / EndMessage. Engine proposes
    EndInvocation. The FSM flips inv/<id> to Completed and runs the
    on-entry actions (output stored, awaiters notified, outbox
    enqueued).
```

There is one handler-hosting path: HTTP/2 to a deployment process, regardless
of whether the handler is written in Go or another language. The
`examples/embedded/` setup runs both the engine and a Go handler in one `main`
for local development, but the engine still reaches it via HTTP/2 — no
in-process fast path exists.

### 7.2 Crash Recovery

```
1.  Node crashes mid-invocation.
2.  Raft detects leader failure, elects new leader on a peer that
    already replicates the partition.
3.  New leader's dragonboat reloads IOnDiskStateMachine — Pebble state
    is already on disk from prior batches; only entries past the
    applied index are replayed.
4.  Partition runner starts on the new leader. ActionCollector starts
    empty.
5.  Invoker scans the inv/ table for rows in Invoked state and
    re-queues them. (Suspended rows wait for their wakers — timers
    rebuilt from timer/, awakeables remain pending.)
6.  Per re-queued invocation, the Invoker resolves the deployment URL,
    opens a fresh HTTP/2 stream, and sends StartMessage with the
    full journal as replay frames. The handler returns cached results
    to the SDK Context calls so user code skips already-completed
    steps; execution resumes at the first un-journaled call.
```

### 7.3 Virtual Object Invocation

```
1. Invocation arrives for VirtualObject "UserAccount" key "user-123"
2. Partition Processor checks VObject FSM for "UserAccount/user-123"
3. FSM state = Idle → transition to Processing, run invocation immediately
4. FSM state = Processing → enqueue invocation in Pebble obj/queue/
5. Current invocation completes → VObject FSM dequeues next → Processing
6. Queue empty → FSM: Draining → Idle
```

### 7.4 Suspension (Waiting on External Event)

```
1. SDK handler calls ctx.Awakeable() → runtime returns (id, promise)
2. Runtime proposes AWAKEABLE journal entry, stores handle in Pebble
3. SDK handler calls ctx.Await(promise) → streams Await command
4. Invocation FSM: Running → Suspended
5. Handler goroutine released, HTTP/2 stream closed
6. External caller POST /restate/awakeables/{id}/resolve with result
7. Ingress proposes CompleteAwakeable command to Raft
8. Entry applied → invocation FSM: Suspended → Running (Resume trigger)
9. Handler re-scheduled, journal replayed to suspension point
10. AWAKEABLE_RESULT streamed to SDK → execution continues
```

---

## 8. Deployment Architecture

### Single Node (Development)
```
┌─────────────────────────────────┐
│  Single Go binary               │
│  - All partitions local         │
│  - Single-node Raft groups      │
│  - Pebble in-process            │
│  - No network replication       │
└─────────────────────────────────┘
```

### Multi-Node (Production)
```
Node A                Node B                Node C
┌────────────┐        ┌────────────┐        ┌────────────┐
│ Part 0 (L) │◀──────▶│ Part 0 (F) │◀──────▶│ Part 0 (F) │  Raft group 0
│ Part 1 (F) │◀──────▶│ Part 1 (L) │◀──────▶│ Part 1 (F) │  Raft group 1
│ Part 2 (F) │◀──────▶│ Part 2 (F) │◀──────▶│ Part 2 (L) │  Raft group 2
└────────────┘        └────────────┘        └────────────┘
L = Raft Leader (active processor)
F = Raft Follower (standby, in-sync replica)
```

Minimum production deployment: 3 nodes (Raft quorum = 2).

---

## 9. Open Questions

| # | Question | Impact | Notes |
|---|---|---|---|
| 1 | Fixed vs. dynamic partition count | Resolved | Fixed at bootstrap (default 64). Split/merge is explicitly not on the roadmap. |
| 2 | Node discovery mechanism | Resolved | Embedded metadata Raft group (`shardID=0`) is authoritative for partition ownership; dragonboat's built-in gossip (memberlist/SWIM, no extra dependency) provides endpoint resolution and a leader hint cache. Static peer bootstrap (`--bootstrap-cluster` / `--join`). No external service required. See §6.2. |
| 3 | In-process Go SDK vs. external SDK only | Resolved | Out-of-process only. All handlers (including Go) speak `protocolv1` over HTTP/2 to the engine — see §6.10. The Go SDK lives at `pkg/sdk` + `pkg/sdk/server`; non-Go SDKs are community-driven. |
| 4 | Partition count default | Resolved | 64 partitions at cluster bootstrap. |
| 5 | Raft replication factor | Resolved | Default 3, configurable per deployment via `--replication-factor`. Three is the minimum that tolerates a single failure with quorum; >3 trades write latency for durability. Decided per deployment, no SAD-level open question remains. |
| 6 | Pebble per-partition vs. shared | Resolved | Per-partition Pebble DB; no `partition_id` prefix in keys. |
| 7 | Exactly-once for non-idempotent external calls | Resolved | Idempotency keys propagate through `Invoke` via the `Dedup` field on `Envelope` (`enginev1/engine.proto`). The dedup table (`dedup/self` for self-proposals, `dedup/arb` for external producers like ingress) is consulted on every apply; duplicates are dropped before state mutation. See `internal/storage/tables/dedup.go` and `internal/storage/tables/idempotency.go`. |
| 8 | SDK protocol versioning | Resolved | Wire protocol (`protocolv1`) tracks restate service-protocol v7 / journal-v2 as a *best-effort* compat target, not bug-for-bug. Negotiated at RegisterDeployment via `discoveryv1.DiscoveryResponse.protocol_version`. |
| 9 | timerfd vs `time.Timer` | Resolved | `time.Timer`; revisit only with measured latency requirements. |
| 10 | `StateStore` alternative implementations | Resolved | `internal/storage.Store` interface; `MemStore` (tests) + `PebbleStore` (production). |
| 11 | Gossip for failure detection + soft state | Resolved | dragonboat's built-in gossip (memberlist/SWIM, vendored inside `lni/dragonboat/v4`) — zero extra dependency. Provides SWIM-based liveness, NodeHostID-stable endpoint resolution, and a `ShardView` leader hint cache. Architectural boundary unchanged: gossip is advisory, Raft (shard 0) is authoritative — eviction and partition assignment always go through a Raft proposal. Soft-state dissemination beyond the per-nodehost `Meta` blob is deferred; revisit only if observed load-hint dissemination requirements outgrow `Meta`. |
| 12 | Object storage for snapshots | Resolved | `SnapshotRepository` interface with filesystem and cloud drivers (S3/GCS/Azure via `gocloud.dev/blob`). Always optional — default deployment is local-only. Hot state never leaves Pebble; only snapshot artifacts and their metadata go to object storage. See §6.12. |
| 13 | Authn/authz model for internal gRPC | Resolved | Single-CA SPIFFE URI identity over mTLS; `internal/auth` package owns `ClaimMapper` + `Authorizer`; per-RPC policy declared in proto via `optionsv1` annotations. TLS layer reduced to chain + URI well-formedness; role enforcement lives entirely in `auth.UnaryInterceptor` / `auth.StreamInterceptor`. JWT/OIDC mapper is an additive future. Ingress + SDK-session authz are separate identity models, out of scope here. See §6.13. |
| 14 | SDK transport for non-Go handlers | Resolved | The engine dials every handler over raw HTTP/2 using `protocolv1`. Same path for Go and non-Go SDKs; no transport variants. See §6.10. |
| OPEN-1 | Joining-node startup against a live cluster | Open | The admin `AddNode` RPC, cluster FSM, and metadata rebalancer that drive cluster-side membership changes work end-to-end (`SyncRequestAddNonVoting` → catch-up → `SyncRequestAddReplica`). The gap is on the joining node's own `reflowd` startup: `Host.StartMetadataShard` and `Host.StartPartition` both hard-code `nh.StartOnDiskReplica(initial, join=false, ...)`. A new peer joining an established Raft group needs `StartOnDiskReplica(nil, join=true, ...)`. Missing pieces are minimal — a `HostConfig.JoinExisting bool` and a `reflowd --join` flag / config key. Tracked as a GitHub issue. |

---

## 10. Risks

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| Journal replay correctness bugs | High | Critical | Extensive property-based testing; formal spec |
| GC pauses causing Raft timeouts | Low | Medium | Tune `RTTMillisecond`/`HeartbeatRTT` generously; revisit if measured in load tests. timerfd integration deferred. |
| Pebble key schema migration | Medium | Medium | Resolved: per-DB `format` key (`internal/storage/format.go`) written on first open and checked on every subsequent open; mismatches fail loud rather than silently corrupting. `VersionBarrier` retired. |
| dragonboat API stability | Medium | Medium | Pinned to v4 pseudo-version; Pebble pinned to dragonboat's expected commit. Watch for an official v4 release. |
| SDK protocol breaking changes | Medium | High | Tracks restate service-protocol-v4 wire format as a best-effort compat target (avoid inventing a competing one). |
| Partition rebalancing data loss | Low | Critical | Test membership changes under load; chaos test coverage in `internal/chaos/`. |

---

## 11. Delivery History

### Single Node Foundation
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
- Prometheus metrics + structured logging + `cmd/reflowd` single binary.
- Integration tests: replay across restart, dedup blocking, timer survives
  restart.

### In-process Go SDK + Invoker

The first-class developer experience: write a Go function, register it
with `reflowd`, have it become a durable goroutine.

- **`sdk.Context`** Go API in `pkg/sdk/` (the durable-execution handle
  exposed to handler authors) and the handler-side HTTP/2 runtime in
  `pkg/sdk/server/` that translates between the wire and the Context
  methods.
- **Per-partition Invoker** (`internal/engine/invoker/`) — session
  bookkeeping per active invocation, journal reader for replay,
  `InvokerEffect` proposals via `Proposer.ProposeSelf`. The actual
  handler runs in a separate process; the Invoker drives it over an
  HTTP/2 frame stream (see §6.10).
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

### Virtual Objects

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

### Combinator Futures

`Promise.all` / `Promise.race` over awakeable / call / signal
completions, persisted as a single journal entry whose pending-set
shrinks as completions land. Lets handlers fan out durable work and
join on the first-N / all-N without bespoke bookkeeping in user code.

### Multi-Node Replication

Target: a 3–10 node cluster. No external coordination service introduced
(see §6.2).

**Embedded metadata Raft + static bootstrap.** Shard 0 hosts node
membership, partition table, assignment epoch; founder/joiner bootstrap via
`--bootstrap-cluster` / `--join`.

**Dynamic membership + failure detection + DR snapshots + mTLS admin.**
Dragonboat gossip (memberlist/SWIM) drives K-of-N liveness; SWIM observers
turn missed probes into `RemoveNode` proposals to shard 0. The cluster
admin CLI lives in `reflowd cluster` (`add-node`, `remove-node`,
`partitions list`, `partition move`). `SnapshotRepository` filesystem driver
wired. Admin Connect RPC server (`adminv1`) protected by mTLS.

**Storage format version marker.** Per-Pebble-DB `uint32` marker
(`internal/storage/format.go`). Refuses to open a DB written by a binary
with a different `StorageFormatVersion`. Replaced the earlier
"command-stream VersionBarrier" sketch.

**Single CA + SPIFFE URI SAN identity.** Collapsed operator-CA + node-CA
into one cluster CA; role moved into the SPIFFE URI SAN
(`spiffe://<td>/<kind>/<name>`). TLS verifier checks chain + URI prefix.

**Proto-annotation authz interceptor.** `proto/optionsv1` defines
`required_spiffe_role` (method) and `default_required_spiffe_role`
(service). Admin service annotated `operator`. `AuditInterceptor` +
`AuthzInterceptor` enforce against the compiled descriptor map.

**Authorizer + ClaimMapper consolidation.** Two-shaped authz across Admin
and Delivery collapsed into one Temporal-shaped `Authorizer` + `ClaimMapper`
seam in `internal/auth`. TLS layer reduced to URI well-formedness; role
enforcement lives entirely in `auth.UnaryInterceptor` /
`auth.StreamInterceptor`. Delivery service annotated `node`. See §6.13.

- **Embedded metadata Raft group** (`shardID = 0`) hosted by the same
  `NodeHost` as partition shards. Holds node list, partition table,
  partition assignment epoch.
- **Static peer bootstrap.** `--bootstrap-cluster` for the founder,
  `--join=<addr>` for joiners. No discovery service required.
- **`reflowd cluster` admin subcommands** for `add-node`, `remove-node`,
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
  publishes its reflow gRPC endpoint via the gossip `Meta` blob and reads
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

### REST ingress + caller metadata (done)

- **REST facade at `/v1/*`** (`internal/ingress/http/`). Mounted on the
  Connect ingress listener via `ingress.Config.ExtraRoutes` — same port,
  same TLS, same auth middleware. Verbs: `call` (submit + long-poll),
  `send` (submit-only), `attach`, `output`, `cancel`, `awakeables/.../resolve`,
  `promises/.../resolve`, `state`. Each handler builds a typed
  `*connect.Request[T]` and delegates to the in-process `*ingress.Server`
  — no business-logic duplication. Long-poll capped at 30s (vs Connect's
  60s) to stay under common LB idle timeouts. `ensureDeadline` middleware
  installs a default request timeout because `*ingress.Server` calls
  dragonboat `SyncRead` under the hood (rejects deadlineless contexts).
- **Caller metadata end-to-end.** `SubmitInvocationRequest.metadata`
  (`map<string,string>`) flows through `InvokeCommand.metadata` →
  `Scheduled.metadata` (slot-0 transient) → `JEInput.metadata` (durable
  in the journal) → `InputCommandMessage.headers` (sorted-by-key for
  deterministic replay bytes) → `wireContext.metadata` →
  `handler.Context.Metadata()`. REST surface convention: HTTP headers
  prefixed `Reflow-Meta-*` are lifted (lowercased + stripped) into the
  proto field, so operator HMAC-verifier middleware can stamp facts
  the durable handler reads without re-verifying.
- **`pkg/hostmux` operator primitive.** Trust-aware host dispatcher with
  atomic-swappable table for runtime reconfig. Lives in `pkg/` because
  it is operator infrastructure, not engine machinery — Reflow itself
  does not import it. Enables multi-tenant SaaS via per-host
  routing without Reflow owning tenant state.

### Production Hardening (in progress)

- **Cloud-backed `SnapshotRepository` drivers (done).** Single
  `BlobRepository` over `gocloud.dev/blob` covers S3, GCS, Azure Blob,
  filesystem, and in-memory. `.meta.json` sidecar per archive. Count,
  age, and GFS tiered retention via a per-shard reaper goroutine.
  Admin `DeleteSnapshot` RPC + `reflowd cluster snapshot delete` CLI.
  Server-side encryption flows through gocloud URL parameters. Restore
  RPC and DR/migration runbooks remain future work. See §6.12.
- Pebble snapshot tuning (compaction, log retention, checkpoint cadence).
- Load testing + chaos testing harness (jepsen-style at small scale).
- Admin API surface: partition status, invocation inspection, replay
  debugger, `purge_journal` / `kill_invocation` operations.
- Operational docs: deployment recipes, backup/restore, upgrade
  procedure (using the per-DB storage format marker from §6.2).
- **Non-Go SDKs (community-driven).** TypeScript / Python / Java / Kotlin
  / Rust SDKs talk to reflow via the same `protocolv1` HTTP/2 wire as the
  Go SDK (§6.10). These ride on whatever effort the community
  contributes; reflow itself guarantees the wire-protocol surface, not
  the SDK quality across languages.

---

## 12. Key Invariants

These must hold at all times and should be validated by tests and assertions:

1. **Journal is append-only.** Entries are never modified after commit.
2. **Replay is deterministic.** Given the same journal, replay always produces the same execution.
3. **One active handler per invocation.** No two goroutines drive the same invocation simultaneously.
4. **One active invocation per Virtual Object key.** Single-writer enforced by VObject FSM.
5. **Timer entries survive restarts.** Timer heap is always rebuilt from Pebble on startup.
6. **State machine transitions are gated on Raft commit.** No state mutation before Raft consensus.
7. **Exactly-once for journal entries.** Raft entry index is the idempotency key for state machine apply.
