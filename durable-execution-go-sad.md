# Solution Architecture Document
# Durable Execution Engine in Go

**Version:** 0.6 (Draft)
**Date:** 2026-05-13
**Status:** Phase 1 + Phase 2 invoker + Phase 4.2 multi-node (mTLS admin,
dynamic membership, DR snapshots) + Phase 4.3 auth consolidation
(single-CA SPIFFE identity, proto-annotation authz, single Authorizer
seam) implemented. Phase 3 (Virtual Objects) and Phase 5 (cloud snapshot
drivers, retention, non-Go SDKs) outstanding.

---

## 1. Overview

Reflow is a durable execution engine designed for Go shops that want a single
self-contained binary instead of a multi-component topology, and that want the
option of running their handlers in-process alongside the runtime. It is
inspired by Restate's design and borrows Restate's wire-level concepts where
applicable, but its operational profile is intentionally different:
Restate-grade durability and exactly-once semantics with SQLite-style
deployment ergonomics.

**One-line pitch.** *Single-binary durable execution for Go. One process,
one data directory, your workflows survive crashes.*

**Positioning.** Reflow is the right choice when:

- Your stack is Go-first and you want durable execution as a library or a
  single-binary service rather than a separate platform to operate.
- You want your handlers to run in-process with the runtime (one process, no
  network hop between handler and journal) — or via a wire protocol when the
  handler is in another language.
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
- **Single self-contained binary.** `reflowd` is the only process the
  operator runs. No external metadata store, no external log servers, no
  required sidecars, no Kubernetes operator. Static peer config is enough
  to form a multi-node cluster.
- **In-process Go SDK as a first-class path.** A Go handler can be a
  function in the same process as the runtime, with no network hop between
  SDK and journal. A wire-protocol path (HTTP/2) is supported for
  cross-language handlers but is not the primary developer experience.
- **Virtual Objects.** Stateful entities with single-writer consistency
  and durable K/V state (Phase 3).
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
  **optional** backend for snapshot archival (Phase 4–5). The default
  deployment uses local filesystem snapshots only and remains
  zero-external-dep.
- **Temporal-scale concurrency targets.** Designed for thousands of
  in-flight invocations on a small cluster, not millions on a hundred-node
  fleet.

---

## 4. Architecture Overview

```
                         ┌─────────────────────────────────────────┐
   SDK Handlers          │              Ingress Layer               │
   (TypeScript,   ──────▶│         HTTP/2 + gRPC gateway           │
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
| gRPC (ingress, admin, delivery, sdk session) | `google.golang.org/grpc` | Apache 2.0 |
| HTTP/2 ingress (REST gateway) | `grpc-ecosystem/grpc-gateway/v2` | BSD-3 |
| Snapshot archival (Phase 4 fs, Phase 5 cloud) | `gocloud.dev/blob` | Apache 2.0 |
| Serialization + IDL | `google.golang.org/protobuf` + `buf` v2 | BSD-3 / Apache 2.0 |
| Authn/Authz | stdlib `crypto/tls`, custom SPIFFE URI mapper + proto-annotation authz (`internal/auth`) | — |
| Structured logging | `log/slog` (stdlib) | — |
| Metrics | `prometheus/client_golang` | Apache 2.0 |

The invocation state machine uses a plain switch over the persisted
`InvocationStatus` discriminated union rather than an FSM library — see §6.4.
No third-party FSM dependency is required.

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
client-facing surface; SDK handler streaming is a *separate* gRPC
service (see §6.10.2), not part of ingress.

**Responsibilities:**
- Parse invocation requests (HTTP/JSON via grpc-gateway or native gRPC).
- Determine the target partition via consistent hashing on
  `(service_name, object_key)`.
- Forward invocation commands to the correct Partition Processor via
  internal gRPC (cross-node) or in-process call (co-located).
- Return invocation ID to caller immediately (async) or stream response
  (sync/await).

**Routing:**
```
partition_id = hash(service_name + "/" + object_key) % num_partitions
```

**gRPC surfaces hosted by reflowd (all distinct, by design):**

| Surface | Port (default) | Auth | Purpose |
|--|--|--|--|
| Ingress (`reflow.ingress.v1`) | 8080 | None today (client identity model TBD) | External callers submit invocations, resolve awakeables |
| SDK session (`reflow.sdk.v1.SessionService`) | 8080 (alongside ingress) | TBD (handler identity model TBD) | Out-of-process handlers — stub returns Unimplemented until routing lands |
| Delivery (`reflow.delivery.v1`) | 8081 | mTLS, `spiffe://<td>/node/*` enforced by `auth.StreamInterceptor` | Cross-partition / cross-node command forwarding |
| Admin (`reflow.admin.v1`) | 8082 | mTLS, `spiffe://<td>/operator/*` enforced by `auth.UnaryInterceptor` | Cluster ops: add/remove node, list partitions, snapshot mgmt |

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
reflow-cluster add-node    --node-id=4 --raft-addr=10.0.0.4:9091 \
                           --gossip-addr=10.0.0.4:9092 \
                           --grpc-endpoint=10.0.0.4:8081 \
                           --node-host-id=<uuid>
reflow-cluster remove-node --node-id=2
```

- **`add-node`** (`internal/engine/admin/server.go:AddNode`): proposes
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
  carries the reflow gRPC endpoint so cross-partition delivery (Phase 4)
  can dial directly by `NodeHostID` without re-reading shard 0 on the hot
  path. On `NOT_LEADER` from the RPC, fall back to shard 0 and retry.
  Gossip is *never* a source of truth — it just makes routing fast and
  decouples node identity from raft addresses (k8s IP churn no longer
  requires a shard-0 proposal).

**Partition count.** Fixed at cluster bootstrap (default 64). Partition
shards are the unit of scalability; rebalancing reassigns shards across
nodes without renaming partition_keys.

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

**`Killed` is not a Restate `InvocationStatus` variant.** It appears
in `InvocationStatusDiscriminants` (line 395) but is never constructed.
The only `Killed` in restate code is `vqueue_table::Status::Killed`
(`restate/crates/storage-api/src/vqueue_table/entry_status.rs:43`),
a queue-entry status one layer down. A killed invocation transitions
to `Completed` with a kill-flavoured failure — same as reflow.

---

### 6.5 Virtual Object State Machine

Phase 3. Same shape as §6.4: a persisted oneof per object key in the
`vobj/` namespace, applied via a plain switch in the FSM. No third-party
FSM library.

```
┌──────┐  enqueue   ┌────────────┐  queue_empty  ┌──────────┐
│ Idle │──────────▶│ Processing │──────────────▶│ Draining │──▶ Idle
└──────┘            └─────┬──────┘               └──────────┘
                          │ enqueue
                          ▼
                    (queue pending invocations,
                     process one at a time)
```

**Single-writer guarantee:** Only one invocation runs per object key at a time. New invocations for a busy object are enqueued in Pebble and dequeued when the current invocation completes or suspends.

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

Phase 1 ships the local-only path: snapshots transit between replicas
over dragonboat's own snapshot-transfer protocol; the only on-disk
artifact is the in-flight Checkpoint dir, deleted after upload. Log is
truncated past the snapshot index. Disk usage stays bounded regardless
of uptime.

Phase 4 introduces the `SnapshotRepository` abstraction (see §6.12):

- Snapshots are still produced by the same Pebble Checkpoint path.
- When a repository is configured, `SaveSnapshot` tees the tar stream
  to both dragonboat's writer and the repository. A joining replica
  attempts a repository download before falling back to dragonboat
  snapshot transfer.
- When no repository is configured (default), behavior is identical to
  Phase 1.

Phase 5 brings the cloud-backed repository drivers (S3, GCS, Azure
Blob), retention policy, and operator-facing `reflow snapshot` commands.

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

state/          state/<service>/<obj_key>/<state_key>            reserved for Phase 3

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

**Implementation (Phase 1):** A single goroutine using `time.Timer` for
wakeups. Honest about GC: under heavy memory pressure, `time.Timer` may
deliver slightly late because the scheduler is itself paused. For Phase 1
this is acceptable — durable timers don't lose entries, they may just fire
late. A `timerfd`+`epoll` upgrade (or migration to a JIT-friendly clock) is
deferred until a measured latency requirement justifies the complexity.

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
   Invoked (pushing `ActInvoke` so the leader can resume execution in
   Phase 2).

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

The runtime communicates with SDK handlers over HTTP/2 using a bidirectional streaming protocol. The SDK handler is a separate process (any language with an SDK).

Reflow supports two ways for a handler to talk to the runtime. Both share
the same `InvokerEffect` shape on the partition's apply path; the
difference is whether the bytes between handler and partition go through a
network socket or a Go function call.

#### 6.10.1 In-process Go handlers (primary path)

A Go handler registered through `reflow.RegisterService(...)` runs in the
same process as the runtime. The Invoker drives it via a typed Go
interface rather than HTTP/2:

- The handler implements `func(ctx reflow.Context, input []byte) ([]byte, error)`.
- `reflow.Context` exposes `Sleep`, `Run`, `Call`, `Get/Set/ClearState`,
  `Awakeable`, etc. — each one synchronously produces an `InvokerEffect`
  proposal and blocks the handler's goroutine until the effect is applied
  and the result is journaled.
- On replay, the same `reflow.Context` calls read from the journal instead
  of executing the side effect.
- No network hop between handler and runtime. No serialisation outside of
  what the journal needs anyway.

This is the recommended path for Go shops. It's also what makes the
"workflows are durable goroutines" pitch real.

#### 6.10.2 Out-of-process handlers via wire protocol (secondary path)

For non-Go handlers, reflow exposes a proto-defined bidirectional
streaming protocol. The wire contract lives in
`proto/sdkv1/sdk.proto`; every frame in either direction is the
`SDKMessage` oneof envelope (StartInvocation, ReplayEntry, EntryAck,
Completion on the engine→SDK side; ProposeEntry, ProposeRunCompletion,
SuspensionRequest, EndInvocation, ErrorMessage on the SDK→engine side;
ErrorMessage either direction). Message shapes mirror restate's
`service-protocol-v4` — same lifecycle, adapted for reflow's lazy-state
+ outbox model.

Compatibility note: reflow tracks restate's wire format as a
*best-effort* compatibility target so existing TS/Python/Java/Kotlin/Rust
SDK semantics translate with minimal adaptation. We do not commit to
bug-for-bug parity, nor to keeping pace with every Restate release.
Non-Go SDKs are explicitly out of scope for Phase 1–3 and ride along
on whatever effort the community contributes.

##### Dual transport

The same `sdkv1` message envelope can ride two different transports.
Reflow ships the first today; the second is documented but unimplemented.

**Transport A — handler dials engine (gRPC bidi, primary).**
The engine hosts `sdkv1.SessionService.Invoke` as a gRPC bidi-streaming
RPC. Out-of-process handler SDKs open one long-lived stream per worker
process and self-register; the engine routes pending `StartInvocation`
frames to a connected stream. Implementation lives in
`internal/sdkstream` (stub today, routing pool when the first non-Go
SDK ships).

- *Wire*: gRPC over HTTP/2 — inherits TLS, the
  `auth.StreamInterceptor` seam, deadlines, status codes, and
  metadata for free. No new dependency.
- *Topology*: handler is the gRPC client; engine is the server.
  Handlers can run behind NAT, no endpoint registry needed.
- *Fit*: long-lived worker pools, stateful handlers, on-prem
  deployments.

**Transport B — engine dials handler (HTTP/2 streaming, future).**
Mirrors Restate's deployment model: handler hosts an HTTP/2 endpoint;
engine dials it per invocation. Same `SDKMessage` payloads, framed as
length-prefixed records over raw HTTP/2 (no gRPC envelope). Engine
needs an endpoint registry — discovered via an admin RPC or pushed
through ingress at handler-register time.

- *Wire*: HTTP/2 streaming with `CUSTOM_MESSAGE_MASK` framing for
  forward-compat (see restate
  `crates/service-protocol-v4/src/message_codec/mod.rs:33`).
- *Topology*: handler is the HTTP server; engine is the client.
  Handler must be reachable on a known endpoint.
- *Fit*: serverless / FaaS handlers (Lambda, Cloudflare Workers),
  autoscaled handler fleets, cold-start-friendly invocations.

Transport B is **additive**, not a replacement for A. Adding it later
introduces a sibling service alongside `SessionService` and a per-handler
endpoint record in shard 0; the partition Invoker chooses which
transport to use based on how the handler registered. No proto
breakage — the envelope shape is shared.

| | Transport A (current) | Transport B (future) |
|--|--|--|
| Direction | Handler → engine | Engine → handler |
| Wire | gRPC bidi over HTTP/2 | Raw HTTP/2 length-prefix |
| Handler discovery | None — self-registers on connect | Endpoint registry in shard 0 |
| NAT/firewall | Friendly | Handler must be reachable |
| Serverless fit | Awkward (long-lived stream) | Native |
| Long-lived workers | Native | Awkward (engine reconnects per invoke) |
| Auth seam | `internal/auth.StreamInterceptor` | TBD (likely JWT/OIDC via header) |

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

### 6.12 Snapshot Repository (Phase 4+)

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
type SnapshotRepository interface {
    // Save streams a snapshot for (shardID, raftIndex). Atomic: if Save
    // returns nil the snapshot is durable and discoverable by List.
    Save(ctx context.Context, shardID, raftIndex uint64, r io.Reader) error

    // Load streams the snapshot identified by (shardID, raftIndex).
    // Returns ErrNotFound if absent.
    Load(ctx context.Context, shardID, raftIndex uint64) (io.ReadCloser, error)

    // List enumerates snapshots for a shard, newest first.
    List(ctx context.Context, shardID uint64) ([]SnapshotID, error)

    // Delete removes a snapshot. Used by retention.
    Delete(ctx context.Context, shardID, raftIndex uint64) error
}
```

**Library: `gocloud.dev/blob`.** Apache 2.0, single interface over S3 /
GCS / Azure Blob / local filesystem / in-memory. The local-filesystem
driver (`fileblob`) is used in development; the in-memory driver
(`memblob`) is used in tests; the cloud drivers ship in Phase 5.

**Object layout:**

```
{prefix}/p{shardID:08d}/snapshot-{raftIndex:020d}.tar
{prefix}/p{shardID:08d}/snapshot-{raftIndex:020d}.meta.json
```

The `.meta.json` carries `{shard_id, raft_index, leader_epoch,
reflow_version, checksum, created_at_ms}` so an operator listing a
bucket can identify snapshots without unpacking them.

**Configuration:**

```bash
reflowd --snapshot-store=s3://bucket/reflow      # cloud
reflowd --snapshot-store=file:///mnt/snaps       # NFS / shared volume
reflowd                                          # default: local only
```

**Wiring into the snapshot path:**

- `Snapshotter.SaveSnapshot` (`internal/engine/snapshotter.go`) tees the
  tar stream to both dragonboat's `io.Writer` and the repository upload
  when a repository is configured. Single Pebble Checkpoint feeds both.
- `Snapshotter.RecoverFromSnapshot`: try repository first
  (`Load(shardID, raftIndex)`); fall back to dragonboat snapshot
  transfer on miss or unconfigured.
- A joining replica's catch-up path consults `List(shardID)` to pick the
  newest available snapshot.

**Retention (Phase 5):** policies pruned by a small reaper goroutine on
the metadata leader:

- `--snapshot-retention-count=N` — keep last N per shard.
- `--snapshot-retention-age=720h` — keep snapshots younger than the
  duration.
- `--snapshot-retention-policy=tiered` — GFS-style: daily for 7d, weekly
  for 4w, monthly for 1y.

**Encryption.** Server-side encryption (S3 SSE-KMS, GCS CMEK, Azure
SSE) is supported by passing the cloud-provider-native flags through;
`gocloud.dev/blob` exposes them as URL parameters. Client-side
encryption is out of scope for Phase 5.

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

Reflow's internal gRPC surfaces (Admin, Delivery, future SDK session) run
under one auth model. The model is:

1. **Transport identity is SPIFFE-shaped, single-CA.**
   All inter-node and operator traffic uses mTLS against a single cluster
   CA. Each peer's leaf certificate carries exactly one URI SAN of the form
   `spiffe://<trust-domain>/<kind>/<name>`. `kind` is `node` for a reflowd
   peer (Delivery surface) or `operator` for a human/automation principal
   (Admin surface). Trust domain is configurable; default `reflow.local`.
   The TLS layer (`pkg/reflow/tls.go`) validates chain + URI
   well-formedness only — it does **not** enforce role at the handshake.

2. **Identity is mapped to Claims by a ClaimMapper.**
   Inside the gRPC interceptor, `internal/auth.CertClaimMapper` reads
   `peer.AuthInfo` → `credentials.TLSInfo` → verified leaf URI and
   produces a `Claims{Kind, Subject, URI, Extensions}` value. The
   `ClaimMapper` interface (Temporal-shaped) is the seam where a future
   `JWTClaimMapper` plugs in for non-cert callers (e.g., ingress clients,
   serverless handlers in Transport B); `AuthInfo.AuthToken` is already
   wired but unread today.

3. **Authorization is declared in the proto IDL.**
   `proto/optionsv1/options.proto` defines two custom options:
   `required_spiffe_role` (MethodOptions) and
   `default_required_spiffe_role` (ServiceOptions). Each service annotates
   itself once; methods override only when they need to differ. At server
   startup, `auth.BuildMethodPolicy` walks the proto descriptor and
   compiles a `map[FullMethod]role`. Drift between an annotated proto
   and missing handler enforcement is impossible — the map is the
   enforcement.

4. **Authorizer is the single decision point.**
   `auth.ProtoPolicyAuthorizer` consults the compiled map and answers
   `Authorize(ctx, claims, &CallTarget{APIName: fullMethod})`. The
   default `RoleMatcher` is exact-Kind equality (`claims.Kind == required`);
   sub-role work (`operator/readonly`) plugs in a path-prefix matcher via
   `WithMatcher(...)`. Fail-closed on unknown methods.

5. **One interceptor pair owns the chain.**
   `auth.UnaryInterceptor` and `auth.StreamInterceptor` run
   `ClaimMapper.GetClaims` → audit log → `Authorizer.Authorize` →
   dispatch (or reject with `Unauthenticated` / `PermissionDenied`).
   The successful handler sees the `Claims` on context via
   `auth.ClaimsFromContext(ctx)`.

**Per-surface enforcement matrix:**

| Surface | TLS | ClaimMapper | Authorizer policy source |
|--|--|--|--|
| Admin | mTLS, well-formed SPIFFE URI | `CertClaimMapper` | `adminv1.Admin` service annotation: `operator` |
| Delivery | mTLS, well-formed SPIFFE URI | `CertClaimMapper` | `deliveryv1.Delivery` service annotation: `node` |
| Ingress | None today | None today | None today (identity model TBD) |
| SDK session | Stub | Stub | Stub (out-of-process handler identity model TBD) |

**Multi-language clients.** Custom proto options ride on standard
`google.protobuf.MethodOptions/ServiceOptions`, so any buf-generated
client can introspect the required role for any method via reflection.
This is the multi-language equivalent of reflow's Go interceptor —
clients can render the policy or pre-check before calling.

**Out of scope today (additive later):**

- JWT/OIDC `ClaimMapper` implementation (interface ships; concrete
  implementation deferred until a specific issuer is chosen).
- Ingress authz model (different identity story — likely workload
  identity, not node/operator).
- SDK-session authz (depends on which transport — A or B — ships first).
- Sub-role taxonomy (`operator/readonly`, `operator/admin`); reflects
  via the existing `WithMatcher` hook.

---

## 7. Key Data Flows

### 7.1 New Invocation (Happy Path)

Steps 1–6 are identical for both handler-hosting paths; step 7 forks
on whether the handler runs in-process (the primary path) or behind a
wire-protocol transport (§6.10.2).

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

Path A — in-process Go handler (primary):
6A. Invoker looks up the registered handler in its registry
    (registered via reflow.RegisterService at boot).
7A. Invoker spawns a session goroutine bound to reflow.Context.
    Each Sleep/Run/Call/State op calls Proposer.ProposeSelf to
    append a JournalEntry; the goroutine blocks on EntryAck.
8A. On handler return, Invoker proposes EndInvocation. The FSM
    flips inv/<id> to Completed and runs the on-entry actions
    (output stored, awaiters notified, outbox enqueued).

Path B — out-of-process handler (Transport A, future-active):
6B. Invoker selects a connected sdkv1.SessionService stream from
    the routing pool (handler dialed in at startup).
7B. Engine sends StartInvocation over the bidi stream. Handler
    streams ProposeEntry / ProposeRunCompletion frames back;
    engine proposes each as InvokerEffect.JournalAppended and
    acks once Raft commits.
8B. Handler streams EndInvocation. Same on-entry actions as 8A.
```

Path A is the typical Go-shop deployment — no network hop between
handler and journal. Path B is the only path when the handler is in
another language.

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
6.  Per re-queued invocation, the Invoker:
      Path A (in-process Go): spawns a fresh session goroutine; the
      JournalReader replays committed entries in order, returning
      cached results to reflow.Context calls so the handler skips
      already-completed steps. Execution resumes at the first
      un-journaled call.
      Path B (out-of-process, future-active): picks a connected
      sdkv1.SessionService stream, sends StartInvocation with
      known_entries = current journal length, streams ReplayEntry
      frames in order, then resumes normal flow.
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
| 3 | In-process Go SDK vs. external SDK only | Resolved | In-process Go SDK is the primary path (§6.10.1). Wire-protocol path supported for non-Go handlers (§6.10.2). |
| 4 | Partition count default | Resolved | 64 partitions at cluster bootstrap. |
| 5 | Raft replication factor | Resolved | Default 3, configurable per deployment via `--replication-factor`. Three is the minimum that tolerates a single failure with quorum; >3 trades write latency for durability. Decided per deployment, no SAD-level open question remains. |
| 6 | Pebble per-partition vs. shared | Resolved | Per-partition Pebble DB implemented in Phase 1; no `partition_id` prefix in keys. |
| 7 | Exactly-once for non-idempotent external calls | Resolved (Phase 2) | Idempotency keys propagate through `Invoke` via the `Dedup` field on `Envelope` (`enginev1/engine.proto`). The dedup table (`dedup/self` for self-proposals, `dedup/arb` for external producers like ingress) is consulted on every apply; duplicates are dropped before state mutation. See `internal/storage/tables/dedup.go` and `internal/storage/tables/idempotency.go`. |
| 8 | SDK protocol versioning | Resolved | Wire protocol tracks restate service-protocol-v4 as a *best-effort* compat target, not bug-for-bug. Phase 2 in-process Go SDK is the primary path; non-Go SDKs ride along on community effort. |
| 9 | timerfd vs `time.Timer` | Resolved | `time.Timer` for Phase 1. Revisit only with measured latency requirements. |
| 10 | `StateStore` alternative implementations | Resolved | `internal/storage.Store` interface; `MemStore` (tests) + `PebbleStore` (production). |
| 11 | Gossip for failure detection + soft state | Resolved | Use dragonboat's built-in gossip (memberlist/SWIM, vendored inside `lni/dragonboat/v4`) starting Phase 4 — zero extra dependency. Provides SWIM-based liveness, NodeHostID-stable endpoint resolution, and a `ShardView` leader hint cache. Architectural boundary unchanged: gossip is advisory, Raft (shard 0) is authoritative — eviction and partition assignment always go through a Raft proposal. Soft-state dissemination beyond the per-nodehost `Meta` blob is deferred; revisit only if observed load-hint dissemination requirements outgrow `Meta`. |
| 12 | Object storage for snapshots | Resolved | `SnapshotRepository` interface lands in Phase 4 (filesystem driver) and Phase 5 (S3/GCS/Azure via `gocloud.dev/blob`). Always optional — default deployment is local-only. Hot state never leaves Pebble; only snapshot artifacts and their metadata go to object storage. See §6.12. |
| 13 | Authn/authz model for internal gRPC | Resolved (Phase 4.3) | Single-CA SPIFFE URI identity over mTLS; `internal/auth` package owns `ClaimMapper` + `Authorizer`; per-RPC policy declared in proto via `optionsv1` annotations. TLS layer reduced to chain + URI well-formedness; role enforcement lives entirely in `auth.UnaryInterceptor` / `auth.StreamInterceptor`. JWT/OIDC mapper is an additive future. Ingress + SDK-session authz are separate identity models, out of scope here. See §6.13. |
| 14 | SDK transport for non-Go handlers | Resolved (additive) | Reflow ships Transport A (handler dials engine, gRPC bidi via `sdkv1.SessionService`); the wire is in place, routing pool is stubbed pending the first non-Go SDK. Transport B (engine dials handler over raw HTTP/2, Restate-style) is documented as additive — same `SDKMessage` envelope, different transport. See §6.10.2. |
| OPEN-1 | Joining-node startup against a live cluster | Open | The admin `AddNode` RPC, cluster FSM, and metadata rebalancer that drive cluster-side membership changes work end-to-end (`SyncRequestAddNonVoting` → catch-up → `SyncRequestAddReplica`). The gap is on the joining node's own `reflowd` startup: `Host.StartMetadataShard` and `Host.StartPartition` both hard-code `nh.StartOnDiskReplica(initial, join=false, ...)`. A new peer joining an established Raft group needs `StartOnDiskReplica(nil, join=true, ...)`. Missing pieces are minimal — a `HostConfig.JoinExisting bool` and a `reflowd --join` flag / config key. Tracked as a GitHub issue. |

---

## 10. Risks

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| Journal replay correctness bugs | High | Critical | Extensive property-based testing; formal spec |
| GC pauses causing Raft timeouts | Low | Medium | Tune `RTTMillisecond`/`HeartbeatRTT` generously; revisit if measured in load tests. timerfd integration deferred. |
| Pebble key schema migration | Medium | Medium | `PartitionMeta.version` reserved; schema versioning to land in Phase 2. |
| dragonboat API stability | Medium | Medium | Pinned to v4 pseudo-version; Pebble pinned to dragonboat's expected commit. Watch for an official v4 release. |
| SDK protocol breaking changes | Medium | High | Phase 2 will adopt restate service-protocol-v4 wire format (avoid inventing a competing one). |
| Partition rebalancing data loss | Low | Critical | Phase 4 concern; test membership changes under load when implementing. |

---

## 11. Phased Delivery

### Phase 1 — Single Node Foundation (DONE)
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
- Integration tests cover: replay across restart, dedup blocking, timer
  survives restart.

**Exit criteria:** Invocations persist and replay correctly across process
restarts on a single node. ✓ (`TestPhase1_SingleNodeReplayAcrossRestart`,
`TestPhase1_TimerSurvivesRestart`, `TestPhase1_DedupBlocksDuplicateIngress`)

### Phase 2 — In-process Go SDK + Invoker (DONE)

The first-class developer experience: write a Go function, register it
with `reflowd`, have it become a durable goroutine.

- **`reflow.Context`** Go API in `pkg/reflow/` and the public API
  surface there.
- **In-process Invoker** (`internal/engine/invoker/`) — registry,
  session goroutines, journal reader for replay, `InvokerEffect`
  proposals via `Proposer.ProposeSelf`.
- **Ingress** — gRPC + grpc-gateway in `internal/ingress/`. Awakeable
  resolution rides the same surface.
- **Journal entry types** beyond Phase 1: `JERun`, `JEGetState` /
  `JESetState` / `JEClearState` / `JEClearAllState`, `JEAwakeable` /
  `JEAwakeableResult`, `JESignal`. Lazy state via `JEGetEagerState` is
  Phase 3.
- **Outbox** (`internal/engine/outbox.go`, `internal/storage/tables/outbox.go`)
  for parent-invocation notifications and cross-partition call results.
- **Exactly-once side-effect replay** via the journal — verified by
  property-style integration tests under
  `internal/engine/integration_phase2_wiring_test.go`.
- **`sdkv1.SessionService` wire** — the proto contract for Transport A.
  Server stub (`internal/sdkstream`) returns `Unimplemented`; routing
  pool lands when the first non-Go SDK appears.

**Exit criteria:** A Go handler with `Sleep`, `Run`, state reads/writes,
and outgoing `Call`s survives mid-execution crashes and resumes
correctly. ✓

### Phase 3 — Virtual Objects
- VObject status (switch on persisted oneof; no FSM library)
- Object K/V state in Pebble (the reserved `state/` namespace)
- Invocation queue per object key
- Single-writer enforcement

**Exit criteria:** Concurrent invocations on the same object key are serialized correctly under failure.

### Phase 4 — Multi-Node Replication (DONE through 4.3d)

Target: a 3–10 node cluster. No external coordination service introduced
(see §6.2). Delivered as four sub-phases:

**4.1 — Embedded metadata Raft + static bootstrap.** Shard 0 hosts
node membership, partition table, assignment epoch; founder/joiner
bootstrap via `--bootstrap-cluster` / `--join`.

**4.2 — Dynamic membership + failure detection + DR snapshots + mTLS
admin (DONE).** Dragonboat gossip (memberlist/SWIM) drives K-of-N
liveness; SWIM observers turn missed probes into `RemoveNode` proposals
to shard 0. `reflow-cluster` CLI lands as a subcommand of `reflowd`
(`add-node`, `remove-node`, `partitions list`, `partition move`).
`SnapshotRepository` filesystem driver wired (cloud drivers deferred to
Phase 5). Admin gRPC server (`adminv1`) protected by mTLS against a
two-CA topology (operator CA + node CA at this point).

**4.3a — Storage format version marker (DONE).** Per-Pebble-DB
`uint32` marker (`internal/storage/format.go`). Refuses to open a DB
written by a binary with a different `StorageFormatVersion`. Replaced
the earlier "command-stream VersionBarrier" sketch.

**4.3b — Single CA + SPIFFE URI SAN identity (DONE).** Collapsed
operator-CA + node-CA into one cluster CA; role moved into the SPIFFE
URI SAN (`spiffe://<td>/<kind>/<name>`). TLS verifier checks chain +
URI prefix at this point.

**4.3c — Proto-annotation authz interceptor (DONE).**
`proto/optionsv1` defines `required_spiffe_role` (method) and
`default_required_spiffe_role` (service). Admin service annotated
`operator`. `AuditInterceptor` + `AuthzInterceptor` enforce against
the compiled descriptor map.

**4.3d — Authorizer + ClaimMapper consolidation (DONE).** Two-shaped
authz across Admin and Delivery collapsed into one Temporal-shaped
`Authorizer` + `ClaimMapper` seam in `internal/auth`. TLS layer reduced
to URI well-formedness; role enforcement lives entirely in
`auth.UnaryInterceptor` / `auth.StreamInterceptor`. Delivery service
annotated `node`. See §6.13.

- **Embedded metadata Raft group** (`shardID = 0`) hosted by the same
  `NodeHost` as partition shards. Holds node list, partition table,
  partition assignment epoch.
- **Static peer bootstrap.** `--bootstrap-cluster` for the founder,
  `--join=<addr>` for joiners. No discovery service required.
- **`reflow-cluster` admin subcommands** (in the same `reflowd` binary)
  for `add-node`, `remove-node`, `partitions list`, `partition move`.
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
- **`SnapshotRepository` abstraction (filesystem driver).** New replicas
  joining a partition try the repository before falling back to
  dragonboat snapshot transfer. Local-fs driver only in Phase 4 (`file://`
  paths); cloud drivers land in Phase 5. See §6.12.

**Exit criteria:** A 3-node cluster sustains invocation progress through
single-node failures with no data loss, recovers when the failed node
returns, and tolerates a planned `remove-node` of any single member.
Chaos tests cover network partitions, leader oscillation, and concurrent
add/remove operations.

### Phase 5 — Production Hardening

- **Cloud-backed `SnapshotRepository` drivers** (S3, GCS, Azure Blob via
  `gocloud.dev/blob`). Retention policies (`count`, `age`, `tiered`).
  Operator-facing `reflow snapshot list/restore/prune` commands.
  Server-side encryption supported via cloud-native flags. Disaster
  recovery + cluster migration runbooks. See §6.12.
- Pebble snapshot tuning (compaction, log retention, checkpoint cadence).
- Load testing + chaos testing harness (jepsen-style at small scale).
- Admin API surface: partition status, invocation inspection, replay
  debugger, `purge_journal` / `kill_invocation` operations.
- Operational docs: deployment recipes, backup/restore, upgrade
  procedure (using the per-DB storage format marker from §6.2).
- **Non-Go SDKs (community-driven).** TypeScript / Python / Java / Kotlin
  / Rust SDKs talk to reflow via the wire-protocol path (§6.10.2). These
  ride on whatever effort the community contributes; reflow itself
  guarantees the wire-protocol surface, not the SDK quality across
  languages.

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
