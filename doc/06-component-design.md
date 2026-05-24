# 6. Component Design

This document details the architecture and design of Reflow's core components.

---

## 6.1 Ingress Layer

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
| ClusterCtl (`reflow.clusterctl.v1`) | 8082 (shared) | Connect/HTTP-2 | mTLS, `spiffe://<td>/operator/*` (+ `node/*` carve-out on `SelfJoin`) | Fleet ops: add/remove node, list partitions, snapshot mgmt, LP transfers + autonomous rebalancer drain |
| Config (`reflow.config.v1`) | 8082 (same listener as ClusterCtl) | Connect/HTTP-2 | mTLS, `spiffe://<td>/operator/*` | App config: deployments, event sources, webhooks, secrets — every kubectl-shaped admin operation operators run between deploys |

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

The engine ↔ handler wire (`proto/protocolv1` frames carried inside
`proto/handlerv1.HandlerService/InvokeStream`) terminates at the
handler-hosted endpoint, not a service hosted by reflowd. See §6.10.

---

## 6.2 Cluster Manager

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

1. Validate config (`Node.ID`, `Node.RaftAddr`, `Storage.DataDir`). Multi-node
   deployments are *allowed* on insecure transport but `Run` emits a WARN log
   (`pkg/reflow/run.go`) so the operator knows the cluster surface is
   unauthenticated; mTLS is the production posture.
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
   creds (cluster CA + node leaf), Connect handler hosted on an h2c
   `net/http` server wrapped by `internal/auth.HTTPMiddleware`,
   `delivery.Client` for outbound, and
   `Host.SetCrossShardSender(client)` so partitions started below get a
   Sender.
5. `Host.StartMetadataShard()` opens `${DataDir}/meta/state`, builds the
   cluster FSM + leadership + proposer, and calls
   `nh.StartOnDiskReplica(initialMembers, /*join=*/ false, fsm, raftCfg)`
   (`internal/engine/host.go:562`). `initialMembers` is built from the full
   peer list as `{node_id → NodeHostID}` targets
   (`host.go:initialMembers`, line 1310) so dragonboat gossip resolves
   them to live raft addrs.
6. For each `shard_id` in `Cluster.Shards`, `Host.StartPartition`
   repeats the per-partition equivalent: open Pebble at
   `${DataDir}/p{shard_id}/state`, build the Invoker bound to the
   partition's tables, register the leadership callbacks, and call
   `nh.StartOnDiskReplica(initialMembers, false, fsm, raftCfg)`
   (`host.go:887`).
7. Optionally build the snapshot producer (`snapshot.FSRepository` +
   `RunProducer` goroutine per shard).
8. Optionally build the admin Connect surface: mTLS server creds,
   one HTTP/2 listener hosting both `reflow.clusterctl.v1.ClusterCtl`
   (fleet ops) and `reflow.config.v1.Config` (app config). Auth runs
   at the HTTP/Connect layer via `internal/auth.HTTPMiddleware`
   (SPIFFE + OIDC chain). The naming mirrors Restate's
   `cluster-ctrl` vs `admin` split, with `admin` flipped to `config`
   to avoid the overloaded word.

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

**Dynamic membership — implemented end-to-end:**

```bash
reflowd cluster add-node    --node-id=4 --raft-addr=10.0.0.4:9091 \
                            --gossip-addr=10.0.0.4:9092 \
                            --grpc-endpoint=10.0.0.4:8081 \
                            --node-host-id=<uuid>
reflowd cluster remove-node --node-id=2
```

- **Operator-driven add (`add-node`, `internal/clusterctl/server.go:AddNode`):**
  proposes `RegisterNode{Member}` to shard 0, then enqueues a
  `BeginRebalanceStep{Kind: PROMOTE_TO_VOTER, AddNodeId}` for every
  partition the new node should hold. The metadata rebalancer
  (`internal/engine/metadata_rebalancer.go`) watches the pending queue
  and on the metadata leader executes the dragonboat-side call:
  `SyncRequestAddNonVoting`, then `SyncRequestAddReplica`. On success
  it proposes `CompleteRebalanceStep`, which updates the persisted
  replica set and bumps `assignment_epoch`.
- **Joiner-driven add (`reflowd run` with `Cluster.JoinExisting=true`):**
  `pkg/reflow/run.go:callSelfJoin` discovers the metadata leader via
  gossip-published `NodeHostMeta.admin_endpoint` and dials the
  leader's `ClusterCtl/SelfJoin` RPC before any local shard starts.
  `ClusterCtl/SelfJoin` shares `addNodeInternal` with the operator path
  but gates on a `node/<req.node_id>` SPIFFE principal so a leaked
  cert can only self-register as its own node id. The joiner then
  calls `nh.StartOnDiskReplica(nil, join=true, ...)` on each shard,
  which dragonboat services via the snapshot+log catch-up path now
  that the membership is in place.
- **`remove-node`**: same shape with `EvictNode` →
  `SyncRequestDeleteReplica`. The leaving node's `reflowd` already
  has the live membership in its NodeHost and exits when dragonboat
  removes its replica.

**Failure detection.** Dragonboat's built-in gossip (memberlist/SWIM,
enabled via `NodeHostConfig.AddressByNodeHostID = true` + `GossipConfig`)
runs SWIM probes between every NodeHost. The metadata leader's
`metadataRebalancer.failureLoop` polls the gossip `NodeHostRegistry`
every second; after `missThreshold` consecutive unreadable observations
of a peer's `NodeHostMeta`, it proposes `EvictNode` to shard 0. Eviction
is a strongly-consistent Raft decision driven by an
eventually-consistent gossip signal.

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

**Partition shard count vs. LP count — two layers.** Reflow's routing
is two-layer: a fixed pool of **4096 logical partitions (LPs)** maps to
**N partition shards** (default `len(cfg.Cluster.Shards)` falling back to
1 single-node). Ingress hashes `(service, object_key)` to an LP; shard 0's
`LPOwnersTable` maps each LP to a shard id. `N` is a hard design invariant
at cluster bootstrap; the LP layer is what gets re-balanced across shards
without ever renaming a partition key. See "Two-layer routing (LP → shard)"
below.

*Rationale (see §9 rows 1 & 4).* Constant `N` keeps shard ownership
deterministic forever: a shard id agreed by stale nodes and current
leaders, with no epoch number or `(N, key)` tuple in the routing equation.
The LP layer absorbs hot-spot rebalancing — when a `(service, object_key)`
gets busy, the operator (or the autonomous rebalancer) moves the *LP*
to a less-loaded shard via the cross-shard transfer saga, not via a
split/merge of `N`. The split/merge protocol (atomic key-range move across
two Pebble DBs + two dragonboat groups while in-flight invocations are
live) is a class of bugs we explicitly opt out of.

Online resize of `N` is **not supported** — `Host.Partitioner()` reads
`cfg.NumPartitionShards` at boot and never recomputes it. Shard
reassignment across nodes (the rebalancer's job) is independent: nodes
still react to `PartitionTable` updates by starting / stopping local
Raft replicas as ownership shifts. LP reassignment across shards (the
autonomous rebalancer's job) is the new third primitive.

**Hard boundary the design enforces:** the metadata Raft group is the only
authoritative source of partition ownership. No node ever processes a
command for a partition it does not own according to its locally-observed
copy of the partition table. A stale node will fail the `IsLeader` check
on its `dragonboat` shard before any side effects can escape. Gossip can
never override this — it only feeds advisory signals (liveness reports,
leader hints, endpoint resolution) and Raft proposals are the only path
to authoritative state changes.

### 6.2.1 Two-layer routing (LP → shard)

Reflow routes through two layers, not one:

```
ingress request
  ↓
hash(service, object_key) % 4096       → logical partition (LP)
  ↓ LPOwnersTable[lp]                  (shard 0, atomic snapshot per node)
shard_id ∈ [1, N]                      → dragonboat group on the owning node
```

The LP layer is the unit of online rebalancing; the shard layer is the
fixed unit of replication. Hot-spot relief is "move an LP between
shards" — never "split a shard."

**Components:**

- **`routing.Partitioner`** (`internal/engine/routing/partitioner.go`)
  is the per-Host singleton hot-path. Holds an
  `atomic.Pointer[map[uint32]uint64]` LP→shard snapshot;
  `ShardForKey(partitionKey) uint64` is a single atomic load + map
  lookup. A per-node routing reconciler swaps the snapshot on each
  `cluster.TableNotifier` wake.
- **`routing.Planner`** (`internal/engine/routing/planner.go`) wraps
  `buraksezer/consistent` (consistent hash with bounded loads, Google
  2017 paper) over the active shard ids. `NewPlanner(shardIDs).PlanAll()`
  returns the desired `map[lp]shard_id` deterministically across
  replicas (xxhash is platform-neutral; constructor sorts shard ids
  before building the ring). `routing.Diff(current, desired)` returns
  the move set in LP-ascending order — used by both the
  metadata-leader bootstrap seed and the autonomous rebalancer.
- **`cluster.LPOwnersTable`** on shard 0 (`internal/engine/cluster/store.go`)
  is the authoritative LP→shard mapping (4096 rows, one per LP).
  Bootstrapped from the planner's `PlanAll()` output; mutated only via
  the LP transfer protocol below.

**Cross-shard LP transfer protocol** (`internal/engine/lp_transfer_*.go`,
`internal/engine/metadata_lpmover.go`). Six-phase saga coordinated by the
`lpMover` goroutine on the metadata-shard leader:

```
INIT → SHIPPING → STAGED → FLIPPED → CLEANED         (happy path)
INIT|SHIPPING|STAGED → ABORTING → ABORTED             (abort branch — never from FLIPPED)
```

1. **`InitiateLPTransfer`** (proposed by `ClusterCtl/TransferLP` or the
   autonomous rebalancer): writes `LPTransferRecord{INIT}` to shard 0.
   Stamps `expected_lpowners_revision` so the eventual `UpsertLPOwner`
   CAS can detect concurrent ownership drift.
2. **`BeginLPTransfer`** → source partition: installs
   `lp_freeze/<lp>` row. Every subsequent LP-touching apply arm checks
   the freeze gate via `partition.checkLPFreeze` and returns
   `errLPFrozen` to the proposer, who retries elsewhere.
3. **SST shipping** (`internal/engine/lp_transfer_sst.go` +
   `lp_transfer_source.go`): the leader's `LPTransferSourceService`
   builds one or more Pebble SSTables over the LP's row range (per-
   namespace, key-ordered) on the source, uploads them to the
   destination via the cross-shard SST-upload RPC, and the destination
   calls `s.db.Ingest` to atomically link them into its keyspace —
   no row-by-row writes, no `batch.Set` in the hot path. Then
   **`ApplyLPTransferSST`** is proposed: destination references the
   ingested SST(s) by `TransferSSTRef` and marks them live;
   `LPStagingTable` tracks the staging-phase refs for in-order
   acknowledgment + duplicate absorption.
4. **`STAGED`** ack returns to shard 0 once the final SST batch has
   been ingested + ApplyLPTransferSST has applied on the destination.
5. **`UpsertLPOwner`** on shard 0 (CAS against
   `expected_lpowners_revision`) atomically flips the routing row.
   This is the point of no return — `isValidLPTransferAdvance` rejects
   `FLIPPED → ABORTING`.
6. **`CommitLPTransfer`** → destination: drops the staging marker.
   **`FinishLPTransfer`** → source: range-deletes every LP-prefixed
   namespace, walks `timer_lp/<lp>/...` first to collect primary
   `timer/<fire>/<id>` keys (the primary keyspace is LP-agnostic so the
   secondary index drives the cleanup). Phase advances to CLEANED.
7. **`RemoveLPTransfer`** after a grace window so an operator polling
   `ListLPTransfers` sees the row before it disappears.

**Autonomous LP rebalancer** (`internal/engine/rebalance/`). Leader-only
goroutine spawned by `MetadataRunner.onBecomeLeader` alongside the
membership-handling `metadataRebalancer` and the saga-executing
`lpMover`. Subscribes to the `RebalanceDrainTable` notifier with a 30s
backstop ticker.

Modes (`cfg.Rebalance.Mode`, default `off`):

| Mode | Goroutine | Observes | Emits metrics | Logs decisions | Proposes |
|------|-----------|----------|---------------|----------------|----------|
| `off` | not started | — | mode gauge=0 | — | — |
| `advisory` | running | yes | all | yes (`would_transfer`) | **no** |
| `auto` | running | yes | all | yes | yes (rate-limited) |

Each tick:

1. SyncRead `PartitionTable`, `LPOwnersTable`, `RebalanceDrainTable`,
   `LPTransferTable` from shard 0.
2. Compute the live shard set = active partition shards − drained.
3. `desired := routing.NewPlanner(live).PlanAll()`.
4. `moves := routing.Diff(current, desired)` (LP-ascending,
   deterministic so two leaders racing across a step-down produce the
   same intent).
5. **Skew metric** = mis-placement fraction = `len(moves) /
   len(desired) × 100`. The planner output is fixed for a given shard
   set, so each completed transfer monotonically decreases the
   numerator.
6. **Hysteresis**: engaged iff (prior engaged AND skew > disengage_pct)
   or (prior not engaged AND skew ≥ engage_pct). Defaults: 15% engage,
   8% disengage.
7. **Rate limit**: cap proposals at `max_concurrent_transfers −
   in_flight`; gate by `min_seconds_between_transfers` cooldown
   against `max(started_at_ms)` across all `LPTransferRecord` rows.
8. **Advisory**: log each candidate move and increment
   `reflow_rebalance_decisions_total{outcome=would_transfer}` — never
   propose.
9. **Auto**: propose `Command_InitiateLPTransfer` for the first
   `capacity` moves. Same path manual `reflowd cluster transfer-lp`
   takes, so autonomous transfers appear in `ListLPTransfers` with no
   extra plumbing.

**Triggers in this version** are limited to membership change
(`PartitionTable` reshapes the planner's input) and operator-requested
drain. Drained shards live in shard 0's `RebalanceDrainTable`
(CAS-revisioned, cluster-managed, runtime-mutable via
`ClusterCtl/RebalanceDrain`). Load-based triggers (per-shard QPS / p99)
and capacity circuit breakers (Pebble L0, write-amp) are explicitly
deferred — durable-execution workloads are bursty enough that reactive
moves on rolling-window load fire mid-burst; capacity is better as a
*destination gate* than a *trigger*.

---

## 6.3 Partition Processor

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

## 6.4 Invocation State Machine

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

### Differences from Restate's `InvocationStatus`

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
(`proto/enginev1/engine.proto:966`) — a separate row per
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

## 6.5 Virtual Object State Machine

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

## 6.6 Replicated Log (dragonboat + Pebble)

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
(pebble `v1.1.5 checkpoint.go:135`) — atomic, online, hard-link-backed
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

## 6.7 Pebble Key Schema

Each partition shard owns its own Pebble DB at
`${DataDir}/p{shardID}/state/`; shard 0 (metadata) lives at
`${DataDir}/meta/state/`. Keys do NOT carry a shard_id prefix —
isolation is at the DB level. Dragonboat keeps its own state under
`${DataDir}/raft/`.

`InvocationId` is the canonical 24-byte raw form (8-byte BE
`partition_key` + 16-byte uuid), mirroring restate
`types/src/identifiers.rs:456-461`. All multi-byte integers in keys are
big-endian so lexicographic byte order equals numeric order.

**LP-prefixing.** Most partition-shard rows live under
`<namespace>/<4-byte BE lp>/...` where `lp = LPFromPartitionKey(pk) =
pk mod 4096`. The LP prefix makes the entire LP keyspace a contiguous
byte range — the cross-shard LP transfer protocol (§6.2, "Two-layer
routing") scans + ships these ranges and `FinishLPTransfer` range-
deletes them on the source. Rows that are intrinsically LP-agnostic
(timer primary keyed by fire time, outbox keyed by per-shard sequence,
workflow-reap due-time index) keep their original shape.

### Partition shards (1..N), `${DataDir}/p{shardID}/state/`

Every LP-prefixed namespace embeds a 4-byte BE LP id followed by a
4-byte BE tenant id (`<lp:4><tenant:4>`) immediately after the
namespace string. The LP-transfer source scan keys on `[ns/<lp>,
ns/<lp+1>)` and captures all tenants on that LP in one byte range;
per-tenant consumers narrow further to `[ns/<lp>/<tenant>,
ns/<lp>/<tenant+1>)`. See `internal/storage/keys/keys.go`.

```
Namespace            Key structure                                                          Value
──────────────────────────────────────────────────────────────────────────────────────────────────
meta                 meta                                                                   PartitionMeta
format               format                                                                 uint32 (storage format marker)

inv/                 inv/<lp:4><tenant:4><id:24>                                            InvocationStatus
journal/             journal/<lp:4><tenant:4><id:24>/<idx:4>                                JournalEntry

timer/               timer/<fire_at_ms:8>/<id:24>                                           uint32 sleep_index   (LP-agnostic primary)
timer_lp/            timer_lp/<lp:4><tenant:4><fire_at_ms:8>/<id:24>                        uint32 sleep_index   (secondary index — rides LP transfer scan)
timer_idx/           timer_idx/<lp:4><tenant:4><id:24>/<fire_at_ms:8>                       (empty)              (secondary index — fast per-invocation cancel)

state/               state/<lp:4><tenant:4><service>/<obj_key>/<state_key>                  VObject K/V state
keylease/            keylease/<lp:4><tenant:4><service>/<obj_key>                           KeyLeaseStatus
idempotency/         idempotency/<lp:4><tenant:4><sha256:32>                                InvocationId         (sha256 of `(service, handler, obj_key, key)`)

awakeable/           awakeable/<lp:4><tenant:4><id:26>                                      AwakeableEntry
signal_inbox/        signal_inbox/<lp:4><tenant:4><id:24>/<name>                            SignalInboxEntry
signal_awaiter/      signal_awaiter/<lp:4><tenant:4><id:24>/<name>                          SignalAwaiter

workflow_run/        workflow_run/<lp:4><tenant:4><service>/<wf_key>                        InvocationId
promise/             promise/<lp:4><tenant:4><service>/<wf_key>/<name>                      PromiseValue
promise_awaiter/     promise_awaiter/<lp:4><tenant:4><service>/<wf_key>/<name>/<idx:4>      PromiseAwaiter
workflow_reap/       workflow_reap/<fire_at_ms:8>/<service>/<wf_key>                        (empty)              (LP-agnostic due-time index)

dedup/self/          dedup/self/<leader_epoch:8>/<seq:8>                                    DedupEntry           (shard-scoped; GC'd per leader epoch)
dedup/arbitrary/     dedup/arbitrary/<lp:4><tenant:4><producer_id>/<seq:8>                  DedupEntry           (LP-prefixed — rides LP transfer scan; LP-agnostic kinds key under LPNoLP=0xFFFF_FFFF)

outbox/              outbox/<seq:8>                                                         OutboxEnvelope       (LP-agnostic; per-shard send sequence)

lp_freeze/           lp_freeze/<lp:4>                                                       LPFreezeRow          (set by BeginLPTransfer; gates LP-touching apply arms)
lp_staging/          lp_staging/<transfer_id>                                               LPStagingRow         (destination-side; tracks ingested SST refs for in-order ack)
```

**Key design decisions:**

- **One Pebble DB per shard.** Removes the need for a shard_id key
  prefix and simplifies snapshot / checkpoint isolation.
- **LP-prefixed namespaces.** The 4-byte BE LP prefix gives the LP
  transfer's source-side scan and destination-side range-delete an
  O(prefix-scan) shape per namespace. Per-tenant scoping (`<tenant:4>`)
  sits inside the LP keyspace so tenants on one LP transfer together,
  and per-tenant code can narrow to a tighter subrange. Adding a new
  LP-prefixed namespace touches a single source of truth:
  `internal/storage/keys/lp_namespaces.go:AllLPNamespaces` — both the
  source-side transfer scan and the apply-path LP validators iterate
  it (`refactor(keys): single registry for LP-prefixed namespaces`).
- **Timer primary is LP-agnostic.** `timer/<fire>/<id>` sorts by
  `(fire_at_ms, id)` so `TimerTable.ScanDue` is a bounded prefix scan
  ordered by due time. `timer_lp/` is the LP-prefixed secondary index
  used by the LP transfer scan; `timer_idx/` is the per-invocation
  cancel index. The three views stay consistent because the apply arm
  writes all three together inside one batch.
- **Dedup namespacing.** `dedup/self/` is shard-scoped per leader
  epoch (GC'd by `GCSelfBelowEpoch` on leader gain); `dedup/arbitrary/`
  is LP-keyed and rides the LP transfer scan so a producer retry after
  an LP flip finds its row already present on the new owner. LP-
  agnostic kinds (today only `OutboxAck`) key under `keys.LPNoLP =
  0xFFFF_FFFF`, a sentinel that is never a real LP (real LPs are
  < 4096) and is therefore never range-deleted by `FinishLPTransfer`.
- **Journal indices** are monotonic per invocation, distinct from the
  Raft log index, so log truncation doesn't leave gaps in the journal
  index space.

### Metadata shard (shard 0), `${DataDir}/meta/state/`

The cluster manager FSM stores everything routing- and config-related.
Each cluster-managed table has a paired `tablerev/<name>` singleton used
by `Envelope.precondition.if_table_revision_eq` for CAS — the FSM signals
mismatch via `Result.Value = ResultValueFailedPrecondition` (returning an
error would halt the shard, per `internal/engine/CLAUDE.md`).

```
Namespace            Key structure                       Value
─────────────────────────────────────────────────────────────────────────
meta                 meta                                PartitionMeta (reuses partition proto; only applied_index + latest_announced_epoch populated)
node/                node/<node_id:8>                    NodeMembership
partition_table      partition_table                     PartitionTable singleton

deployment/          deployment/<deployment_id>          DeploymentRecord
deployment_idx/      deployment_idx/<service>\0<handler> deployment_id (ascii) — (service, handler) → current owner

eventsrc/            eventsrc/<name>                     EventSourceRecord
webhooksrc/          webhooksrc/<name>                   WebhookSourceRecord
secret/              secret/<name>                       SecretRecord (pointer fields only — blob_uri + kek_uri; plaintext never traverses Raft)

tenant/              tenant/<tenant_id:4>                TenantRecord (id=0 reserved for default-tenant sentinel; never persisted)
tenant_name_idx/     tenant_name_idx/<name>              4-byte BE tenant_id (name → id secondary index for create-vs-update resolution)
tenant_dek/          tenant_dek/<tenant_id:4>            TenantDEKRecord (per-tenant data-encryption-key reference; per-node TenantDEKResolver iterates on each notifier wake)

auditlog/            auditlog/<raft_index:8>             AuditLogRecord (append-only config-change audit; written in same Batch as the audited mutation; retention GC range-deletes by raft_index span derived from ts_ms)

lpowner/             lpowner/<lp:4>                      LPOwnerRecord  (the LP → shard_id routing table)
lptransfer/          lptransfer/<transfer_id>            LPTransferRecord (in-flight LP transfer saga)
rebalance_drain/     rebalance_drain/<shard_id:8>        RebalanceDrainRecord (operator-requested shard drains)

tablerev/            tablerev/<table_name>               TableRevision (CAS singleton per cluster-managed table)
```

---

## 6.8 Journal and Replay

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

## 6.9 Timer Service

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

## 6.10 SDK Protocol

Handlers run as separate Go processes that host `pkg/handler`, a Connect
HTTP/2 server speaking the wire protocol defined in
`proto/protocolv1/protocol.proto`. The engine dials the handler's
`HandlerService.InvokeStream` (`proto/handlerv1`) as a Connect bidi
stream per invocation; engine→SDK and SDK→engine frames flow in opposite
directions over the same stream. Polyglot SDKs (TS/Python/Java/Rust/...)
ride the same wire — there is no Go-specific fast path.

### 6.10.1 Wire shape

Each Connect stream message is a `protocolv1.Frame` carrying a 64-bit
big-endian header field (16-bit type code | 16-bit flags | 32-bit
payload length) and the protobuf payload bytes. Type codes are
namespaced:

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

### 6.10.2 Engine-side dispatch

`internal/engine/handlerclient/connectclient` owns the engine → handler
dial: a Connect `HandlerServiceClient` per registered deployment, one
`InvokeStream` per invocation. `internal/engine/invoker/wireSession`
translates between the Connect bidi stream and the partition's
`InvokerEffect` propose path: every command frame becomes a
journal-entry propose; every notification frame is delivered as a
completion.

There is no in-process fast path. `examples/embedded/main.go` shows
running the engine and a Go handler in one binary for local dev — the
engine still reaches the handler over a loopback Connect/HTTP/2
connection, identical to the production path.

### 6.10.3 State read journaling — deliberately partial

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

## 6.11 Storage Pluggability

Bifrost's provider abstraction (local vs. replicated loglet) is replaced by three independent pluggability points already present in the stack. No custom provider layer is required.

### `vfs.FS` — Pebble filesystem backend

Pebble abstracts the filesystem via `vfs.FS`. Swapping the implementation changes the storage medium with no other code changes:

```go
// production
pebble.Open(dir, &pebble.Options{FS: vfs.Default})

// development / CI — fully in-memory, zero disk I/O
pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
```

`vfs.MemFS` replaces Bifrost's `local-loglet` for development and test environments. There is no special code path — the same Partition Processor runs against either backend.

### `ILogDB` — dragonboat Raft log storage

dragonboat exposes the Raft log store behind an interface:

```go
type ILogDB interface {
    SaveRaftState(updates []pb.Update, shardID uint64) error
    IterateEntries(ents []pb.Entry, size uint64, shardID uint64, ...) ([]pb.Entry, uint64, error)
    // ...
}
```

The default implementation (`tan`) uses Pebble internally. A custom `ILogDB` can back the Raft log with any durable store — remote object storage, an alternative embedded DB, or a test double — without touching the engine.

### `StateStore` — state machine storage interface

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

### Pluggability map

| Bifrost concept | Go equivalent | Pluggable via |
|---|---|---|
| `local-loglet` (dev/test) | `vfs.MemFS` | Pebble `vfs.FS` |
| `local-loglet` (production) | `vfs.Default` | Pebble `vfs.FS` |
| `replicated-loglet` | dragonboat Raft group | `ILogDB` interface |
| Alternative Raft log backends | Custom `ILogDB` impl | `ILogDB` interface |
| State machine storage | `StateStore` interface | Custom impl |

---

## 6.12 Snapshot Repository

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

## 6.13 Authentication & Authorization

Reflow's HTTP/2 surfaces (Connect ingress, REST `/v1/*`, internal
Admin/Delivery) share one inbound auth chain built on
`connectrpc.com/authn`. Two identity planes coexist; they serve
different purposes and are not interchangeable.

**Two planes, one principal model:**

| Plane | Identity primitive | Issued by | Runtime dep | Used for |
|---|---|---|---|---|
| SPIFFE / mTLS | `spiffe://<td>/<kind>/<name>` URI SAN on the verified leaf | Reflow's offline CA (`reflowd pki`) | none | cluster mesh + ClusterCtl/Config/Delivery |
| OIDC bearer | JWT with claims mapped to `Principal{Kind, Subject}` | Customer's IdP | IdP reachable | ingress + optionally ClusterCtl/Config via `kind=operator` |

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
| ClusterCtl (`reflow.clusterctl.v1.ClusterCtl`) | mTLS (operator), or via OIDC bearer | SPIFFE → operator/node; bearer → claim-mapped principal | `clusterctl_operator` rule allows `operator/*`; `clusterctl_node_selfjoin` carve-out allows `node/*` on the SelfJoin path (with `checkSelfJoinPrincipal` further requiring URI's `<id>` == `req.node_id`) |
| Config (`reflow.config.v1.Config`) | mTLS (operator), or via OIDC bearer | SPIFFE → operator; bearer → claim-mapped principal | `config_operator` rule allows `operator/*` |
| Delivery (`reflow.delivery.v1.Delivery`) | mTLS (node) | SPIFFE only; bearer ignored (no IdP path for streaming inter-node) | `delivery_node` rule allows `node/*` |
| Connect ingress (`reflow.ingress.v1`) | Optional (h2c or TLS via `cfg.Ingress.Creds`) | Either; falls through to anonymous when neither present | `ingress_open` rule has no principal constraint by default; operators tighten via `cfg.Auth.PolicyFile` |
| REST ingress (`/v1/*`) | Same listener as Connect ingress | Same as Connect ingress | `ingress_rest_open` rule covers `/v1/*` through `/v1/*/*/*/*/*` |
| Engine → handler (`protocolv1`) | Out of scope here — owned by `pkg/reflow/creds` driver + `pkg/handler.Config` verifying via `RootCAs` / `AllowedSPIFFE` |

**Why both planes:** dropping SPIFFE in favor of OIDC-only would break
load-bearing pieces: (1) `ClusterCtl/SelfJoin`'s NodeID-binding gate
requires identity bound to a key in node-X's secret store, which mTLS
provides natively; (2) the dragonboat Raft transport and Delivery RPC
are TCP/streaming surfaces with no header to put a bearer token on;
(3) cluster admin must work when the IdP is unreachable, so the
offline CA is the dependency-free credential path. Dropping OIDC in
favor of SPIFFE-only is fine for closed deployments but limits
ingress to either anonymous-via-policy or mTLS-only (no SaaS-friendly
JWT path).

### 6.13.1 OIDC as an operator credential

The composed AuthFunc means a single `kind=operator` claim from an
OIDC token produces the same `Principal{Kind: "operator", Subject:
…}` value as an offline-CA `spiffe://td/operator/…` leaf. The
`starter_policy.json` `clusterctl` and `config` rules both match on
`operator/*` regardless of how the principal was established, so an
OIDC-authenticated CI pipeline can run any `reflowd cluster ...` or
`reflowd config ...` subcommand without ever holding an mTLS cert.

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
matches `operator/*`, can hit `/reflow.clusterctl.v1.ClusterCtl/*`
and `/reflow.config.v1.Config/*`. The `/` in the subject is
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

- Per-tenant ingress identity story for multi-tenant SaaS; today
  operators discriminate tenants via `pkg/hostmux` + per-host
  middleware that sets `Reflow-Meta-*` headers.

---

## 6.14 Webhook Ingress

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

**Storage model — cluster-managed via shard 0.** Webhook sources are
durable rows in shard 0's `WebhookSourceTable` (alongside deployments
and event sources). Per-node `Manager` instances subscribe to a
`TableNotifier`, pull a fresh snapshot on wake (5s ticker backstop),
and atomically swap a path→source map (`atomic.Pointer`). One stable
subtree route at `/webhooks/`; the handler reads `r.URL.Path` and
looks up the live snapshot. Adding, rotating, or removing a webhook
is an operator CLI call — `reflowd config webhooks list / delete`
and `reflowd config apply -f <file>` (`kind: WebhookSource`) — propagated to
every node via Raft, no cluster restart.

**Config shape — operator workflow:**

```yaml
kind: WebhookSource
metadata: { name: stripe-prod }
spec:
  path: /webhooks/stripe
  verifier: stripe
  secret_ref:
    remote_encrypted:
      blob_uri: s3://reflow-secrets/stripe.bin
      kek_uri:  blobkms+s3://reflow-keys/master.key
  invocation:
    service: stripe-events
    handler: receive
    metadata: { environment: prod }
```

There is no koanf-bootstrap path for webhooks (unlike event sources,
which retain a bootstrap seed). Operators always go through
`reflowd config upsert-webhook` or `reflowd config apply -f <file>`
post-start. Secrets are a separate problem from webhook routing and
deserve their own admin surface — bundling them in koanf would mean a
plaintext-secret seed file by default.

**Secret resolution — by reference into a separate SecretTable.**
`WebhookSourceRecord` carries a `secret_name` string referencing a
row in shard 0's `SecretTable`. The webhook record contains NO
ciphertext or KMS material; the SecretTable is the single source of
truth for all named secrets in the cluster (used by webhooks today,
and by future consumers — event-source vendor credentials, outbound
HMAC signing keys, OIDC client secrets — without per-consumer
duplication).

Per-node `internal/secretstore` Resolvers reconcile the SecretTable
on the same notifier/ticker pattern as every other shard-0 table
(5s ticker backstop). Each pass fetches ciphertext for every row
via `gocloud.dev/blob.ReadAll`, dispatches the KEK URI through Tink's
process-global `KMSClient` registry, decrypts with `AAD = []byte
(secret.name)`, and atomically swaps a fresh name→bytes map. The
webhook Manager (and future consumers) call `Resolver.Lookup(name)`
on each reconcile pass — single `atomic.Pointer.Load`, no per-call
KMS trip. Resolve failure preserves the previously-resolved bytes so
a transient blob/KMS hiccup doesn't knock dependent consumers offline.

AAD binds ciphertext to row identity, not to the consumer. Multiple
webhooks may share one `secret_name`; renaming the secret is a
re-encrypt operation, by design.

Four KMS providers ship in-binary at `pkg/kms/{blob,awskms,gcpkms,
hcvault}/` (always-linked, config-gated, matching the event-source
backends' pattern): BlobKMS (`blobkms+<gocloud-uri>` — the
no-managed-KMS fallback), AWS KMS (`aws-kms://...`), GCP Cloud KMS
(`gcp-kms://...`), HashiCorp Vault Transit (`hcvault://...`). The
first three self-register from `init()` and pick credentials up from
the host's environment (env vars, IAM role, workload identity, etc.);
Vault opts in via `cfg.KMS.Vault.TokenFile`. Operators wiring
additional providers register from `main` before `reflow.Run` — Tink
dispatches by first-supporting URI prefix.

BlobKMS' on-disk shape is `boot_key(32B) || serialized_encrypted_
keyset` — the boot key encrypts a Tink AEAD keyset that's the
operational KEK. This enables multi-key rotation (add new key to the
keyset, mark primary; old ciphertexts still decrypt via non-primary
entries) and primitive swap (`aead.New(handle)` today;
`hybrid.NewHybridEncrypt(handle)` or `keyderivation.New(handle)`
tomorrow) without a wire change.

Operator workflow:

```
reflowd config init-kek --blob-uri=file:///etc/reflow/kek.bin
echo -n "ghs_xxx" | reflowd config create-secret \
  --name=github-hmac --kek-uri=blobkms+file:///etc/reflow/kek.bin \
  --blob-uri=s3://reflow-secrets/github.bin
reflowd config upsert-webhook \
  --name=github-prod --path=/webhooks/github --verifier=github \
  --secret=github-hmac --service=svc --handler=on
```

Resolve path is hand-instrumented via
`reflow_secretstore_decrypt_total{kek_scheme}` /
`_errors_total{kek_scheme,stage}` / `_seconds` because Tink's
`monitoring.Client` is exported but `RegisterMonitoringClient` lives
in `tink-go/v2/internal/internalregistry` (blocked from external
import in v2.6). Per-secret detail is logged rather than labelled to
keep counter cardinality bounded across operator-managed secret
fleets.

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

- Per-source rate limiting: webhook bursts (Stripe retries,
  GitHub app installs) can saturate ingress; today the engine's
  generic `cfg.Ingress.HTTP.MaxBodyBytes` + tcp-level limits are
  the only knobs.
- Outbound webhook delivery (Reflow → external system): handler
  code can use `net/http` directly; durable retries are the
  handler's responsibility via the SDK's `Run` combinator.
- Asymmetric (hybrid) encryption for tenant-pushed secrets: the
  current Tink-keyset shape accommodates `tink-go/v2/hybrid` as a
  template-swap, but no concrete use case justifies the API
  surface today. Lands when self-service-tenant or JWE-payload
  vendors arrive.
- Journal/state encryption-at-rest: sketched, not scheduled — uses
  `tink-go/v2/keyderivation` to derive per-`object_key` AEADs
  from a master keyset (itself encrypted by the same KEK pipeline
  the SecretStore already uses). Pebble values today are plaintext;
  disk-level encryption is the operator's concern.

---

## 6.15 Configuration Planes

**Status: proposed.** Today every config knob lives in `pkg/reflow.Config`
(koanf, file + env, restart-to-change). The recent multi-tenant work
(`feat(cluster/tenants)`, `feat(auth): per-tenant OIDC`,
`feat(engine/encstore): per-tenant DEK`, `feat(ingress/quota): per-tenant`,
`feat(audit): durable config-change audit log`) has begun moving cluster
state onto shard 0 with per-node reconcilers, but the public `Config`
struct and CLI surface still conflate three distinct administrative
planes. This section is the target architecture that the remaining
migration is aimed at; current-state and migration gap are in the last
subsection.

The shape generalizes the cluster-managed-app-config pattern (§6.14, §11
"Cluster-managed app config", §11 "Unified secret management") to *all*
cluster policy, and bifurcates audit and identity along the same plane
boundaries.

### 6.15.1 Three administrative planes

| Plane | Owner | Storage | Examples |
|---|---|---|---|
| **Platform** | Cluster operator | Bootstrap koanf + shard-0 `PlatformConfigTable` | Listener binds, mesh CA bundle, KMS provider enablement, audit retention, snapshot URL + interval + tiered retention, rebalance mode, ingress/admin behavior, default tenant quotas, cluster-default operator OIDC issuers |
| **Tenant lifecycle** | Cluster operator, per-tenant | Shard-0 `TenantTable` via `ClusterCtl` RPCs | Create/delete tenant, quotas, BYOK KEK URI + credential ref, tenant audit retention + sink config, optional per-tenant handler CA bundle |
| **Tenant config** | Tenant admin, scoped to own tenant | Shard-0 tenant-prefixed tables via `Config` RPCs (keyed by `tenant_id`) | Event sources, webhook sources, secrets, additional OIDC issuers, deployments, secret rotation |

Plane boundaries are enforced by the starter policy: platform and
tenant-lifecycle mutations require an `operator/*` principal (with
`tenant_id` claim absent or `0`); tenant-config mutations accept either
an `operator/*` god-mode principal or a `tenant-admin/*` principal whose
mapped `tenant_id` equals `record.tenant_id`. The Config service
inspects the principal's tenant binding on every mutation and rejects
cross-tenant writes with `PermissionDenied` — no silent scoping, no
opportunity for a tenant-admin to read or mutate another tenant's row
by guessing names.

### 6.15.2 CLI flags + env vs Raft split

koanf goes away entirely. Bootstrap is small enough that a CLI flag
set plus env-var fallbacks beats a config-file abstraction.

| Layer | Contents | Why local / Why fleet |
|---|---|---|
| **CLI flags + env (this node's bootstrap, restart-to-change)** | `--id`, `--raft`, `--raft-advertised`, `--gossip`, `--gossip-advertised`, `--delivery`, `--data-dir`, `--bootstrap` ∣ `--join=<addr> --join-token=<tok>` ∣ optional `--root-cert-pin=sha256:…`, `--shards=1,2,3`, `--log-level`, `--metrics`, `--ca-issuer={builtin,acme,static-files}` (overrides `PlatformConfig.pki.mode` on first `--bootstrap` only; subsequent joiners read mode from shard 0). Env-var fallback for every flag (`REFLOW_ID`, `REFLOW_DATA_DIR`, …). Cert material lives under `${data_dir}/certs/` owned by CertMagic; no `--tls-cert-file` flag. | Needed before this node can talk to anyone. Per-node and divergent by design (different IDs, ports, host-local paths). |
| **Shard-0 `PlatformConfigTable` (fleet-wide policy, runtime)** | Authoritative mesh root CA cert (PEM), PKI issuer mode + per-mode settings (§6.15.4), KMS provider enablement, audit retention + GC cadence, snapshot URL + interval + tiered retention + scratch dir, rebalance mode + thresholds, handler `EagerStateMaxBytes`, ingress behavior (`MaxBodyBytes`, `MaxPollMs`, `TrustedProxies`, REST on/off), admin behavior, default tenant quotas, cluster-default operator OIDC issuers, auth policy content | Fleet-wide policy; drift between nodes is a bug. Auditable (the audit log on shard 0 already records mutations) and dynamically reconfigurable without rolling restarts. |

`pkg/reflow/config` package and the `koanf` + `koanf/providers/*`
dependencies all delete. Bootstrap goes through `flag.FlagSet` +
`os.Getenv` only, exposed via `cmd/reflowd serve [flags]`. The static
`cfg.Cluster.Peers` topology disappears: first node uses
`serve --bootstrap`, every subsequent node uses
`serve --join=<one-leader-addr> --join-token=<tok>` (see §6.15.7).

The split is honest about what each layer is: **CLI/env answers "how
does this node get on the network?"; Raft answers "what is this
cluster's policy?"** Anything that can legitimately differ per node
stays at the process boundary; anything that should be fleet-consistent
moves to Raft.

### 6.15.3 Chicken-and-egg, resolved

1. **First node, brand-new cluster.** `serve --bootstrap` triggers a
   one-time init sequence: generate a cluster root key (KMS-wrapped
   via the existing SecretStore KEK pipeline, §6.14), self-sign the
   root cert via the configured Issuer, propose the root cert + a
   seed `PlatformConfig` (from CLI defaults + the `--ca-issuer` flag)
   into shard 0 with `if_table_revision_eq=0` — the same seed-once
   pattern used by `EventSources.Sources` today (§11 "Cluster-managed
   app config"). The builtin Issuer then mints this node's leaf as
   part of the same atomic batch. Subsequent `--bootstrap` invocations
   against an already-initialized cluster are rejected.
2. **Restart while Raft is unreachable.** The per-node reconciler
   caches the last-known `PlatformConfig` in local Pebble (shard-0
   state is already on-disk via `internal/engine/cluster/store.go`).
   Listeners come up from the cached snapshot, reconcile against the
   authoritative copy when shard 0 catches up. Mechanically identical
   to the `internal/secretstore` Resolver — `atomic.Pointer` swapped
   post-reconcile.
3. **Trust bootstrap for new joiners.** No pre-issued cert; the
   joiner has only a one-time join token (`--join-token=<tok>`) and
   optionally a root-cert-hash pin (`--root-cert-pin=sha256:…`). The
   first call to `ClusterCtl/SelfJoin` dials the leader with
   `InsecureSkipVerify=true` (or pin-verified when `--root-cert-pin`
   is set), presents the token + a CSR, receives a freshly-minted
   leaf + the authoritative root cert PEM, and from that moment on
   uses full mTLS verification against the pinned root. CA rotation
   is fleet-wide via shard-0 mutation; CertMagic on every node picks
   up the new root in the next reconcile pass. See §6.15.7 for the
   full dance.
4. **Bind-address vs behavior.** Listener `Addr` and `Disabled` stay
   local (you may legitimately want one node's ingress disabled for a
   maintenance window without proposing a fleet-wide change). The
   behavioral knobs (`MaxBodyBytes`, `MaxPollMs`, `TrustedProxies`,
   REST on/off) move to Raft.
5. **Hot-swap mechanics.** Most knobs hot-swap trivially via the
   existing `atomic.Pointer` pattern — consumers do a single `Load()`
   on the hot path. A few (auth policy content, listener cert paths)
   need the "build new, drain old" pattern the webhook `Manager`
   already uses (`internal/ingress/webhook/manager.go`).

Loss of "ssh in and edit a yaml" debuggability is replaced by
`reflowd config platform set ...` against the admin RPC. This is a
feature: every change flows through the audited path, no quiet
out-of-band edits.

### 6.15.4 PKI via CertMagic + pluggable Issuer

Today's offline `reflowd pki {init-ca|issue-cert|issue-operator}`
subcommands, the `internal/pki` package, the per-node cert paths in
koanf, the SPIFFE URI SAN extraction, and the operator's manual
cert-rotation choreography all collapse into a CertMagic-managed
lifecycle with a single configurable seam: `certmagic.Issuer`.

Three issuer modes ship in-binary, always-linked and config-gated —
same pattern as KMS providers in `pkg/kms/{aws,gcp,blob,hcvault}/` and
event-source backends in `internal/ingress/eventsource/factory_*.go`:

| Mode | When to use | Implementation |
|---|---|---|
| **`builtin`** (default sub-mode `signing_mode = local`) | No external CA; the cluster is its own CA. Default for non-k8s deployments. A custom `certmagic.Issuer` impl that signs leaves with a cluster root key stored in shard 0 as a `SecretRecord` — Tink keyset, KMS-wrapped at rest through the existing SecretStore KEK pipeline (§6.14, `pkg/kms/{aws,gcp,blob,hcvault}/`). Per-node `SecretStore` Resolver decrypts at boot; plaintext `*ecdsa.PrivateKey` sits in `atomic.Pointer` and `crypto/x509.CreateCertificate` signs in-process. Every issuance is proposed through the FSM, so the audit log records every cert event (issuance, renewal, revocation). An opt-in `signing_mode = kms_remote` sub-mode keeps the private key inside the KMS HSM (see below). | `pkg/pki/builtin/` — `internal/pki`'s existing `crypto/x509` code refactored behind the `Issuer` interface. ~300 LOC. |
| **`acme`** | External ACME-compatible CA: Smallstep CA, HashiCorp Vault PKI's ACME endpoint, AWS Private CA, anything that speaks ACME. Best for organizations with an existing private-CA fleet. | `certmagic.ACMEIssuer` configured with `CA = <operator-provided-URL>`. Zero custom code; CertMagic already implements this. |
| **`static-files`** | Kubernetes deployments using cert-manager (cert-manager-csi-driver, csi-driver-spiffe, trust-manager); operator-provisioned sidecar injection; any "the cert is already on disk and rotates externally" setup. | CertMagic's `CacheUnmanagedCertificatePEMFile` + a filesystem watcher that re-loads on cert change. ~50 LOC of wiring; no Issuer impl — the external system *is* the issuer, Reflow only consumes. |

Mode selection is `PlatformConfig.pki.mode` (so it's fleet-consistent
and auditable; flipping `builtin` → `acme` is one platform-config
mutation). The `--ca-issuer` CLI flag is honored only on the very
first `serve --bootstrap` (chicken-and-egg — shard 0 is empty so there
is no fleet config yet); subsequent joiners and steady-state operation
read the mode from `PlatformConfigTable`.

The `Issuer` interface is the single integration seam. Adding a fourth
mode (Reflow ↔ Vault Transit via Vault's signing API, direct
Kubernetes `CertificateRequest` CR creation, an HSM-backed signer) is
one new implementor without disturbing existing modes.

**Optional `signing_mode = kms_remote` for compliance:**

For environments where the cluster root *private key must never enter
Reflow's memory* (FIPS-mandated configurations, some financial-services
and gov audits), the `builtin` Issuer accepts an opt-in sub-mode that
swaps signer construction without changing the `Issuer` interface or
any downstream code:

| Sub-mode | Signer | Root key location |
|---|---|---|
| `local` (default) | Loads the SecretStore-resolved plaintext `*ecdsa.PrivateKey` into memory; `crypto/x509.CreateCertificate` signs in-process. | Encrypted at rest (Tink keyset, KMS-wrapped via `pkg/kms/`); plaintext in Reflow memory after boot. |
| `kms_remote` | A `crypto.Signer` adapter whose `Sign(digest, opts)` dispatches to the KMS provider's asymmetric-sign API (AWS KMS `Sign`, GCP KMS `AsymmetricSign`, Vault Transit `sign`). Public key fetched once at startup; private key never accessible to Reflow. | Inside the KMS HSM; never extractable to any host. Every cert issuance is one KMS round-trip (~50–150ms — acceptable since issuance is rare: joins + ~monthly renewals, never on hot paths). |

`PlatformConfig.pki.builtin.signing_mode` selects between them;
`signing_key_uri` carries the KMS URI in both cases (in `local` it's
the wrapping KEK for the SecretRecord; in `kms_remote` it's the signing
key itself). Switching modes requires generating a new root and
re-bootstrapping — the public keys differ, so all existing leaves
become invalid. Not a runtime flip.

The CertMagic `Issuer` interface, the FSM audit-log path, identity
extraction, and the renewal/revocation RPCs are identical across
sub-modes. Only signer construction in `pkg/pki/builtin/` changes.
`local` is the right v1 default; `kms_remote` is for the compliance
case and is over-engineering otherwise (the memory-extraction threat
implies host-root compromise, at which point the attacker has many
other things to steal).

**CertMagic-side ownership (regardless of mode):**

- Cert files live at `${data_dir}/certs/<...>` via CertMagic's
  `FileStorage`.
- Renewal scheduler runs in-process; renewals re-call the configured
  Issuer (`builtin` → Raft round-trip via `ClusterCtl/RenewCert`;
  `acme` → ACME re-issuance against the configured CA URL;
  `static-files` → no-op, external rotation handles it).
- Hot-reload into listener `*tls.Config` happens via the
  `GetCertificate` callback — no listener restart on rotation.
- OCSP stapling is automatic where the issuer supports OCSP (`builtin`
  doesn't ship an OCSP responder in v1 — operator picks short leaf
  lifetimes instead; revocation propagates via `PlatformConfigTable`
  notifier).

**Identity extraction (replaces SPIFFE URI SAN parsing):**

- `CN=node/<id>` for cluster nodes. `ClusterCtl/AddNode` and
  `ClusterCtl/SelfJoin` gate on `CN.id == req.node_id` — same
  defense-in-depth as today's SPIFFE check (`checkSelfJoinPrincipal`),
  just reading a different field on the leaf.
- `CN=operator/<name>` for cluster operators authenticating via mTLS.
- Tenant admins authenticate via OIDC only (per §6.15.1) — never via
  cluster-CA-signed certs. Tenant ↔ engine handler RPCs use either
  the cluster mesh CA (when tenants accept "use ours") or a per-tenant
  CA bundle attached to the `TenantRecord` (BYO).

**Removed by this rework:**

- `cmd/reflowd/pki.go` and the offline
  `reflowd pki {init-ca,issue-cert,issue-operator}` subcommands.
- `internal/pki/` (refactored into `pkg/pki/builtin/`).
- `pkg/spiffe/` and the SPIFFE URI parsing in
  `internal/auth/spiffe_authfunc.go` (renamed → `mesh_authfunc.go`,
  switches to CN extraction).
- `cfg.Auth.TrustDomain`, listener `--tls-cert-file` /
  `--tls-key-file` flags, manual per-node cert-path management.
- The CA-bundle-file-in-koanf bootstrap step (replaced by the
  join-token + optional pin dance in §6.15.7).

The auth chain in §6.13 stays structurally identical — still
"verify TLS chain + extract identity from leaf" — just with a
different identity-extraction function and CertMagic owning the
material lifecycle.

### 6.15.5 Audit bifurcation

Mutations on the three planes route to two logical audit streams
backed by one physical table:

| Stream | Records | Retention controlled by | Sink controlled by |
|---|---|---|---|
| **Cluster audit** (`AuditLogTable`, `tenant_id=0`) | Platform + tenant-lifecycle plane mutations: create/delete tenant, set quota, change platform config, rotate mesh CA, upsert cluster-default OIDC, etc. | `PlatformConfig.audit.retention_duration` (currently `AuditConfig.RetentionDuration` in koanf — moves to Raft) | `PlatformConfig.audit.sink` (currently `AuditConfig.Logger` — moves to Raft as a sink descriptor; operator wires the concrete `slog.Handler` programmatically) |
| **Tenant audit** (`AuditLogTable`, keyed by `tenant_id`) | Tenant-config plane mutations: upsert event source, rotate secret, add OIDC issuer, etc. | `TenantRecord.audit.retention_duration` (per-tenant) | `TenantRecord.audit.sink` (per-tenant — tenant admin pipes to their own SIEM) |

One physical table keyed by `(tenant_id, ts_ms, seq)` is operationally
simpler than two and matches the schema we already have:
`AuditLogRecord.tenant_id` exists today (proto field 5). The
access-control boundary is enforced by the Config service — a
tenant-admin's read query is scoped by their principal's `tenant_id`.
A two-table physical split is a defensive option if a tenant ever
needs sovereignty over their audit (separate Pebble keyspace, separate
retention loop) and is deferred until a real requirement appears.

The retention GC pass on the metadata leader (`Command_GcAuditLog`)
already accepts a `before_ts_ms` bound; extending it to a
`(tenant_id, before_ts_ms)` tuple lets per-tenant retention land
without changing the GC mechanic.

### 6.15.6 Current state and migration gap

Landed (recent commits):

- Shard-0 `TenantTable` with `ClusterCtl` CRUD + `tenant_name_idx`.
- `tenant_id` propagated end-to-end through the apply path:
  `InvokeCommand` → `Scheduled` (slot-0 transient) → `JEInput`
  (durable) → `InputCommandMessage` headers → `ctx.Metadata()`.
- Per-tenant `TenantDEKTable` + value-AEAD wrapper in
  `internal/storage/encstore`.
- Per-tenant OIDC reconciliation against `TenantTable` notifier
  (`internal/auth/tenant_oidc_reconciler.go`) — cluster-default
  issuers seed every snapshot, per-tenant issuers fold in on top.
- Per-tenant in-flight invocation quota.
- `AuditLogTable` on shard 0 with config-change audit log + retention
  GC (cluster-audit stream is live; tenant-audit stream awaits the
  per-tenant retention/sink wiring).

Not yet landed (the work this section motivates):

1. **`PlatformConfigTable` + bootstrap-seed path** — proto, FSM,
   ClusterCtl RPCs, CLI (`reflowd config platform {get,set,describe}`),
   per-node reconciler with the `atomic.Pointer`-swap pattern. The
   biggest single piece.
2. **`tenant_id` on `EventSourceRecord`, `WebhookSourceRecord`,
   `SecretRecord`** — currently flat. Required for tenant-admin
   authz to mean anything. Webhook path uniqueness becomes per-tenant
   (`/webhooks/<tenant_id>/...` or virtual-host-keyed at the
   `pkg/hostmux` seam).
3. **CLI restructure** — `reflowd config eventsources` →
   `reflowd config --tenant=<id> eventsources` (or
   `reflowd tenant <id> eventsources`); top-level
   `reflowd config platform ...` becomes the platform-only entry.
4. **koanf removal entirely** — delete `pkg/reflow/config`, drop the
   `koanf` + `koanf/providers/*` deps, replace with `flag` + env-var
   read in `cmd/reflowd serve`. The `Config` struct on `pkg/reflow`
   survives only as a programmatic embed-time type for SDK consumers
   wanting to construct a Host without the CLI.
5. **CertMagic adoption + `pkg/pki/{builtin,acme,static-files}/`** —
   refactor `internal/pki` → `pkg/pki/builtin/` (implements
   `certmagic.Issuer`), wire `acme` mode (no custom code beyond
   reading the operator-provided URL out of `PlatformConfig`) and
   `static-files` mode (filesystem watcher +
   `CacheUnmanagedCertificatePEMFile`). Delete `cmd/reflowd/pki.go`
   and `pkg/spiffe/`. Rename `internal/auth/spiffe_authfunc.go` →
   `mesh_authfunc.go`, switch to CN extraction.
6. **Join-token bootstrap** — `JoinTokenTable` on shard 0,
   `reflowd cluster token issue` CLI subcommand, `ClusterCtl/SelfJoin`
   rewritten to consume tokens + CSRs + mint leaves via the
   configured Issuer. `ClusterCtl/RenewCert` for steady-state
   renewal. `RevocationRecord` for revocation. See §6.15.7.
7. **EventSources koanf bootstrap removed** — moot once koanf is
   gone; event sources are tenant-admin-managed only.
8. **Audit bifurcation wiring** — `AuditLogRecord.tenant_id` already
   exists; add per-tenant retention duration on `TenantRecord` and
   per-tenant sink descriptor, extend `Command_GcAuditLog` to the
   `(tenant_id, before_ts_ms)` tuple.
9. **Authz: path-glob → Cedar.** Embed Cedar schema at
   `pkg/authz/schema.cedar`. Add `PlatformConfigTable.cluster_authz_policy_text`
   and `TenantRecord.tenant_authz_policy_text` proto fields. Implement
   `policyHandler` middleware against `cedar.Authorize` with a
   procedure→resource-extractor map. Implement upload-time checks:
   schema validation (cedar-go ships `x/exp/schema/validate`), AST
   structural lint (~150 LOC), PBT invariant test in CI. Delete
   `starter_policy.json`, `cfg.Auth.PolicyFile`, and the 30s polling
   watcher. Per-node reconciler with `atomic.Pointer[*cedar.PolicySet]`
   swap on `PlatformConfigTable` / `TenantTable` notifier wake. See
   §6.15.8.

Reflow is pre-production (per `CLAUDE.md`); each item lands as a
non-backward-compatible refactor with no compat shims.

### 6.15.7 Join-token bootstrap

Replaces the offline cert-provisioning flow. Modeled on kubeadm's
discovery-token pattern, adapted to Reflow's "everything-fleet-wide-
goes-through-Raft" shape so every cert event is audit-logged on
shard 0 like every other state change.

**Operator workflow:**

```
# 1. Operator mints a one-time token (any leader-resolved node):
reflowd cluster token issue --node-id=4 --ttl=1h
→ TOKEN=reflow-jt-<base32>
→ ROOT_PIN=sha256:<hash>      # optional, printed alongside

# 2. New node boots with the token:
reflowd serve --id=4 --data-dir=/var/lib/reflow \
              --raft=:5400 --gossip=:5401 --delivery=:5402 \
              --join=node1.example.com:5500 \
              --join-token=$TOKEN \
              [--root-cert-pin=$ROOT_PIN]
```

**Internal sequence (`cmd/reflowd serve` startup path):**

1. CertMagic, in the configured mode, generates a private key for
   this node and constructs a CSR with `CN=node/4` (matching `--id`).
2. Node dials `--join=<addr>` with a `tls.Config` that either:
   - sets `InsecureSkipVerify=true` (no pin — TOFU), or
   - sets a custom `VerifyPeerCertificate` checking the leaf chain
     to a root whose `sha256(DER)` matches `--root-cert-pin`.
3. Calls `ClusterCtl/SelfJoin{token, csr, node_id}`. This is the
   only RPC that accepts non-mTLS authentication, gated by token
   presentation.
4. Cluster server:
   1. Verifies token: hash matches a `JoinTokenRecord` row on
      shard 0, `record.allowed_node_id == req.node_id`,
      `record.expires_at_ms` in the future, `record.consumed_at_ms == 0`.
   2. Verifies CSR's `CN` matches `record.allowed_node_id`.
   3. Dispatches CSR through the configured Issuer (`builtin` →
      sign with cluster root; `acme` → forward CSR upstream;
      `static-files` → reject, this mode doesn't support online
      issuance and joiners must arrive with cert+key already
      mounted).
   4. Proposes one atomic Raft batch: `RegisterNode` +
      `PromoteToVoter` + `ConsumeJoinToken` (sets `consumed_at_ms`,
      prevents replay) + `IssueCertRecord` (audit attribution).
   5. Returns `IssuedLeaf{cert_pem, chain_pem, root_cert_pem,
      not_after_ms}` plus the current `PlatformConfig` snapshot.
5. New node:
   1. Persists leaf + chain to `${data_dir}/certs/` via CertMagic.
   2. Pins `root_cert_pem` as the trust anchor in its local
      snapshot (fail-loud if `--root-cert-pin` was provided and
      doesn't match).
   3. Switches all subsequent client+server `tls.Config`s to
      full mTLS verification against the pinned root.
   4. Starts dragonboat shards normally.

**`JoinTokenRecord` shape (shard-0 table):**

- `token_hash`: SHA-256 of the plaintext token (plaintext shown to
  the operator once at issuance, never stored).
- `allowed_node_id`: the only node-ID this token authorizes.
- `expires_at_ms`: TTL bound (default 1h, configurable per-token).
- `consumed_at_ms`: 0 until used; set atomically with `RegisterNode`
  to prevent replay.
- `issued_by`: operator principal that called `cluster token issue`
  (for audit attribution).

**Trust model:**

- With `--root-cert-pin`: bootstrap is fully authenticated end-to-end;
  the dial verifies the leader's leaf against a pre-known root before
  any token is sent. Token is bound to a node-id, so even a leaked
  token can't register a different identity.
- Without the pin: TOFU at the moment of `SelfJoin`. An active MITM
  could intercept the token and register themselves as node-4.
  Mitigated by short TTL (default 1h), bound-node-id (attacker would
  also have to race the legitimate node-4 to register first), and
  the audit log row that records every `RegisterNode` (operator sees
  the unexpected join in their SIEM within minutes).

**Steady-state lifecycle:**

- CertMagic renewal scheduler fires ~30 days before leaf expiry
  (configurable per-issuer in `PlatformConfig.pki`).
- Renewal RPC is `ClusterCtl/RenewCert{csr}` — full mTLS, no token
  needed (the existing leaf is the credential).
- Cluster verifies caller identity matches CSR's `CN`, re-issues
  via the configured Issuer, proposes `IssueCertRecord` into shard 0.

**Revocation:**

- `reflowd cluster revoke-cert --node-id=N` proposes a
  `RevocationRecord` into shard 0. Auth chain on every node consults
  the revocation set during chain verification — one
  `atomic.Pointer.Load` on the hot path; revocations propagate via
  the `PlatformConfigTable` notifier path.
- No OCSP responder in v1. Short leaf lifetimes (operator-tunable,
  default 30d) + fast revocation propagation cover the same risk
  surface without the OCSP infrastructure.

### 6.15.8 Authz via Cedar

Today's path-glob policy (`internal/auth/starter_policy.json`) can
express plane separation ("`operator/*` may call
`/reflow.clusterctl.v1.ClusterCtl/*`") but cannot express the
predicate tenant isolation requires:
*a TenantAdmin principal may mutate an EventSourceRecord only when
`record.tenant_id == principal.tenant_id`*. That comparison lives on
the request body, not the URL, so a path-glob engine collapses to
"every handler remembers the check" — a class of bug that should
not exist in 2026 (and is exactly the kind of cross-cutting concern
an authz engine exists to centralize).

The refactor moves authz onto Cedar
(`github.com/cedar-policy/cedar-go` v1.6.2), with policies stored on
shard 0 like the rest of cluster policy (§6.15.2). Cedar fits Reflow
because the security model is IAM-shaped — principals, actions,
resources, context with attribute predicates — not a general-purpose
policy language. The cedar-go dep is ~2-3 MB, the policy language is
type-checked at upload time, and the AST is exposed for the
structural lint passes Reflow uses to gate tenant uploads.

**Schema — the trust shape (`pkg/authz/schema.cedar`, embedded via
`//go:embed`):**

```cedar
// Principal entity types — one per authn outcome
entity ClusterOperator;                     // mTLS CN=operator/<name> or OIDC kind=operator
entity Node { node_id: Long };              // mTLS CN=node/<id>
entity TenantAdmin {
    tenant_id: Long,                        // from OIDC tenant claim
    subject:   String
};
entity AnonymousJoiner;                     // SelfJoin token-bearer, no cert yet

// Resource entity types — one per persisted record + each action target
entity EventSourceRecord   { tenant_id: Long, name: String };
entity WebhookSourceRecord { tenant_id: Long, name: String };
entity SecretRecord        { tenant_id: Long, name: String };
entity DeploymentRecord    { tenant_id: Long, name: String };
entity TenantRecord        { tenant_id: Long };
entity PlatformConfig;
entity Invocation          { tenant_id: Long, service: String };

// Actions — Cedar enforces principal/resource type pairs at upload
action UpsertEventSource    appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action UpsertWebhook        appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action UpsertSecret         appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action SubmitInvocation     appliesTo { principal: [ClusterOperator, TenantAdmin], resource: Invocation };
action AddNode              appliesTo { principal: [ClusterOperator],              resource: PlatformConfig };
action SelfJoin             appliesTo { principal: [AnonymousJoiner],              resource: PlatformConfig };
action UpsertTenant         appliesTo { principal: [ClusterOperator],              resource: TenantRecord };
action UpsertPlatformConfig appliesTo { principal: [ClusterOperator],              resource: PlatformConfig };
// ... one action per Connect RPC across ClusterCtl + Config + Ingress
```

The `appliesTo` clauses are the *first* line of tenant isolation: a
`TenantAdmin` principal cannot even attempt `AddNode` — Cedar's
parser rejects the policy at upload time. No interceptor code
enforces this; the schema does.

**The four foundational cluster policies
(`PlatformConfigTable.cluster_authz_policy_text`):**

```cedar
// 1. Cluster operators have god-mode.
permit (principal in ClusterOperator::"*", action, resource);

// 2. Nodes can only call inter-node Delivery RPCs.
permit (
    principal in Node::"*",
    action in [Action::"DeliveryDeliver", Action::"DeliveryListUndelivered"],
    resource
);

// 3. Token-bearing joiners can call exactly one RPC.
permit (
    principal == AnonymousJoiner::"joiner",
    action == Action::"SelfJoin",
    resource
);

// 4. THE tenant-isolation rule. The entire cross-tenant guarantee
//    lives on this when-clause.
permit (
    principal in TenantAdmin::"*",
    action in TenantConfigActions,        // see schema
    resource
)
when {
    resource.tenant_id == principal.tenant_id &&
    principal.tenant_id > 0               // 0 is reserved for cluster
};

// Default-deny: anything not permitted is denied.
```

Seven lines, no handler code, no drift. The `when` clause is the
entire cross-tenant guarantee — replaces ~15 hand-written
`if principal.tenant_id != record.tenant_id { PermissionDenied }`
checks that would otherwise have to live in every config-plane
handler.

**Per-tenant policies (`TenantRecord.tenant_authz_policy_text`)**
are optional Cedar text that *restricts* the cluster policy further
— never escalates. Common shape:

```cedar
// Tenant 12 locks deployments to a specific CI service principal.
permit (
    principal == TenantAdmin::"ci-bot@t12",
    action in [Action::"UpsertDeployment", Action::"DeleteDeployment"],
    resource
)
when { resource.tenant_id == 12 };

forbid (
    principal in TenantAdmin::"*",
    action in [Action::"UpsertDeployment", Action::"DeleteDeployment"],
    resource
)
when { principal != TenantAdmin::"ci-bot@t12" };
```

The structural lint (below) ensures a tenant cannot upload a policy
that affects another tenant's resources or grants itself privileges
outside its own tenant.

**Resource extraction at the policyHandler middleware
(`internal/auth/policy.go`):**

```go
// One central central central map: Connect procedure → (Action, resource-extractor).
// Auto-generatable from a proto annotation; hand-written acceptable
// for v1 — the surface is ~15 RPCs.
var procedureMap = map[string]procEntry{
    "/reflow.config.v1.Config/UpsertEventSource": {
        action: "UpsertEventSource",
        resource: func(msg proto.Message) cedar.EntityUID {
            m := msg.(*configv1.UpsertEventSourceRequest)
            return cedar.NewEntityUID("EventSourceRecord", m.Record.Name)
        },
        attrs: func(msg proto.Message) types.Record {
            m := msg.(*configv1.UpsertEventSourceRequest)
            return types.NewRecord(map[types.String]types.Value{
                "tenant_id": types.Long(m.Record.TenantId),
                "name":      types.String(m.Record.Name),
            })
        },
    },
    // ... one entry per RPC
}

func evalAuthz(ctx context.Context, procedure string, principal Principal, msg proto.Message) error {
    entry := procedureMap[procedure]
    principalUID := buildPrincipalUID(principal)        // e.g. TenantAdmin::"alice@t12"
    resourceUID  := entry.resource(msg)

    entities := types.EntityMap{
        principalUID: {Attributes: buildPrincipalAttrs(principal)},
        resourceUID:  {Attributes: entry.attrs(msg)},
    }
    req := cedar.Request{
        Principal: principalUID,
        Action:    cedar.NewEntityUID("Action", entry.action),
        Resource:  resourceUID,
    }
    policySet := authz.Snapshot.Load()                  // atomic.Pointer[*cedar.PolicySet]
    decision, _ := cedar.Authorize(policySet, entities, req)
    if decision != cedar.Allow {
        return connect.NewError(connect.CodePermissionDenied, errPolicyDenied)
    }
    return nil
}
```

One central evaluation point; tenant_id flows from request body into
the policy engine through a central central seam. Per-request cost: one
`atomic.Pointer.Load` + one `cedar.Authorize` call — single-digit
microseconds against prepared policies.

**Three-layer upload-time guarantee for tenant policies:**

| Layer | What it catches | Cedar-go primitive |
|---|---|---|
| **(1) Schema validation** | Policy references a field that doesn't exist (e.g., `resource.secret_kek_uri` when the resource is `EventSourceRecord`). Policy violates an `appliesTo` clause (TenantAdmin on AddNode). | `x/exp/schema/validate.Validator` — ships in v1.6.2 |
| **(2) AST structural lint** | Tenant policy without `resource.tenant_id == principal.tenant_id` in its `when` clause; tenant policy referencing a principal other than `TenantAdmin`; tenant policy referencing a resource type whose schema lacks `tenant_id`; tenant policy using `principal in ClusterOperator::"*"`. | Walks the `cedar/ast.PolicySet`; ~150 LOC. Reflow-specific. Caches the proven invariant at upload, so runtime eval doesn't need to re-prove it. |
| **(3) Property-based invariant test** | On policy commit, generates *N* synthetic `(principal, resource)` pairs with mismatched `tenant_id`s via `rapid` (matches `internal/engine/pbt_test.go` style), asserts every result is `Deny`. Runs in CI on every `cluster_authz_policy_text` mutation; runs as a fast sample (~50 pairs) on every tenant policy upload. | `x/exp/batch.Authorize` for sweeping; `pgregory.net/rapid` for generation. |

Honest caveat: cedar-go v1.6.2 does not ship the formal SMT-based
analyzer the Rust reference impl provides. Layers (1)+(2)+(3) cover
the practical surface — schema rejects type errors, structural lint
forbids ill-shaped tenant policies by *construction*, PBT catches
escape attempts on random inputs. When cedar-go upstreams formal
analysis (`x/exp/analyze` is an active workstream), Reflow layers
it on as a fourth check without disturbing the existing seam.

**Storage + reconciler shape (matches the rest of §6.15):**

| Where | What | Owner |
|---|---|---|
| Embedded binary (`pkg/authz/schema.cedar`, `//go:embed`) | Cedar schema — entities + actions. Versioned alongside protos; ships with the binary. | Reflow |
| `PlatformConfigTable.cluster_authz_policy_text` | The four foundational policies. Mutation via `Config.UpsertClusterAuthzPolicy` RPC. Pre-commit checks: schema validation + PBT invariant tests against the *cluster* policy. | Cluster operator |
| `TenantRecord.tenant_authz_policy_text` | Optional per-tenant Cedar text — additive restrictions only. Mutation via `Config.UpsertTenantAuthzPolicy` RPC, scoped to the principal's tenant_id. Pre-commit checks: schema validation + structural lint (layer 2) + fast PBT sample. | Tenant admin |
| Per-node | `atomic.Pointer[*cedar.PolicySet]` — compiled bundle of (cluster ∪ all tenants). Swapped post-reconcile on `PlatformConfigTable` / `TenantTable` notifier wake. | Reconciler |

**Removed by this rework:**

- `internal/auth/starter_policy.json` (embedded path-glob policy).
- `cfg.Auth.PolicyFile` (koanf field — moot post-§6.15.2 anyway).
- The 30s polling watcher in `internal/auth/policy_watcher.go`.
- ~15 hand-written `if principal.tenant_id != record.tenant_id`
  guard clauses across Config-plane handlers (replaced by the Cedar
  `when`-clause + structural lint).
- Principal-string-glob matching helpers in `internal/auth/glob.go`.

**The transition (string match → entity comparison):**

| Old (path-glob + principal string match) | New (Cedar) |
|---|---|
| Principal `"tenant-admin/12/alice"` ↔ glob `"tenant-admin/12/*"` | Principal `TenantAdmin::"alice@t12"` with attrs `{tenant_id=12, subject="alice"}` |
| Path `/reflow.config.v1.Config/UpsertEventSource` ↔ glob `/reflow.config.v1.Config/*` | Action `Action::"UpsertEventSource"` |
| No record-level check — every handler must remember | Resource `EventSourceRecord::"kafka-prod"` with attrs `{tenant_id=12, name="kafka-prod"}` + the schema-enforced `when { resource.tenant_id == principal.tenant_id }` rule |
| "Did principal tenant_id match record tenant_id?" lives in handler code (drifts, easy to forget) | Lives in one Cedar `when` clause + a structural lint that proves tenant uploads can't escape (single source of truth) |
