# Solution Architecture Document
# Durable Execution Engine in Go

**Version:** 0.5 (Draft)
**Date:** 2026-05-11
**Status:** Phase 1 implemented; positioning + multi-node design locked in

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
| Raft consensus + replication | `lni/dragonboat` v4 | Apache 2.0 |
| Embedded K/V storage | `cockroachdb/pebble` | Apache 2.0 |
| gRPC / SDK protocol (Phase 2+) | `google.golang.org/grpc` | Apache 2.0 |
| HTTP/2 ingress (Phase 2+) | `net/http` (stdlib) | — |
| Snapshot archival (Phase 4 fs, Phase 5 cloud) | `gocloud.dev/blob` | Apache 2.0 |
| Serialization | `google.golang.org/protobuf` | BSD-3 |
| Structured logging | `log/slog` (stdlib) | — |
| Metrics | `prometheus/client_golang` | Apache 2.0 |

The invocation state machine uses a plain switch over the persisted
`InvocationStatus` discriminated union rather than an FSM library — see §6.4.
No third-party FSM dependency is required.

`pebble` is pinned to `v0.0.0-20221207173255-0f086d933dac` because
`lni/dragonboat/v4` (the pre-release in `go.mod`) embeds an older Pebble
`vfs.FS` shape; upgrading Pebble requires either a newer dragonboat release
or a `replace` directive aligning the two.

---

## 6. Component Design

### 6.1 Ingress Layer

Accepts invocations from external callers and SDK service-to-service calls.

**Responsibilities:**
- Authenticate and parse invocation requests (HTTP/2 + JSON or protobuf)
- Determine the target partition via consistent hashing on `(service_name, object_key)`
- Forward invocation commands to the correct Partition Processor via internal gRPC
- Return invocation ID to caller immediately (async) or stream response (sync/await)

**Routing:**
```
partition_id = hash(service_name + "/" + object_key) % num_partitions
```

**SDK Endpoint:**
Each SDK handler connects back to the ingress over HTTP/2. The runtime drives execution by streaming journal entries; the SDK handler streams commands back. This is a long-lived bidirectional stream per active invocation.

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
- Schema / version barrier: the minimum reflow-server version that must be
  running cluster-wide before higher-versioned commands are allowed (mirrors
  restate `VersionBarrier` at `crates/wal-protocol/src/v2.rs:226`).

**Bootstrap (no discovery service required):**

```bash
# Founder
reflowd --node-id=1 --raft-addr=10.0.0.1:9091 \
        --data-dir=/var/lib/reflow --bootstrap-cluster

# Joiners
reflowd --node-id=2 --raft-addr=10.0.0.2:9091 \
        --data-dir=/var/lib/reflow --join=10.0.0.1:9091
reflowd --node-id=3 --raft-addr=10.0.0.3:9091 \
        --data-dir=/var/lib/reflow --join=10.0.0.1:9091
```

Each node:

1. Constructs a `dragonboat.NodeHost`.
2. The founder starts shard `0` (the metadata group) with
   `initialMembers = {1: addr1}` and `join=false`.
3. Joiners contact the founder via `--join=<addr>`; the founder proposes
   `RequestAddReplica` against shard `0` and the joiner brings up the
   metadata replica with `join=true`.
4. Once the metadata group has a leader, that leader assigns partitions
   (round-robin or consistent-hash over the node set) and proposes the
   `PartitionTable`.
5. Every node observes the partition table and locally calls
   `StartOnDiskReplica` for the partitions assigned to it. Partitions it
   no longer owns are stopped via `StopShard`.

**Dynamic membership (in-binary CLI, no external service):**

```bash
reflow-cluster add-node    --target=10.0.0.4:9091 --node-id=4
reflow-cluster remove-node --node-id=2
```

These subcommands of `reflowd` proposing through the metadata group:

- *Add:* metadata leader proposes the new node into shard `0`, then drives
  per-partition `RequestAddNonVoting` → wait-for-catch-up →
  `RequestAddReplica` membership changes for the partitions being
  reassigned. The new node receives the partition table from shard `0`
  and starts the partitions it now owns.
- *Remove:* metadata leader proposes partition reassignments first
  (`RequestAddReplica` for replacement holders, then `RequestRemoveReplica`
  for the leaving node), and only removes the node from shard `0` last.

**Failure detection.** Dragonboat's built-in gossip (memberlist/SWIM,
enabled via `NodeHostConfig.AddressByNodeHostID = true` + `GossipConfig`)
runs SWIM probes between every NodeHost. Each observer turns `K`
consecutive failed probes against node `X` into a `RemoveNode` proposal
to shard `0`; the metadata leader, seeing reports above the eviction
threshold, commits the membership change. Eviction is a
strongly-consistent decision driven by an eventually-consistent signal.

**Discovery & endpoint resolution.** Two complementary sources:

- *Authoritative (shard 0):* partition table (`shard_id → [node_id ...]`),
  assignment epoch, schema version barrier. All routing decisions that
  affect correctness read from here.
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

```
                    ┌──────────┐
               ┌───▶│ Pending  │
               │    └────┬─────┘
               │         │ scheduled
               │    ┌────▼─────┐
               │    │ Scheduled│
               │    └────┬─────┘
               │         │ handler_ready
               │    ┌────▼─────┐
          retry│    │  Running │◀─────────┐
               │    └────┬─────┘          │ resume
               │         │                │
               │    ┌────▼─────┐    ┌─────┴────┐
               │    │  Failed  │    │Suspended │
               │    └────┬─────┘    └──────────┘
               │         │ has_retries
               └─────────┘
                         │ no_retries
                    ┌────▼─────┐
                    │  Dead    │
                    └──────────┘
                         
                    ┌──────────┐
                    │Completed │  (terminal, from Running)
                    └──────────┘
```

**Triggers:** `Schedule`, `HandlerReady`, `Suspend`, `Resume`, `Complete`, `Fail`, `Retry`

**On entry actions:**
- `Running` → stream journal to SDK handler, begin execution
- `Suspended` → release handler goroutine, persist suspension state
- `Completed` → write output to state, notify waiting callers
- `Retrying` → compute backoff, propose timer entry

---

### 6.5 Virtual Object State Machine

Also implemented with `qmuntal/stateless`. One FSM per active object key.

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

For non-Go handlers, reflow exposes an HTTP/2 bidirectional streaming
endpoint mirroring the shape of restate's `service-protocol-v4`. Each
message is a `RuntimeMessage` (runtime → SDK) or `SDKMessage` (SDK →
runtime); the framing reserves a `CUSTOM_MESSAGE_MASK` for forward-compat
(see restate `crates/service-protocol-v4/src/message_codec/mod.rs:33`).

Compatibility note: reflow tracks restate's wire format as a
*best-effort* compatibility target so existing TS/Python/Java/Kotlin/Rust
SDKs can connect with minimal adaptation. We do not commit to bug-for-bug
parity, nor to keeping pace with every Restate release. Non-Go SDKs are
explicitly out of scope for Phase 1–3 and ride along on whatever effort
the community contributes.

**Connection lifecycle (wire-protocol path):**
1. SDK handler registers its endpoint with the ingress on startup.
2. When the runtime schedules an invocation, it opens an HTTP/2 stream to
   the handler.
3. Runtime streams journal entries (replay or new commands).
4. Handler streams commands back (side effects, state reads/writes, output).
5. Stream closed on completion or suspension.

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

## 7. Key Data Flows

### 7.1 New Invocation (Happy Path)

```
1. Client POST /invoke/MyService/myMethod
2. Ingress parses request, computes partition_id
3. Ingress proposes InvokeCommand to Raft (partition_id)
4. Raft commits → state machine applies → InvocationRecord written to Pebble
5. Invocation FSM: Pending → Scheduled
6. Partition Processor finds available SDK connection for MyService
7. Invocation FSM: Scheduled → Running
8. Runtime opens HTTP/2 stream to SDK handler
9. Runtime streams INPUT journal entry
10. SDK handler executes, streams commands back
11. For each command: Partition Processor proposes journal entry to Raft
12. Entry committed → state machine applies → result streamed back to SDK
13. SDK streams OUTPUT command
14. Invocation FSM: Running → Completed
15. Output stored in Pebble, caller notified
```

### 7.2 Crash Recovery

```
1. Node crashes mid-invocation
2. Raft detects leader failure, elects new leader (another node with the partition)
3. New leader's dragonboat reloads IOnDiskStateMachine from Pebble snapshot
4. Replays any Raft entries after snapshot
5. Partition Processor starts on new leader
6. Scans Pebble for Running/Suspended invocations
7. Running invocations: FSM set back to Scheduled (handler connection lost)
8. Re-opens HTTP/2 stream to SDK handler
9. Streams full journal (from sequence 0) — SDK replays, skips already-done steps
10. Execution continues from last committed journal entry
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
| 5 | Raft replication factor | Open | Default 3; configurable per deployment; recommended minimum 3 in production. Decided per deployment. |
| 6 | Pebble per-partition vs. shared | Resolved | Per-partition Pebble DB implemented in Phase 1; no `partition_id` prefix in keys. |
| 7 | Exactly-once for non-idempotent external calls | Open (Phase 2) | Propagate idempotency keys through the `Invoke` command (restate-style); the dedup table already supports the Arbitrary producer path. |
| 8 | SDK protocol versioning | Resolved | Wire protocol tracks restate service-protocol-v4 as a *best-effort* compat target, not bug-for-bug. Phase 2 in-process Go SDK is the primary path; non-Go SDKs ride along on community effort. |
| 9 | timerfd vs `time.Timer` | Resolved | `time.Timer` for Phase 1. Revisit only with measured latency requirements. |
| 10 | `StateStore` alternative implementations | Resolved | `internal/storage.Store` interface; `MemStore` (tests) + `PebbleStore` (production). |
| 11 | Gossip for failure detection + soft state | Resolved | Use dragonboat's built-in gossip (memberlist/SWIM, vendored inside `lni/dragonboat/v4`) starting Phase 4 — zero extra dependency. Provides SWIM-based liveness, NodeHostID-stable endpoint resolution, and a `ShardView` leader hint cache. Architectural boundary unchanged: gossip is advisory, Raft (shard 0) is authoritative — eviction and partition assignment always go through a Raft proposal. Soft-state dissemination beyond the per-nodehost `Meta` blob is deferred; revisit only if observed load-hint dissemination requirements outgrow `Meta`. |
| 12 | Object storage for snapshots | Resolved | `SnapshotRepository` interface lands in Phase 4 (filesystem driver) and Phase 5 (S3/GCS/Azure via `gocloud.dev/blob`). Always optional — default deployment is local-only. Hot state never leaves Pebble; only snapshot artifacts and their metadata go to object storage. See §6.12. |

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

### Phase 2 — In-process Go SDK + Invoker

The first-class developer experience lands here: write a Go function,
register it with `reflowd`, and have it become a durable goroutine.

- **`reflow.Context`** Go API: `Sleep`, `Run`, `Call`, `OneWayCall`,
  `Get/Set/ClearState`, `Awakeable`, `SendSignal`.
- **In-process Invoker** that drives registered Go handlers, performs
  journal replay on resume, and produces `InvokerEffect` proposals via the
  partition's `Proposer.ProposeSelf`.
- **Ingress.** Local HTTP/gRPC entrypoint for invocation submission and
  awakeable resolution. This is also where `reflow-cluster ...` admin
  commands land.
- **Full journal entry types** beyond Phase 1's minimum: `Run` (generic
  deterministic side-effect), full state ops, awakeable, signals.
- **Exactly-once side-effect replay** via the journal.

Out-of-process / wire-protocol handler support is *also* in Phase 2 but
strictly secondary — implemented as a thin shim that turns wire messages
into the same `InvokerEffect` proposals the in-process Invoker emits.
SDKs in non-Go languages are not part of Phase 2 itself.

**Exit criteria:** A Go handler with `Sleep`, `Run`, state reads/writes,
and outgoing `Call`s survives mid-execution process crashes and resumes
correctly. The handler is registered as a Go function in a `reflowd`-linked
binary; no separate handler process is required.

### Phase 3 — Virtual Objects
- VObject status (switch on persisted oneof; no FSM library)
- Object K/V state in Pebble (the reserved `state/` namespace)
- Invocation queue per object key
- Single-writer enforcement

**Exit criteria:** Concurrent invocations on the same object key are serialized correctly under failure.

### Phase 4 — Multi-Node Replication

Target: a 3–10 node cluster. No external coordination service introduced
(see §6.2).

- **Embedded metadata Raft group** (`shardID = 0`) hosted by the same
  `NodeHost` as partition shards. Holds node list, partition table,
  partition assignment epoch, schema version barrier.
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
  procedure (using the version barrier from §6.2).
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
