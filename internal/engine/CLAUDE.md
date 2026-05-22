# internal/engine — editor's notes

Rules for editing inside this package. Scope is the FSM, leadership, proposer, action dispatch, snapshotter, and the leader-only services. For what the package *is* see the repo-root CLAUDE.md.

## Goroutine model — read first

Pretty much every bug that's ever hidden in this package was a goroutine-affinity bug. Know which goroutine your code runs on:

| Surface | Goroutine | What's allowed |
|---|---|---|
| `Partition.Update` | dragonboat apply (serial per shard) | DB writes via `Batch`, pushes to `ActionCollector`. **Must not block on a Raft propose** (that would block the very apply that needs to finish for the propose to commit — deadlock). |
| `Partition.Lookup`, `Partition.SaveSnapshot` | dragonboat read / snapshot (concurrent with `Update`) | Read-only against the current `Snapshotter.Store()`. SaveSnapshot uses an online Pebble checkpoint; the DB stays open. |
| `Partition.Open` | dragonboat bootstrap | MUST return the highest applied Raft index from storage (`MetaTable.AppliedIndex`). |
| `OnActions` (= `PartitionRunner.dispatchActions`) | apply goroutine, after batch commit | Local non-blocking dispatch only — `timers.Push`, `outbox.Push`, `invoker.StartInvocation`. **No Raft propose.** |
| `OnSnapshotPersisted` | dragonboat snapshot goroutine | Non-blocking send into a buffered-1 channel. That's it. |
| `Leadership.OnRaftLeaderChange` | dragonboat `RaftEventListener` goroutine | MUST NOT block. Any propose work is dispatched to a background goroutine. |
| `Leadership.OnAnnounceLeader` | FSM apply path | This is where the real `Follower↔Leader` transition fires `onBecomeLeader` / `onStepDown`. The raft-event-listener path only kicks off candidacy. |
| `onBecomeLeader` / `onStepDown` | serial w.r.t. each other (called from `OnAnnounceLeader`) | May spawn leader-scoped goroutines. `onBecomeLeader` rebuilds timer + outbox from storage; `onStepDown` cancels `leaderCtx`, stops the invoker, then waits for timer + outbox `done` channels. |
| `TimerService.Run`, `OutboxService.Run` | leader-scoped goroutines (one each, lifetime = current leader epoch) | May `ProposeSelf` / `ProposeIngress` (SyncPropose blocks until commit). |
| `metadataRebalancer.failureLoop` / `stepLoop` | leader-scoped goroutines on shard 0 only | Drives dragonboat membership APIs + proposes shard-0 commands. |
| `raftEventListener` (host.go) | dragonboat event goroutine | Forwards leader-change events to the right `Leadership`. Non-blocking. |

If you're adding a callback, document which goroutine fires it and what's banned. The existing per-field comments in `partition.go` (`OnActions`, `OnSnapshotPersisted`) are the template — match them.

## The Action pattern (do not bypass)

The FSM in `partition.go` is pure: during `Update` it writes to the in-flight `storage.Batch` and pushes `Action`s into `ActionCollector`. After `batch.Commit`, if and only if this node is the partition leader, `OnActions(collector.Drain())` is called inline. The runner's `dispatchActions` translates Actions to local service pushes — it does not propose to Raft.

Hard rules:

- **`isLeader` is sampled once per batch** (top of `Update`). Every entry in the same batch sees the same gate. Don't re-read `Leadership.IsLeader()` per entry.
- **Followers clear the collector** before and after iteration; never let a follower-applied Action leak.
- **Returning an error from `Update` halts the shard** (dragonboat v4 `statemachine/disk.go:113`). Logical / unknown-command / bad-envelope failures MUST be logged-and-continued; the applied-index still bumps. Only genuine storage failures return.
- **Don't add new side effects directly to `Update`.** Express them as a new `Action`, drain via the collector, dispatch in `dispatchActions`. Anything proposed back into Raft from the dispatch path needs its own goroutine (the Invoker's session model is the existing template).

Mirrors restate `crates/worker/src/partition/state_machine/actions.rs`. Reflow's set is intentionally narrower — no notification/abort/ingress-response actions; wake path uses respawn-via-`ActInvoke`. Don't reintroduce the missing variants without a concurrent-reader plan.

## In-batch read coherence

Tables in `tables/` are generic over `storage.Reader`. Inside `Update`, bind them to the `Batch`, not to the snapshotter's `Store`:

```go
inv     := tables.InvocationTable{S: batch}
journal := tables.JournalTable{S: batch}
// ...
```

This gives read-your-writes coherence within a single multi-entry apply batch (pebble.IndexedBatch). Multi-entry batches under partition-heal previously stranded ~3% of invocations because entry-(K+M) saw the row as Free after entry-K had already written it.

**Apply-path reads against `snapshotter.Store()` directly are wrong.** Reach for the `Batch`.

## Leadership two-phase, and the epoch-bump trap

Two callbacks, two goroutines:

1. `OnRaftLeaderChange` (raft event goroutine) — observes the raft-level leader. If `raftLeaderID == ourID`, kick off candidacy: bump `leaderEpoch`, proposer `SetEpoch`, propose `AnnounceLeader` from a background goroutine.
2. `OnAnnounceLeader` (apply path) — when the `AnnounceLeader` for our epoch commits and we see it apply, we promote ourselves to `Leader` and call `onBecomeLeader`. A higher-epoch announce from someone else demotes us to `Follower`.

Two failure modes you must keep covered when editing `leadership.go`:

- **Re-run candidacy from both `Follower` and `Candidate`.** Under load, a Candidate whose announce never landed can be re-elected by dragonboat — a Follower-only guard would silently swallow the signal and the partition stays headless. Symptom: invocations stranded in `Scheduled`, observed in `TestChaos_LeaderLoss`.
- **Bump `leaderEpoch` past `latestAnnouncedEpoch`, not just past our own prior value.** SelfProposal dedup keys are `(leader_epoch, seq)` with no node_id; two leaders sharing an epoch collide on disk, and the new leader's `AnnounceLeader` gets silently dropped as a duplicate. Symptom: raft says we're leader, reflow never knows, partition is headless until restart.

Both are commented in `OnRaftLeaderChange`; mirror that level of comment density when you touch this code.

## Leader-only services lifecycle

`TimerService` and `OutboxService` instances are **recreated on every leader gain** because their `done` channels are single-use (a second `defer close(done)` panics). The runner pattern:

1. `onBecomeLeader` constructs fresh `TimerService` + `OutboxService` against `snapshotter.Store()`, rebinds the invoker's tables, calls `Rebuild` on each (reloads heap / queue from storage), starts the invoker, then spawns the two `Run` goroutines under a fresh `leaderCtx`.
2. `onStepDown` cancels `leaderCtx`, calls `invoker.Stop` (drains running sessions; once stopped no new timer/outbox actions arrive), then waits on `timerDone` + `outboxDone`. The old instances are then dropped on the floor.

Don't reuse a stale `TimerService` / `OutboxService`. Don't hold a reference past the `onStepDown` return.

Apply-on-startup edge case: when a node opens its store, the FSM may apply pre-existing committed entries whose `ActInvoke` actions go through `dispatchActions` while the Invoker is not yet started — those calls are dropped. The new leader's `onBecomeLeader` therefore calls `invoker.ResumeNonTerminal(InvocationTable)` to re-spawn sessions from disk. Preserve that call.

## Outbox cross-shard pop semantics

The outbox is the only place where a Raft-applied command pops a row that lives on a different shard:

- **Same-shard outbox** (sender == receiver): apply path pops the local row in the same batch it applies. Atomic.
- **Cross-shard outbox**: the producer's row is on another shard's `OutboxTable` and we cannot touch it. Instead, on apply, the receiver enqueues an `OutboxAck` envelope addressed back to the producer shard. The ack flows over Raft (same Delivery pipeline) and pops the producer-side row on apply.
- **Ack-for-ack guard**: when applying a command whose payload is itself an `OutboxAck`, do not emit a return ack. `OutboxAck.apply` already pops; a return ack would loop forever.

The dispatch lifecycle is in `partition.go` (look for `isOutboxProducer` / `parseOutboxProducerShard`) and the leader-side shuffler is in `outbox.go`. Failure cases: `SyncPropose` returning success means the receiver applied (row already popped); returning error means the row is still present and the next leader's `Rebuild` re-proposes — `Arbitrary` dedup absorbs the duplicate.

## Snapshotter (Pebble swap dance)

`SaveSnapshot` is online: it takes a fresh Pebble `Checkpoint` to a timestamped sibling dir, tars it to the writer, then removes the checkpoint. The current DB stays open the whole time.

`RecoverFromSnapshot` is the close-swap-reopen path:

1. Close current store.
2. Move `dataDir` → `dataDir.old` (rollback target).
3. Untar to `dataDir.staging` and rename to `dataDir`.
4. Reopen via the `StoreFactory` and replace `current`.
5. Remove `dataDir.old` only on success; on any failure restore from `.old` and surface the error.

Callers must re-read `Snapshotter.Store()` after a recovery — the pointer changes. The apply path already does (it dereferences at the top of every `Update`).

Pebble v1.1.5 detail: `Checkpoint` requires the destination dir **not to exist**. Use a fresh timestamped path.

## Proposer (RaftProposer)

- Wraps `dragonboat.NodeHost.SyncPropose` with reflow envelope framing + dedup stamping.
- OnDisk state machine ⇒ proposals use `GetNoOPSession`, not a regular client session.
- `SyncPropose` requires a `Context` with a deadline; the proposer attaches a 5s default when the caller didn't.
- `SetEpoch(epoch)` resets `nextSeq` to 0. Always call it on `onBecomeLeader` / leader-epoch bump. If you change SelfProposal seq allocation, preserve this reset — re-using `(epoch, seq)` triggers dedup absorption (silent message loss).
- Retries on `dragonboat.IsTempError` with `25ms ± 50ms` jitter to avoid replica-wide synchronization on leadership churn. Real errors (including `ErrShardClosed`) surface.

## Metadata shard (shard 0) specifics

The metadata FSM (`internal/engine/cluster`) reuses `Leadership` + `Snapshotter` + the same `RaftProposer` shape — the runner shape (`MetadataRunner`) deliberately mirrors `PartitionRunner` so the two stay aligned. Differences:

- Only accepts `AnnounceLeader`, `RegisterNode`, `UpdatePartitionTable`, and the rebalancer commands (`AddNode`, `EvictNode`, `CompleteRebalanceStep`, ...). Every other variant is logged and dropped — keep the forward-compat behavior.
- `onBecomeLeader` spawns the `metadataRebalancer`, which runs two goroutines:
  - `failureLoop` polls dragonboat's gossip `NodeHostRegistry` every second; after `missThreshold` consecutive unreadable observations of a peer's `NodeHostMeta`, proposes `EvictNode`.
  - `stepLoop` walks `PartitionTable.pending`, drives each step against the local NodeHost (`SyncRequestAddNonVoting` → `SyncRequestAddReplica` → `SyncRequestDeleteReplica`), and proposes `CompleteRebalanceStep` on success.
- `onBecomeLeader` also spawns `newLPMover(...).run`, which drives the in-flight `LPTransferTable` saga one phase per tick. And, when `cfg.Rebalance.Mode != off`, the autonomous LP `rebalance.Balancer` — three distinct leader goroutines with non-overlapping concerns: `metadataRebalancer` handles dragonboat membership, `lpMover` actuates transfers, the LP `Balancer` proposes them. The Balancer's only side effect is `Command_InitiateLPTransfer` — the same path the `reflowd cluster transfer-lp` CLI takes — so manual + autonomous transfers share one observability surface (`ListLPTransfers`).
- Bootstrap on leader gain proposes a static `UpdatePartitionTable` + a `RegisterNode` per peer. Both are idempotent: `UpdatePartitionTable` is a singleton overwrite; `RegisterNode` upserts.
- `OnPartitionTable` is the apply-path hook that drives ownership-based shard `StartPartition` / `StopPartition` on the metadata leader's host. Runs inline on the apply goroutine — handler offloads to a goroutine before doing any work.

## InvocationStatus FSM (`invocation_fsm.go`)

The transitions are pure functions returning `(newStatus, []Action, error)`. The apply path logs `ErrInvalidTransition` and continues — never propagate it to dragonboat. Each transition has a small docblock listing the allowed `from → to` arrows; preserve that style when adding cases.

Wake path is respawn-based, not notification-based: a `Suspended → Invoked` arrow on any journal append emits `ActInvoke` to start a fresh session. The session reads the journal from disk and replays. This is the deliberate divergence from restate's notification-channel model and the reason several of restate's Action variants are absent here.

## Things that look fine and aren't

- Reading `snapshotter.Store()` inside `Update` — use the `Batch`.
- Calling `proposer.ProposeSelf` from `dispatchActions` or `OnSnapshotPersisted` or `OnRaftLeaderChange` — wrong goroutine, deadlocks or stalls dragonboat.
- Returning an error from `Update` on a malformed envelope or unknown command — that halts the whole shard. Bump `applied_index` and continue.
- Holding a reference to `TimerService` / `OutboxService` past `onStepDown`.
- Bumping `leaderEpoch` past only `l.leaderEpoch` — must also clear `latestAnnouncedEpoch` (see leadership notes above).
- Allocating a new SelfProposal seq without `SetEpoch` resetting on leader transition — duplicates get silently dedup'd.
- Adding a callback without saying which goroutine it fires on and what's banned in it.

## Tests

- Unit tests sit next to each file (`partition_test.go`, `leadership_test.go`, ...). Use the in-memory `storage.Store` where you can — Pebble bringup is slow.
- Integration tests live in `engine_test` package (`integration_*_test.go`) and use `internal/loadgen` for the cluster bootstrap. They share the same in-process cluster shape as the chaos suite — keep `loadgen.NewCluster` agnostic of the `loadtest` build tag.
- `pbt_test.go` + `pbt_helpers_test.go` use `pgregory.net/rapid`. The collector / FSM purity makes the apply path a good PBT target — keep transitions deterministic w.r.t. `nowMs` (pulled from `Header.created_at_ms`, not local clock).
- `dragonboat` uses real ports and real disks; tests that allocate addresses go through `loadgen.FreeLocalAddr` to avoid collisions under `-parallel`.
