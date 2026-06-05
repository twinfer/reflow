# 7. Key Data Flows

This document details the primary runtime data flows and execution sequences in the Reflw engine.

---

## 7.1 New Invocation (Happy Path)

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

---

## 7.2 Crash Recovery

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

---

## 7.3 Virtual Object Invocation

```
1. Invocation arrives for VirtualObject "UserAccount" key "user-123"
2. Partition Processor reads KeyLeaseStatus for ("UserAccount", "user-123")
3. State = IDLE → atomically flip to ACTIVE with active_invocation_id set,
   invocation runs immediately
4. State = ACTIVE → append InvocationId to KeyLeaseStatus.queue (FIFO);
   the invocation row stays in Scheduled until the lease frees
5. Current invocation completes → apply path pops queue[0] into
   active_invocation_id and emits ActInvoke for the next holder
6. Queue empty and active completes → state flips ACTIVE → IDLE
```

---

## 7.4 Suspension (Waiting on External Event)

```
1. SDK handler calls ctx.Awakeable() → runtime returns (id, promise)
2. Runtime proposes AWAKEABLE journal entry, stores handle in Pebble
3. SDK handler calls ctx.Await(promise) → streams Await command
4. Invocation FSM: Running → Suspended
5. Handler goroutine released, HTTP/2 stream closed
6. External caller POST /v1/awakeables/{awakeable_id}/resolve with result
7. Ingress proposes CompleteAwakeable command to Raft
8. Entry applied → invocation FSM: Suspended → Running (Resume trigger)
9. Handler re-scheduled, journal replayed to suspension point
10. AWAKEABLE_RESULT streamed to SDK → execution continues
```
