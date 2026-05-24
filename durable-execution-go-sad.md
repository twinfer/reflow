# Solution Architecture Document
# Durable Execution Engine in Go

**Version:** 0.8 (Draft)
**Date:** 2026-05-22
**Status:** Single-node foundation, Connect bidi wire protocol between
engine and Go SDK (`pkg/handler`), Virtual Objects (single-writer gate,
idempotency, retry policy, eager state, attach RPCs), combinator futures
(`Promise.all` / `Promise.race`), multi-node replication (mTLS admin,
dynamic membership including joiner-driven `SelfJoin`, DR snapshots),
auth (SPIFFE mTLS for cluster mesh + OIDC bearer for ingress, embedded
starter policy with hot reload), cluster-managed app config (shard-0
tables for deployments, event sources, webhooks, secrets — Connect-RPC
admin surface split into `ClusterCtl` for fleet ops + `Config` for app
config, kubectl-shaped `reflowd config apply / get / export` CLI),
two-layer routing (4096 logical partitions over N shards, consistent-hash
planner, six-phase cross-shard LP transfer saga, autonomous LP rebalancer
with `off | advisory | auto` modes + operator drain) all implemented.
Non-Go SDKs outstanding.

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
  that hosts `pkg/handler` (HTTP/2). The engine talks to it via the
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
  │  │ Processor      │  │   │  │                │  │
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

This section details the architecture and design of Reflow's core components: Ingress, Cluster Manager, Partition Processor, FSM, and more.

👉 **Detailed documentation:** [Component Design](doc/06-component-design.md)

---

## 7. Key Data Flows

This section details the primary runtime data flows, execution sequences, crash recovery, and execution suspension.

👉 **Detailed documentation:** [Key Data Flows](doc/07-key-data-flows.md)

---

## 8. Deployment Architecture

This section describes the deployment options and topology layout of a Reflow cluster (Single-Node and Multi-Node).

👉 **Detailed documentation:** [Deployment Architecture](doc/08-deployment-architecture.md)

---

## 9. Open Questions

This section tracks resolved and outstanding architectural open questions for the Reflow engine.

👉 **Detailed documentation:** [Open Questions](doc/09-open-questions.md)

---

## 10. Risks

This section tracks identified system risks, assessed likelihood/severity, and their active mitigations.

👉 **Detailed documentation:** [Risks & Mitigations](doc/10-risks.md)

---

## 11. Delivery History

This section chronicles the historical progress and development phases of the Reflow engine.

👉 **Detailed documentation:** [Delivery History](doc/11-delivery-history.md)

---

## 12. Key Invariants

These correctness invariants must hold at all times and should be validated by tests and assertions.

👉 **Detailed documentation:** [Key Invariants](doc/12-key-invariants.md)
