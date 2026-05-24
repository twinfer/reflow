# 8. Deployment Architecture

This document describes the deployment options and topology layout of a Reflow cluster.

---

## Single Node (Development)

```
┌─────────────────────────────────┐
│  Single Go binary               │
│  - All partitions local         │
│  - Single-node Raft groups      │
│  - Pebble in-process            │
│  - No network replication       │
└─────────────────────────────────┘
```

---

## Multi-Node (Production)

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
