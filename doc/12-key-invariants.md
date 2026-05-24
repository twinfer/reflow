# 12. Key Invariants

These correctness invariants must hold at all times and should be validated by tests and assertions:

1. **Journal is append-only.** Entries are never modified after commit.
2. **Replay is deterministic.** Given the same journal, replay always produces the same execution.
3. **One active handler per invocation.** No two goroutines drive the same invocation simultaneously.
4. **One active invocation per Virtual Object key.** Single-writer enforced by VObject FSM.
5. **Timer entries survive restarts.** Timer heap is always rebuilt from Pebble on startup.
6. **State machine transitions are gated on Raft commit.** No state mutation before Raft consensus.
7. **Exactly-once for journal entries.** Raft entry index is the idempotency key for state machine apply.
