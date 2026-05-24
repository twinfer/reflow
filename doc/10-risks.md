# 10. Risks

This document tracks system risks, likelihood/severity assessment, and their active mitigations.

---

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| Journal replay correctness bugs | High | Critical | Extensive property-based testing; formal spec |
| GC pauses causing Raft timeouts | Low | Medium | Tune `RTTMillisecond`/`HeartbeatRTT` generously; revisit if measured in load tests. timerfd integration deferred. |
| Pebble key schema migration | Medium | Medium | Resolved: per-DB `format` key (`internal/storage/format.go`) written on first open and checked on every subsequent open; mismatches fail loud rather than silently corrupting. `VersionBarrier` retired. |
| dragonboat API stability | Medium | Medium | Pinned to v4 pseudo-version; Pebble pinned to dragonboat's expected commit. Watch for an official v4 release. |
| SDK protocol breaking changes | Medium | High | Tracks restate service-protocol v7 / journal-v2 wire format as a best-effort compat target (avoid inventing a competing one). |
| Partition rebalancing data loss | Low | Critical | Test membership changes under load; chaos test coverage in `internal/chaos/`. |
