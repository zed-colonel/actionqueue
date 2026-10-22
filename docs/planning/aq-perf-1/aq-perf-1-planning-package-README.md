# AQ-PERF-1 Planning Package

ActionQueue 0.2.0's mutation and recovery costs grow with retained history: every mutation copies
the whole projection, and bootstrap replays the complete WAL. WorldInterface's WIP-07 measured
this at its supported scale. This package plans the fix for one 0.3.0 release. It changes no
normative contract; recovery and retention decisions are recorded as ADRs.

## Read in this order

1. `aq-perf-1-implementation-plan.md`: the baseline facts with file references, the constraints, and
   each item's approach and design decisions.
2. `aq-perf-1-work-breakdown.yaml`: the items, their dependencies, risk and exit gates, for import.
3. `SHA256SUMS`: the package's file hashes.

The declared checks the plan relies on are in `.craftingtable/checks.json`, committed with this
package.

## Items

| Item | Delivers |
|---|---|
| AQ-P1 | The retained-history scale benchmark and the 0.2.0 baseline |
| AQ-P2 | Mutation cost independent of retained history, with the crash suites unchanged |
| AQ-P3 | Bootstrap bounded by the WAL tail, and full verification as an explicit command |
| AQ-P4 | The retention of terminal history, as an accepted ADR only |
| AQ-P5 | The 0.3.0 release candidate; the operator verifies and publishes it after merge |
