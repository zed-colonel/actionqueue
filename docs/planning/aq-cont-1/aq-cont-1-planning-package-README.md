# AQ-CONT-1 Development Planning Package — Developmental Execution Revision

This revision incorporates the durable-execution lessons obtained while exploring the OHIO moving-frontier concept without turning ActionQueue into a research system, evaluation engine, or domain ontology.

The `AQ-CONT-1` protocol, persistence authority, scheduler semantics, and fourteen-PR critical path remain unchanged. The revision adds two hardening invariants and a conformance profile for carrying developmental campaign work through existing bounded causal and continuation seams.

## Read in this order

1. `actionqueue-hardening-implementation-ready.md` — normative architecture, now including `AQ-H19` and `AQ-H20`.
2. `aq-cont-1-developmental-campaign-execution-profile.md` — focused explanation of neutral campaign attribution and the execution/evaluation boundary.
3. `developmental-diagnostics-cross-stack-profile.yaml` — shared Exoskeleton/WorldInterface/ActionQueue ownership and terminology profile.
4. `aq-cont-1-implementation-plan.md` — repository-level `AQ-PLAN-2` delivery plan and unchanged fourteen-PR critical path.
5. `aq-cont-1-work-breakdown.yaml` — machine-readable dependencies, risk, exit gates, and additive developmental-profile work.
6. `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` — eighteen conformance cases for attribution neutrality, durability, recovery, semantic non-ownership, privacy, and observability.
7. `constitutional-stack-implementation-contracts.yaml` — canonical base cross-stack ownership and handoff rules; unchanged by this profile.
8. `aq-cont-1-validation.json` — package validation results.
9. `SHA256SUMS` — package file hashes.

## What changed

- The architecture date is August 10, 2026 and the implementation plan is identified as `AQ-PLAN-2`.
- `AQ-H19` states that campaign and intervention-arm correlation is neutral attribution, not authority, priority, routing, budget, or acceptance.
- `AQ-H20` states that queue lifecycle and timing facts are execution evidence, not task correctness, arm preference, binding-constraint diagnosis, saturation, or candidate acceptance.
- The existing `trace_id`, `correlation_id`, `CausationLink`, and bounded `origin_ref` seams are sufficient. No campaign primitive, metadata map, WAL family, projection, scheduler branch, or new authority path is introduced.
- Structural inspection may filter by exact correlation or origin reference and show ordinary scheduling-field differences. It never reports a winning arm or evaluation conclusion.
- The conformance inventory adds eighteen `AQ-DD-*` cases with replay and crash variants.

## Implementation posture

The implementation remains a pre-production clean break. Existing ActionQueue source and fixtures are evidence, not a compatibility obligation. The active target runtime contains one authority path and no pre-`AQ-CONT-1` store reader.

Developmental work remains ordinary opaque work. Rich campaign state stays in Exoskeleton or the specialized application behind one immutable bounded reference. ActionQueue supplies durable execution and reconstructible lineage only.
