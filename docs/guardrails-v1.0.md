# ActionQueue Architectural Guardrails

## Purpose

This document defines the architectural and product boundaries that protect ActionQueue's correctness guarantees. These guardrails govern what changes are acceptable and what boundaries must not be crossed.

## Core guardrail principles

- Preserve contract truth over feature expansion.
- Keep single-node correctness primary.
- Keep mutation authority and invariant boundaries intact.
- Prefer inspectability and deterministic replay over surface-area growth.

## Architectural boundaries

The following remain outside the scope of ActionQueue v1.0:

1. Clustering / consensus / multi-node runtime
2. WASM plugins / hot-swappable plugin host
3. Admin SPA / bundled web UI

## Shipped capability families

The following have been implemented and are covered by acceptance tests:

1. **Cron / calendar scheduling** -- `RunPolicy::Cron` with 7-field UTC expressions and rolling window derivation (`workflow` feature)
2. **DAG dependency scheduling** -- `DependencyGate` with cycle detection and failure propagation (`workflow` feature)
3. **Task hierarchy** -- parent-child cascade cancellation and completion gating (`workflow` feature)
4. **Budget enforcement** -- per-task budget tracking, suspend/resume, event subscriptions (`budget` feature)
5. **Remote actor coordination** -- registration, heartbeat monitoring, capability routing, department grouping (`actor` feature)
6. **Multi-tenant platform** -- tenant isolation, RBAC, append-only ledgers, approval workflows (`platform` feature)

## Decision check before accepting work

Use this gate for every incoming change request:

1. Does it preserve single-node posture?
2. Does it avoid creating new mutation lanes outside the validated authority model?
3. Does it avoid introducing new runtime capability families without acceptance test coverage?
4. Does it align with charter + invariant boundaries?

If any answer is "no", the change requires explicit design review.

## Canonical boundary references

- `actionqueue-charter.md`
- `invariant-boundaries-v1.0.md`
- `docs/acceptance-test-taxonomy.md`
