# conformance/aq-cont-1

The `AQ-CONT-1` conformance package owned by ActionQueue. Downstream contracts
(`WI-FABRIC-2`, `EXO-V3`) pin an exact revision of this package before claiming
continuation semantics.

| File | Purpose |
|---|---|
| `manifest.yaml` | Contract revision, normative document hashes, fixture schema, fixture inventory, acceptance matrices |
| `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` | Eighteen `AQ-DD-*` developmental-neutrality cases (attribution neutrality, durable execution, recovery and idempotency, semantic non-ownership, privacy and observability); executed with replay and crash variants by `AQ-13` |
| `contract-boundaries.json` | Policy driving the repository boundary checks in `tests/conformance/contract_boundaries.rs` |

Fixture directories and black-box drivers are added by later pull requests. The manifest's
fixture inventory is intentionally empty at `AQ-01`.
