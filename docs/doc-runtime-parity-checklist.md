# Documentation and runtime parity — 0.2.0

| Surface | Current reference | Verification |
|---|---|---|
| Embedded admission, signal, wait and checkpoint delivery | [Consumer](examples/downstream-handoff.md) | Independent production consumer tests |
| HTTP v2 and CLI controls/inspection | [AQ-12 API](aq-12-apis.md) | Public conformance drivers over all transports |
| Manifest 1, WAL 1, snapshot/projection 9 | [Store format](data-dir-format-v1.0.md) | Target persistence and cross-feature process tests |
| Recovery and offline transfer | [Operator guide](wal-recovery-guide.md) | Corruption, process cuts, backup and restore evidence |
| Base hierarchy and transactional child admission; optional cron | [Workflow](../crates/actionqueue-workflow/README.md) | Compound-child and expanded workflow tests |
| Developmental attribution neutrality | [Conformance](../conformance/aq-cont-1/README.md) | Eighteen developmental cases and full report |
| Registry package usability | [Release](releases/0.2.0.md) | Cargo archives, isolated directory-source consumer |

`python3 -B scripts/check-docs.py` checks active Markdown file links. Release gates
also scan generated ActionQueue API HTML using the existing forbidden-symbol
policy. Frozen contracts, planning evidence and archive content remain unchanged;
these link checks do not redefine their policy exemptions.
