# AQ-03 implementation handoff

Date: 2026-09-10. Branch: `ct/aq-03-61ef4a57`.

## Result

AQ-CONT-1 has a fresh, independently identifiable version-1 persistence lineage.
Target replay, rejection of old stores, verified offline backup/restore, and exact
projection equivalence are executable conformance gates.

The storage-owned opening path validates a bounded manifest before acquiring the
persistent shared/exclusive lock and revalidates identity under that lock. New stores
publish through synced sibling staging directories. The lock remains owned for the
authority/runtime lifetime, including daemons with controls disabled. No production
filesystem writer can be constructed from a path alone.

The target WAL has explicit record/schema identifiers, store identity, contiguous
sequence numbers, header and payload checksums, and a 16 MiB payload limit. Its first
record binds the immutable manifest. Versioned payload structs freeze the persisted
task, run, and scheduling-policy layouts independently of feature-gated enum order.
Shared semantic validation prepares mutations before durable append and rejects
impossible replay state. Store feature profiles remain immutable even in richer builds.

Snapshots restore the authoritative projection directly, including priority, transition
times, attempt/output history, active leases, controls, dependencies, and enabled feature
records. Canonical SHA-256 includes durable state and sequence; an independently
generated known-answer vector checks the implementation. Snapshot publication verifies
the covered WAL prefix. Complete WAL history remains mandatory.

Only incomplete final target frames following a validated semantic prefix qualify for
explicit repair. Complete corruption, incompatible versions, unknown record families,
invalid payloads and sequence violations never qualify. Snapshot fallback is limited to
physical damage. Readable future versions in incomplete frames still halt recovery;
diagnostics identify the component and supported/found versions.

A failed directory sync after rename is reported even when the complete destination
is already visible. Initialization distinguishes its own published identity from a
concurrent winner; inspection and verified restore can recover a successfully renamed
store after that uncertain publication result.

The existing `actionqueue-cli` binary now offers `storage inspect`, `storage backup`, and
`storage restore`. Transfers verify file inventories, lengths, SHA-256 checksums, identity,
sequence and both recovery paths before publication. They preserve source bytes and
reject overlapping paths, symlinks, traversal, unexpected entries and populated
destinations. Inspection and backup never initialize their source.

## Verification

All checks passed on the final implementation:

| Command | Result |
|---|---|
| `cargo test --workspace` | Pass |
| `cargo test --workspace --features workflow` | Pass |
| `cargo test --workspace --features workflow,budget,actor,platform` | Pass |
| `cargo aq-conformance` | Pass |
| `cargo fmt --all -- --check` | Pass |
| `cargo build --workspace` | Pass |
| `conformance/aq-cont-1/cross-feature-persistence.sh` | Pass |
| `git diff --check` | Pass |

The target persistence binary passes 22 tests in the base conformance invocation and
24 with all production features. The separate producer/reader builds prove compatible
reads, stable base-record encoding, unsupported-profile refusal, and rejection of feature
writes outside the store profile without relying on workspace feature unification.
Stable rustfmt reports the repository's existing nightly-only-option warnings and exits
successfully. Logs are `.aq-checks/final-*.log`, with command exit codes in
`.aq-checks/final-results.txt`.

Target persistence coverage includes initialization races; process-kill lock release;
no-write refusal of malformed, legacy and future stores; every partial WAL frame cut;
complete/interior corruption; snapshot cuts through retry/lease/output/control state;
rich feature state; map-order-independent digests; failure injection; backup inventories;
restore refusal; and successful next-sequence mutation after restore. CLI smoke tests
exercise the actual inspect/backup/restore commands. The conformance alias now includes
target persistence as well as old-store rejection.

## Implementation choices and operator attention

- Prepared mutations clone the complete projection. The retained reducer mutates several
  private maps per operation and has no affected-map patch interface. This preserves
  pre-append validation and atomic publication at a throughput/memory cost; incremental
  preparation can be introduced without changing v1 bytes.
- Snapshot recovery also validates the complete WAL and compares both projections.
  This intentionally costs full-history replay until a separately specified compaction
  and trusted history boundary exists.
- Exact hydration exposed two retained-model issues: a future scheduled run can become
  Ready after its creation time, and a RetryWait prefix can still own the lease pending
  its release record. Validation now accepts those legitimate persisted prefixes.
- The archived capture example was removed from Cargo build-target registration because
  its frozen path-only writer API describes the retired lineage. Its source and all
  frozen contracts, planning-package inputs and archived evidence remain unchanged.
  Low-level framing fixtures use explicit test-support constructors; target tests use
  real initialized stores.
- Storage SHA-256 is introduced in AQ-03, with the narrow timing clarification recorded
  in ADR-003. Admission canonicalization remains AQ-04. Admission, compound disposition,
  signals, waits and attributed controls have reserved unsupported record identifiers;
  their execution and continuation recovery remain with their owning work items.

There is no migration or feature-profile upgrade path. Existing pre-contract stores
must remain separate from fresh target stores. External artifact bytes are outside queue
backups; their opaque durable references remain preserved. No changes were pushed.
Local command logs are in the ignored `.aq-checks/` directory; the isolated dependency
cache is in `.aq-cargo/`. Both remain inside this worktree.
