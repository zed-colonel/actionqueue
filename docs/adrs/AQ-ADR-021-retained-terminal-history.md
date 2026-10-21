# AQ-ADR-021 — Retained terminal history

Status: **Accepted for design only** for AQ-P4 / AQ-PERF-1.

Authority: the operator answered “Yes, for design only” to lossless terminal-run
archival with a complete retained WAL. The design handoff records this guidance
at `2026-10-21T05:54:04.109Z`; the implementation run's
`plan/craftingtable-operator-decisions.json`,
`decisions[0].designRecovery.instructions`, carries the same guidance with
`recordedAt: 2026-10-21T05:55:41.277Z` (run
`64b807ea-873a-4d8d-ad34-c24f50d7c1cb`, design recovery
`2d142523-3c56-462c-bba4-9b74e86c486b`). This ADR records acceptance of that design
direction; it is not an implementation or verification receipt for archival.

AQ-P4 delivers this ADR and its index entry. It changes no source code, WAL,
snapshot, backup or digest format, release version, or frozen contract. The
implementation below must be separately scheduled after AQ-PERF-1. It extends
[ADR-020](AQ-ADR-020-bounded-snapshot-bootstrap.md)'s checkpoint design without
superseding its current behavior.

## Context and decision

AQ-P2 makes mutations independent of retained history, and AQ-P3 bounds ordinary
bootstrap replay by the checkpoint tail. Neither removes terminal runs from the
resident projection. Run records, state histories and attempt histories continue
to consume memory even when they no longer participate in execution.

Choose lossless archival: move eligible terminal run records and their complete
histories into immutable, indexed archive segments. The resident execution view
and the archive together represent the complete logical projection. The complete
WAL remains authoritative and sufficient to reconstruct it from sequence 1.
Archive placement changes physical representation, not logical history.

This applies the preservation rules in the
[hardening contract §21.5](../contracts/actionqueue-hardening-implementation-ready.md#215-retention)
conservatively. Logical deletion, admission expiry, task archival, signal payload
reclamation, WAL truncation, and disk reclamation are outside the first follow-up.
Online archival and background archive garbage collection are also deferred.

## Preservation and eligibility

Terminality alone does not establish archival eligibility. Determine eligibility
at a verified WAL boundary under exclusive store ownership, accounting for all
active coordination and historical validation that may read the record.

| Evidence | First implementation's preservation rule |
|---|---|
| Active tasks, runs, waits and pending resume contexts | Keep resident, including terminal evidence required by active dependencies, required children, waits, resumes, checkpoints, cancellation and validation. |
| Task and admission records | Keep complete records resident indefinitely. Exact retries return the same identities; conflicting retries retain their existing rejection. No producer retry horizon is shortened. |
| Signals eligible for active waits | Preserve existing matching, protection and `SignalsRetired` semantics. Historical accepted starts and attempts must continue to protect their signal evidence after archival. |
| Causal records | Preserve all records and links across storage tiers, including parent/child and dependency relationships, admission context, and trace/correlation/origin associations. |
| Audit attribution | Preserve control records and original WAL sequences, operation-to-target associations, attempt ownership, accepted starts, dispositions, finish origins and cancellation evidence. |
| Checkpoint and signal payloads | Preserve immutable artifact references and recorded hashes exactly. Archival does not fetch, rewrite or garbage-collect external artifacts. |
| Terminal run records | Preserve full run identity and state, state and attempt history order, lease metadata, counters and per-task occurrence order. Move only records whose removal from resident execution access preserves behavior. |

Keep resident summaries needed for scheduling and validation. Once/Repeat/Cron
accounting must include archived occurrences: total, active and completed counts,
last-scheduled facts, terminal outcomes and historical child evidence cannot be
computed from resident runs alone. Retain required evidence resident until the
follow-up proves an equivalent bounded access path. Preserve extension records
and feature boundaries, including workflow, budget, actor and platform state.

## Logical digest and verification

Preserve logical-v9 SHA-256 over the **complete resident-plus-archived logical
view**. Use the existing canonical field encoding, ordering and normalization,
logical version and metadata, WAL/signal sequences, and total logical task/run
counts. The physical snapshot schema must not replace logical-v9 metadata in
the digest. Filenames, segment boundaries, archive descriptors, cache contents
and resident-only counts do not enter it. Changing placement at the same WAL
boundary must produce exactly the same digest.

Canonical hashing sorts runs by identity, so equal digests alone do not establish
operational equivalence. Full verification must also compare per-task operational
run order and inspection associations, including control targets and attempt
ownership, at the checkpoint boundary and after the tail. Stream bounded archive
records in canonical order; do not hydrate all retained history just to hash it.
Full digest and verification work remains proportional to retained history.

`ReplayReducer::trim_terminal_history` is unsuitable for this design. It removes
state histories, attempt histories and lease metadata for selected terminal runs
without a WAL event or sequence advance, while retaining their run identities.
It skips runs with waits or accepted-start evidence and invalidates revision and
checkpoint provenance. Those removed fields are digest-bearing `SnapshotRun`
data. A snapshot written before trimming does not make the later trimmed image
equivalent to complete WAL replay. Archival must preserve these fields and their
logical presence rather than call this helper.

## Future formats and compatibility

The follow-up must introduce all of the following explicitly; none is silently
added to an existing schema:

- A versioned archive codec with bounded, length-delimited records and indexes
  for run identity, task occurrence order and history pagination. Split large
  histories into bounded records. Check lengths before allocation, counts,
  offsets, numeric tags, references, duplicate identities and trailing bytes.
- A new physical snapshot schema beyond schema 10, with archive descriptors and
  enough resident coordination state to recover without loading terminal history.
  Keep WAL framing 1 and logical projection/digest version 9 unchanged.
- A versioned archive-aware backup descriptor and inventory. Current backup
  descriptor v1 accepts only `manifest.json`, `store.lock`,
  `wal/actionqueue.wal` and optional `snapshots/snapshot.bin`; it cannot carry
  archives. Archive-aware backup/restore is a release prerequisite.

Checkpoint and archive descriptors must bind store identity, manifest/feature
profile, format versions, archive generation, covered WAL sequence and exact
boundary/prefix commitment, segment identities, lengths, record counts and index
bounds. Use domain-separated integrity hashes for archive records/indexes and
segment descriptors, distinct from the logical projection digest. Bind all bytes
needed to interpret records and their placement; verify references against the
descriptor and store before exposing a history view. These hashes provide
integrity evidence, not authentication.

Keep individual records, indexes and descriptors bounded as retained history
grows. Use bounded index pages rather than one unbounded segment list, and freeze
byte layouts, limits and version tags with fixtures in the implementation.

Provide a source-preserving offline upgrade from schema 10 to a new destination.
Fully verify source and destination, preserve store identity, admissions and all
logical history, and compare logical digests, operational order and inspection
associations. Recompute physical WAL bindings if the manifest changes, following
ADR-020's upgrade rules. Publish only a verified destination; leave the source
untouched. Define supported upgrade ranges and fixtures for every supported
released format, including the schema-9 path through the existing upgrade.
Writable opening must not silently migrate. Older binaries must reject the new
schema precisely; unknown archive/backup versions and unsupported feature
profiles must also fail precisely. No downgrade is promised.

## WAL, publication and recovery

Retain the complete WAL from sequence 1, including admissions and all terminal
history. A placement-only archival operation needs no new logical WAL event:
segments and checkpoints remain reconstructible acceleration artifacts. Any
future deletion, admission expiry or WAL truncation needs a separate durable
design and contract review; this ADR authorizes none of them.

Begin with explicit offline maintenance while the daemon and runtime are stopped,
under exclusive ownership of the store. Do not weaken mutation authority or
prepare–append–sync–publish semantics.

1. Fully verify the source WAL and every referenced checkpoint/archive, then pin
   the coherent source revision and determine eligibility.
2. Write new immutable segments and indexes to unique staging paths. Verify their
   bounds, identities, integrity and logical contents; sync files and directories
   before making any checkpoint reference them.
3. Build a checkpoint bound to the same verified WAL boundary. Publish it by
   synced temporary file, atomic rename and directory sync, preserving ADR-020's
   publication serialization and installed-sequence safeguards.
4. Recover the published generation and verify complete WAL equivalence before
   reporting archival success. A verification failure is an explicit failure,
   even if publication has already installed the candidate.

A crash before checkpoint publication leaves the previous generation usable.
Preserve all segments referenced by either a recoverable old generation or the
possibly installed new generation through rename/directory-sync failures. Readers
and backups pin a generation for their lifetime. Unreferenced staging files may
be cleaned separately; no referenced generation may be reclaimed. Publication
failure must never expose a checkpoint with only part of its archive available.

Extend ADR-020's physical-damage fallback to the entire checkpoint/archive
generation. Ordinary startup may discard a missing or physically damaged
acceleration generation and reconstruct the complete projection from the retained
WAL before serving work. It must not skip individual records or treat absent
history as empty. Store identity, feature/version compatibility, semantic and WAL
boundary mismatches remain fatal. After a generation is exposed, missing archive
data is a storage error to its reader, not a successful partial result.

Explicit `store verify` must reject any damaged or missing referenced archive,
even if WAL-only reconstruction is possible. Backup/restore must inventory, copy,
sync and verify every referenced segment and index with the checkpoint and WAL,
and verify the staged destination before publication. If backup uses ADR-020's
permission to omit physically damaged acceleration data, it must omit the whole
checkpoint/archive generation and fully validate the complete WAL; it cannot
publish a descriptor with dangling references. Included generations always
receive full verification.

## Execution and history interfaces

Separate resident execution access from fallible logical-history reads. Resident
getters must not silently perform filesystem I/O. History readers return owned
or shared records and bounded pages pinned to one coherent revision/generation;
they cover resident and archived identities without duplicates or omissions.
Archive lookup failure returns a storage error, never empty history or false
`NotFound`. Generation construction/publication remains a storage service under
the store lock, separate from mutation and inspection consumers.

Preserve tenant authorization, payload redaction, existing pagination order and
cursor semantics, trace/correlation/origin lookup, control attribution and attempt
ownership. Archive location cannot bypass authorization. Inspection, admission,
scheduling, dependency/child resolution, cancellation and signal retirement must
observe the same logical facts across both storage tiers. Revision identity and
checkpoint provenance must cover the selected resident roots and archive
generation together, so placement cannot reuse proof for a different image.

## Separately scheduled follow-up

This scope is outside AQ-PERF-1 and adds no predecessor to AQ-P4 or AQ-P5. Ship
archival only after all four stages and their evidence are complete.

| Stage | Likely implementation areas | Required evidence |
|---|---|---|
| Resident/history separation | Storage `recovery/{reducer,projection,changes,task_status,inspection}.rs`; runtime inspection and dispatch | Equivalent admission, scheduling, cancellation, coordination and inspection across resident and archived history; bounded execution access. |
| Archive and checkpoint formats | New storage archive module; `snapshot/`; `store/{manifest,upgrade,verify,backup}.rs` | Bounded decoding and indexes, supported-version fixtures, source-preserving migration, integrity checks, crash-safe publication, complete backup/restore. |
| Offline maintenance | Runtime store services, CLI and operational documentation | Exclusive ownership, verified archival and restart, coherent generation pinning, actionable compatibility/storage errors and failure recovery. |
| Acceptance and performance | Acceptance/chaos suites and `benches/retained_history.rs` | WAL-only versus archived-checkpoint-plus-tail equivalence and memory/performance measurements, with all adopted checks and relevant feature matrices passing. |

Acceptance must cover exact admission retries and conflicts; Once/Repeat/Cron
accounting; dependencies, required children and historical outcomes; active waits,
pending resumes and checkpoints; signal protection after archived accepted
starts; control attribution and attempt ownership; tenant-scoped inspection,
redaction and pagination across tiers. Compare digest, operational order and
inspection associations both at the archival cut and after a nonempty WAL tail.

Exercise missing, corrupt, truncated and substituted segments/indexes, cross-store
and feature/version mismatches, oversized lengths/counts, duplicate or invalid
references, and publication crashes before/after each sync and rename boundary.
Prove ordinary physical-damage fallback, fatal semantic/compatibility failures,
explicit verification rejection, source preservation, and backup/restore with
complete and omitted damaged acceleration generations.

Measure resident terminal-history memory separately from remaining task/admission
and coordination indexes, archive size and complete WAL size. Measure mutation,
inspection, bootstrap, archival and full-verification cost separately, including
long-lived readers that pin old roots/generations. Archival may reduce resident
memory; task/admission indexes and the complete WAL still grow. Full verification
and WAL-only recovery still scale with history. No measured savings or bounded
total memory/disk claim is made by this design.

## Evidence for the decision

The code baseline inspected for AQ-P4 is
`8dfd099de75677bd20e74112edc8b25dc6d57d7f`:

- [AQ-PERF-1 plan, AQ-P4](../planning/aq-perf-1/aq-perf-1-implementation-plan.md#aq-p4-retained-terminal-history-design-only)
  defines the design-only scope and preservation/digest/WAL obligations.
- [Hardening contract §§21.3–21.6](../contracts/actionqueue-hardening-implementation-ready.md#213-projection-and-snapshot-state)
  and [invariant boundaries §3](../invariant-boundaries-v1.0.md#3-source-of-truth-hierarchy)
  require preserved evidence and reconstructible authoritative history.
- [Reducer](../../crates/actionqueue-storage/src/recovery/reducer.rs)
  (`trim_terminal_history`), [snapshot build](../../crates/actionqueue-storage/src/snapshot/build.rs)
  and [projection digest](../../crates/actionqueue-storage/src/recovery/projection.rs)
  establish trimming's digest/provenance effect and logical-v9 ordering.
- [Full verifier](../../crates/actionqueue-storage/src/store/verify.rs)
  compares digest, operational order and inspection associations; [backup/restore](../../crates/actionqueue-storage/src/store/backup.rs)
  has a fixed v1 inventory that excludes archives.
- [ADR-020](AQ-ADR-020-bounded-snapshot-bootstrap.md) establishes schema 10,
  complete-WAL retention, publication, migration and physical-damage fallback.
