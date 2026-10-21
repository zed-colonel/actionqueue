# AQ-ADR-019: Structurally shared projections

Status: Accepted for AQ-P2; performance acceptance requires the measurements below.

Use `imbl` persistent hash maps for unordered lookup, ordered maps and sets for
ordered indexes, and a storage-owned `History<T>` backed by a persistent vector
for chronological histories. Map and history nodes hold `Arc` records: copying a
node copies references, and only the record being edited is copied on write.
Nested growing buckets and histories also use persistent collections. Immutable
WAL and snapshot DTOs remain ordinary owned records.

Keep prepare–append–sync–publish and the reducer's compound staging. Complete
read-only validation followed by infallible application, and undo journals, would
require a much broader atomicity rewrite. On an after-publication fault restore
the original root before fencing; recovery still observes the committed WAL.

Hash iteration is not an ordering promise. Ordered indexes retain ascending key
order, histories retain insertion order, and snapshot extraction and canonical
normalization retain their existing order and encoding. Derived indexes, revision
identity and the latest committed change are excluded from persistence and digests.

The cost is extra pointer allocations, reference counts, and logarithmic path
copying. Long-lived readers retain old paths and records until they drop their
roots. Record copies and candidate visits are measured separately from shallow
projection clones; wholesale copied run counts must remain zero.

The measured spike below uses the retained-history workload from AQ-P1. Final
scale observations and acceptance results follow the linked measurement protocol
and remain run-local verification artifacts.

Runtime consumers use `ProjectionChange` for the last frame and sequence cursors
for multi-frame services. The cursor indexes hold **one latest modification per
task/run**, moving that entry on each change; they do not retain an event queue.
Ordered range lookup finds changes since a consumer's cursor. Cancellation
candidates follow reverse dependency edges and required children, in sorted rounds.
Task summaries preserve Once/Repeat/Cron terminality. Hydration rebuilds these
indexes before validating historical relationships. Active wait ownership and
attempt positions avoid scans of retained waits or per-run attempt history.
Bootstrap also seeds terminal cache cleanup once from the recovered hierarchy.
Incremental consumers start at the recovered sequence, so unchanged terminal
tasks must already be queued for cleanup rather than awaiting another mutation.

The daemon publishes shared roots. For equal sequences, shared process-local
revision identity proves equality; otherwise it compares canonical digests and
fails closed on divergence. Both direct `apply` and history trimming invalidate
identity. An independently reconstructed equal image adopts the authority's roots
so later unchanged publications use the identity check.

The measured implementation spike used the unchanged AQ-P1 filesystem workload,
with immediate durability for fixture setup and the measured 100-operation batches.
At 1,000 / 10,000 retained runs, admission means were 1.119 / 1.035 ms and
cancellation means 1.183 / 1.052 ms. Each batch used 200 shallow root clones.
All growing fields use persistent roots; their clones share retained records.
Record copies were 200 per admission batch and 1,000 per cancellation batch at both sizes.
WAL-only bootstrap took 0.113 / 1.377 seconds; snapshot plus tail took
0.889 / 7.778 seconds. This supports retaining staged mutation instead of an
in-place or undo-journal rewrite. Final repeated scale measurements are recorded
in the AQ-P2 performance report.

The old full-projection and wholesale-run-copy counters no longer had increment
sites after this conversion and were removed in review remediation (F-002).
Structural assertions bound actual record materializations per mutation and count
root clones; inspection of the persistent field types establishes that root cloning
does not copy retained collections. A constant zero counter is not evidence of that.

Borrowed record getters remain available. History getters now return `History<T>`
instead of a contiguous slice; consumers that need ownership use `to_vec`.
This is an in-memory Rust API change for the planned 0.3.0 release. WAL event DTOs,
snapshot schema 9, canonical normalization, and digest encoding are unchanged.

A follow-up review of the spike added an active-run index per task. Compound task
cancellation and its subscription observations visit only unfinished runs, even
when one repeating task owns a large terminal history. The structural workload
also counts cancellation run candidates; a focused 1,001-run case requires one
visit for its one unfinished run. The final record-copy counts include this index.

[Measurement protocol](../performance/retained-history-aq-p2.md) specifies the
repeated scale run, runtime and daemon boundaries, and memory observations.
Raw results and the final acceptance report are run-local verification artifacts.
The memory representation adds one `Arc` slot per map/history record plus shared
tree nodes and derived indexes. Cursor-index space is bounded by task/run count;
it does not grow with repeated mutations of the same objects. Whole-process RSS
also includes snapshot DTOs and canonical digest intermediates, which AQ-P2 does
not change. Long-lived readers can retain old paths until their roots are dropped.

Attempt positions are keyed by run and attempt, retaining the first position for
legacy duplicate identities. This preserves the historical first-match finish
rejection while avoiding scans or copies of earlier attempt records. Accepted
starts continue to enforce globally unique attempt identities.

Subscription cancellation authorization uses direct identity lookup. Cron
replenishment validation uses the same total/active/last-scheduled summary as
terminality, so validating a new occurrence does not scan retained occurrences.
