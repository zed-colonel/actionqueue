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

Measurements and acceptance results will be recorded after the measured spike and
scale runs, using the retained-history workload from AQ-P1.

Runtime consumers use `ProjectionChange` for the last frame and sequence cursors
for multi-frame services. The cursor indexes hold **one latest modification per
task/run**, moving that entry on each change; they do not retain an event queue.
Ordered range lookup finds changes since a consumer's cursor. Cancellation
candidates follow reverse dependency edges and required children, in sorted rounds.
Task summaries preserve Once/Repeat/Cron terminality. Hydration rebuilds these
indexes before validating historical relationships. Active wait ownership and
attempt positions avoid scans of retained waits or per-run attempt history.

The daemon publishes shared roots. For equal sequences, shared process-local
revision identity proves equality; otherwise it compares canonical digests and
fails closed on divergence. Both direct `apply` and history trimming invalidate
identity. An independently reconstructed equal image adopts the authority's roots
so later unchanged publications use the identity check.

The measured implementation spike used the unchanged AQ-P1 filesystem workload,
with immediate durability for fixture setup and the measured 100-operation batches.
At 1,000 / 10,000 retained runs, admission means were 1.119 / 1.035 ms and
cancellation means 1.183 / 1.052 ms. Each batch used 200 shallow root clones,
zero full projection copies and zero wholesale copied run entries. Record copies
were 200 per admission batch and 1,000 per cancellation batch at both sizes.
WAL-only bootstrap took 0.113 / 1.377 seconds; snapshot plus tail took
0.889 / 7.778 seconds. This supports retaining staged mutation instead of an
in-place or undo-journal rewrite. Final repeated scale measurements are recorded
in the AQ-P2 performance report.

Borrowed record getters remain available. History getters now return `History<T>`
instead of a contiguous slice; consumers that need ownership use `to_vec`.
This is an in-memory Rust API change for the planned 0.3.0 release. WAL event DTOs,
snapshot schema 9, canonical normalization, and digest encoding are unchanged.
