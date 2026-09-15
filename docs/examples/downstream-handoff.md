# Independent downstream handoff

The [consumer source](../../examples/downstream-handoff/src/main.rs) is an unpublished,
standalone Cargo package with exact released dependencies. It imports no acceptance
helpers and enables no `testing` feature. From a candidate checkout:

```sh
python3 -B scripts/check-consumer.py
```

The helper copies the consumer to scratch and supplies explicit checkout overrides
in a separate Cargo configuration. After operator publication, verify registry
resolution without those overrides:

```sh
cargo test --manifest-path examples/downstream-handoff/Cargo.toml
mkdir -p "$TMPDIR/aq-handoff"
cargo run --manifest-path examples/downstream-handoff/Cargo.toml -- "$TMPDIR/aq-handoff" developmental early
```

Use a fresh empty directory for each run. Omit `developmental` for ordinary
attribution; omit `early` for a signal after the run becomes awaiting. The caller
must durably create the parent directory before using it for persistent work.
The sample assumes a single writer on a local POSIX filesystem; a production
application needs transaction isolation for concurrent writers and its own
retention policy. A failed application sync is ambiguous: stop and reopen.

The application commits intent and the complete admission request by file sync,
atomic replacement and directory sync. It deliberately loses the first queue
acknowledgement, reopens both stores, and obtains `AlreadyExists` for the same
request. Changed payload meaning is rejected. Entries are acknowledged only after
confirmed admission. Outcome recording and signal-outbox creation use the same
application commit; a lost signal acknowledgement follows the same retry rule.

The handler returns a durable wait with exact correlation, `AnyRetained` eligibility,
and a checkpoint. The late path restarts while awaiting. The resumed physical
attempt verifies the original checkpoint and receives the signal through
`ExecutorContext.input.resume_context`. Public inspection returns attempts and
structural lineage. Duplicate signals cannot create another sample or another
successful run.

Developmental mode adds only an opaque reference to immutable application-owned
context. Neither mode changes scheduling, protocol primitives, persistence
authority or authorization. The host explicitly supplies `HostControlContext`.
References grant no authority; retries are physical attempts, not new samples.
The example deliberately reports queue completion alongside application verification
rejection. Queue outcomes establish no correctness, winner or acceptance judgment.

Certification is ActionQueue-owned reference certification of this example and
the conformance adapter protocol. No WorldInterface or Exoskeleton revision is
certified. Those projects must run their own adapter evidence for their release.
