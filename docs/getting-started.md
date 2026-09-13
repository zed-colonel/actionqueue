# Getting started with 0.2.0

Use Rust 1.89.0 and a fresh target store. Existing 0.1.x stores cannot be migrated
by this release. Build the operational binary from the repository root:

```sh
cargo build --locked --workspace
cargo run --locked -p actionqueue-cli -- --help
```

For an embedded application, implement `ExecutorHandler::execute`, returning an
`AttemptDisposition`, and bootstrap `ActionQueueEngine` with `RuntimeConfig` and
an explicit trusted `HostControlContext`. Persist a complete `EnsureTaskRequest`
in your own outbox before calling `BootstrappedEngine::ensure_task`. Exact retries
return the original admission; changed meaning conflicts. After a timeout or lost
response, retain the request and retry after recovery. Never mint a new identity
merely because an acknowledgement was lost.

The [standalone consumer](examples/downstream-handoff.md) is executable source for
this full flow, including ordinary/developmental attribution, early/late signals,
checkpoint resume, restart, and queue completion with application rejection.
It uses published interfaces and exact `0.2.0` dependencies.

A handler can return `AttemptDisposition::awaiting` with a `WaitSpec` and checkpoint.
A matching durably admitted signal resumes it through
`ExecutorContext.input.resume_context`. Physical retries remain attempts of the
same execution unit. Child requests belong in the parent's compound disposition;
persist application identities before constructing retryable batches.

## CLI and HTTP

`actionqueue` supports `ensure-task`, `signal admit`, resource inspection, traces,
cancellation, wait resolution, and offline store operations. JSON request files
carry the same validated types as the embedded API. See
[API and command reference](aq-12-apis.md) and the [CLI README](../crates/actionqueue-cli/README.md)
for exact flags. Use `--help` on a subcommand before constructing its request.

```sh
actionqueue store inspect --data-dir /srv/actionqueue
actionqueue backup --data-dir /srv/actionqueue --output /srv/backups/queue-001
actionqueue restore --input /srv/backups/queue-001 --data-dir /srv/restored-queue
```

Stop the store owner before backup/restore. Destinations must be absent or empty
as required by the operation. External artifact bytes are backed up separately
by the application.

Online operations use `--daemon http://127.0.0.1:PORT` and `--token-file PATH` or
`ACTIONQUEUE_TOKEN`. The host owns authentication, scope and control enablement.
References in request JSON confer no privileges. Inspection redacts payloads,
external locators and free-text errors and uses bounded pages and revision-bound
cursors. Health/readiness and aggregate metrics are separate from object access.
