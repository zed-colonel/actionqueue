# Known failure cases at the pre-AQ-CONT-1 baseline

These are behaviours of the frozen baseline that the `AQ-CONT-1` architecture classifies as
**reject** or **replace**. They are recorded here as evidence of why the clean break exists.
Each entry names the baseline seam, how the failure manifests, and the pull request that
removes it. None of these are bugs to fix on `main`; they are contract gaps.

| # | Baseline seam | Failure mode | Evidence at baseline | Removed by |
|---|---|---|---|---|
| 1 | `SubmissionChannel` / `TaskSubmissionPort` (`crates/actionqueue-workflow/src/submission.rs`) | Dynamic child submission is an unbounded, fire-and-forget channel. A coordinator attempt can complete while its child submissions are lost if the process dies between handler return and channel drain; the parent has no durable record that children were required. | `tests/acceptance/dynamic_submission.rs` proves the happy path only; no test can prove atomicity because the seam has none. | `AQ-09` |
| 2 | `EventFilter::Custom` / `ActionQueueEvent::CustomEvent` (`crates/actionqueue-core/src/subscription.rs`, `event.rs`) | External wake-up rides on in-memory tick events. A custom event emitted while the engine is down, or before the subscription is durably created, is lost; a subscription cannot be resolved by replay. | `tests/acceptance/custom_event_subscription.rs` covers in-process delivery only. | `AQ-10` |
| 3 | `Suspended` used as generic waiting | The only non-terminal parked state is `Suspended` (budget preemption). Waiting on an external callback must either hold a lease in `Running` or abuse `Suspended`, conflating preemption with continuation. | `tests/acceptance/suspend_resume.rs`, `suspended_concurrency_key.rs` | `AQ-02`, `AQ-06` |
| 4 | `HandlerOutput` (`crates/actionqueue-executor-local/src/handler.rs`) | Handler results carry opaque output bytes but cannot express "await this condition with this checkpoint" or "these children are required". Continuation state must be smuggled into output bytes. | `tests/acceptance/handler_output_roundtrip.rs` | `AQ-08` |
| 5 | Task submission returns without a knowable durable outcome | A caller that loses the response after submit cannot tell whether the task exists; retrying creates a duplicate task with a fresh ID. No admission key or digest exists. | `docs/examples/idempotency-runid.md` documents RunId idempotency for effects, not admission. | `AQ-04` |
| 6 | `ActorCapabilities` / `required_capabilities` | Routing labels are named as if they were authority. Downstream adapters have treated them as permission checks. | `tests/acceptance/capability_matching.rs`, `department_routing.rs` | `AQ-02`, `AQ-11` |
| 7 | Control mutations without attribution | Cancel, pause, resume, and reprioritize mutations record a sequence and timestamp but no host-attested actor context; audit cannot say who acted. | `tests/acceptance/cancellation.rs` | `AQ-11` |
| 8 | No store manifest | A data directory is identified only by WAL framing bytes (`version = 5`) and the snapshot JSON `schema_version = 8`. Nothing prevents a future runtime from opening and rewriting it. | `docs/data-dir-format-v1.0.md` | `AQ-03` |
| 9 | Blocking-pool saturation in tests | Handlers using `spawn_blocking` with `thread::sleep` saturate the Tokio blocking pool on two-vCPU runners, producing intermittent hangs; CI serializes with `--test-threads=1`. Not a contract gap, but a known operational fragility to retire with the executor cutover. | `.github/workflows/ci.yml` comment; `.cargo/config.toml` `RUST_TEST_THREADS=4` | `AQ-08`, `AQ-13` |
| 10 | Snapshot promotion strictness | Commit `b770a66` removed an overly strict `scheduled_at > created_at` check that rejected valid Ready runs in snapshots. Recorded because it shows snapshot validation rules were being tuned empirically rather than derived from a fixed record model. | `git log b770a66` | `AQ-03` |
| 11 | Snapshot serialization order | Two snapshots built from byte-identical WALs differ because projection collections are serialized in hash-map iteration order. A deterministic projection digest (architecture §21.4) cannot be computed over baseline snapshots. | Observed while capturing `selected-snapshot-fixtures/` (see `../README.md`) | `AQ-03` |

## What is deliberately not listed

- Performance regressions: no baseline threshold exists (see `../performance-baseline/`).
- Flaky tests: none were observed in the freeze runs (see `../characterization-results/summary.json`).
