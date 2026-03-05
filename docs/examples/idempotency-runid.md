# Idempotency with RunId (v0.1)

## Why this matters

ActionQueue guarantees durable internal accounting, but external side effects still follow the reality clause.

If a crash happens after an external side effect but before durable completion is recorded, recovery may re-dispatch the run. This is expected uncertainty behavior, not a contract breach.

Operational rule:

- Treat `RunId` as the stable idempotency key for external side effects.
- Treat `AttemptId` as lineage/diagnostics only.

## Contract anchors

- `RunId` and `AttemptId` are present in executor request contracts.
- Retry semantics are attempt-scoped and bounded by `max_attempts`.
- Durable completion prevents intentional redispatch once committed.

## Minimal pattern

1. Receive request containing `run_id` and `attempt_id`.
2. Check idempotency store keyed by `run_id`.
3. If present, return previously committed result.
4. If absent, execute side effect and atomically persist dedup record keyed by `run_id`.
5. Return success.

## Example data model

Use any durable store (SQL/kv/event projection). Example schema:

```text
external_effect_dedup
- run_id (PRIMARY KEY)
- external_effect_id
- result_payload
- created_at
```

## Rust-style pseudocode

```rust
struct EffectReceipt {
    run_id: String,
    external_effect_id: String,
    payload: Vec<u8>,
}

fn handle_effect(run_id: &str, attempt_id: &str, payload: &[u8]) -> Result<EffectReceipt, String> {
    // 1) Fast replay path
    if let Some(existing) = dedup_store_get(run_id)? {
        return Ok(existing);
    }

    // 2) Perform side effect (HTTP call, DB write, etc.)
    // The outbound request should also include run_id where the external API supports it.
    let external_effect_id = call_external_system(run_id, payload)?;

    // 3) Persist dedup receipt keyed by run_id
    let receipt = EffectReceipt {
        run_id: run_id.to_string(),
        external_effect_id,
        payload: payload.to_vec(),
    };

    dedup_store_insert_if_absent(run_id, &receipt)?;

    // 4) Return deterministic receipt
    log::info!("handled effect run_id={run_id} attempt_id={attempt_id}");
    Ok(receipt)
}
```

## AttemptId guidance

`AttemptId` should be used for:

- traces/log correlation,
- audit reconstruction,
- distinguishing retries in diagnostics.

`AttemptId` should **not** be used as the external idempotency key.

## Concurrency key alignment

When side effects can collide by business domain, set `concurrency_key` to that collision domain (for example `customer:123` or `repo:main`).

This keeps single-flight behavior aligned with external contention boundaries while idempotency remains keyed by `RunId`.

## Failure scenarios and expected behavior

### Crash after external effect, before completion durability

- ActionQueue may replay/re-dispatch run due to uncertainty.
- Adapter receives same `RunId` with a new `AttemptId`.
- Dedup lookup by `RunId` returns existing receipt.
- No duplicate external side effect is applied.

### Retryable failure before external effect commit

- ActionQueue increments attempt lineage and retries within `max_attempts`.
- No dedup record exists yet; side effect can proceed when retry succeeds.

## Anti-patterns to reject

- Dedup keyed by timestamp/random nonce.
- Dedup keyed by `AttemptId`.
- In-memory-only dedup state.
- Treating timeout as proof of no external effect.

## Suggested verification surfaces

- Acceptance uncertainty/recovery proof: `tests/acceptance/crash_recovery.rs`.
- Retry cap proof: `tests/acceptance/retry_cap.rs`.
- Observability proof for ID surfaces: `tests/acceptance/observability.rs`.

