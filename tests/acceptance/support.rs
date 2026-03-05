// Test support helpers are compiled into each acceptance test binary separately.
// Functions used by a subset of tests appear unused in other binaries.
#![allow(dead_code)]

//! Shared deterministic acceptance helpers for Phase 8 black-box proofs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use actionqueue_cli::args::SubmitArgs;
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AppliedMutation, AttemptStartCommand, DurabilityPolicy, LeaseAcquireCommand,
    LeaseExpireCommand, LeaseReleaseCommand, MutationAuthority, MutationCommand,
    RunStateTransitionCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_daemon::config::DaemonConfig;
use actionqueue_engine::concurrency::lifecycle::{
    evaluate_state_transition, KeyLifecycleContext, LifecycleResult,
};
use actionqueue_engine::index::scheduled::ScheduledIndex;
use actionqueue_engine::scheduler::attempt_finish::submit_attempt_finish_via_authority;
use actionqueue_engine::scheduler::promotion::{
    promote_scheduled_to_ready_via_authority, PromotionParams,
};
use actionqueue_executor_local::ExecutorResponse;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::Service;

/// Durable completion evidence captured from authority-lane transitions.
#[derive(Debug, Clone)]
pub struct CompletionEvidence {
    /// The run that was completed.
    pub run_id: actionqueue_core::ids::RunId,
    /// The completed attempt lineage identifier.
    pub attempt_id: AttemptId,
    /// Final durable WAL sequence after completion transition.
    pub final_sequence: u64,
}

/// Durable pre-restart checkpoint evidence for crash-recovery assertions.
#[derive(Debug, Clone)]
pub struct CrashRecoveryCheckpoint {
    /// Run identity captured before restart.
    pub run_id: RunId,
    /// Ordered attempt IDs visible before restart.
    pub pre_restart_attempt_ids: Vec<AttemptId>,
    /// Durable run state before restart.
    pub pre_restart_state: RunState,
    /// Latest durable WAL sequence before restart.
    pub pre_restart_sequence: u64,
}

/// Lease payload evidence parsed from `/api/v1/runs/:id`.
#[derive(Debug, Clone)]
pub struct LeaseSnapshotEvidence {
    /// Lease owner identity.
    pub owner: String,
    /// Lease expiry timestamp.
    pub expiry: u64,
    /// Lease acquire timestamp.
    pub acquired_at: u64,
    /// Lease update timestamp.
    pub updated_at: u64,
}

/// Deterministic Once submission specification for concurrency-key acceptance scenarios.
#[derive(Debug, Clone)]
pub struct ConcurrencyRunSpec {
    /// Fixed task identifier literal used in acceptance orchestration.
    pub task_id_literal: &'static str,
    /// Optional raw constraints JSON passed to CLI submit.
    pub constraints_json: Option<String>,
}

/// Deterministic task/run handle captured for concurrency-key acceptance scenarios.
#[derive(Debug, Clone)]
pub struct ConcurrencyRunHandle {
    /// Task identifier for the submitted once task.
    pub task_id: TaskId,
    /// Sole run identifier derived for the task.
    pub run_id: RunId,
    /// Operator-visible concurrency key, or none when absent.
    pub concurrency_key: Option<String>,
}

/// Evidence emitted by key-gated transition helpers in concurrency-key acceptance scenarios.
#[derive(Debug, Clone)]
pub struct ConcurrencyGateOutcomeEvidence {
    /// Run identity evaluated by the key gate.
    pub run_id: RunId,
    /// Source state for the evaluated transition.
    pub from: RunState,
    /// Destination state for the evaluated transition.
    pub to: RunState,
    /// Lifecycle gate outcome token (`Acquired`, `KeyOccupied`, `Released`, or `NoAction`).
    pub gate_outcome: String,
    /// Whether authority transition submission was applied.
    pub transition_applied: bool,
}

/// Creates a deterministic, isolated acceptance data directory under `target/tmp/`.
pub fn unique_data_dir(label: &str) -> PathBuf {
    let dir = PathBuf::from("target").join("tmp").join(format!(
        "{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("acceptance data dir should be creatable");
    dir
}

/// Submits a Once task through the CLI submit flow and returns JSON output payload.
pub fn submit_once_task_via_cli(task_id: &str, data_dir: &Path) -> Value {
    let args = SubmitArgs {
        data_dir: Some(data_dir.to_path_buf()),
        task_id: task_id.to_string(),
        payload_path: None,
        content_type: None,
        run_policy: "once".to_string(),
        constraints: None,
        metadata: None,
        json: true,
    };

    let output = actionqueue_cli::cmd::submit::run(args).expect("cli submit should succeed");
    let value = match output {
        actionqueue_cli::cmd::CommandOutput::Json(value) => value,
        actionqueue_cli::cmd::CommandOutput::Text(text) => {
            panic!("expected JSON output from submit helper, got text: {text}")
        }
    };

    assert_eq!(value["command"], "submit");
    assert_eq!(value["run_policy"], "once");
    assert_eq!(value["runs_created"], 1);

    value
}

/// Submits a Once task through CLI submit flow with optional raw constraints JSON.
pub fn submit_once_task_with_constraints_via_cli(
    task_id: &str,
    data_dir: &Path,
    constraints_json: Option<&str>,
) -> Value {
    let args = SubmitArgs {
        data_dir: Some(data_dir.to_path_buf()),
        task_id: task_id.to_string(),
        payload_path: None,
        content_type: None,
        run_policy: "once".to_string(),
        constraints: constraints_json.map(str::to_string),
        metadata: None,
        json: true,
    };

    let output = actionqueue_cli::cmd::submit::run(args).expect("cli submit should succeed");
    let value = match output {
        actionqueue_cli::cmd::CommandOutput::Json(value) => value,
        actionqueue_cli::cmd::CommandOutput::Text(text) => {
            panic!("expected JSON output from submit helper, got text: {text}")
        }
    };

    assert_eq!(value["command"], "submit");
    assert_eq!(value["run_policy"], "once");
    assert_eq!(value["runs_created"], 1);

    value
}

/// Submits a Once task through CLI submit flow with explicit `max_attempts` constraints.
pub fn submit_once_task_with_max_attempts_via_cli(
    task_id: &str,
    data_dir: &Path,
    max_attempts: u32,
) -> Value {
    submit_once_task_with_constraints_via_cli(
        task_id,
        data_dir,
        Some(&format!("{{\"max_attempts\":{max_attempts}}}")),
    )
}

/// Submits a Once task with optional concurrency key and deterministic max-attempts constraints.
pub fn submit_once_task_with_concurrency_key_via_cli(
    task_id: &str,
    data_dir: &Path,
    concurrency_key: Option<&str>,
) -> Value {
    let constraints_json = match concurrency_key {
        Some(key) => format!(r#"{{"concurrency_key":"{key}","max_attempts":1}}"#),
        None => r#"{"max_attempts":1}"#.to_string(),
    };
    submit_once_task_with_constraints_via_cli(task_id, data_dir, Some(&constraints_json))
}

/// Promotes the sole run for a task from Scheduled to Ready via authority lane.
pub fn promote_task_run_to_ready_via_authority(data_dir: &Path, task_id: TaskId) -> RunId {
    promote_single_run_to_ready_via_authority(data_dir, task_id)
}

/// Submits a deterministic run-state transition through storage mutation authority.
pub fn transition_run_state_via_authority(
    data_dir: &Path,
    run_id: RunId,
    from: RunState,
    to: RunState,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                sequence, run_id, from, to, sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("run-state transition should succeed");

    sequence
}

/// Evaluates `Leased -> Running` with lifecycle key-gate policy and conditionally applies transition.
pub fn evaluate_and_apply_running_transition_with_key_gate(
    data_dir: &Path,
    run_id: RunId,
    concurrency_key: Option<&str>,
    key_gate: &mut actionqueue_engine::concurrency::key_gate::KeyGate,
) -> ConcurrencyGateOutcomeEvidence {
    let from = RunState::Leased;
    let to = RunState::Running;

    let lifecycle = evaluate_state_transition(
        from,
        to,
        KeyLifecycleContext::new(
            concurrency_key.map(str::to_string),
            run_id,
            key_gate,
            actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    let (gate_outcome, transition_applied) = match lifecycle {
        LifecycleResult::Acquired { .. } => {
            transition_run_state_via_authority(data_dir, run_id, from, to);
            ("Acquired".to_string(), true)
        }
        LifecycleResult::NoAction { .. } => {
            transition_run_state_via_authority(data_dir, run_id, from, to);
            ("NoAction".to_string(), true)
        }
        LifecycleResult::KeyOccupied { .. } => ("KeyOccupied".to_string(), false),
        LifecycleResult::Released { .. } => {
            panic!("leased->running transition should not emit Released lifecycle outcome")
        }
    };

    ConcurrencyGateOutcomeEvidence { run_id, from, to, gate_outcome, transition_applied }
}

/// Evaluates `Running -> terminal` with lifecycle key-gate policy and applies transition.
pub fn evaluate_and_apply_running_exit_with_key_gate(
    data_dir: &Path,
    run_id: RunId,
    concurrency_key: Option<&str>,
    terminal_state: RunState,
    key_gate: &mut actionqueue_engine::concurrency::key_gate::KeyGate,
) -> ConcurrencyGateOutcomeEvidence {
    assert!(
        matches!(terminal_state, RunState::Completed | RunState::Failed | RunState::Canceled),
        "terminal_state must be one of Completed, Failed, or Canceled"
    );

    let from = RunState::Running;
    let to = terminal_state;

    let lifecycle = evaluate_state_transition(
        from,
        to,
        KeyLifecycleContext::new(
            concurrency_key.map(str::to_string),
            run_id,
            key_gate,
            actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    let gate_outcome = match lifecycle {
        LifecycleResult::Released { .. } => "Released".to_string(),
        LifecycleResult::NoAction { .. } => "NoAction".to_string(),
        LifecycleResult::Acquired { .. } => {
            panic!("running->terminal transition should not emit Acquired lifecycle outcome")
        }
        LifecycleResult::KeyOccupied { .. } => {
            panic!("running->terminal transition should not emit KeyOccupied lifecycle outcome")
        }
    };

    transition_run_state_via_authority(data_dir, run_id, from, to);

    ConcurrencyGateOutcomeEvidence { run_id, from, to, gate_outcome, transition_applied: true }
}

/// Returns the `/api/v1/runs` row for a specific run identifier.
pub async fn runs_list_entry_by_run_id(router: &mut axum::Router<()>, run_id: RunId) -> Value {
    let runs = get_json(router, "/api/v1/runs").await;
    let entries = runs["runs"].as_array().expect("runs list should be an array");
    entries
        .iter()
        .find(|entry| entry["run_id"] == run_id.to_string())
        .cloned()
        .unwrap_or_else(|| panic!("run id {run_id} should exist in /api/v1/runs response"))
}

/// Asserts `/api/v1/runs` exposes exact concurrency-key truth for a run row.
pub async fn assert_runs_list_concurrency_key(
    router: &mut axum::Router<()>,
    run_id: RunId,
    expected_key: Option<&str>,
) {
    let entry = runs_list_entry_by_run_id(router, run_id).await;
    match expected_key {
        Some(key) => assert_eq!(entry["concurrency_key"], Value::String(key.to_string())),
        None => assert!(entry["concurrency_key"].is_null(), "concurrency_key should be null"),
    }
}

/// Returns the sole run ID for a task, asserting exact cardinality of one.
pub fn single_run_id_for_task(data_dir: &Path, task_id: TaskId) -> RunId {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run_ids = recovery.projection.run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "task should have exactly one run in retry-cap scenarios");
    run_ids[0]
}

/// Promotes the sole run for a task from Scheduled to Ready via authority lane.
pub fn promote_single_run_to_ready_via_authority(data_dir: &Path, task_id: TaskId) -> RunId {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run_ids = recovery.projection.run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "task should have exactly one run for single-run promotion");
    let run_id = run_ids[0];

    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let scheduled = ScheduledIndex::from_runs(
        authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == actionqueue_core::run::state::RunState::Scheduled)
            .cloned()
            .collect::<Vec<actionqueue_core::run::run_instance::RunInstance>>(),
    );
    let first_sequence = next_sequence(authority.projection().latest_sequence());
    let promotion = promote_scheduled_to_ready_via_authority(
        &scheduled,
        PromotionParams::new(u64::MAX, first_sequence, first_sequence, DurabilityPolicy::Immediate),
        &mut authority,
    )
    .expect("promotion via authority should succeed");
    assert_eq!(
        promotion.outcomes().len(),
        1,
        "single-run promotion should emit exactly one outcome"
    );
    assert!(
        matches!(
            promotion.outcomes()[0].applied(),
            AppliedMutation::RunStateTransition {
                run_id: promoted_run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
            } if *promoted_run_id == run_id
        ),
        "single-run promotion outcome should prove scheduled->ready for the expected run"
    );

    run_id
}

/// Submits lease-acquire via authority for the specified run and returns the durable sequence.
pub fn lease_acquire_for_run_via_authority(
    data_dir: &Path,
    run_id: RunId,
    owner: &str,
    expiry: u64,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
                sequence, run_id, owner, expiry, sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease acquire should succeed");
    sequence
}

/// Submits lease-expire via authority for the specified run and returns the durable sequence.
pub fn lease_expire_for_run_via_authority(
    data_dir: &Path,
    run_id: RunId,
    owner: &str,
    expiry: u64,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::LeaseExpire(LeaseExpireCommand::new(
                sequence, run_id, owner, expiry, sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease expire should succeed");
    sequence
}

/// Submits lease-release via authority for the specified run and returns the durable sequence.
pub fn lease_release_for_run_via_authority(
    data_dir: &Path,
    run_id: RunId,
    owner: &str,
    expiry: u64,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
                sequence, run_id, owner, expiry, sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease release should succeed");
    sequence
}

/// Deterministic attempt-outcome plan entries for retry-cap acceptance scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcomePlan {
    /// Attempt finishes as success and run terminally completes.
    Success,
    /// Attempt finishes as retryable failure and may retry while under cap.
    RetryableFailure,
    /// Attempt finishes as terminal failure and run terminally fails.
    TerminalFailure,
    /// Attempt finishes as timeout and may retry while under cap.
    Timeout,
}

/// Durable execution evidence captured for retry-cap acceptance proofs.
#[derive(Debug, Clone)]
pub struct RetryCapExecutionEvidence {
    /// Run identity used during the scenario.
    pub run_id: RunId,
    /// Attempt IDs durably recorded for the run in execution order.
    pub attempt_ids: Vec<AttemptId>,
    /// Terminal run state after planned outcomes are applied.
    pub final_state: RunState,
    /// Final durable WAL sequence after execution.
    pub final_sequence: u64,
}

/// Executes a deterministic attempt-outcome sequence via storage mutation authority.
pub fn execute_attempt_outcome_sequence_via_authority(
    data_dir: &Path,
    task_id: TaskId,
    max_attempts: u32,
    outcomes: &[AttemptOutcomePlan],
) -> RetryCapExecutionEvidence {
    assert!(max_attempts >= 1, "max_attempts must be >= 1 for retry-cap acceptance scenarios");
    assert!(
        !outcomes.is_empty(),
        "retry-cap acceptance scenarios must provide at least one planned attempt outcome"
    );

    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let run_id = {
        let run_ids = authority.projection().run_ids_for_task(task_id);
        assert_eq!(run_ids.len(), 1, "task should have exactly one run in retry-cap scenarios");
        run_ids[0]
    };

    let scheduled = ScheduledIndex::from_runs(
        authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == actionqueue_core::run::state::RunState::Scheduled)
            .cloned()
            .collect::<Vec<actionqueue_core::run::run_instance::RunInstance>>(),
    );
    let first_sequence = next_sequence(authority.projection().latest_sequence());
    let promotion = promote_scheduled_to_ready_via_authority(
        &scheduled,
        PromotionParams::new(u64::MAX, first_sequence, first_sequence, DurabilityPolicy::Immediate),
        &mut authority,
    )
    .expect("promotion via authority should succeed");
    assert_eq!(promotion.outcomes().len(), 1, "single run should be promoted to ready");

    let mut attempt_ids = Vec::new();

    for (index, outcome) in outcomes.iter().copied().enumerate() {
        let attempt_number =
            u32::try_from(index + 1).expect("attempt index should fit within u32 bounds");

        let leased_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    leased_sequence,
                    run_id,
                    RunState::Ready,
                    RunState::Leased,
                    leased_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("ready -> leased should succeed");

        let running_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    running_sequence,
                    run_id,
                    RunState::Leased,
                    RunState::Running,
                    running_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("leased -> running should succeed");

        let attempt_suffix = u128::from(attempt_number);
        let attempt_id =
            AttemptId::from_str(&format!("00000000-0000-0000-0000-{attempt_suffix:012x}"))
                .expect("deterministic attempt id should parse");
        let attempt_start_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    attempt_start_sequence,
                    run_id,
                    attempt_id,
                    attempt_start_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start should succeed");

        let response = executor_response_for_outcome(outcome, attempt_number);
        let attempt_finish_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = {
            let __finish_cmd =
                actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                    attempt_finish_sequence,
                    run_id,
                    attempt_id,
                    &response,
                    attempt_finish_sequence,
                );
            submit_attempt_finish_via_authority(
                __finish_cmd,
                DurabilityPolicy::Immediate,
                &mut authority,
            )
        }
        .expect("attempt finish should succeed");

        attempt_ids.push(attempt_id);

        match outcome {
            AttemptOutcomePlan::Success => {
                let completed_sequence = next_sequence(authority.projection().latest_sequence());
                let _ = authority
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            completed_sequence,
                            run_id,
                            RunState::Running,
                            RunState::Completed,
                            completed_sequence,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .expect("running -> completed should succeed");
                break;
            }
            AttemptOutcomePlan::TerminalFailure => {
                let failed_sequence = next_sequence(authority.projection().latest_sequence());
                let _ = authority
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            failed_sequence,
                            run_id,
                            RunState::Running,
                            RunState::Failed,
                            failed_sequence,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .expect("running -> failed should succeed");
                break;
            }
            AttemptOutcomePlan::RetryableFailure | AttemptOutcomePlan::Timeout => {
                if attempt_number < max_attempts {
                    let retry_wait_sequence =
                        next_sequence(authority.projection().latest_sequence());
                    let _ = authority
                        .submit_command(
                            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                                retry_wait_sequence,
                                run_id,
                                RunState::Running,
                                RunState::RetryWait,
                                retry_wait_sequence,
                            )),
                            DurabilityPolicy::Immediate,
                        )
                        .expect("running -> retry_wait should succeed");

                    let ready_sequence = next_sequence(authority.projection().latest_sequence());
                    let _ = authority
                        .submit_command(
                            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                                ready_sequence,
                                run_id,
                                RunState::RetryWait,
                                RunState::Ready,
                                ready_sequence,
                            )),
                            DurabilityPolicy::Immediate,
                        )
                        .expect("retry_wait -> ready should succeed");
                } else {
                    let failed_sequence = next_sequence(authority.projection().latest_sequence());
                    let _ = authority
                        .submit_command(
                            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                                failed_sequence,
                                run_id,
                                RunState::Running,
                                RunState::Failed,
                                failed_sequence,
                            )),
                            DurabilityPolicy::Immediate,
                        )
                        .expect("running -> failed should succeed");
                    break;
                }
            }
        }
    }

    let expected_attempt_count = expected_attempt_count_for_outcomes(max_attempts, outcomes);
    assert_eq!(
        attempt_ids.len(),
        expected_attempt_count,
        "executed attempt count should match expected retry-cap terminal boundary"
    );

    let run = authority
        .projection()
        .get_run_instance(&run_id)
        .expect("run should remain present in projection after execution");

    RetryCapExecutionEvidence {
        run_id,
        attempt_ids,
        final_state: run.state(),
        final_sequence: authority.projection().latest_sequence(),
    }
}

/// Submits a Repeat task through the CLI submit flow and returns JSON output payload.
pub fn submit_repeat_task_via_cli(
    task_id: &str,
    data_dir: &Path,
    count: u32,
    interval_secs: u64,
) -> Value {
    let run_policy = format!("repeat:{count}:{interval_secs}");
    let args = SubmitArgs {
        data_dir: Some(data_dir.to_path_buf()),
        task_id: task_id.to_string(),
        payload_path: None,
        content_type: None,
        run_policy: run_policy.clone(),
        constraints: None,
        metadata: None,
        json: true,
    };

    let output = actionqueue_cli::cmd::submit::run(args).expect("cli submit should succeed");
    let value = match output {
        actionqueue_cli::cmd::CommandOutput::Json(value) => value,
        actionqueue_cli::cmd::CommandOutput::Text(text) => {
            panic!("expected JSON output from submit helper, got text: {text}")
        }
    };

    assert_eq!(value["command"], "submit");
    assert_eq!(value["run_policy"], run_policy);
    assert_eq!(value["runs_created"], count);

    value
}

/// Bootstraps a daemon HTTP router from storage state and feature settings.
pub fn bootstrap_http_router(data_dir: &Path, metrics_enabled: bool) -> axum::Router<()> {
    let config = DaemonConfig {
        data_dir: data_dir.to_path_buf(),
        metrics_bind: if metrics_enabled {
            Some(std::net::SocketAddr::from(([127, 0, 0, 1], 9090)))
        } else {
            None
        },
        ..DaemonConfig::default()
    };

    let state =
        actionqueue_daemon::bootstrap::bootstrap(config).expect("daemon bootstrap should succeed");
    state.http_router().clone().with_state(())
}

/// Executes an in-process GET request and parses a JSON response payload.
pub async fn get_json(router: &mut axum::Router<()>, path: &str) -> Value {
    let response = send_get(router, path).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.expect("body should collect").to_bytes();
    serde_json::from_slice(&bytes).expect("response should be valid JSON")
}

/// Executes an in-process GET request and returns raw text response body.
pub async fn get_text(router: &mut axum::Router<()>, path: &str) -> String {
    let response = send_get(router, path).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.expect("body should collect").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("response should be utf-8")
}

/// Returns ordered attempt IDs from `/api/v1/runs/:id` as stable strings.
pub async fn run_get_attempt_ids(router: &mut axum::Router<()>, run_id: RunId) -> Vec<String> {
    let run_get_path = format!("/api/v1/runs/{run_id}");
    let run_get = get_json(router, &run_get_path).await;
    run_get["attempts"]
        .as_array()
        .expect("attempts should be an array")
        .iter()
        .map(|entry| {
            entry["attempt_id"]
                .as_str()
                .expect("attempt_id should be serialized as string")
                .to_string()
        })
        .collect()
}

/// Executes `/api/v1/runs/:id` and returns the parsed response payload.
pub async fn run_get(router: &mut axum::Router<()>, run_id: RunId) -> Value {
    let run_get_path = format!("/api/v1/runs/{run_id}");
    get_json(router, &run_get_path).await
}

/// Returns `/api/v1/runs` rows sorted by stable `run_id` string.
pub async fn runs_list_rows_sorted_by_run_id(router: &mut axum::Router<()>) -> Vec<Value> {
    let runs = get_json(router, "/api/v1/runs").await;
    let mut rows = runs["runs"].as_array().expect("runs list should be an array").to_vec();
    rows.sort_by(|left, right| {
        left["run_id"]
            .as_str()
            .expect("run_id should be serialized as string")
            .cmp(right["run_id"].as_str().expect("run_id should be serialized as string"))
    });
    rows
}

/// Asserts run identity stability across restart boundaries.
pub fn assert_run_id_stability(before: RunId, after: RunId) {
    assert_eq!(before, after, "run id must remain stable across restart boundary");
}

/// Asserts no additional task-scoped runs were created beyond expected IDs.
pub fn assert_no_new_task_runs(data_dir: &Path, task_id: TaskId, expected_ids: &[RunId]) {
    assert_task_run_count_and_ids(data_dir, task_id, expected_ids);
}

/// Captures pre-restart durable checkpoint for crash-recovery scenarios.
pub fn capture_checkpoint(data_dir: &Path, run_id: RunId) -> CrashRecoveryCheckpoint {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run = recovery
        .projection
        .get_run_instance(&run_id)
        .expect("run should exist while capturing checkpoint");
    let pre_restart_attempt_ids = recovery
        .projection
        .get_attempt_history(&run_id)
        .unwrap_or(&[])
        .iter()
        .map(|entry| entry.attempt_id())
        .collect();

    CrashRecoveryCheckpoint {
        run_id,
        pre_restart_attempt_ids,
        pre_restart_state: run.state(),
        pre_restart_sequence: recovery.projection.latest_sequence(),
    }
}

/// Returns parsed lease evidence from `/api/v1/runs/:id` when a lease is active.
pub async fn current_lease_from_run_get(
    router: &mut axum::Router<()>,
    run_id: RunId,
) -> Option<LeaseSnapshotEvidence> {
    let run_get_path = format!("/api/v1/runs/{run_id}");
    let run_get = get_json(router, &run_get_path).await;
    let lease = &run_get["lease"];
    if lease.is_null() {
        return None;
    }

    Some(LeaseSnapshotEvidence {
        owner: lease["owner"]
            .as_str()
            .expect("lease owner should be present as string")
            .to_string(),
        expiry: lease["expiry"].as_u64().expect("lease expiry should be present as u64"),
        acquired_at: lease["acquired_at"]
            .as_u64()
            .expect("lease acquired_at should be present as u64"),
        updated_at: lease["updated_at"]
            .as_u64()
            .expect("lease updated_at should be present as u64"),
    })
}

/// Completes the submitted Once run through validated mutation authority only.
pub fn complete_once_run_via_authority(data_dir: &Path, task_id: TaskId) -> CompletionEvidence {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run_id = recovery
        .projection
        .run_ids_for_task(task_id)
        .into_iter()
        .next()
        .expect("one run should exist for once task");

    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let scheduled = ScheduledIndex::from_runs(
        authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == actionqueue_core::run::state::RunState::Scheduled)
            .cloned()
            .collect::<Vec<actionqueue_core::run::run_instance::RunInstance>>(),
    );
    let first_sequence = next_sequence(authority.projection().latest_sequence());
    let promotion = promote_scheduled_to_ready_via_authority(
        &scheduled,
        PromotionParams::new(u64::MAX, first_sequence, first_sequence, DurabilityPolicy::Immediate),
        &mut authority,
    )
    .expect("promotion via authority should succeed");
    assert_eq!(promotion.outcomes().len(), 1);

    let leased_sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                leased_sequence,
                run_id,
                RunState::Ready,
                RunState::Leased,
                leased_sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("ready -> leased should succeed");

    let running_sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                running_sequence,
                run_id,
                RunState::Leased,
                RunState::Running,
                running_sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("leased -> running should succeed");

    let attempt_id = AttemptId::from_str("00000000-0000-0000-0000-000000000001")
        .expect("fixed attempt id should parse");
    let attempt_start_sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(
                attempt_start_sequence,
                run_id,
                attempt_id,
                attempt_start_sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt start should succeed");

    let attempt_finish_sequence = next_sequence(authority.projection().latest_sequence());
    let _ = {
        let __finish_cmd =
            actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                attempt_finish_sequence,
                run_id,
                attempt_id,
                &ExecutorResponse::Success { output: None },
                attempt_finish_sequence,
            );
        submit_attempt_finish_via_authority(
            __finish_cmd,
            DurabilityPolicy::Immediate,
            &mut authority,
        )
    }
    .expect("attempt finish should succeed");

    let completed_sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                completed_sequence,
                run_id,
                RunState::Running,
                RunState::Completed,
                completed_sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("running -> completed should succeed");

    CompletionEvidence { run_id, attempt_id, final_sequence: completed_sequence }
}

/// Returns task-scoped run IDs from authoritative storage projection in deterministic order.
pub fn task_run_ids_from_storage(data_dir: &Path, task_id: TaskId) -> Vec<RunId> {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    recovery.projection.run_ids_for_task(task_id)
}

/// Reloads projection and returns stored attempt IDs for a run in stable order.
pub fn attempt_ids_for_run_from_storage(data_dir: &Path, run_id: RunId) -> Vec<AttemptId> {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    recovery
        .projection
        .get_attempt_history(&run_id)
        .unwrap_or(&[])
        .iter()
        .map(|entry| entry.attempt_id())
        .collect()
}

/// Reloads projection and asserts exact durable run state.
pub fn assert_run_state_from_storage(data_dir: &Path, run_id: RunId, expected: RunState) {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run = recovery
        .projection
        .get_run_instance(&run_id)
        .expect("run should exist in projection when asserting durable state");
    assert_eq!(run.state(), expected, "durable run state should match expected state");
}

/// Completes all runs for a task through mutation authority only, returning per-run evidence.
pub fn complete_all_task_runs_via_authority(
    data_dir: &Path,
    task_id: TaskId,
) -> Vec<CompletionEvidence> {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let run_ids = recovery.projection.run_ids_for_task(task_id);

    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let scheduled = ScheduledIndex::from_runs(
        authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == actionqueue_core::run::state::RunState::Scheduled)
            .cloned()
            .collect::<Vec<actionqueue_core::run::run_instance::RunInstance>>(),
    );
    let first_sequence = next_sequence(authority.projection().latest_sequence());
    let promotion = promote_scheduled_to_ready_via_authority(
        &scheduled,
        PromotionParams::new(u64::MAX, first_sequence, first_sequence, DurabilityPolicy::Immediate),
        &mut authority,
    )
    .expect("promotion via authority should succeed");
    assert_eq!(
        promotion.outcomes().len(),
        run_ids.len(),
        "all task runs should be promoted to ready"
    );

    let mut evidence = Vec::with_capacity(run_ids.len());
    for (index, run_id) in run_ids.iter().copied().enumerate() {
        let leased_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    leased_sequence,
                    run_id,
                    RunState::Ready,
                    RunState::Leased,
                    leased_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("ready -> leased should succeed");

        let running_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    running_sequence,
                    run_id,
                    RunState::Leased,
                    RunState::Running,
                    running_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("leased -> running should succeed");

        let attempt_suffix = u128::try_from(index + 1).expect("attempt index should fit in u128");
        let attempt_id =
            AttemptId::from_str(&format!("00000000-0000-0000-0000-{attempt_suffix:012x}"))
                .expect("deterministic attempt id should parse");
        let attempt_start_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    attempt_start_sequence,
                    run_id,
                    attempt_id,
                    attempt_start_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start should succeed");

        let attempt_finish_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = {
            let __finish_cmd =
                actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                    attempt_finish_sequence,
                    run_id,
                    attempt_id,
                    &ExecutorResponse::Success { output: None },
                    attempt_finish_sequence,
                );
            submit_attempt_finish_via_authority(
                __finish_cmd,
                DurabilityPolicy::Immediate,
                &mut authority,
            )
        }
        .expect("attempt finish should succeed");

        let completed_sequence = next_sequence(authority.projection().latest_sequence());
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    completed_sequence,
                    run_id,
                    RunState::Running,
                    RunState::Completed,
                    completed_sequence,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("running -> completed should succeed");

        evidence.push(CompletionEvidence {
            run_id,
            attempt_id,
            final_sequence: completed_sequence,
        });
    }

    evidence
}

/// Reloads projection and asserts exact task-scoped run identity/cardinality parity.
pub fn assert_task_run_count_and_ids(data_dir: &Path, task_id: TaskId, expected: &[RunId]) {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");

    let mut actual = recovery.projection.run_ids_for_task(task_id);
    let mut expected_sorted = expected.to_vec();
    actual.sort();
    expected_sorted.sort();

    assert_eq!(
        actual.len(),
        expected_sorted.len(),
        "task run cardinality must match expected exact-N"
    );
    assert_eq!(actual, expected_sorted, "task run identity set must match expected exact-N IDs");
}

/// Returns a parsed metrics sample value by exact metric name and exact label map.
pub fn metrics_sample_value(metrics_text: &str, metric_name: &str, labels: &[(&str, &str)]) -> f64 {
    let expected_labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<BTreeMap<_, _>>();

    let samples = parse_metrics_samples(metrics_text);
    samples
        .iter()
        .find(|sample| sample.name == metric_name && sample.labels == expected_labels)
        .unwrap_or_else(|| {
            panic!("missing metric sample: {metric_name} with labels {expected_labels:?}")
        })
        .value
}

/// Returns the exact observed label values for a metric family and label key.
pub fn metrics_label_values(
    metrics_text: &str,
    metric_name: &str,
    label_key: &str,
) -> BTreeSet<String> {
    parse_metrics_samples(metrics_text)
        .into_iter()
        .filter(|sample| sample.name == metric_name)
        .filter_map(|sample| sample.labels.get(label_key).cloned())
        .collect()
}

/// Returns the exact label-key vocabulary observed for a metric family.
pub fn metrics_label_keys(metrics_text: &str, metric_name: &str) -> BTreeSet<String> {
    parse_metrics_samples(metrics_text)
        .into_iter()
        .filter(|sample| sample.name == metric_name)
        .flat_map(|sample| sample.labels.into_keys())
        .collect()
}

/// Returns the number of samples present for an exact metric family name.
pub fn metrics_sample_count(metrics_text: &str, metric_name: &str) -> usize {
    parse_metrics_samples(metrics_text)
        .into_iter()
        .filter(|sample| sample.name == metric_name)
        .count()
}

/// Returns true when any emitted sample name starts with the provided prefix.
pub fn metrics_has_prefix(metrics_text: &str, prefix: &str) -> bool {
    parse_metrics_samples(metrics_text).into_iter().any(|sample| sample.name.starts_with(prefix))
}

/// Reloads projection and asserts no scheduled runs remain.
pub fn assert_no_scheduled_runs_left(data_dir: &Path) {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let scheduled = ScheduledIndex::from_runs(
        recovery
            .projection
            .run_instances()
            .filter(|r| r.state() == actionqueue_core::run::state::RunState::Scheduled)
            .cloned()
            .collect::<Vec<actionqueue_core::run::run_instance::RunInstance>>(),
    );
    assert!(scheduled.is_empty(), "completed once flow must not leave scheduled runs");
}

#[derive(Debug, Clone)]
struct MetricSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

fn parse_metrics_samples(body: &str) -> Vec<MetricSample> {
    body.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let mut parts = trimmed.split_whitespace();
            let metric_key = parts.next().expect("metric key should exist");
            let value = parts
                .next()
                .expect("metric value should exist")
                .parse::<f64>()
                .expect("metric value should parse as f64");

            let (name, labels) = parse_metric_key(metric_key);
            Some(MetricSample { name, labels, value })
        })
        .collect()
}

fn parse_metric_key(metric_key: &str) -> (String, BTreeMap<String, String>) {
    let Some(label_start) = metric_key.find('{') else {
        return (metric_key.to_string(), BTreeMap::new());
    };

    let label_end = metric_key.rfind('}').expect("labeled metric key should include closing brace");
    let name = metric_key[..label_start].to_string();
    let mut labels = BTreeMap::new();
    let labels_str = &metric_key[label_start + 1..label_end];

    if labels_str.is_empty() {
        return (name, labels);
    }

    for assignment in labels_str.split(',') {
        let mut key_value = assignment.splitn(2, '=');
        let key = key_value.next().expect("label key should exist").to_string();
        let raw_value = key_value.next().expect("label value should exist");
        let value = raw_value.trim_matches('"').to_string();
        labels.insert(key, value);
    }

    (name, labels)
}

async fn send_get(router: &mut axum::Router<()>, path: &str) -> axum::response::Response {
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .expect("request should build");
    let mut service = router.as_service::<Body>();
    service.call(request).await.expect("request should succeed")
}

pub fn next_sequence(latest_sequence: u64) -> u64 {
    latest_sequence.checked_add(1).expect("next sequence should not overflow u64")
}

// ---------------------------------------------------------------------------
// Shared truth structs and assertion helpers for stats and metrics
// ---------------------------------------------------------------------------

/// Deterministic `/api/v1/stats` truth expectation set.
///
/// Used by crash_recovery, concurrency_key, observability, and lease_expiry
/// acceptance tests to assert aggregate parity from the stats endpoint.
#[derive(Debug, Clone, Copy)]
pub struct StatsTruth {
    pub total_tasks: usize,
    pub total_runs: usize,
    pub attempts_total: u32,
    pub scheduled: usize,
    pub ready: usize,
    pub leased: usize,
    pub running: usize,
    pub retry_wait: usize,
    pub completed: usize,
    pub failed: usize,
    pub canceled: usize,
}

/// Deterministic `/metrics` truth expectation set.
///
/// Used by crash_recovery, concurrency_key, observability, and lease_expiry
/// acceptance tests to assert metrics value parity from the Prometheus endpoint.
#[derive(Debug, Clone, Copy)]
pub struct MetricsTruth {
    pub scheduled: f64,
    pub ready: f64,
    pub leased: f64,
    pub running: f64,
    pub retry_wait: f64,
    pub completed: f64,
    pub failed: f64,
    pub canceled: f64,
    pub attempts_success: f64,
    pub attempts_failure: f64,
    pub attempts_timeout: f64,
}

/// Asserts required aggregate parity truth from `/api/v1/stats`.
pub async fn assert_stats_truth(router: &mut axum::Router<()>, expected: StatsTruth) {
    let stats = get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["total_tasks"], expected.total_tasks);
    assert_eq!(stats["total_runs"], expected.total_runs);
    assert_eq!(stats["attempts_total"], expected.attempts_total);
    assert_eq!(stats["runs_by_state"]["scheduled"], expected.scheduled);
    assert_eq!(stats["runs_by_state"]["ready"], expected.ready);
    assert_eq!(stats["runs_by_state"]["leased"], expected.leased);
    assert_eq!(stats["runs_by_state"]["running"], expected.running);
    assert_eq!(stats["runs_by_state"]["retry_wait"], expected.retry_wait);
    assert_eq!(stats["runs_by_state"]["completed"], expected.completed);
    assert_eq!(stats["runs_by_state"]["failed"], expected.failed);
    assert_eq!(stats["runs_by_state"]["canceled"], expected.canceled);
}

/// Asserts required aggregate parity truth from `/metrics`.
pub async fn assert_metrics_truth(router: &mut axum::Router<()>, expected: MetricsTruth) {
    let metrics = get_text(router, "/metrics").await;

    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "scheduled")]),
        expected.scheduled
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "ready")]),
        expected.ready
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "leased")]),
        expected.leased
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "running")]),
        expected.running
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "retry_wait")]),
        expected.retry_wait
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "completed")]),
        expected.completed
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "failed")]),
        expected.failed
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_runs_total", &[("state", "canceled")]),
        expected.canceled
    );

    assert_eq!(metrics_sample_value(&metrics, "actionqueue_runs_ready", &[]), expected.ready);
    assert_eq!(metrics_sample_value(&metrics, "actionqueue_runs_running", &[]), expected.running);

    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_attempts_total", &[("result", "success")]),
        expected.attempts_success
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_attempts_total", &[("result", "failure")]),
        expected.attempts_failure
    );
    assert_eq!(
        metrics_sample_value(&metrics, "actionqueue_attempts_total", &[("result", "timeout")]),
        expected.attempts_timeout
    );
}

// ---------------------------------------------------------------------------
// Authority-lane helpers for attempt start/finish (shared across test files)
// ---------------------------------------------------------------------------

/// Submits one authority-lane attempt-start command and returns durable sequence.
pub fn submit_attempt_start_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );
    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(
                sequence, run_id, attempt_id, sequence,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt start should succeed");
    sequence
}

/// Submits one authority-lane attempt-finish command and returns durable sequence.
pub fn submit_attempt_finish_response_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
    response: &ExecutorResponse,
) -> u64 {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );
    let sequence = next_sequence(authority.projection().latest_sequence());
    let _ = {
        let finish_cmd =
            actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                sequence, run_id, attempt_id, response, sequence,
            );
        submit_attempt_finish_via_authority(finish_cmd, DurabilityPolicy::Immediate, &mut authority)
    }
    .expect("attempt finish should succeed");
    sequence
}

fn executor_response_for_outcome(
    outcome: AttemptOutcomePlan,
    attempt_number: u32,
) -> ExecutorResponse {
    match outcome {
        AttemptOutcomePlan::Success => ExecutorResponse::Success { output: None },
        AttemptOutcomePlan::RetryableFailure => ExecutorResponse::RetryableFailure {
            error: format!("retryable failure at attempt {attempt_number}"),
        },
        AttemptOutcomePlan::TerminalFailure => ExecutorResponse::TerminalFailure {
            error: format!("terminal failure at attempt {attempt_number}"),
        },
        AttemptOutcomePlan::Timeout => ExecutorResponse::Timeout { timeout_secs: 5 },
    }
}

fn expected_attempt_count_for_outcomes(
    max_attempts: u32,
    outcomes: &[AttemptOutcomePlan],
) -> usize {
    let mut executed = 0usize;

    for (index, outcome) in outcomes.iter().copied().enumerate() {
        let attempt_number =
            u32::try_from(index + 1).expect("attempt index should fit within u32 bounds");
        executed += 1;

        let terminal = match outcome {
            AttemptOutcomePlan::Success | AttemptOutcomePlan::TerminalFailure => true,
            AttemptOutcomePlan::RetryableFailure | AttemptOutcomePlan::Timeout => {
                attempt_number >= max_attempts
            }
        };

        if terminal {
            break;
        }
    }

    executed
}
