//! Stats command execution path.

use serde::Serialize;
use serde_json::json;

use crate::args::{StatsArgs, StatsOutputFormat};
use crate::cmd::{resolve_data_dir, CliError, CommandOutput};

/// Executes stats command flow.
pub fn run(args: StatsArgs) -> Result<CommandOutput, CliError> {
    let data_dir = resolve_data_dir(args.data_dir.as_deref());
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(
        &data_dir,
    )
    .map_err(|error| {
        CliError::runtime(
            "storage_bootstrap_failed",
            format!("unable to load storage projection: {error}"),
        )
    })?;

    let projection = recovery.projection;
    let summary = StatsSummary::from_projection(&projection);

    match args.format {
        StatsOutputFormat::Json => Ok(CommandOutput::Json(json!({
            "command": "stats",
            "data_dir": data_dir.display().to_string(),
            "summary": summary,
        }))),
        StatsOutputFormat::Text => Ok(CommandOutput::Text(format!(
            "command=stats\ndata_dir={}\ntotal_tasks={}\ntotal_runs={}\nlatest_sequence={}\\
             nruns_scheduled={}\nruns_ready={}\nruns_leased={}\nruns_running={}\\
             nruns_retry_wait={}\nruns_completed={}\nruns_failed={}\nruns_canceled={}\\
             nattempts_total={}",
            data_dir.display(),
            summary.total_tasks,
            summary.total_runs,
            summary.latest_sequence,
            summary.runs_by_state.scheduled,
            summary.runs_by_state.ready,
            summary.runs_by_state.leased,
            summary.runs_by_state.running,
            summary.runs_by_state.retry_wait,
            summary.runs_by_state.completed,
            summary.runs_by_state.failed,
            summary.runs_by_state.canceled,
            summary.attempts_total,
        ))),
    }
}

/// Deterministic run-state counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RunsByState {
    /// Count of `Scheduled` runs.
    pub scheduled: usize,
    /// Count of `Ready` runs.
    pub ready: usize,
    /// Count of `Leased` runs.
    pub leased: usize,
    /// Count of `Running` runs.
    pub running: usize,
    /// Count of `RetryWait` runs.
    pub retry_wait: usize,
    /// Count of `Completed` runs.
    pub completed: usize,
    /// Count of `Failed` runs.
    pub failed: usize,
    /// Count of `Canceled` runs.
    pub canceled: usize,
}

impl RunsByState {
    const fn zero() -> Self {
        Self {
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        }
    }
}

/// Deterministic aggregate stats summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StatsSummary {
    /// Number of tracked tasks.
    pub total_tasks: usize,
    /// Number of tracked runs.
    pub total_runs: usize,
    /// Latest projection sequence.
    pub latest_sequence: u64,
    /// Run counts by lifecycle state.
    pub runs_by_state: RunsByState,
    /// Total attempts across tracked runs.
    pub attempts_total: u64,
}

impl StatsSummary {
    /// Builds deterministic stats from authoritative projection state.
    pub fn from_projection(
        projection: &actionqueue_storage::recovery::reducer::ReplayReducer,
    ) -> Self {
        let mut runs_by_state = RunsByState::zero();
        let mut attempts_total = 0u64;

        for run in projection.run_instances() {
            match run.state() {
                actionqueue_core::run::state::RunState::Scheduled => runs_by_state.scheduled += 1,
                actionqueue_core::run::state::RunState::Ready => runs_by_state.ready += 1,
                actionqueue_core::run::state::RunState::Leased => runs_by_state.leased += 1,
                actionqueue_core::run::state::RunState::Running => runs_by_state.running += 1,
                actionqueue_core::run::state::RunState::RetryWait => runs_by_state.retry_wait += 1,
                actionqueue_core::run::state::RunState::Suspended => {}
                actionqueue_core::run::state::RunState::Completed => runs_by_state.completed += 1,
                actionqueue_core::run::state::RunState::Failed => runs_by_state.failed += 1,
                actionqueue_core::run::state::RunState::Canceled => runs_by_state.canceled += 1,
            }
            attempts_total = attempts_total.saturating_add(u64::from(run.attempt_count()));
        }

        Self {
            total_tasks: projection.task_count(),
            total_runs: projection.run_count(),
            latest_sequence: projection.latest_sequence(),
            runs_by_state,
            attempts_total,
        }
    }
}
