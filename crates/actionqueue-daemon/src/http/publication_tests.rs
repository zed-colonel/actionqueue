use super::*;
use actionqueue_core::{
    mutation::{
        DurabilityPolicy, EnginePauseCommand, EngineResumeCommand, MutationAuthority,
        MutationCommand,
    },
    time::clock::MockClock,
};
use actionqueue_storage::{
    recovery::work,
    wal::{fs_writer::WalFsWriter, writer::WalWriter, WalAppendTelemetry},
};
mod surface_history {
    use actionqueue_core::{
        ids::TaskId,
        run::{RunInstance, RunState},
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    use actionqueue_storage::{
        recovery::reducer::ReplayReducer,
        wal::event::{WalEvent, WalEventType as E},
    };
    pub fn history(n: usize) -> ReplayReducer {
        let mut p = ReplayReducer::new();
        for _ in 0..n {
            let task_id = TaskId::new();
            let task_spec = TaskSpec::new(
                task_id,
                TaskPayload::new(vec![1, 2, 3]),
                RunPolicy::Once,
                Default::default(),
                Default::default(),
            )
            .unwrap();
            let run = RunInstance::new_scheduled(task_id, 1, 1).unwrap();
            for event in [
                E::TaskCreated { task_spec, timestamp: 1 },
                E::RunCreated { run_instance: run.clone() },
                E::RunCanceled { run_id: run.id(), timestamp: 2 },
            ] {
                p.apply(&WalEvent::new(p.latest_sequence() + 1, event)).unwrap();
            }
            assert_eq!(p.get_run_state(&run.id()), Some(&RunState::Canceled));
        }
        p
    }
}

fn fixture(
    n: usize,
) -> (RouterState, StorageMutationAuthority<WalFsWriter, ReplayReducer>, std::path::PathBuf) {
    let p = surface_history::history(n);
    let path = std::env::temp_dir()
        .join(format!("publication-{}.wal", actionqueue_core::ids::TaskId::new()));
    let a = StorageMutationAuthority::new(
        WalFsWriter::new_raw_for_test(path.clone()).unwrap(),
        p.clone(),
    )
    .with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::Store,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("publication").unwrap(),
        ),
    });
    let state = Arc::new(RouterStateInner::new(
        crate::bootstrap::RouterConfig { control_enabled: true, metrics_enabled: false },
        Arc::new(RwLock::new(p)),
        RouterObservability {
            metrics: Arc::new(crate::metrics::registry::MetricsRegistry::new(None).unwrap()),
            wal_append_telemetry: WalAppendTelemetry::new(),
            clock: Arc::new(MockClock::new(1000)),
            recovery_observations: RecoveryObservations::zero(),
        },
        crate::bootstrap::ReadyStatus::ready(),
    ));
    (state, a, path)
}
fn mutate(a: &mut StorageMutationAuthority<WalFsWriter, ReplayReducer>) {
    let sequence = a.projection().latest_sequence() + 1;
    let c = if a.projection().is_engine_paused() {
        MutationCommand::EngineResume(EngineResumeCommand::new(sequence, 1000))
    } else {
        MutationCommand::EnginePause(EnginePauseCommand::new(sequence, 1000))
    };
    let _ = a.submit_command(c, DurabilityPolicy::Immediate).unwrap();
}
#[test]
fn published_roots_share_identity_and_unchanged_publication_does_no_copy() {
    let (state, mut a, path) = fixture(7);
    for _ in 0..3 {
        mutate(&mut a);
        work::reset_projection_work();
        sync_projection(&state, &a).unwrap();
        assert!(read_projection(&state).unwrap().shares_revision_with(a.projection()));
        assert_eq!(work::projection_work().shallow_projection_clones, 1);
        work::reset_projection_work();
        sync_projection(&state, &a).unwrap();
        assert_eq!(work::projection_work(), work::ProjectionWork::default());
    }
    let (writer, _) = a.into_parts();
    writer.close().unwrap();
    std::fs::remove_file(path).unwrap();
}
#[test]
#[ignore = "manual optimized scale measurements; timing is informational"]
fn publication_scale_measurements() {
    use std::time::Instant;
    for repetition in 1..=5 {
        for n in [1000, 10000, 100000] {
            let (state, mut a, path) = fixture(n);
            let mut publish = 0;
            let mut unchanged = 0;
            for _ in 0..100 {
                mutate(&mut a);
                work::reset_projection_work();
                let at = Instant::now();
                sync_projection(&state, &a).unwrap();
                publish += at.elapsed().as_nanos();
                let w = work::projection_work();
                assert_eq!(w.shallow_projection_clones, 1);
                assert_eq!(w.full_projection_clones, 0);
                assert_eq!(w.cloned_run_entries, 0);
                let at = Instant::now();
                sync_projection(&state, &a).unwrap();
                unchanged += at.elapsed().as_nanos();
            }
            eprintln!(
                "{}",
                serde_json::json!({"repetition":repetition,"retained_terminal_runs":n,"publication_ns_per_operation":publish as f64/100.,"unchanged_ns_per_operation":unchanged as f64/100.})
            );
            let (writer, _) = a.into_parts();
            writer.close().unwrap();
            std::fs::remove_file(path).unwrap();
        }
    }
}
