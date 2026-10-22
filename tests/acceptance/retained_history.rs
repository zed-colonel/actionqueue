#[path = "../../benches/support/retained_history.rs"]
mod retained_history;

#[test]
fn retained_history_has_shared_structural_costs_and_recovery_parity() {
    // Empty and nonempty retained prefixes use the same 100-operation batches as the bench.
    for n in [0, 7] {
        let [result] = retained_history::run_repeated(n, 1).try_into().unwrap();
        assert_eq!(result["retained_terminal_runs"], n);
        assert_eq!(result["tail_events"], 200);
        assert_eq!(result["final_sequence"], 1 + 2 * (n + retained_history::BATCH));
    }
}

#[test]
fn repeated_samples_start_from_identical_durable_prefixes_and_independent_files() {
    let results = retained_history::run_repeated(7, 2);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["prefix_wal_sha256"], results[1]["prefix_wal_sha256"]);
    for result in results {
        assert_eq!(result["cache_conditions"], "warm OS cache; no cache eviction; alternating order; independent copied files per repetition");
        assert_eq!(result["retained_terminal_runs"], 7);
        assert_eq!(result["prefix_events"], 15);
        assert_eq!(result["final_sequence"], 215);
        assert_eq!(result["final_runs"], 107);
    }
}

#[test]
fn single_sample_reports_live_seed_cache_conditions() {
    let results = retained_history::run_repeated(7, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["cache_conditions"],
        "warm OS cache; no cache eviction; WAL-first order; live seed authority"
    );
}

#[tokio::test]
async fn durable_progress_counts_between_ticks_recovered_tails_and_failed_publication() {
    use actionqueue_core::{
        control::*,
        ids::TaskId,
        mutation::*,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
        time::clock::MockClock,
    };
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
    use actionqueue_storage::{
        snapshot::{
            checkpoint::publish_checkpoint,
            loader::{SnapshotFsLoader, SnapshotLoader},
        },
        store::*,
    };
    #[derive(Clone)]
    struct Handler;
    impl actionqueue_executor_local::handler::ExecutorHandler for Handler {
        fn execute(
            &self,
            _: actionqueue_executor_local::handler::ExecutorContext,
        ) -> actionqueue_core::disposition::AttemptDisposition {
            panic!("paused engine must not dispatch")
        }
    }
    let host = HostControlContext {
        actor_id: None,
        scope: ControlScope::Store,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("publication-progress").unwrap(),
        ),
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap()
        .with_host(host.clone());
    let _ = a
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(2, 1000)),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    let c = a.capture_checkpoint(1000).unwrap();
    publish_checkpoint(c).unwrap();
    drop(a);
    let host = HostControlContext { scope: ControlScope::SingleTenant, ..host };
    let sequence = || {
        SnapshotFsLoader::with_version(dir.path().join("snapshots/snapshot.bin"), 2)
            .load()
            .unwrap()
            .unwrap()
            .metadata
            .wal_sequence
    };
    let config = RuntimeConfig {
        data_dir: dir.path().to_owned(),
        snapshot_event_threshold: Some(2),
        ..Default::default()
    };
    let mut engine = ActionQueueEngine::new(config.clone(), Handler)
        .bootstrap_with_clock(MockClock::new(1000))
        .unwrap()
        .with_host(host.clone());
    for _ in 0..2 {
        engine
            .submit_task(
                TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::new(vec![]),
                    RunPolicy::Once,
                    Default::default(),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(sequence(), 2);
    fault::fail_once("snapshot_before_rename");
    assert!(engine.tick().await.is_err());
    assert_eq!(sequence(), 2);
    let tick = engine.tick().await.unwrap();
    assert!(tick.engine_paused);
    assert_eq!(tick.dispatched, 0);
    assert_eq!(sequence(), 4);
    engine.shutdown().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::ReadWrite)
        .unwrap()
        .into_authority()
        .unwrap()
        .with_host(HostControlContext { scope: ControlScope::Store, ..host.clone() });
    let _ = a
        .submit_command(
            MutationCommand::EngineResume(EngineResumeCommand::new(5, 1001)),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    let _ = a
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(6, 1002)),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    drop(a);
    let mut engine = ActionQueueEngine::new(config, Handler)
        .bootstrap_with_clock(MockClock::new(1002))
        .unwrap()
        .with_host(host);
    assert_eq!(sequence(), 4);
    let tick = engine.tick().await.unwrap();
    assert!(tick.engine_paused);
    assert_eq!(tick.dispatched, 0);
    assert_eq!(sequence(), 6);
    engine.shutdown().unwrap();
    assert_eq!(verify_store(dir.path()).unwrap().snapshot_sequence, 6);
}
