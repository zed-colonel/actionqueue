//! A retained terminal projection for coordination/publication measurements.
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
