//! A lease release or expiry that returns a run to Ready must match a Ready
//! subscription created while the run was leased, exactly as the pre-AQ-P2 reducer
//! did, in live, hydrated and replayed projections with the same canonical digest.
use crate::{
    recovery::{projection::ProjectionImageV8, reducer::ReplayReducer, replay::ReplayDriver},
    snapshot::build::build_snapshot_from_projection,
    wal::{
        event::{WalEvent, WalEventType as E},
        fs_reader::WalFsReader,
        fs_writer::WalFsWriter,
        writer::WalWriter,
    },
};
use actionqueue_core::{
    ids::{RunId, TaskId},
    run::{RunInstance, RunState},
    subscription::{EventFilter, SubscriptionId},
    task::{
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};

fn lease_close_replay(released: bool) {
    // These identities, payloads and timestamps reproduce a fixed serialized
    // seven-event stream. The fixed digest was measured with aq-perf-1 0ab8196,
    // whose reducer still observed every unmatched subscription around each event.
    let task_id: TaskId = "32442bf3-8ea9-4755-a7fa-9fc7e66e0c9d".parse().unwrap();
    let run_id: RunId = "8e56f7e8-dc91-4f79-ad7b-9a18946cef72".parse().unwrap();
    let subscription_id: SubscriptionId = "a305a618-2a2c-4732-aa17-02aa490dd0d7".parse().unwrap();
    let task_spec = TaskSpec::new(
        task_id,
        TaskPayload::new(vec![]),
        RunPolicy::Once,
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let close = if released {
        E::LeaseReleased { run_id, owner: "w".into(), expiry: 100, timestamp: 100 }
    } else {
        E::LeaseExpired { run_id, owner: "w".into(), expiry: 100, timestamp: 100 }
    };
    let events: Vec<_> = [
        E::TaskCreated { task_spec, timestamp: 1 },
        E::RunCreated {
            run_instance: RunInstance::new_scheduled_with_id(run_id, task_id, 1, 1).unwrap(),
        },
        E::RunStateChanged {
            run_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 2,
        },
        E::LeaseAcquired { run_id, owner: "w".into(), expiry: 100, timestamp: 3 },
        E::RunStateChanged {
            run_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 3,
        },
        E::SubscriptionCreated {
            subscription_id,
            task_id,
            filter: EventFilter::RunStateChanged { task_id, state: RunState::Ready },
            timestamp: 4,
        },
        close,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, event)| WalEvent::new(i as u64 + 1, event))
    .collect();

    let mut live = ReplayReducer::new();
    for event in &events[..6] {
        live.apply(event).unwrap();
    }
    assert_eq!(live.get_subscription(&subscription_id).unwrap().matched_sequence, None);
    let prefix = live.clone();
    let snapshot = build_snapshot_from_projection(&prefix, 4).unwrap();
    let mut hydrated = ReplayReducer::from_projection_image(ProjectionImageV8(snapshot)).unwrap();
    live.apply(&events[6]).unwrap();
    hydrated.apply(&events[6]).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lease.wal");
    let mut writer = WalFsWriter::new_raw_for_test(path.clone()).unwrap();
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();
    let mut driver = ReplayDriver::new(WalFsReader::new(path).unwrap(), ReplayReducer::new());
    driver.run().unwrap();
    let replayed = driver.into_reducer();

    for projection in [&live, &hydrated, &replayed] {
        assert_eq!(projection.get_run_state(&run_id), Some(&RunState::Ready));
        assert_eq!(
            projection.get_subscription(&subscription_id).unwrap().matched_sequence,
            Some(7)
        );
        assert_eq!(projection.matched_subscriptions().collect::<Vec<_>>(), vec![subscription_id]);
        assert_eq!(
            projection.projection_digest().unwrap().hex,
            "096c6677f5a6679a14b419622d111a4c91c09b70f5b60fbb94614c20f5b023ba"
        );
    }
    assert_eq!(prefix.get_run_state(&run_id), Some(&RunState::Leased));
    assert_eq!(prefix.get_subscription(&subscription_id).unwrap().matched_sequence, None);
}

#[test]
fn lease_expiry_matches_ready_subscription_with_baseline_digest() {
    lease_close_replay(false);
}

#[test]
fn lease_release_matches_ready_subscription_with_baseline_digest() {
    lease_close_replay(true);
}
