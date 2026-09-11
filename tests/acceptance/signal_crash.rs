mod signal_support;
use std::sync::Mutex;

use actionqueue_core::continuation::*;
use actionqueue_storage::{mutation::MutationAuthorityError, store::fault};
use signal_support::*;
static SERIAL: Mutex<()> = Mutex::new(());
const POINTS: &[&str] = &[
    "wal_before_append",
    "wal_partial_frame",
    "wal_before_sync",
    "authority_before_publish",
    "authority_after_publish",
];
#[test]
fn failed_admissions_fence_writer_and_authority_until_recovery() {
    let _guard = SERIAL.lock().unwrap();
    for point in POINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open(dir.path());
        admit(&mut a, 1, 42).unwrap();
        let before = a.projection().projection_digest().unwrap();
        fault::fail_once(point);
        assert!(admit(&mut a, 2, 43).is_err());
        assert!(a.recovery_required());
        if *point != "authority_after_publish" {
            assert_eq!(a.projection().projection_digest().unwrap(), before);
        }
        assert!(matches!(
            a.lookup_signal(&envelope(1, 900)),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        assert!(matches!(
            submit(&mut a, envelope(2, 900)),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        let (writer, projection) = a.into_parts();
        let a = Authority::new(writer, projection);
        assert!(a.recovery_required());
        drop(a);
        let mut a = reopen(dir.path());
        let committed = a.projection().signals().get_signal(None, &id(2)).is_some();
        if matches!(*point, "wal_before_append" | "wal_partial_frame") {
            assert!(!committed);
        }
        if matches!(*point, "authority_before_publish" | "authority_after_publish") {
            assert!(committed);
        }
        let outcome = admit(&mut a, 2, 900).unwrap();
        assert_eq!(matches!(outcome, AdmitSignalOutcome::AlreadyExists { .. }), committed);
        assert_eq!(outcome.sequence().get(), 2);
        assert_eq!(a.projection().latest_sequence(), 3);
        assert_eq!(sequences(&a, &filter(), 0), [1, 2]);
    }
}
#[test]
#[ignore = "subprocess crash helper"]
fn crash_child() {
    let root = std::path::PathBuf::from(std::env::var("AQ_SIGNAL_CRASH_ROOT").unwrap());
    let point = std::env::var("AQ_SIGNAL_CRASH_POINT").unwrap();
    let mut a = open(&root.join("store"));
    admit(&mut a, 1, 42).unwrap();
    fault::pause_once(&point);
    admit(&mut a, 2, 43).unwrap();
    panic!("crash boundary not reached");
}
#[test]
fn subprocess_kill_at_each_commit_boundary_preserves_signal_identity_and_order() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        time::Duration,
    };
    let _guard = SERIAL.lock().unwrap();
    for point in POINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--ignored", "--nocapture"])
            .env("AQ_SIGNAL_CRASH_ROOT", dir.path())
            .env("AQ_SIGNAL_CRASH_POINT", point)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if line.unwrap().contains("AQ_CRASH_BOUNDARY") {
                    tx.send(()).unwrap();
                    break;
                }
            }
        });
        let ready = rx.recv_timeout(Duration::from_secs(15));
        child.kill().unwrap();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        ready.unwrap();
        assert!(!status.success());
        let mut a = reopen(&dir.path().join("store"));
        let committed = a.projection().signals().get_signal(None, &id(2)).is_some();
        if matches!(*point, "wal_before_append" | "wal_partial_frame") {
            assert!(!committed);
        }
        if matches!(*point, "authority_before_publish" | "authority_after_publish") {
            assert!(committed);
        }
        let outcome = admit(&mut a, 2, 900).unwrap();
        assert_eq!(matches!(outcome, AdmitSignalOutcome::AlreadyExists { .. }), committed);
        assert_eq!(outcome.sequence().get(), 2);
        assert_eq!(
            a.projection().signals().get_signal(None, &id(2)).unwrap().envelope().received_at,
            if committed { 43 } else { 900 }
        );
    }
}
#[test]
fn uncertain_retirement_never_reactivates_after_reopen() {
    use actionqueue_core::{
        ids::SignalSequence, limits::SignalRetentionPolicy, time::clock::MockClock,
    };
    let _guard = SERIAL.lock().unwrap();
    for point in POINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open(dir.path());
        admit(&mut a, 1, 1).unwrap();
        admit(&mut a, 2, 1).unwrap();
        a.set_signal_retention_policy(SignalRetentionPolicy {
            minimum_age_secs: 0,
            minimum_sequence_window: 0,
        });
        fault::fail_once(point);
        assert!(actionqueue_runtime::signals::retire_signals(
            &mut a,
            vec![SignalSequence::new(1)],
            Default::default(),
            &MockClock::new(2)
        )
        .is_err());
        assert!(a.recovery_required());
        drop(a);
        let mut a = reopen(dir.path());
        let retired = a.signal_statistics().retired == 1;
        if matches!(*point, "authority_before_publish" | "authority_after_publish") {
            assert!(retired);
        }
        if matches!(*point, "wal_before_append" | "wal_partial_frame") {
            assert!(!retired);
        }
        admit(&mut a, 1, 1000).unwrap();
        assert_eq!(a.signal_statistics().retired, usize::from(retired));
        assert_eq!(sequences(&a, &filter(), 0), if retired { vec![2] } else { vec![1, 2] });
    }
}
