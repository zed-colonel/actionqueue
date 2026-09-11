#![allow(dead_code, unused_imports)]
include!("wait_support.rs");

#[test]
#[ignore = "subprocess kill helper"]
fn wait_crash_child() {
    let path = std::path::PathBuf::from(std::env::var("AQ_WAIT_CRASH_ROOT").unwrap());
    let stage: usize = std::env::var("AQ_WAIT_CRASH_STAGE").unwrap().parse().unwrap();
    let mut a = s::open(&path);
    let r = running(&mut a, 1, None, false);
    let w: WaitId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse().unwrap();
    if stage == 0 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
    }
    establish_wait(&mut a, r, spec(w, None));
    if stage == 1 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
    }
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    if stage == 2 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
    }
    reconcile(&mut a, 30).unwrap();
    if stage == 3 {
        let r2 = running(&mut a, 2, None, false);
        establish_wait(&mut a, r2, spec(WaitId::new(), None));
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
        reconcile(&mut a, 31).unwrap();
    }
    panic!("crash boundary missed");
}
#[test]
fn subprocess_kill_after_establishment_signal_resolution_and_during_fanout() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    for stage in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "wait_crash_child", "--ignored", "--nocapture"])
            .env("AQ_WAIT_CRASH_ROOT", &path)
            .env("AQ_WAIT_CRASH_STAGE", stage.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if line.unwrap().contains("AQ_CRASH_BOUNDARY") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        let ready = rx.recv_timeout(std::time::Duration::from_secs(15));
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        ready.unwrap();
        let mut a = s::reopen(&path);
        if stage == 0 {
            let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
        }
        reconcile(&mut a, 30).unwrap();
        assert_invariants(&a);
        assert_eq!(a.projection().waits().active_count(), 0);
        assert_eq!(a.projection().waits().records().count(), if stage == 3 { 2 } else { 1 });
        let before = seq(&a);
        assert_eq!(reconcile(&mut a, 30).unwrap(), 0);
        assert_eq!(before, seq(&a));
    }
}
