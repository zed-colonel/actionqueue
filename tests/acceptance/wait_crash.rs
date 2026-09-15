#![allow(dead_code, unused_imports)]
include!("wait_support.rs");
#[path = "../conformance/harness/process.rs"]
mod process;

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
    for stage in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "wait_crash_child", "--ignored", "--nocapture"])
            .env("AQ_WAIT_CRASH_ROOT", &path)
            .env("AQ_WAIT_CRASH_STAGE", stage.to_string());
        process::kill_at_prefix(cmd, "AQ_CRASH_BOUNDARY authority_before_publish ");
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
