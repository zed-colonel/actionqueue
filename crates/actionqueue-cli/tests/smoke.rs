//! Phase 7 CLI smoke tests.

use std::path::PathBuf;
use std::process::Command;

/// Returns command invocation for `actionqueue-cli` binary under test.
fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_actionqueue-cli"))
}

fn unique_data_dir(label: &str) -> PathBuf {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let pid = std::process::id();
    PathBuf::from(format!("target/tmp/{label}-{pid}-{now}"))
}

#[test]
fn daemon_command_json_success_uses_stdout_and_zero_exit() {
    let data_dir = unique_data_dir("smoke-daemon-json");

    let output = cli()
        .args([
            "daemon",
            "--data-dir",
            data_dir.to_str().expect("data dir path should be UTF-8"),
            "--bind",
            "127.0.0.1:9898",
            "--json",
        ])
        .output()
        .expect("daemon command should execute");

    assert!(output.status.success(), "daemon command should exit with code 0");

    let stdout = String::from_utf8(output.stdout).expect("stdout should be valid UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be valid UTF-8");

    assert!(stderr.trim().is_empty(), "success path must keep stderr empty");

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("daemon --json output should be valid JSON");
    assert_eq!(parsed["command"], "daemon");
    assert_eq!(parsed["bind_address"], "127.0.0.1:9898");
    assert!(parsed["ready"].is_boolean());
}

#[test]
fn submit_once_json_success_creates_one_run() {
    let data_dir = unique_data_dir("smoke-submit-once");

    let output = cli()
        .args([
            "submit",
            "--data-dir",
            data_dir.to_str().expect("data dir path should be UTF-8"),
            "--task-id",
            "123e4567-e89b-12d3-a456-426614174000",
            "--run-policy",
            "once",
            "--json",
        ])
        .output()
        .expect("submit command should execute");

    assert!(output.status.success(), "submit once should succeed");

    let stdout = String::from_utf8(output.stdout).expect("stdout should be valid UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be valid UTF-8");
    assert!(stderr.trim().is_empty(), "success path must keep stderr empty");

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("submit --json output should be valid JSON");
    assert_eq!(parsed["command"], "submit");
    assert_eq!(parsed["run_policy"], "once");
    assert_eq!(parsed["runs_created"], 1);
}

#[test]
fn submit_repeat_json_success_creates_repeat_runs() {
    let data_dir = unique_data_dir("smoke-submit-repeat");

    let output = cli()
        .args([
            "submit",
            "--data-dir",
            data_dir.to_str().expect("data dir path should be UTF-8"),
            "--task-id",
            "223e4567-e89b-12d3-a456-426614174000",
            "--run-policy",
            "repeat:3:60",
            "--json",
        ])
        .output()
        .expect("submit repeat command should execute");

    assert!(output.status.success(), "submit repeat should succeed");

    let stdout = String::from_utf8(output.stdout).expect("stdout should be valid UTF-8");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("submit repeat JSON should parse");
    assert_eq!(parsed["command"], "submit");
    assert_eq!(parsed["run_policy"], "repeat:3:60");
    assert_eq!(parsed["runs_created"], 3);
}

#[test]
fn stats_formats_return_consistent_deterministic_fields() {
    let data_dir = unique_data_dir("smoke-stats-json");
    drop(
        actionqueue_storage::store::open_store(
            &data_dir,
            actionqueue_storage::store::OpenOptions::Initialize { features: vec![] },
        )
        .unwrap(),
    );

    let output = cli()
        .args([
            "stats",
            "--data-dir",
            data_dir.to_str().expect("data dir path should be UTF-8"),
            "--format",
            "json",
        ])
        .output()
        .expect("stats command should execute");

    assert!(output.status.success(), "stats json should succeed");
    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stats JSON should parse deterministically");
    assert_eq!(parsed["command"], "stats");
    assert!(parsed["summary"]["total_tasks"].is_u64());
    assert!(parsed["summary"]["total_runs"].is_u64());
    assert!(parsed["summary"]["latest_sequence"].is_u64());
    assert!(parsed["summary"]["runs_by_state"].is_object());

    let text_output = cli()
        .args(["stats", "--data-dir", data_dir.to_str().unwrap(), "--format", "text"])
        .output()
        .expect("stats text command should execute");
    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).unwrap();
    let mut expected =
        vec!["command=stats".to_string(), format!("data_dir={}", data_dir.display())];
    for key in ["total_tasks", "total_runs", "latest_sequence"] {
        expected.push(format!("{key}={}", parsed["summary"][key]));
    }
    for state in [
        "scheduled",
        "ready",
        "leased",
        "running",
        "retry_wait",
        "suspended",
        "awaiting",
        "completed",
        "failed",
        "canceled",
    ] {
        expected.push(format!("runs_{state}={}", parsed["summary"]["runs_by_state"][state]));
    }
    expected.push(format!("attempts_total={}", parsed["summary"]["attempts_total"]));
    assert_eq!(text.lines().collect::<Vec<_>>(), expected);
}

#[test]
fn invalid_usage_emits_structured_stderr_and_non_zero_exit() {
    let output = cli()
        .args(["submit", "--run-policy", "once"])
        .output()
        .expect("invalid usage invocation should execute");

    assert!(!output.status.success(), "invalid usage should be non-zero");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    let payload: serde_json::Value =
        serde_json::from_str(&stderr).expect("stderr payload should be JSON");

    assert_eq!(payload["error_kind"], "validation");
    assert_eq!(payload["error_code"], "input_validation_failed");
    assert!(payload["message"].as_str().is_some());
}

#[test]
fn storage_commands_verify_roundtrip_and_never_initialize_inspection() {
    let base = unique_data_dir("smoke-storage");
    let source = base.join("source");
    let backup = base.join("backup");
    let dest = base.join("restored");
    let refused = cli()
        .args(["storage", "inspect", "--data-dir", source.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(!source.exists());
    let submitted = cli()
        .args([
            "submit",
            "--data-dir",
            source.to_str().unwrap(),
            "--task-id",
            "123e4567-e89b-12d3-a456-426614174099",
            "--run-policy",
            "once",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));
    let inspect = cli()
        .args(["storage", "inspect", "--data-dir", source.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(inspect.status.success());
    let before: serde_json::Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(before["task_count"], 1);
    let copied = cli()
        .args([
            "storage",
            "backup",
            "--data-dir",
            source.to_str().unwrap(),
            "--output",
            backup.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(copied.status.success(), "{}", String::from_utf8_lossy(&copied.stderr));
    let restored = cli()
        .args([
            "storage",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--data-dir",
            dest.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(restored.status.success(), "{}", String::from_utf8_lossy(&restored.stderr));
    let after = cli()
        .args(["storage", "inspect", "--data-dir", dest.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    let after: serde_json::Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after["projection_digest"], before["projection_digest"]);
    assert_eq!(after["manifest"]["store_id"], before["manifest"]["store_id"]);
    let refused = cli()
        .args([
            "storage",
            "restore",
            "--input",
            backup.to_str().unwrap(),
            "--data-dir",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    std::fs::remove_dir_all(base).unwrap();
}

#[cfg(unix)]
#[test]
fn restore_rejects_fifo_descriptor_and_inventory_without_blocking() {
    use actionqueue_storage::{
        recovery::bootstrap::recover_read_only,
        snapshot::{
            build::build_snapshot_from_projection,
            writer::{SnapshotFsWriter, SnapshotWriter},
        },
        store::{backup_store, open_store, OpenOptions},
        wal::repair::RepairPolicy,
    };
    use std::{
        fs,
        os::unix::fs::FileTypeExt,
        process::Stdio,
        time::{Duration, Instant},
    };
    let base = unique_data_dir("smoke-restore-fifo");
    let source = base.join("source");
    let backup = base.join("backup");
    let dest = base.join("restored");
    let session = open_store(&source, OpenOptions::Initialize { features: vec![] }).unwrap();
    let projection = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut writer = SnapshotFsWriter::new(&session).unwrap();
    writer.write(&build_snapshot_from_projection(&projection, 0).unwrap()).unwrap();
    writer.close().unwrap();
    drop(session);
    backup_store(&source, &backup).unwrap();
    let names = [
        "backup.json",
        "manifest.json",
        "store.lock",
        "wal/actionqueue.wal",
        "snapshots/snapshot.bin",
    ];
    let originals: Vec<_> = names.iter().map(|name| fs::read(backup.join(name)).unwrap()).collect();
    for (index, name) in names.iter().enumerate() {
        let path = backup.join(name);
        fs::remove_file(&path).unwrap();
        assert!(Command::new("mkfifo").arg(&path).status().unwrap().success());
        let mut child = cli()
            .args(["storage", "restore", "--input"])
            .arg(&backup)
            .arg("--data-dir")
            .arg(&dest)
            .arg("--json")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "restore blocked on FIFO {name}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success(), "restore accepted FIFO {name}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("regular file"),
            "unexpected rejection for {name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!dest.exists());
        assert!(fs::symlink_metadata(&path).unwrap().file_type().is_fifo());
        for (other_index, other_name) in names.iter().enumerate() {
            if other_index != index {
                assert_eq!(fs::read(backup.join(other_name)).unwrap(), originals[other_index]);
            }
        }
        fs::remove_file(path).unwrap();
        fs::write(backup.join(name), &originals[index]).unwrap();
    }
    fs::remove_dir_all(base).unwrap();
}
