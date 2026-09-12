//! Phase 7 CLI smoke tests.

use std::path::PathBuf;
use std::process::Command;

/// Returns command invocation for `actionqueue-cli` binary under test.
fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_actionqueue"))
}

fn unique_data_dir(label: &str) -> PathBuf {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let pid = std::process::id();
    std::env::temp_dir().join(format!("{label}-{pid}-{now}"))
}

#[test]
fn storage_commands_verify_roundtrip_and_never_initialize_inspection() {
    let base = unique_data_dir("smoke-storage");
    let source = base.join("source");
    let backup = base.join("backup");
    let dest = base.join("restored");
    let refused = cli()
        .args(["store", "inspect", "--data-dir", source.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(!source.exists());
    let request_file = base.join("request.json");
    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(&request_file, serde_json::to_vec(&request()).unwrap()).unwrap();
    let submitted = cli()
        .args(["ensure-task", "--offline", "--data-dir"])
        .arg(&source)
        .arg("--file")
        .arg(&request_file)
        .arg("--json")
        .output()
        .unwrap();
    assert!(submitted.status.success(), "{}", String::from_utf8_lossy(&submitted.stderr));
    let inspect = cli()
        .args(["store", "inspect", "--data-dir", source.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(inspect.status.success());
    let before: serde_json::Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(before["task_count"], 1);
    let copied = cli()
        .args([
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
        .args(["store", "inspect", "--data-dir", dest.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    let after: serde_json::Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after["projection_digest"], before["projection_digest"]);
    assert_eq!(after["manifest"]["store_id"], before["manifest"]["store_id"]);
    let refused = cli()
        .args([
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
    use std::{
        fs,
        os::unix::fs::FileTypeExt,
        process::Stdio,
        time::{Duration, Instant},
    };

    use actionqueue_storage::{
        recovery::bootstrap::recover_read_only,
        snapshot::{
            build::build_snapshot_from_projection,
            writer::{SnapshotFsWriter, SnapshotWriter},
        },
        store::{backup_store, open_store, OpenOptions},
        wal::repair::RepairPolicy,
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
            .args(["restore", "--input"])
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

fn request() -> actionqueue_core::admission::EnsureTaskRequest {
    use actionqueue_core::{
        admission::EnsureTaskRequest,
        ids::TaskId,
        task::{
            constraints::TaskConstraints,
            metadata::TaskMetadata,
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    EnsureTaskRequest::for_task(
        TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"CLI_SECRET_PAYLOAD".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap(),
        vec![],
    )
    .unwrap()
}
#[test]
fn canonical_offline_admission_retry_conflict_and_redaction() {
    let base = unique_data_dir("aq12-offline");
    std::fs::create_dir_all(&base).unwrap();
    let store = base.join("store");
    let file = base.join("request.json");
    let q = request();
    std::fs::write(&file, serde_json::to_vec(&q).unwrap()).unwrap();
    for result in ["Created", "AlreadyExists"] {
        let out = cli()
            .args(["ensure-task", "--offline", "--data-dir"])
            .arg(&store)
            .arg("--file")
            .arg(&file)
            .arg("--json")
            .output()
            .unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert!(v[result].is_object());
    }
    let out = cli()
        .args(["task", "inspect", &q.task_spec().id().to_string(), "--offline", "--data-dir"])
        .arg(&store)
        .arg("--json")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(!String::from_utf8_lossy(&out.stdout).contains("CLI_SECRET_PAYLOAD"));
    let mut changed = q.task_spec().clone();
    changed.set_payload(actionqueue_core::task::task_spec::TaskPayload::new(
        b"DIFFERENT_SECRET".to_vec(),
    ));
    let changed = actionqueue_core::admission::EnsureTaskRequest::new(
        q.admission_key().clone(),
        changed,
        vec![],
        q.causal_context().clone(),
        None,
    )
    .unwrap();
    std::fs::write(&file, serde_json::to_vec(&changed).unwrap()).unwrap();
    let out = cli()
        .args(["ensure-task", "--offline", "--data-dir"])
        .arg(&store)
        .arg("--file")
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("conflict"));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("DIFFERENT_SECRET"));
    for old in ["submit", "stats", "storage"] {
        assert!(!cli().arg(old).output().unwrap().status.success());
    }
    std::fs::remove_dir_all(base).unwrap();
}
#[cfg(unix)]
#[test]
fn daemon_serves_authenticated_cli_requests_and_releases_store_on_sigterm() {
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
        time::{Duration, Instant},
    };
    let base = unique_data_dir("aq12-serve");
    std::fs::create_dir_all(&base).unwrap();
    let store = base.join("store");
    let auth = base.join("auth.json");
    let token = base.join("token");
    let q = request();
    let file = base.join("request.json");
    std::fs::write(&file, serde_json::to_vec(&q).unwrap()).unwrap();
    let secret = "0123456789abcdef0123456789abcdef";
    std::fs::write(&token, secret).unwrap();
    let attribution = actionqueue_core::causal::ControlMutationContext::new(
        actionqueue_core::bounded::OpaqueRef::new("operator").unwrap(),
    );
    std::fs::write(&auth,serde_json::to_vec(&serde_json::json!([{"token":secret,"actor_id":null,"scope":"SingleTenant","attribution":attribution}])).unwrap()).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap().to_string();
    let metrics_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_address = metrics_reservation.local_addr().unwrap().to_string();
    drop(reservation);
    drop(metrics_reservation);
    let mut child = cli()
        .args(["daemon", "--data-dir"])
        .arg(&store)
        .args([
            "--bind",
            &address,
            "--metrics-bind",
            &metrics_address,
            "--enable-control",
            "--auth-file",
        ])
        .arg(&auth)
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if let Ok(line) = line {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    if v["bind_address"].is_string() {
                        let _ = tx.send(v);
                        break;
                    }
                }
            }
        }
    });
    let startup = match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(v) => v,
        Err(e) => {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("startup failed {e}: {}", String::from_utf8_lossy(&output.stderr))
        }
    };
    let mut metrics = std::net::TcpStream::connect(&metrics_address).unwrap();
    metrics.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    use std::io::{Read, Write};
    metrics
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut metrics_text = String::new();
    metrics.read_to_string(&mut metrics_text).unwrap();
    assert!(metrics_text.contains("actionqueue_admission_total"));
    let url = format!("http://{}", startup["bind_address"].as_str().unwrap());
    let out = cli()
        .args(["ensure-task", "--daemon", &url, "--token-file"])
        .arg(&token)
        .arg("--file")
        .arg(&file)
        .arg("--json")
        .output()
        .unwrap();
    let denied = cli().args(["store", "inspect", "--data-dir"]).arg(&store).output().unwrap();
    assert!(Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap()
        .success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("shutdown timed out");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(!denied.status.success());
    let after =
        cli().args(["store", "inspect", "--data-dir"]).arg(&store).arg("--json").output().unwrap();
    assert!(after.status.success(), "{}", String::from_utf8_lossy(&after.stderr));
    assert!(!String::from_utf8_lossy(&out.stdout).contains(secret));
    std::fs::remove_dir_all(base).unwrap();
}
