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
fn stats_json_success_returns_deterministic_shape() {
    let data_dir = unique_data_dir("smoke-stats-json");

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
