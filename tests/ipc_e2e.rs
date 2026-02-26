//! IPC E2E tests
//!
//! Integration tests for `ato ipc` subcommands.
//! Tests are CLI-only via `assert_cmd` since ato-cli is a binary crate.
//!
//! Test categories:
//! - 13d.1: `ato ipc status / start / stop` CLI round-trip
//! - 13d.4: Error cases (missing toml, not-found service)
//!
//! IPC validation rules (IPC-001 through IPC-007) and JSON-RPC/schema
//! tests are in unit tests inside `src/ipc/validate.rs`, `src/ipc/jsonrpc.rs`,
//! and `src/ipc/schema.rs` (97 tests total, run via `cargo test --bin ato`).

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;
use tempfile::TempDir;

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn capsule() -> Command {
    Command::cargo_bin("ato").expect("capsule binary not found")
}

fn fixture_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.1: `ato ipc` Help / Discovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_status_help() {
    capsule()
        .args(["ipc", "status", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Show status of running IPC services",
        ));
}

#[test]
fn ipc_start_help() {
    capsule()
        .args(["ipc", "start", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Start an IPC service"));
}

#[test]
fn ipc_stop_help() {
    capsule()
        .args(["ipc", "stop", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Stop a running IPC service"));
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.1: `ato ipc status` (empty)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_status_shows_no_services() {
    capsule()
        .args(["ipc", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No IPC services running"));
}

#[test]
fn ipc_status_json_returns_empty_array() {
    capsule()
        .args(["ipc", "status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("[]"));
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.1: `ato ipc start` — Registration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_start_registers_service() {
    capsule()
        .args(["ipc", "start"])
        .arg(fixture_dir("ipc_service"))
        .assert()
        .success()
        .stdout(
            predicate::str::contains("registered").or(predicate::str::contains("already running")),
        );
}

#[test]
fn ipc_start_json_output_is_valid() {
    let output = capsule()
        .args(["ipc", "start", "--json"])
        .arg(fixture_dir("ipc_service"))
        .output()
        .expect("run ato ipc start");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).expect("JSON output");
    assert!(
        json.get("status").is_some() || json.get("error").is_some(),
        "Expected 'status' or 'error' key, got: {}",
        stdout,
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.1: `ato ipc stop` — Deregistration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_stop_reports_not_found() {
    capsule()
        .args(["ipc", "stop", "--name", "nonexistent-svc-e2e-test"])
        .assert()
        .success()
        .stderr(predicate::str::contains("not running").or(predicate::str::contains("not_found")));
}

#[test]
fn ipc_stop_json_reports_not_found() {
    let output = capsule()
        .args([
            "ipc",
            "stop",
            "--name",
            "nonexistent-svc-e2e-test",
            "--json",
        ])
        .output()
        .expect("run ato ipc stop");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).expect("JSON output");
    assert_eq!(json["error"], "not_found");
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.1: Start → Stop Round-trip
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_start_then_stop_roundtrip() {
    // Start
    capsule()
        .args(["ipc", "start", "--json"])
        .arg(fixture_dir("ipc_service"))
        .assert()
        .success();

    // Stop
    capsule()
        .args(["ipc", "stop", "--name", "test-svc", "--json"])
        .assert()
        .success();
}

// ═══════════════════════════════════════════════════════════════════════════
// 13d.4: Error Cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ipc_start_fails_without_capsule_toml() {
    let temp = TempDir::new().unwrap();

    capsule()
        .args(["ipc", "start"])
        .arg(temp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("capsule.toml not found"));
}

#[test]
fn ipc_start_with_no_ipc_section_uses_fallback_name() {
    let temp = TempDir::new().unwrap();
    std::fs::write(
        temp.path().join("capsule.toml"),
        r#"
schema_version = "1.0"
name = "no-ipc"
version = "0.1.0"
type = "app"

[execution]
runtime = "source"
entrypoint = "echo hello"
"#,
    )
    .unwrap();

    capsule()
        .args(["ipc", "start"])
        .arg(temp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("registered").or(predicate::str::contains("no-ipc")));
}
