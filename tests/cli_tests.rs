use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));
}

#[test]
fn test_cli_version() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("capsule"));
}

#[test]
fn test_cli_invalid_command() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("invalid-command")
        .assert()
        .failure()
        .stderr(predicate::str::contains("error:"));
}

#[test]
fn test_setup_command_exists() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("setup")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Setup and download engines"));
}

#[test]
fn test_ps_command_exists() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("ps")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("List running capsules"));
}

#[test]
fn test_close_command_exists() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("close")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Stop a running capsule"));
}

#[test]
fn test_logs_command_exists() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("logs")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Show logs of a running capsule"));
}

#[test]
fn test_open_command_requires_path() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("open")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));
}

#[test]
fn test_pack_command_with_init_flag() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("pack")
        .arg("--init")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Initialize capsule.toml interactively",
        ));
}

#[test]
fn test_pack_command_with_key_flag() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("pack")
        .arg("--key")
        .arg("/path/to/key")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Path to signing key"));
}

#[test]
fn test_json_flag_exists() {
    let mut cmd = Command::cargo_bin("capsule").unwrap();
    cmd.arg("--json")
        .arg("ps")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Emit machine-readable JSON output",
        ));
}
