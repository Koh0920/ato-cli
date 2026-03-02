use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary Commands:"))
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("install"))
        .stdout(predicate::str::contains("init"))
        .stdout(predicate::str::contains("build"))
        .stdout(predicate::str::contains("search"));
}

#[test]
fn test_cli_version() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("ato"));
}

#[test]
fn test_cli_invalid_command() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("invalid-command")
        .assert()
        .failure()
        .stderr(predicate::str::contains("error:"));
}

#[test]
fn test_help_hides_legacy_commands() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(" open ").not())
        .stdout(predicate::str::contains(" pack ").not())
        .stdout(predicate::str::contains(" close ").not())
        .stdout(predicate::str::contains(" auth ").not())
        .stdout(predicate::str::contains(" setup ").not());
}

#[test]
fn test_ps_command_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("ps")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("List running capsules"));
}

#[test]
fn test_stop_command_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("stop")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Stop a running capsule"));
}

#[test]
fn test_logs_command_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("logs")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Show logs of a running capsule"));
}

#[test]
fn test_login_help_shows_optional_token() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["login", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--token <TOKEN>"))
        .stdout(predicate::str::contains("[OPTIONS]").or(predicate::str::contains("Options:")));
}

#[test]
fn test_search_help_uses_store_api_default() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["search", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--tag <TAGS>"))
        .stdout(predicate::str::contains(
            "Registry URL (default: https://api.ato.run)",
        ));
}

#[test]
fn test_run_command_accepts_default_path() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("run")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").not());
}

#[test]
fn test_run_help_shows_yes_flag() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["run", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--skill <SKILL>"))
        .stdout(predicate::str::contains("--yes"))
        .stdout(predicate::str::contains("--registry"))
        .stdout(predicate::str::contains("default: https://api.ato.run"));
}

#[test]
fn test_run_skill_conflicts_with_from_skill() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["run", "--skill", "demo", "--from-skill", "/tmp/SKILL.md"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn test_build_command_with_init_flag() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("build")
        .arg("--init")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Initialize capsule.toml interactively",
        ));
}

#[test]
fn test_build_command_with_key_flag() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("build")
        .arg("--key")
        .arg("/path/to/key")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Path to signing key"));
}

#[test]
fn test_json_flag_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.arg("--json")
        .arg("ps")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Emit machine-readable JSON output",
        ));
}

#[test]
fn test_publish_command_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["publish", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Publish capsule"))
        .stdout(predicate::str::contains("--registry <REGISTRY>"))
        .stdout(predicate::str::contains("--ci"))
        .stdout(predicate::str::contains("--dry-run"))
        .stdout(predicate::str::contains("--no-tui"));
}

#[test]
fn test_registry_command_is_public() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["registry", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("serve"));
}

#[test]
fn test_registry_serve_help_has_auth_token() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["registry", "serve", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--auth-token <AUTH_TOKEN>"));
}

#[test]
fn test_key_command_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["key", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Manage signing keys"));
}

#[test]
fn test_config_engine_install_exists() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["config", "engine", "install", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Download and install an engine"));
}

#[test]
fn test_legacy_open_still_available() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["open", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: ato open"))
        .stdout(predicate::str::contains("--yes"))
        .stdout(predicate::str::contains("--registry"));
}

#[test]
fn test_legacy_setup_still_available() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["setup", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Engine name to install"));
}

#[test]
fn test_build_invalid_manifest_outputs_single_json_error() {
    let tmp = tempdir().unwrap();
    std::fs::write(tmp.path().join("capsule.toml"), "name =\n").unwrap();

    let output = Command::cargo_bin("ato")
        .unwrap()
        .args(["--json", "build", tmp.path().to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(lines.len(), 1, "unexpected stdout: {}", stdout);

    let value: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(value["schema_version"], "1");
    assert_eq!(value["type"], "error");
    assert_eq!(value["code"], "E001");
}

#[test]
fn test_publish_json_error_uses_diagnostic_envelope() {
    let output = Command::cargo_bin("ato")
        .unwrap()
        .args(["publish", "--json"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(value["ok"], false);
    assert_eq!(value["code"], "CI_ONLY_PUBLISH");
}

#[test]
fn test_source_rebuild_help_uses_ref_flag() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args(["source", "rebuild", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--ref <REFERENCE>"));
}

#[test]
fn test_source_rebuild_accepts_reference_alias() {
    let mut cmd = Command::cargo_bin("ato").unwrap();
    cmd.args([
        "source",
        "rebuild",
        "--source-id",
        "src_123",
        "--reference",
        "main",
        "--registry",
        "http://127.0.0.1:9",
    ])
    .assert()
    .failure()
    .stderr(
        predicate::str::contains("Failed to preflight source operation")
            .or(predicate::str::contains("Source operation requires authentication")),
    );
}
