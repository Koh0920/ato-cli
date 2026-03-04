use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use capsule_core::execution_plan::derive::compile_execution_plan;
use capsule_core::router::ExecutionProfile;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn ato_cmd() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ato"))
}

fn fixture_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    if !dst.exists() {
        fs::create_dir_all(dst).expect("failed to create destination fixture directory");
    }

    for entry in fs::read_dir(src).expect("failed to read source fixture directory") {
        let entry = entry.expect("failed to read fixture entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());

        if from.is_dir() {
            copy_dir_recursive(&from, &to);
        } else {
            fs::copy(&from, &to).expect("failed to copy fixture file");
        }
    }
}

fn write_capsule_lock(workspace_root: &Path, fixture_name: &str) {
    let manifest_path = workspace_root.join("capsule.toml");
    let manifest_text = fs::read_to_string(&manifest_path).expect("failed to read manifest");

    let mut hasher = Sha256::new();
    hasher.update(manifest_text.as_bytes());
    let hash = format!("sha256:{:x}", hasher.finalize());

    let mut lock_content = format!(
        "version = \"1\"\n\n[meta]\ncreated_at = \"2026-02-23T00:00:00Z\"\nmanifest_hash = \"{}\"\n\n[targets]\n",
        hash
    );

    if fixture_name == "future-glibc-capsule" {
        lock_content.push_str(
            "\n[targets.\"x86_64-unknown-linux-gnu\".constraints]\nglibc = \"glibc-999.0\"\n",
        );
    }
    if fixture_name == "glibc-mismatch-capsule" {
        lock_content.push_str(
            "\n[targets.\"x86_64-unknown-linux-gnu\".constraints]\nglibc = \"glibc-2.17\"\n",
        );
    }

    fs::write(workspace_root.join("capsule.lock"), lock_content)
        .expect("failed to write capsule.lock");
}

fn prepare_fixture_workspace(fixture_name: &str) -> (TempDir, PathBuf) {
    let source = fixture_dir(fixture_name);
    let temp = TempDir::new().expect("failed to create fixture workspace");
    let workspace_root = temp.path().join(fixture_name);
    copy_dir_recursive(&source, &workspace_root);
    write_capsule_lock(&workspace_root, fixture_name);

    if fixture_name == "glibc-mismatch-capsule" {
        write_mock_elf_with_dt_verneed(&workspace_root.join("app.bin"), "GLIBC_2.99");
    }

    (temp, workspace_root)
}

fn write_mock_elf_with_dt_verneed(path: &Path, required_glibc: &str) {
    const ELF_HEADER_SIZE: usize = 64;
    const PROGRAM_HEADER_SIZE: usize = 56;
    const PROGRAM_HEADERS: usize = 2;
    const DYNAMIC_OFFSET: usize = 0x100;
    const DYNAMIC_SIZE: usize = 32;
    const STRING_OFFSET: usize = 0x200;
    const DT_VERNEED: u64 = 0x6fff_fffe;
    const FILE_SIZE: usize = 0x280;

    let mut bytes = vec![0u8; FILE_SIZE];
    let file_size_u64 = FILE_SIZE as u64;

    bytes[0] = 0x7f;
    bytes[1] = b'E';
    bytes[2] = b'L';
    bytes[3] = b'F';
    bytes[4] = 2;
    bytes[5] = 1;
    bytes[6] = 1;

    bytes[16..18].copy_from_slice(&2u16.to_le_bytes());
    bytes[18..20].copy_from_slice(&62u16.to_le_bytes());
    bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
    bytes[32..40].copy_from_slice(&(ELF_HEADER_SIZE as u64).to_le_bytes());
    bytes[40..48].copy_from_slice(&0u64.to_le_bytes());
    bytes[48..52].copy_from_slice(&0u32.to_le_bytes());
    bytes[52..54].copy_from_slice(&(ELF_HEADER_SIZE as u16).to_le_bytes());
    bytes[54..56].copy_from_slice(&(PROGRAM_HEADER_SIZE as u16).to_le_bytes());
    bytes[56..58].copy_from_slice(&(PROGRAM_HEADERS as u16).to_le_bytes());

    let ph0 = ELF_HEADER_SIZE;
    bytes[ph0..ph0 + 4].copy_from_slice(&2u32.to_le_bytes());
    bytes[ph0 + 4..ph0 + 8].copy_from_slice(&0u32.to_le_bytes());
    bytes[ph0 + 8..ph0 + 16].copy_from_slice(&(DYNAMIC_OFFSET as u64).to_le_bytes());
    bytes[ph0 + 16..ph0 + 24].copy_from_slice(&(DYNAMIC_OFFSET as u64).to_le_bytes());
    bytes[ph0 + 24..ph0 + 32].copy_from_slice(&(DYNAMIC_OFFSET as u64).to_le_bytes());
    bytes[ph0 + 32..ph0 + 40].copy_from_slice(&(DYNAMIC_SIZE as u64).to_le_bytes());
    bytes[ph0 + 40..ph0 + 48].copy_from_slice(&(DYNAMIC_SIZE as u64).to_le_bytes());
    bytes[ph0 + 48..ph0 + 56].copy_from_slice(&8u64.to_le_bytes());

    let ph1 = ELF_HEADER_SIZE + PROGRAM_HEADER_SIZE;
    bytes[ph1..ph1 + 4].copy_from_slice(&1u32.to_le_bytes());
    bytes[ph1 + 4..ph1 + 8].copy_from_slice(&5u32.to_le_bytes());
    bytes[ph1 + 8..ph1 + 16].copy_from_slice(&0u64.to_le_bytes());
    bytes[ph1 + 16..ph1 + 24].copy_from_slice(&0u64.to_le_bytes());
    bytes[ph1 + 24..ph1 + 32].copy_from_slice(&0u64.to_le_bytes());
    bytes[ph1 + 32..ph1 + 40].copy_from_slice(&file_size_u64.to_le_bytes());
    bytes[ph1 + 40..ph1 + 48].copy_from_slice(&file_size_u64.to_le_bytes());
    bytes[ph1 + 48..ph1 + 56].copy_from_slice(&0x1000u64.to_le_bytes());

    bytes[DYNAMIC_OFFSET..DYNAMIC_OFFSET + 8].copy_from_slice(&DT_VERNEED.to_le_bytes());
    bytes[DYNAMIC_OFFSET + 8..DYNAMIC_OFFSET + 16]
        .copy_from_slice(&(STRING_OFFSET as u64).to_le_bytes());

    bytes[DYNAMIC_OFFSET + 16..DYNAMIC_OFFSET + 24].copy_from_slice(&0u64.to_le_bytes());
    bytes[DYNAMIC_OFFSET + 24..DYNAMIC_OFFSET + 32].copy_from_slice(&0u64.to_le_bytes());

    let marker = required_glibc.as_bytes();
    let end = STRING_OFFSET + marker.len();
    bytes[STRING_OFFSET..end].copy_from_slice(marker);
    bytes[end] = 0;

    fs::write(path, &bytes).expect("failed to write mock ELF fixture");
}

fn prepare_consent_home(fixture_root: &Path) -> TempDir {
    let home = TempDir::new().expect("failed to create temporary HOME");
    let consent_dir = home.path().join(".ato").join("consent");
    fs::create_dir_all(&consent_dir).expect("failed to create consent dir");

    let manifest_path = fixture_root.join("capsule.toml");
    let compiled = compile_execution_plan(&manifest_path, ExecutionProfile::Dev, None)
        .expect("failed to compile execution plan for fixture");
    let plan = compiled.execution_plan;

    let record = serde_json::json!({
        "scoped_id": plan.consent.key.scoped_id,
        "version": plan.consent.key.version,
        "target_label": plan.consent.key.target_label,
        "policy_segment_hash": plan.consent.policy_segment_hash,
        "provisioning_policy_hash": plan.consent.provisioning_policy_hash,
        "approved_at": "2026-02-23T00:00:00Z"
    });

    fs::write(
        consent_dir.join("executionplan_v1.jsonl"),
        format!("{}\n", record),
    )
    .expect("failed to seed consent store");

    home
}

fn run_with_seeded_consent(
    fixture_name: &str,
    args: &[&str],
    extra_envs: &[(&str, &str)],
) -> Output {
    let (_workspace, fixture) = prepare_fixture_workspace(fixture_name);
    let home = prepare_consent_home(&fixture);

    let mut cmd = ato_cmd();
    cmd.arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());

    for arg in args {
        cmd.arg(arg);
    }
    for (key, value) in extra_envs {
        cmd.env(key, value);
    }

    cmd.output().expect("failed to execute ato")
}

fn run_without_seeded_consent(
    fixture_name: &str,
    args: &[&str],
    extra_envs: &[(&str, &str)],
) -> Output {
    let (_workspace, fixture) = prepare_fixture_workspace(fixture_name);
    let home = TempDir::new().expect("failed to create temporary HOME");

    let mut cmd = ato_cmd();
    cmd.arg("run")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());

    for arg in args {
        cmd.arg(arg);
    }
    for (key, value) in extra_envs {
        cmd.env(key, value);
    }

    cmd.output().expect("failed to execute ato")
}

fn find_built_capsule_path(workspace_root: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();

    let capsule_dir = workspace_root.join(".ato");
    if capsule_dir.is_dir() {
        found.extend(
            fs::read_dir(&capsule_dir)
                .expect("failed to read .ato directory")
                .filter_map(|entry| entry.ok().map(|v| v.path()))
                .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("capsule")),
        );
    }

    found.extend(
        fs::read_dir(workspace_root)
            .expect("failed to read workspace root")
            .filter_map(|entry| entry.ok().map(|v| v.path()))
            .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("capsule")),
    );

    found.sort();
    found
        .into_iter()
        .next()
        .expect("built capsule archive not found")
}

fn resolve_test_nacelle_path() -> PathBuf {
    if let Ok(path) = std::env::var("NACELLE_PATH") {
        let nacelle = PathBuf::from(path);
        if nacelle.exists() {
            return nacelle;
        }
    }

    let candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../nacelle/target/debug/nacelle")
        .canonicalize()
        .expect("failed to resolve default nacelle path for tests");
    assert!(
        candidate.exists(),
        "nacelle binary not found for tests: {}",
        candidate.display()
    );
    candidate
}

fn tamper_lock_manifest_hash(workspace_root: &Path) {
    let lock_path = workspace_root.join("capsule.lock");
    let raw = fs::read_to_string(&lock_path).expect("failed to read capsule.lock");
    let tampered = raw.replace(
        "manifest_hash = \"sha256:",
        "manifest_hash = \"sha256:deadbeef",
    );
    fs::write(&lock_path, tampered).expect("failed to tamper capsule.lock");
}

fn add_egress_allow_host(workspace_root: &Path, host: &str) {
    let manifest_path = workspace_root.join("capsule.toml");
    let raw = fs::read_to_string(&manifest_path).expect("failed to read capsule.toml");

    let marker = "egress_allow = [";
    let start = raw
        .find(marker)
        .expect("egress_allow declaration not found in fixture manifest");
    let list_start = start + marker.len();
    let end_rel = raw[list_start..]
        .find(']')
        .expect("egress_allow closing bracket not found");
    let list_end = list_start + end_rel;

    let current = raw[list_start..list_end].trim();
    let quoted_host = format!("\"{}\"", host);
    if current.contains(&quoted_host) {
        return;
    }

    let next_list = if current.is_empty() {
        quoted_host
    } else {
        format!("{}, {}", current, quoted_host)
    };

    let mut updated = String::new();
    updated.push_str(&raw[..list_start]);
    updated.push_str(&next_list);
    updated.push_str(&raw[list_end..]);

    fs::write(&manifest_path, updated).expect("failed to update egress_allow list");
}

#[cfg(unix)]
fn write_mock_nacelle_without_sandbox(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
set -eu

if [ "${1:-}" = "internal" ] && [ "${2:-}" = "--input" ] && [ "${3:-}" = "-" ] && [ "${4:-}" = "features" ]; then
    while IFS= read -r _line; do :; done || true
  printf '%s\n' '{"data":{"capabilities":{"sandbox":[]}}}'
  exit 0
fi

echo "unsupported invocation" >&2
exit 2
"#;
    fs::write(path, script).expect("failed to write mock nacelle script");
    let mut perms = fs::metadata(path)
        .expect("failed to stat mock nacelle script")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("failed to chmod mock nacelle script");
}

#[cfg(unix)]
fn write_mock_uv(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  echo "uv 0.0.0-test"
  exit 0
fi
exit 0
"#;
    fs::write(path, script).expect("failed to write mock uv script");
    let mut perms = fs::metadata(path)
        .expect("failed to stat mock uv script")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("failed to chmod mock uv script");
}

fn spawn_redirect_server(location: &str) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind redirect server");
    let port = listener
        .local_addr()
        .expect("failed to resolve redirect server addr")
        .port();

    let location = location.to_string();
    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let started = Instant::now();
            let mut req = Vec::new();
            let mut buf = [0u8; 1024];

            while started.elapsed() < Duration::from_secs(2) {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        req.extend_from_slice(&buf[..n]);
                        if req.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::WouldBlock
                            || err.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }

            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                location
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    (port, handle)
}

fn spawn_plain_http_server(body: &str) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind plain http server");
    let port = listener
        .local_addr()
        .expect("failed to resolve plain http server addr")
        .port();
    let payload = body.to_string();

    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let started = Instant::now();
            let mut req = Vec::new();
            let mut buf = [0u8; 1024];

            while started.elapsed() < Duration::from_secs(2) {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        req.extend_from_slice(&buf[..n]);
                        if req.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::WouldBlock
                            || err.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                payload.len(),
                payload
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    (port, handle)
}

fn extract_policy_violation_target(stderr: &str) -> Option<String> {
    for line in stderr.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        if value.get("code").and_then(|v| v.as_str()) != Some("ATO_ERR_POLICY_VIOLATION") {
            continue;
        }
        if let Some(target) = value.get("target").and_then(|v| v.as_str()) {
            return Some(target.to_string());
        }
    }
    None
}

fn normalize_host_from_target(target: &str) -> String {
    target
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or(target)
        .split(':')
        .next()
        .unwrap_or(target)
        .to_string()
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_5_non_interactive_missing_consent_denied() {
    let output = run_without_seeded_consent("network-exfil-capsule", &[], &[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\""),
        "Missing policy violation JSONL; stderr={} ",
        stderr
    );
    assert!(
        stderr.contains("consent") || stderr.contains("ExecutionPlan consent"),
        "Missing consent-deny reason; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_5_yes_flag_does_not_bypass_missing_consent() {
    let output = run_without_seeded_consent("network-exfil-capsule", &["--yes"], &[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\""),
        "Missing policy violation JSONL when --yes is supplied without consent; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("consent") || stderr.contains("ExecutionPlan consent"),
        "Expected consent rejection even with --yes; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_14_reconsent_required_on_policy_change() {
    let (_workspace, fixture) = prepare_fixture_workspace("malicious-npm-capsule");
    let home = prepare_consent_home(&fixture);

    let v1_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato for v1");

    assert!(
        v1_output.status.success(),
        "v1 should run successfully with seeded consent; stderr={}",
        String::from_utf8_lossy(&v1_output.stderr)
    );

    add_egress_allow_host(&fixture, "api.evil.com");
    write_capsule_lock(&fixture, "malicious-npm-capsule");

    let v2_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato for v2");

    assert!(
        !v2_output.status.success(),
        "v2 must fail-closed and require re-consent"
    );

    let stderr = String::from_utf8_lossy(&v2_output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\""),
        "Missing structured consent error code for re-consent flow; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("consent") || stderr.contains("ExecutionPlan consent"),
        "Expected re-consent requirement in stderr; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify package-lock fallback behavior before implementation"]
fn test_15_npm_package_lock_fallback_success() {
    let (_workspace, fixture) = prepare_fixture_workspace("npm-fallback-capsule");
    let home = prepare_consent_home(&fixture);

    let build_output = ato_cmd()
        .arg("build")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato build");

    assert!(
        build_output.status.success(),
        "build should succeed with package-lock fallback fixture; stderr={}",
        String::from_utf8_lossy(&build_output.stderr)
    );

    let archive_path = find_built_capsule_path(&fixture);
    let run_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&archive_path)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato run");

    assert!(
        run_output.status.success(),
        "run should succeed with package-lock fallback; stderr={}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        stdout.contains("npm package-lock fallback OK"),
        "expected fallback fixture output; stdout={}",
        stdout
    );
}

#[test]
#[ignore = "TDD Red phase: verify air-gap cached execution before implementation"]
fn test_16_airgap_offline_execution_success() {
    let (_workspace, fixture) = prepare_fixture_workspace("airgap-npm-fallback-capsule");
    let home = prepare_consent_home(&fixture);

    let warmup_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute warmup run");

    assert!(
        warmup_output.status.success(),
        "warmup run must succeed before offline replay; stderr={}",
        String::from_utf8_lossy(&warmup_output.stderr)
    );

    let offline_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute offline run");

    assert!(
        offline_output.status.success(),
        "offline run must succeed from local cache only; stderr={}",
        String::from_utf8_lossy(&offline_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&offline_output.stdout);
    assert!(
        stdout.contains("airgap cached-only run OK"),
        "expected offline fixture output; stdout={}",
        stdout
    );
}

#[test]
#[ignore = "TDD Red phase: verify tier2 filesystem isolation before implementation"]
#[cfg(unix)]
fn test_17_tier2_native_fs_isolation_enforced() {
    let (_workspace, fixture) = prepare_fixture_workspace("tier2-fs-isolation-capsule");
    let home = prepare_consent_home(&fixture);

    let leak_outside = fixture
        .parent()
        .expect("fixture must have parent")
        .join("pwned-outside.txt");
    let leak_tmp = PathBuf::from("/tmp/ato_host_leak_test_17.txt");
    let _ = fs::remove_file(&leak_outside);
    let _ = fs::remove_file(&leak_tmp);

    let uv_dir = TempDir::new().expect("failed to create temp dir for mock uv");
    let uv_path = uv_dir.path().join("uv");
    write_mock_uv(&uv_path);

    let base_path = std::env::var("PATH").unwrap_or_default();
    let merged_path = if base_path.is_empty() {
        uv_dir.path().display().to_string()
    } else {
        format!("{}:{}", uv_dir.path().display(), base_path)
    };
    let nacelle_path = resolve_test_nacelle_path();

    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg("--unsafe-bypass-sandbox")
        .arg("--nacelle")
        .arg(&nacelle_path)
        .arg(&fixture)
        .env("HOME", home.path())
        .env("PATH", merged_path)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute tier2 fs isolation fixture");

    let strict_ci = std::env::var("ATO_STRICT_CI")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let host_limited = stderr.contains("pfctl failed to load anchor")
            || stderr.contains("Sandbox unavailable")
            || stderr.contains("No compatible native sandbox backend is available");
        if host_limited {
            assert!(
                !strict_ci,
                "strict CI requires sandbox bootstrap; got host-limited failure: {}",
                stderr
            );
            eprintln!(
                "skipping tier2 fs isolation assertion on this host: {}",
                stderr
            );
            return;
        }
    }

    assert!(
        output.status.success(),
        "tier2 native run should confirm FS isolation; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("tier2 fs isolation enforced"),
        "expected isolation success marker; stdout={}",
        stdout
    );

    let safe_file = fixture.join("output").join("safe.txt");
    assert!(
        safe_file.exists(),
        "allowed write should create output/safe.txt"
    );
    assert!(
        !leak_outside.exists(),
        "write outside sandbox unexpectedly succeeded: {}",
        leak_outside.display()
    );
    assert!(
        !leak_tmp.exists(),
        "write to /tmp unexpectedly succeeded: {}",
        leak_tmp.display()
    );
}

#[test]
#[ignore = "TDD Red phase: verify --from-skill is fail-closed when consent is missing"]
fn test_18_from_skill_missing_consent_denied() {
    let home = TempDir::new().expect("failed to create temporary HOME");
    let skill_path = fixture_dir("skill-default-deny").join("SKILL.md");

    let output = ato_cmd()
        .arg("run")
        .arg("--from-skill")
        .arg(&skill_path)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato --from-skill");

    assert!(
        !output.status.success(),
        "from-skill run must fail without consent"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\""),
        "Missing policy violation JSONL for --from-skill consent denial; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("consent") || stderr.contains("ExecutionPlan consent"),
        "Expected consent-deny reason for --from-skill path; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify structured self-healing loop with policy JSONL"]
fn test_19_self_healing_loop_recovers_from_policy_violation() {
    let (_workspace, fixture) = prepare_fixture_workspace("network-exfil-capsule");
    let (port, server_handle) = spawn_plain_http_server("heal-ok");

    let script = format!(
        "const response = await fetch(\"http://127.0.0.1:{}/heal\");\nconsole.log(await response.text());\n",
        port
    );
    fs::write(fixture.join("main.ts"), script).expect("failed to rewrite fixture script");

    let deny_home = prepare_consent_home(&fixture);
    let deny_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", deny_home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute deny phase");

    assert!(!deny_output.status.success(), "deny phase must fail first");
    let deny_stderr = String::from_utf8_lossy(&deny_output.stderr);
    let target = extract_policy_violation_target(&deny_stderr)
        .expect("policy violation JSONL with target must be present");
    let host = normalize_host_from_target(&target);
    assert!(
        host == "127.0.0.1" || host == "localhost",
        "unexpected policy target host: {} (stderr={})",
        host,
        deny_stderr
    );

    add_egress_allow_host(&fixture, &host);
    write_capsule_lock(&fixture, "network-exfil-capsule");

    let healed_home = prepare_consent_home(&fixture);
    let healed_output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", healed_home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute healed phase");

    let _ = server_handle.join();

    assert!(
        healed_output.status.success(),
        "healed phase should succeed after allowlist patch; stderr={}",
        String::from_utf8_lossy(&healed_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&healed_output.stdout);
    assert!(
        stdout.contains("heal-ok"),
        "expected local server payload after healing; stdout={}",
        stdout
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_2_deno_lock_missing_fail_closed() {
    let output = run_with_seeded_consent("deno-lock-missing-capsule", &[], &[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_PROVISIONING_LOCK_INCOMPLETE\""),
        "Missing lock incomplete JSONL; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("deno.lock"),
        "Expected deno.lock reference in error; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_3_native_python_uv_lock_missing_fail_closed() {
    let output = run_with_seeded_consent(
        "native-python-no-uv-lock-capsule",
        &["--unsafe-bypass-sandbox"],
        &[],
    );

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_PROVISIONING_LOCK_INCOMPLETE\""),
        "Missing lock incomplete JSONL for native python uv.lock preflight; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("uv.lock"),
        "Expected uv.lock requirement to be surfaced; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_3_native_python_uv_binary_missing_fail_closed() {
    let output = run_with_seeded_consent(
        "native-python-with-uv-lock-capsule",
        &["--unsafe-bypass-sandbox"],
        &[("PATH", "")],
    );

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_PROVISIONING_LOCK_INCOMPLETE\""),
        "Missing lock incomplete JSONL for uv CLI preflight; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("uv CLI") || stderr.contains("uv run --offline"),
        "Expected uv CLI requirement to be surfaced; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_13_lockfile_tampered_rejected_before_runtime() {
    let (_workspace, fixture) = prepare_fixture_workspace("network-exfil-capsule");
    tamper_lock_manifest_hash(&fixture);
    let home = prepare_consent_home(&fixture);

    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_LOCKFILE_TAMPERED\""),
        "Missing lockfile tampered JSONL; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
#[cfg(unix)]
fn test_8_consent_store_permissions_are_hardened() {
    use std::os::unix::fs::PermissionsExt;

    let (_workspace, fixture) = prepare_fixture_workspace("network-exfil-capsule");
    let home = TempDir::new().expect("failed to create temporary HOME");

    let output = ato_cmd()
        .arg("run")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato");

    assert!(
        !output.status.success(),
        "run is expected to fail due to missing consent in non-interactive mode"
    );

    let consent_dir = home.path().join(".ato").join("consent");
    let consent_file = consent_dir.join("executionplan_v1.jsonl");
    assert!(consent_dir.exists(), "consent directory must be created");
    assert!(consent_file.exists(), "consent file must be created");

    let dir_mode = fs::metadata(&consent_dir)
        .expect("failed to stat consent directory")
        .permissions()
        .mode()
        & 0o777;
    let file_mode = fs::metadata(&consent_file)
        .expect("failed to stat consent file")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(dir_mode, 0o700, "consent directory mode must be 0700");
    assert_eq!(file_mode, 0o600, "consent file mode must be 0600");
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_1_web_entrypoint_outside_public_allowlist_rejected() {
    let output = run_without_seeded_consent("web-path-traversal-capsule", &["--yes"], &[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ATO_ERR_POLICY_VIOLATION")
            && stderr.contains("entrypoint")
            && stderr.contains("public allowlist"),
        "Expected fail-closed manifest validation for traversal-like entrypoint; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
#[cfg(unix)]
fn test_9_native_sandbox_unavailable_fail_closed_even_with_unsafe_flag() {
    let (_workspace, fixture) = prepare_fixture_workspace("native-sandbox-unavailable-capsule");
    let home = prepare_consent_home(&fixture);
    let nacelle_dir = TempDir::new().expect("failed to create temp dir for mock binaries");
    let nacelle_path = nacelle_dir.path().join("nacelle");
    let uv_path = nacelle_dir.path().join("uv");
    write_mock_nacelle_without_sandbox(&nacelle_path);
    write_mock_uv(&uv_path);

    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg("--unsafe-bypass-sandbox")
        .arg(&fixture)
        .env("HOME", home.path())
        .env("NACELLE_PATH", &nacelle_path)
        .env("PATH", nacelle_dir.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_COMPAT_HARDWARE\""),
        "Missing compat hardware JSONL when sandbox backend unavailable; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("sandbox") && stderr.contains("not available"),
        "Expected explicit no-sandbox backend failure; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_10_redirect_escape_to_disallowed_host_blocked() {
    let (_workspace, fixture) = prepare_fixture_workspace("redirect-escape-capsule");
    let (port, redirect_thread) = spawn_redirect_server("https://api.evil.com/");

    let main_ts = fixture.join("main.ts");
    let script = fs::read_to_string(&main_ts).expect("failed to read redirect fixture script");
    let rendered = script.replace("__REDIRECT_PORT__", &port.to_string());
    fs::write(&main_ts, rendered).expect("failed to render redirect fixture script");

    let home = prepare_consent_home(&fixture);
    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg(&fixture)
        .env("HOME", home.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute ato");

    let _ = redirect_thread.join();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\"") && stderr.contains("api.evil.com"),
        "Expected redirect escape host to be blocked; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_6_npm_lifecycle_isolation() {
    let pwn_target = PathBuf::from("/tmp/ato_pwned_test_6");
    let _ = fs::remove_file(&pwn_target);

    let output = run_with_seeded_consent("malicious-npm-capsule", &[], &[]);

    assert!(
        output.status.success(),
        "fixture should run successfully before checking side effects; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !pwn_target.exists(),
        "FAIL-CLOSED BROKEN: postinstall script executed"
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_7_network_exfiltration_blocked() {
    let output = run_with_seeded_consent("network-exfil-capsule", &[], &[]);

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_POLICY_VIOLATION\""),
        "Missing structured error code in stderr: {}",
        stderr
    );
    assert!(
        stderr.contains("api.evil.com"),
        "Missing violation target in stderr: {}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_8_secret_fd_injection_no_env_leak() {
    let output = run_with_seeded_consent(
        "env-dump-capsule",
        &[],
        &[("OPENAI_API_KEY", "sk-secret-do-not-leak")],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("sk-secret-do-not-leak"),
        "CRITICAL LEAK: Secret found in process.env"
    );
    assert!(
        output.status.success(),
        "Script failed to read secret from FD; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_11_glibc_preflight_rejection() {
    let output = run_with_seeded_consent("future-glibc-capsule", &["--unsafe-bypass-sandbox"], &[]);

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_COMPAT_HARDWARE\""),
        "Missing hardware compat error; stderr={} ",
        stderr
    );
    assert!(
        stderr.to_ascii_lowercase().contains("glibc"),
        "Error should mention glibc mismatch; stderr={}",
        stderr
    );
}

#[test]
#[ignore = "TDD Red phase: verify fail-closed behavior before implementation"]
fn test_12_elf_overrides_lock_preflight() {
    let output =
        run_with_seeded_consent("glibc-mismatch-capsule", &["--unsafe-bypass-sandbox"], &[]);

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"code\":\"ATO_ERR_COMPAT_HARDWARE\""),
        "Missing compat hardware error; stderr={}",
        stderr
    );
    assert!(
        stderr.contains("2.99"),
        "ELF-required glibc version must win over lock metadata; stderr={}",
        stderr
    );
}
