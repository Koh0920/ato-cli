use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use tempfile::TempDir;

struct ServerGuard {
    child: std::process::Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

fn local_tcp_bind_available() -> bool {
    TcpListener::bind("127.0.0.1:0").is_ok()
}

fn is_permission_denied(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
            .unwrap_or(false)
    }) || {
        let msg = err.to_string().to_ascii_lowercase();
        msg.contains("permission denied") || msg.contains("operation not permitted")
    }
}

fn start_local_registry_or_skip(
    ato: &Path,
    data_dir: &Path,
    test_name: &str,
) -> Result<Option<(ServerGuard, String)>> {
    if !local_tcp_bind_available() {
        eprintln!(
            "skipping {test_name}: local TCP bind is not permitted in this environment"
        );
        return Ok(None);
    }

    match start_local_registry(ato, data_dir) {
        Ok(v) => Ok(Some(v)),
        Err(err) if is_permission_denied(&err) => {
            eprintln!("skipping {test_name}: {}", err);
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

fn wait_for_well_known(base_url: &str) -> Result<()> {
    let url = format!("{}/.well-known/capsule.json", base_url);
    for _ in 0..60 {
        if let Ok(resp) = reqwest::blocking::get(&url) {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!("local registry did not become ready: {}", url);
}

fn run_ato(ato: &Path, args: &[&str], cwd: &Path) -> Result<std::process::Output> {
    Command::new(ato)
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to run ato {:?}", args))
}

fn run_ato_with_home(
    ato: &Path,
    args: &[&str],
    cwd: &Path,
    home_dir: &Path,
) -> Result<std::process::Output> {
    Command::new(ato)
        .args(args)
        .current_dir(cwd)
        .env("HOME", home_dir)
        .output()
        .with_context(|| format!("failed to run ato {:?}", args))
}

fn start_local_registry(ato: &Path, data_dir: &Path) -> Result<(ServerGuard, String)> {
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{}", port);
    let child = Command::new(ato)
        .args([
            "registry",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--data-dir",
            data_dir.to_string_lossy().as_ref(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn local registry server")?;
    let guard = ServerGuard { child };
    wait_for_well_known(&base_url)?;
    Ok((guard, base_url))
}

fn build_publish_install(
    ato: &Path,
    project_dir: &Path,
    base_url: &str,
    scoped_id: &str,
    capsule_name: &str,
    install_cwd: &Path,
    home_dir: &Path,
) -> Result<()> {
    let build = run_ato_with_home(ato, &["build", "."], project_dir, home_dir)?;
    assert!(
        build.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    let capsule_path = project_dir.join(format!("{}.capsule", capsule_name));
    assert!(
        capsule_path.exists(),
        "capsule artifact not found: {}",
        capsule_path.display()
    );

    let publish = run_ato_with_home(
        ato,
        &["publish", "--registry", base_url, "--json"],
        project_dir,
        home_dir,
    )?;
    assert!(
        publish.status.success(),
        "publish failed: {}",
        String::from_utf8_lossy(&publish.stderr)
    );

    let install = run_ato_with_home(
        ato,
        &["install", scoped_id, "--registry", base_url],
        install_cwd,
        home_dir,
    )?;
    assert!(
        install.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&install.stderr)
    );

    Ok(())
}

#[test]
fn e2e_local_registry_build_publish_install_search_download() -> Result<()> {
    let ato = assert_cmd::cargo::cargo_bin("ato");
    let tmp = TempDir::new().context("create temp dir")?;
    let home_dir = tmp.path().join("home");
    std::fs::create_dir_all(&home_dir)?;
    let data_dir = tmp.path().join("registry-data");
    let project_dir = tmp.path().join("project");
    std::fs::create_dir_all(&project_dir)?;

    std::fs::write(
        project_dir.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-local"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "deno"
runtime_version = "1.46.3"
entrypoint = "main.ts"
"#,
    )?;
    std::fs::write(
        project_dir.join("main.ts"),
        r#"console.log("hello local registry");"#,
    )?;
    std::fs::write(
        project_dir.join("deno.lock"),
        r#"{"version":"5","specifiers":{},"packages":{}}"#,
    )?;

    let Some((_guard, base_url)) =
        start_local_registry_or_skip(&ato, &data_dir, "e2e_local_registry_build_publish_install_search_download")?
    else {
        return Ok(());
    };

    build_publish_install(
        &ato,
        &project_dir,
        &base_url,
        "local/test-local",
        "test-local",
        tmp.path(),
        &home_dir,
    )?;

    let search = run_ato_with_home(
        &ato,
        &["search", "test-local", "--registry", &base_url, "--json"],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );
    let body = String::from_utf8(search.stdout).context("search stdout utf8")?;
    let value: serde_json::Value = serde_json::from_str(&body).context("search json parse")?;
    let capsules = value
        .get("capsules")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        capsules
            .iter()
            .any(|capsule| capsule.get("slug").and_then(|v| v.as_str()) == Some("test-local")),
        "search response missing test-local capsule"
    );

    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build client")?;
    let resp = client
        .get(format!(
            "{}/v1/capsules/by/local/test-local/download?version=1.0.0",
            base_url
        ))
        .send()
        .context("download endpoint call")?;
    assert_eq!(resp.status(), reqwest::StatusCode::FOUND);
    assert!(
        resp.headers().get("location").is_some(),
        "download endpoint should return Location header"
    );

    Ok(())
}

#[test]
fn e2e_local_registry_web_static_build_publish_install() -> Result<()> {
    let ato = assert_cmd::cargo::cargo_bin("ato");
    let tmp = TempDir::new().context("create temp dir")?;
    let home_dir = tmp.path().join("home");
    std::fs::create_dir_all(&home_dir)?;
    let static_port = if local_tcp_bind_available() {
        reserve_port()
    } else {
        eprintln!("skipping e2e_local_registry_web_static_build_publish_install: local TCP bind is not permitted in this environment");
        return Ok(());
    };
    let data_dir = tmp.path().join("registry-data");
    let project_dir = tmp.path().join("web-static-project");
    std::fs::create_dir_all(project_dir.join("dist"))?;

    std::fs::write(
        project_dir.join("capsule.toml"),
        format!(
            r#"schema_version = "0.2"
name = "test-web-static"
version = "1.0.0"
type = "app"
default_target = "static"

[targets.static]
runtime = "web"
driver = "static"
entrypoint = "dist"
port = {static_port}
"#
        ),
    )?;
    std::fs::write(
        project_dir.join("dist").join("index.html"),
        r#"<!doctype html><title>web static</title>"#,
    )?;

    let Some((_guard, base_url)) =
        start_local_registry_or_skip(&ato, &data_dir, "e2e_local_registry_web_static_build_publish_install")?
    else {
        return Ok(());
    };
    build_publish_install(
        &ato,
        &project_dir,
        &base_url,
        "local/test-web-static",
        "test-web-static",
        tmp.path(),
        &home_dir,
    )?;

    let run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-web-static",
            "--registry",
            &base_url,
            "--yes",
            "--background",
        ],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        run.status.success(),
        "run should start in background; stderr={}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Best-effort cleanup in case background process was started.
    let _ = run_ato_with_home(
        &ato,
        &["close", "--all", "--force"],
        tmp.path(),
        &home_dir,
    );

    Ok(())
}

#[test]
fn e2e_local_registry_node_python_run_fail_closed() -> Result<()> {
    let ato = assert_cmd::cargo::cargo_bin("ato");
    let tmp = TempDir::new().context("create temp dir")?;
    let home_dir = tmp.path().join("home");
    std::fs::create_dir_all(&home_dir)?;
    let data_dir = tmp.path().join("registry-data");
    let node_no_lock = tmp.path().join("node-no-lock");
    let node_with_lock = tmp.path().join("node-with-lock");
    let node_policy_violation = tmp.path().join("node-policy-violation");
    let python_no_lock = tmp.path().join("python-no-lock");
    let python_with_lock = tmp.path().join("python-with-lock");
    std::fs::create_dir_all(&node_no_lock)?;
    std::fs::create_dir_all(&node_with_lock)?;
    std::fs::create_dir_all(&node_policy_violation)?;
    std::fs::create_dir_all(&python_no_lock)?;
    std::fs::create_dir_all(&python_with_lock)?;

    std::fs::write(
        node_no_lock.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-node-no-lock"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "node"
runtime_version = "20.12.0"
entrypoint = "main.js"
"#,
    )?;
    std::fs::write(
        node_no_lock.join("main.js"),
        r#"console.log("node no lock");"#,
    )?;

    std::fs::write(
        node_with_lock.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-node-with-lock"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "node"
runtime_version = "20.12.0"
entrypoint = "main.js"
"#,
    )?;
    std::fs::write(
        node_with_lock.join("main.js"),
        r#"console.log("node with lock");"#,
    )?;
    std::fs::write(node_with_lock.join("package-lock.json"), "{}")?;

    std::fs::write(
        node_policy_violation.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-node-policy-violation"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "node"
runtime_version = "20.12.0"
entrypoint = "main.js"
"#,
    )?;
    std::fs::write(
        node_policy_violation.join("main.js"),
        r#"fetch("https://example.com").then((res) => console.log(res.status));"#,
    )?;
    std::fs::write(node_policy_violation.join("package-lock.json"), "{}")?;

    std::fs::write(
        python_no_lock.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-python-no-lock"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "python"
runtime_version = "3.11.9"
entrypoint = "main.py"
"#,
    )?;
    std::fs::write(python_no_lock.join("main.py"), r#"print("python no lock")"#)?;

    std::fs::write(
        python_with_lock.join("capsule.toml"),
        r#"schema_version = "0.2"
name = "test-python-with-lock"
version = "1.0.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "python"
runtime_version = "3.11.9"
entrypoint = "main.py"
"#,
    )?;
    std::fs::write(
        python_with_lock.join("main.py"),
        r#"print("python with lock")"#,
    )?;
    std::fs::write(python_with_lock.join("uv.lock"), "# uv lock")?;

    let Some((_guard, base_url)) =
        start_local_registry_or_skip(&ato, &data_dir, "e2e_local_registry_node_python_run_fail_closed")?
    else {
        return Ok(());
    };

    build_publish_install(
        &ato,
        &node_no_lock,
        &base_url,
        "local/test-node-no-lock",
        "test-node-no-lock",
        tmp.path(),
        &home_dir,
    )?;
    build_publish_install(
        &ato,
        &node_with_lock,
        &base_url,
        "local/test-node-with-lock",
        "test-node-with-lock",
        tmp.path(),
        &home_dir,
    )?;
    build_publish_install(
        &ato,
        &python_no_lock,
        &base_url,
        "local/test-python-no-lock",
        "test-python-no-lock",
        tmp.path(),
        &home_dir,
    )?;
    build_publish_install(
        &ato,
        &python_with_lock,
        &base_url,
        "local/test-python-with-lock",
        "test-python-with-lock",
        tmp.path(),
        &home_dir,
    )?;
    build_publish_install(
        &ato,
        &node_policy_violation,
        &base_url,
        "local/test-node-policy-violation",
        "test-node-policy-violation",
        tmp.path(),
        &home_dir,
    )?;

    let node_no_lock_run = run_ato_with_home(
        &ato,
        &["run", "local/test-node-no-lock", "--registry", &base_url],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        !node_no_lock_run.status.success(),
        "node no lock run must fail-closed"
    );
    let node_no_lock_stderr = String::from_utf8_lossy(&node_no_lock_run.stderr);
    assert!(
        node_no_lock_stderr.contains("ATO_ERR_PROVISIONING_LOCK_INCOMPLETE"),
        "expected lock incomplete JSONL for node no lock; stderr={}",
        node_no_lock_stderr
    );
    assert!(
        node_no_lock_stderr.contains("package-lock.json")
            && !node_no_lock_stderr.contains("pnpm-lock.yaml")
            && !node_no_lock_stderr.contains("yarn.lock"),
        "expected node lockfile requirement to be surfaced; stderr={}",
        node_no_lock_stderr
    );

    let node_with_lock_run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-node-with-lock",
            "--registry",
            &base_url,
            "--yes",
        ],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        node_with_lock_run.status.success(),
        "node with lock should run without --sandbox; stderr={}",
        String::from_utf8_lossy(&node_with_lock_run.stderr)
    );
    let node_with_lock_stderr = String::from_utf8_lossy(&node_with_lock_run.stderr);
    assert!(
        !node_with_lock_stderr.contains("ATO_ERR_POLICY_VIOLATION"),
        "node with lock should not emit policy violation; stderr={}",
        node_with_lock_stderr
    );
    assert!(
        !node_with_lock_stderr.contains("--sandbox"),
        "node with lock should not require --sandbox; stderr={}",
        node_with_lock_stderr
    );

    let node_policy_violation_run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-node-policy-violation",
            "--registry",
            &base_url,
            "--yes",
        ],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        !node_policy_violation_run.status.success(),
        "node permission violation case must fail-closed"
    );
    let node_policy_violation_stderr = String::from_utf8_lossy(&node_policy_violation_run.stderr);
    assert!(
        node_policy_violation_stderr.contains("ATO_ERR_POLICY_VIOLATION")
            || node_policy_violation_stderr.contains("PermissionDenied: Requires net access"),
        "expected policy violation signal for node permission violation; stderr={}",
        node_policy_violation_stderr
    );

    let python_no_lock_run = run_ato_with_home(
        &ato,
        &["run", "local/test-python-no-lock", "--registry", &base_url],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        !python_no_lock_run.status.success(),
        "python no lock run must fail-closed"
    );
    let python_no_lock_stderr = String::from_utf8_lossy(&python_no_lock_run.stderr);
    assert!(
        python_no_lock_stderr.contains("ATO_ERR_PROVISIONING_LOCK_INCOMPLETE"),
        "expected lock incomplete JSONL for python no lock; stderr={}",
        python_no_lock_stderr
    );
    assert!(
        python_no_lock_stderr.contains("uv.lock"),
        "expected uv.lock requirement to be surfaced; stderr={}",
        python_no_lock_stderr
    );

    let python_with_lock_run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-python-with-lock",
            "--registry",
            &base_url,
        ],
        tmp.path(),
        &home_dir,
    )?;
    assert!(
        !python_with_lock_run.status.success(),
        "python with lock without --sandbox must fail"
    );
    let python_with_lock_stderr = String::from_utf8_lossy(&python_with_lock_run.stderr);
    assert!(
        python_with_lock_stderr.contains("ATO_ERR_POLICY_VIOLATION"),
        "expected policy violation JSONL for python without --sandbox; stderr={}",
        python_with_lock_stderr
    );
    assert!(
        python_with_lock_stderr.contains("source/native|python execution requires explicit")
            && python_with_lock_stderr.contains("--sandbox"),
        "expected --sandbox requirement to be surfaced; stderr={}",
        python_with_lock_stderr
    );

    let node_with_lock_unsafe_yes_run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-node-with-lock",
            "--registry",
            &base_url,
            "--sandbox",
            "--yes",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let node_with_lock_unsafe_yes_stderr =
        String::from_utf8_lossy(&node_with_lock_unsafe_yes_run.stderr);
    assert!(
        node_with_lock_unsafe_yes_run.status.success(),
        "node with lock should also run with --sandbox --yes; stderr={}",
        node_with_lock_unsafe_yes_stderr
    );
    assert!(
        !node_with_lock_unsafe_yes_stderr.contains("ATO_ERR_CONSENT_REQUIRED"),
        "unexpected consent-required in --sandbox --yes node run; stderr={}",
        node_with_lock_unsafe_yes_stderr
    );
    assert!(
        !node_with_lock_unsafe_yes_stderr
            .contains("source/native|python execution requires explicit --sandbox opt-in"),
        "unexpected sandbox requirement in --sandbox --yes node run; stderr={}",
        node_with_lock_unsafe_yes_stderr
    );
    assert!(
        !node_with_lock_unsafe_yes_stderr
            .contains("package-lock.json is required for source/node Tier1 execution"),
        "unexpected node lockfile error in --sandbox --yes node run; stderr={}",
        node_with_lock_unsafe_yes_stderr
    );

    let python_with_lock_unsafe_yes_run = run_ato_with_home(
        &ato,
        &[
            "run",
            "local/test-python-with-lock",
            "--registry",
            &base_url,
            "--sandbox",
            "--yes",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let python_with_lock_unsafe_yes_stderr =
        String::from_utf8_lossy(&python_with_lock_unsafe_yes_run.stderr);
    assert!(
        !python_with_lock_unsafe_yes_stderr.contains("ATO_ERR_CONSENT_REQUIRED"),
        "unexpected consent-required in --sandbox --yes python run; stderr={}",
        python_with_lock_unsafe_yes_stderr
    );
    assert!(
        !python_with_lock_unsafe_yes_stderr
            .contains("source/native|python execution requires explicit --sandbox opt-in"),
        "unexpected sandbox opt-in error in --sandbox --yes python run; stderr={}",
        python_with_lock_unsafe_yes_stderr
    );
    assert!(
        !python_with_lock_unsafe_yes_stderr
            .contains("uv.lock is required for source/python execution"),
        "unexpected uv.lock error in --sandbox --yes python run; stderr={}",
        python_with_lock_unsafe_yes_stderr
    );

    Ok(())
}
