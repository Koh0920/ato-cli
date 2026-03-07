mod fail_closed_support;

use std::fs;
use std::process::Stdio;
use std::thread;

use fail_closed_support::*;
use tempfile::TempDir;

fn prepare_auto_bootstrap_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    prepare_fixture_workspace("native-shell-capsule")
}

#[cfg(unix)]
#[test]
fn auto_bootstrap_happy_path_uses_local_release_server() {
    let version = "v9.9.9";
    let release_root = TempDir::new().expect("failed to create release root");
    let binary_path = write_mock_nacelle_release(release_root.path(), version);
    let binary_name = binary_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("binary name must be utf-8")
        .to_string();
    let server = spawn_static_file_server(release_root.path().to_path_buf());

    let (_workspace, fixture) = prepare_auto_bootstrap_fixture();
    let home = TempDir::new().expect("failed to create temporary HOME");

    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg("--sandbox")
        .arg(&fixture)
        .env("HOME", home.path())
        .env("ATO_NACELLE_AUTO_BOOTSTRAP", "force")
        .env("ATO_NACELLE_VERSION", version)
        .env("ATO_NACELLE_RELEASE_BASE_URL", &server.base_url)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute auto-bootstrap happy path");

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let installed = home
        .path()
        .join(".ato")
        .join("engines")
        .join(format!("nacelle-{}", version));
    assert!(
        installed.exists(),
        "installed engine missing: {}",
        installed.display()
    );

    let config_path = home.path().join(".ato").join("config.toml");
    let config = fs::read_to_string(&config_path).expect("config.toml must exist after bootstrap");
    assert!(
        config.contains(&installed.display().to_string()),
        "config={config}"
    );

    assert_eq!(
        server.request_count(&format!("/{}/{binary_name}.sha256", version)),
        1,
        "checksum should be requested exactly once in single-run happy path"
    );
    assert_eq!(
        server.request_count(&format!("/{}/{}", version, binary_name)),
        1,
        "binary should be downloaded exactly once in single-run happy path"
    );
}

#[cfg(unix)]
#[test]
fn auto_bootstrap_offline_fail_closed_without_prefetched_engine() {
    let version = "v9.9.8";
    let (_workspace, fixture) = prepare_auto_bootstrap_fixture();
    let home = TempDir::new().expect("failed to create temporary HOME");

    let output = ato_cmd()
        .arg("run")
        .arg("--yes")
        .arg("--sandbox")
        .arg(&fixture)
        .env("HOME", home.path())
        .env("ATO_NACELLE_AUTO_BOOTSTRAP", "force")
        .env("ATO_OFFLINE", "1")
        .env("ATO_NACELLE_VERSION", version)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute offline fail-closed path");

    assert!(
        !output.status.success(),
        "stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ATO_ERR_ENGINE_MISSING"), "stderr={stderr}");
    assert!(
        stderr.contains("ATO_OFFLINE") || stderr.contains("auto-bootstrap is disabled"),
        "stderr={stderr}"
    );
}

#[cfg(unix)]
#[test]
fn auto_bootstrap_shared_install_path_downloads_binary_once() {
    let version = "v9.9.7";
    let release_root = TempDir::new().expect("failed to create release root");
    let binary_path = write_mock_nacelle_release(release_root.path(), version);
    let binary_name = binary_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("binary name must be utf-8")
        .to_string();
    let server = spawn_static_file_server(release_root.path().to_path_buf());

    let (_workspace_a, fixture_a) = prepare_auto_bootstrap_fixture();
    let (_workspace_b, fixture_b) = prepare_auto_bootstrap_fixture();
    let home = TempDir::new().expect("failed to create temporary HOME");
    let home_path = home.path().to_path_buf();
    let base_url = server.base_url.clone();

    let run_one = |fixture: std::path::PathBuf| {
        let home_path = home_path.clone();
        let base_url = base_url.clone();
        thread::spawn(move || {
            ato_cmd()
                .arg("run")
                .arg("--yes")
                .arg("--sandbox")
                .arg(&fixture)
                .env("HOME", &home_path)
                .env("ATO_NACELLE_AUTO_BOOTSTRAP", "force")
                .env("ATO_NACELLE_VERSION", version)
                .env("ATO_NACELLE_RELEASE_BASE_URL", &base_url)
                .stderr(Stdio::piped())
                .stdout(Stdio::piped())
                .output()
                .expect("failed to execute concurrent auto-bootstrap path")
        })
    };

    let first = run_one(fixture_a);
    let second = run_one(fixture_b);
    let first_output = first.join().expect("first runner panicked");
    let second_output = second.join().expect("second runner panicked");

    assert!(
        first_output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&first_output.stderr)
    );
    assert!(
        second_output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&second_output.stderr)
    );

    assert_eq!(
        server.request_count(&format!("/{}/{}", version, binary_name)),
        1,
        "binary download should be shared by the install lock"
    );
}
