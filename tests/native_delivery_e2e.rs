#![allow(deprecated)]

use std::fs;
use std::io::Cursor;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use capsule_core::packers::payload::build_distribution_manifest;
use capsule_core::types::CapsuleManifest;
use tempfile::TempDir;
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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

fn start_local_registry_or_skip(
    ato: &Path,
    data_dir: &Path,
    test_name: &str,
) -> Result<Option<(ServerGuard, String)>> {
    if !local_tcp_bind_available() {
        eprintln!("skipping {test_name}: local TCP bind is not permitted in this environment");
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

fn run_ato_with_home(ato: &Path, args: &[&str], cwd: &Path, home_dir: &Path) -> Result<Output> {
    Command::new(ato)
        .args(args)
        .current_dir(cwd)
        .env("HOME", home_dir)
        .output()
        .with_context(|| format!("failed to run ato {:?}", args))
}

fn run_command(program: &str, args: &[&str], cwd: &Path) -> Result<Output> {
    Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to run {} {:?}", program, args))
}

fn require_success(output: Output, context: &str) -> Result<Output> {
    if output.status.success() {
        return Ok(output);
    }
    anyhow::bail!(
        "{} failed\nstdout:\n{}\nstderr:\n{}",
        context,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn command_on_path(program: &str) -> bool {
    Command::new("which")
        .arg(program)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn command_available(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn sample_project_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/sample-native-capsule")
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("failed to stat {}", source.display()))?;
    if metadata.file_type().is_symlink() {
        #[cfg(unix)]
        {
            let target = fs::read_link(source)
                .with_context(|| format!("failed to read symlink {}", source.display()))?;
            std::os::unix::fs::symlink(&target, destination).with_context(|| {
                format!(
                    "failed to recreate symlink {} -> {}",
                    destination.display(),
                    target.display()
                )
            })?;
            return Ok(());
        }
        #[cfg(not(unix))]
        {
            anyhow::bail!("symlink copy is not supported on this platform");
        }
    }
    if metadata.is_dir() {
        fs::create_dir_all(destination)
            .with_context(|| format!("failed to create {}", destination.display()))?;
        for entry in fs::read_dir(source)
            .with_context(|| format!("failed to read directory {}", source.display()))?
        {
            let entry = entry?;
            let child_source = entry.path();
            let child_name = entry.file_name();
            if child_name == "node_modules" || child_name == "target" {
                continue;
            }
            copy_tree(&child_source, &destination.join(child_name))?;
        }
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "failed to copy {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    fs::set_permissions(destination, metadata.permissions())
        .with_context(|| format!("failed to set permissions on {}", destination.display()))?;
    Ok(())
}

fn assert_executable(path: &Path, context: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = fs::metadata(path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            anyhow::bail!(
                "{} is not executable: {} (mode {:o})",
                context,
                path.display(),
                mode
            );
        }
    }
    let _ = context;
    Ok(())
}

fn compute_tree_digest(root: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hash_tree_node(root, Path::new(""), &mut hasher)?;
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn hash_tree_node(path: &Path, relative: &Path, hasher: &mut blake3::Hasher) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let file_type = metadata.file_type();

    if file_type.is_dir() {
        if !relative.as_os_str().is_empty() {
            update_tree_header(hasher, b"dir", relative, mode_bits(&metadata));
        }
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("failed to read directory {}", path.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("failed to enumerate directory {}", path.display()))?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let child_path = entry.path();
            let child_relative = if relative.as_os_str().is_empty() {
                PathBuf::from(entry.file_name())
            } else {
                relative.join(entry.file_name())
            };
            hash_tree_node(&child_path, &child_relative, hasher)?;
        }
        return Ok(());
    }

    if file_type.is_symlink() {
        update_tree_header(hasher, b"symlink", relative, 0);
        let target = fs::read_link(path)
            .with_context(|| format!("failed to read symlink {}", path.display()))?;
        hasher.update(target.as_os_str().to_string_lossy().as_bytes());
        hasher.update(b"\0");
        return Ok(());
    }

    if file_type.is_file() {
        update_tree_header(hasher, b"file", relative, mode_bits(&metadata));
        hasher.update(format!("{}\0", metadata.len()).as_bytes());
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        hasher.update(&bytes);
        hasher.update(b"\0");
        return Ok(());
    }

    anyhow::bail!(
        "unsupported filesystem entry in digest walk: {}",
        path.display()
    )
}

fn update_tree_header(hasher: &mut blake3::Hasher, kind: &[u8], relative: &Path, mode: u32) {
    hasher.update(kind);
    hasher.update(b"\0");
    hasher.update(relative.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hasher.update(format!("{:o}", mode).as_bytes());
    hasher.update(b"\0");
}

#[cfg(unix)]
fn mode_bits(metadata: &fs::Metadata) -> u32 {
    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn mode_bits(_metadata: &fs::Metadata) -> u32 {
    0
}

fn prepare_sample_workspace(tmp: &TempDir) -> Result<PathBuf> {
    let source = sample_project_dir();
    let destination = tmp.path().join("sample-native-capsule");
    copy_tree(&source, &destination)?;
    let cargo_toml = destination.join("src-tauri/Cargo.toml");
    let mut cargo_body = fs::read_to_string(&cargo_toml)
        .with_context(|| format!("failed to read {}", cargo_toml.display()))?;
    if !cargo_body.contains("[workspace]") {
        cargo_body.push_str("\n[workspace]\n");
        fs::write(&cargo_toml, cargo_body)
            .with_context(|| format!("failed to write {}", cargo_toml.display()))?;
    }
    Ok(destination)
}

fn build_sample_app(sample_dir: &Path) -> Result<PathBuf> {
    let app_path =
        sample_dir.join("src-tauri/target/release/bundle/macos/sample-native-capsule.app");
    if app_path.exists() {
        return Ok(app_path);
    }

    require_success(
        run_command("deno", &["install"], sample_dir)?,
        "deno install",
    )?;
    require_success(
        run_command(
            "deno",
            &["task", "tauri", "build", "--bundles", "app"],
            sample_dir,
        )?,
        "deno task tauri build --bundles app",
    )?;

    if !app_path.exists() {
        anyhow::bail!("built app not found: {}", app_path.display());
    }
    Ok(app_path)
}

fn create_pack_project(tmp: &TempDir, built_app: &Path) -> Result<PathBuf> {
    let pack_dir = tmp.path().join("native-pack-project");
    fs::create_dir_all(&pack_dir)?;

    let app_dir = pack_dir.join("sample-native-capsule.app");
    copy_tree(built_app, &app_dir)?;
    assert_executable(
        &app_dir.join("Contents/MacOS/sample-native-capsule"),
        "copied sample app binary",
    )?;
    require_success(
        run_command(
            "codesign",
            &["--remove-signature", app_dir.to_string_lossy().as_ref()],
            tmp.path(),
        )?,
        "codesign --remove-signature",
    )?;

    fs::write(
        pack_dir.join("ato.delivery.toml"),
        r#"schema_version = "exp-0.1"
[artifact]
framework = "tauri"
stage = "unsigned"
target = "darwin/arm64"
input = "sample-native-capsule.app"
[finalize]
tool = "codesign"
args = ["--deep", "--force", "--sign", "-", "sample-native-capsule.app"]
"#,
    )?;

    Ok(pack_dir)
}

fn build_payload_tar_bytes(root: &Path) -> Result<Vec<u8>> {
    let mut paths = WalkDir::new(root)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.into_path())
        .filter(|path| path != root)
        .collect::<Vec<_>>();
    paths.sort();

    let mut out = Vec::new();
    let mut builder = tar::Builder::new(&mut out);
    builder.follow_symlinks(false);
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("failed to relativize {}", path.display()))?;
        builder
            .append_path_with_name(&path, relative)
            .with_context(|| format!("failed to append {}", path.display()))?;
    }
    builder.finish()?;
    drop(builder);
    Ok(out)
}

fn append_generated_tar_entry(
    builder: &mut tar::Builder<&mut Vec<u8>>,
    path: &str,
    bytes: &[u8],
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();
    builder.append_data(&mut header, path, Cursor::new(bytes))?;
    Ok(())
}

fn build_native_capsule(tmp: &TempDir, pack_dir: &Path) -> Result<PathBuf> {
    let payload_tar = build_payload_tar_bytes(pack_dir)?;
    let manifest: CapsuleManifest = toml::from_str(
        r#"schema_version = "0.2"
name = "sample-native-capsule"
version = "0.1.1"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "static"
path = "sample-native-capsule.app"
"#,
    )
    .context("parse native capsule manifest")?;
    let (_distribution_manifest, manifest_toml_bytes) =
        build_distribution_manifest(&manifest, &payload_tar)
            .map_err(anyhow::Error::from)
            .context("build distribution manifest")?;
    let payload_tar_zst = zstd::stream::encode_all(Cursor::new(payload_tar), 3)?;

    let artifact_path = tmp.path().join("sample-native-capsule.capsule");
    let mut out = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut out);
        append_generated_tar_entry(&mut builder, "capsule.toml", &manifest_toml_bytes)?;
        append_generated_tar_entry(&mut builder, "payload.tar.zst", &payload_tar_zst)?;
        builder.finish()?;
    }
    fs::write(&artifact_path, out)
        .with_context(|| format!("failed to write {}", artifact_path.display()))?;
    Ok(artifact_path)
}

#[test]
fn e2e_native_delivery_sample_tauri_unsigned_finalize() -> Result<()> {
    if !cfg!(target_os = "macos") || std::env::consts::ARCH != "aarch64" {
        eprintln!("skipping e2e_native_delivery_sample_tauri_unsigned_finalize: darwin/arm64 only");
        return Ok(());
    }
    if !sample_project_dir().exists() {
        eprintln!(
            "skipping e2e_native_delivery_sample_tauri_unsigned_finalize: sample project missing"
        );
        return Ok(());
    }
    if !command_on_path("deno") || !command_available("deno", &["--version"]) {
        eprintln!("skipping e2e_native_delivery_sample_tauri_unsigned_finalize: deno unavailable");
        return Ok(());
    }
    if !command_on_path("codesign") {
        eprintln!(
            "skipping e2e_native_delivery_sample_tauri_unsigned_finalize: codesign unavailable"
        );
        return Ok(());
    }

    let ato = assert_cmd::cargo::cargo_bin("ato");
    let tmp = TempDir::new().context("create temp dir")?;
    let home_dir = tmp.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let data_dir = tmp.path().join("registry-data");

    let sample_dir = prepare_sample_workspace(&tmp)?;
    let built_app = build_sample_app(&sample_dir)?;
    let pack_dir = create_pack_project(&tmp, &built_app)?;
    let artifact_path = build_native_capsule(&tmp, &pack_dir)?;

    let Some((_guard, base_url)) = start_local_registry_or_skip(
        &ato,
        &data_dir,
        "e2e_native_delivery_sample_tauri_unsigned_finalize",
    )?
    else {
        return Ok(());
    };

    let artifact = artifact_path.to_string_lossy().to_string();
    let publish = run_ato_with_home(
        &ato,
        &[
            "publish",
            "--deploy",
            "--registry",
            &base_url,
            "--artifact",
            &artifact,
            "--scoped-id",
            "local/sample-native-capsule",
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    require_success(publish, "publish native capsule")?;

    let fetch_ref = format!(
        "127.0.0.1:{}/sample-native-capsule:0.1.1",
        base_url.rsplit(':').next().unwrap_or_default()
    );
    let fetch = run_ato_with_home(
        &ato,
        &["fetch", &fetch_ref, "--json"],
        tmp.path(),
        &home_dir,
    )?;
    let fetch = require_success(fetch, "fetch native capsule")?;
    let fetch_json: serde_json::Value =
        serde_json::from_slice(&fetch.stdout).context("parse fetch json")?;
    let fetched_dir = PathBuf::from(
        fetch_json["cache_dir"]
            .as_str()
            .context("fetch cache_dir missing")?,
    );
    let parent_digest = fetch_json["parent_digest"]
        .as_str()
        .context("fetch parent_digest missing")?
        .to_string();

    let parent_app = fetched_dir.join("artifact/sample-native-capsule.app");
    let parent_binary = parent_app.join("Contents/MacOS/sample-native-capsule");
    assert_executable(&parent_binary, "fetched parent binary")?;
    let parent_codesign = run_command(
        "codesign",
        &["-dv", parent_app.to_string_lossy().as_ref()],
        tmp.path(),
    )?;
    assert!(
        !parent_codesign.status.success(),
        "fetched parent artifact must remain unsigned"
    );

    let dist_dir = tmp.path().join("dist");
    let finalize = run_ato_with_home(
        &ato,
        &[
            "finalize",
            fetched_dir.to_string_lossy().as_ref(),
            "--allow-external-finalize",
            "--output-dir",
            dist_dir.to_string_lossy().as_ref(),
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let finalize = require_success(finalize, "finalize native capsule")?;
    let finalize_json: serde_json::Value =
        serde_json::from_slice(&finalize.stdout).context("parse finalize json")?;
    let derived_app = PathBuf::from(
        finalize_json["derived_app_path"]
            .as_str()
            .context("derived_app_path missing")?,
    );
    let provenance_path = PathBuf::from(
        finalize_json["provenance_path"]
            .as_str()
            .context("provenance_path missing")?,
    );
    let derived_digest = finalize_json["derived_digest"]
        .as_str()
        .context("derived_digest missing")?
        .to_string();
    let derived_binary = derived_app.join("Contents/MacOS/sample-native-capsule");
    assert_executable(&derived_binary, "finalized derived binary")?;

    let verify = run_command(
        "codesign",
        &[
            "--verify",
            "--deep",
            "--strict",
            derived_app.to_string_lossy().as_ref(),
        ],
        tmp.path(),
    )?;
    require_success(verify, "verify derived app codesign")?;

    let provenance: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&provenance_path)
            .with_context(|| format!("failed to read {}", provenance_path.display()))?,
    )
    .context("parse provenance json")?;
    assert_eq!(
        provenance["parent_digest"].as_str(),
        Some(parent_digest.as_str())
    );
    assert_eq!(
        provenance["derived_digest"].as_str(),
        Some(derived_digest.as_str())
    );
    assert_eq!(provenance["finalize_tool"].as_str(), Some("codesign"));

    let second_finalize = run_ato_with_home(
        &ato,
        &[
            "finalize",
            fetched_dir.to_string_lossy().as_ref(),
            "--allow-external-finalize",
            "--output-dir",
            dist_dir.to_string_lossy().as_ref(),
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let second_finalize = require_success(second_finalize, "second finalize native capsule")?;
    let second_json: serde_json::Value =
        serde_json::from_slice(&second_finalize.stdout).context("parse second finalize json")?;
    assert_eq!(
        second_json["parent_digest"].as_str(),
        Some(parent_digest.as_str())
    );
    assert_ne!(
        second_json["output_dir"].as_str(),
        finalize_json["output_dir"].as_str(),
        "finalize must create a fresh derived directory each run"
    );

    Ok(())
}

#[test]
fn e2e_native_delivery_projection_symlink_lifecycle() -> Result<()> {
    if !cfg!(target_os = "macos") || std::env::consts::ARCH != "aarch64" {
        eprintln!("skipping e2e_native_delivery_projection_symlink_lifecycle: darwin/arm64 only");
        return Ok(());
    }
    if !sample_project_dir().exists() {
        eprintln!(
            "skipping e2e_native_delivery_projection_symlink_lifecycle: sample project missing"
        );
        return Ok(());
    }
    if !command_on_path("deno") || !command_available("deno", &["--version"]) {
        eprintln!("skipping e2e_native_delivery_projection_symlink_lifecycle: deno unavailable");
        return Ok(());
    }
    if !command_on_path("codesign") {
        eprintln!(
            "skipping e2e_native_delivery_projection_symlink_lifecycle: codesign unavailable"
        );
        return Ok(());
    }

    let ato = assert_cmd::cargo::cargo_bin("ato");
    let tmp = TempDir::new().context("create temp dir")?;
    let home_dir = tmp.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let data_dir = tmp.path().join("registry-data");

    let sample_dir = prepare_sample_workspace(&tmp)?;
    let built_app = build_sample_app(&sample_dir)?;
    let pack_dir = create_pack_project(&tmp, &built_app)?;
    let artifact_path = build_native_capsule(&tmp, &pack_dir)?;

    let Some((_guard, base_url)) = start_local_registry_or_skip(
        &ato,
        &data_dir,
        "e2e_native_delivery_projection_symlink_lifecycle",
    )?
    else {
        return Ok(());
    };

    let artifact = artifact_path.to_string_lossy().to_string();
    let publish = run_ato_with_home(
        &ato,
        &[
            "publish",
            "--deploy",
            "--registry",
            &base_url,
            "--artifact",
            &artifact,
            "--scoped-id",
            "local/sample-native-capsule",
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    require_success(publish, "publish native capsule")?;

    let fetch_ref = format!(
        "127.0.0.1:{}/sample-native-capsule:0.1.1",
        base_url.rsplit(':').next().unwrap_or_default()
    );
    let fetch = run_ato_with_home(
        &ato,
        &["fetch", &fetch_ref, "--json"],
        tmp.path(),
        &home_dir,
    )?;
    let fetch = require_success(fetch, "fetch native capsule")?;
    let fetch_json: serde_json::Value =
        serde_json::from_slice(&fetch.stdout).context("parse fetch json")?;
    let fetched_dir = PathBuf::from(
        fetch_json["cache_dir"]
            .as_str()
            .context("fetch cache_dir missing")?,
    );

    let dist_dir = tmp.path().join("dist");
    let finalize = run_ato_with_home(
        &ato,
        &[
            "finalize",
            fetched_dir.to_string_lossy().as_ref(),
            "--allow-external-finalize",
            "--output-dir",
            dist_dir.to_string_lossy().as_ref(),
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let finalize = require_success(finalize, "finalize native capsule")?;
    let finalize_json: serde_json::Value =
        serde_json::from_slice(&finalize.stdout).context("parse finalize json")?;
    let derived_app = PathBuf::from(
        finalize_json["derived_app_path"]
            .as_str()
            .context("derived_app_path missing")?,
    );
    let fetched_artifact_dir = fetched_dir.join("artifact");
    let fetched_digest_before = compute_tree_digest(&fetched_artifact_dir)?;
    let derived_digest_before = compute_tree_digest(&derived_app)?;

    let launcher_dir = tmp.path().join("Applications");
    let project = run_ato_with_home(
        &ato,
        &[
            "project",
            derived_app.to_string_lossy().as_ref(),
            "--launcher-dir",
            launcher_dir.to_string_lossy().as_ref(),
            "--json",
        ],
        tmp.path(),
        &home_dir,
    )?;
    let project = require_success(project, "project finalized native capsule")?;
    let project_json: serde_json::Value =
        serde_json::from_slice(&project.stdout).context("parse project json")?;
    let projection_id = project_json["projection_id"]
        .as_str()
        .context("projection_id missing")?
        .to_string();
    let projected_path = PathBuf::from(
        project_json["projected_path"]
            .as_str()
            .context("projected_path missing")?,
    );
    let metadata_path = PathBuf::from(
        project_json["metadata_path"]
            .as_str()
            .context("metadata_path missing")?,
    );

    let projected_meta = fs::symlink_metadata(&projected_path)
        .with_context(|| format!("failed to stat {}", projected_path.display()))?;
    assert!(projected_meta.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&projected_path)
            .with_context(|| format!("failed to read {}", projected_path.display()))?,
        derived_app
    );
    assert!(metadata_path.exists());
    assert_eq!(
        compute_tree_digest(&fetched_artifact_dir)?,
        fetched_digest_before
    );
    assert_eq!(compute_tree_digest(&derived_app)?, derived_digest_before);

    let project_ls = run_ato_with_home(&ato, &["project", "ls", "--json"], tmp.path(), &home_dir)?;
    let project_ls = require_success(project_ls, "list projections")?;
    let project_ls_json: serde_json::Value =
        serde_json::from_slice(&project_ls.stdout).context("parse project ls json")?;
    let projections = project_ls_json["projections"]
        .as_array()
        .context("project ls projections missing")?;
    assert!(projections.iter().any(|projection| {
        projection["projection_id"].as_str() == Some(projection_id.as_str())
            && projection["state"].as_str() == Some("ok")
    }));

    let orphaned_app = derived_app
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sample-native-capsule-orphaned.app");
    fs::rename(&derived_app, &orphaned_app).with_context(|| {
        format!(
            "failed to move derived app {} -> {}",
            derived_app.display(),
            orphaned_app.display()
        )
    })?;

    let broken_ls = run_ato_with_home(&ato, &["project", "ls", "--json"], tmp.path(), &home_dir)?;
    let broken_ls = require_success(broken_ls, "list broken projections")?;
    let broken_ls_json: serde_json::Value =
        serde_json::from_slice(&broken_ls.stdout).context("parse broken project ls json")?;
    let broken_projection = broken_ls_json["projections"]
        .as_array()
        .context("broken project ls projections missing")?
        .iter()
        .find(|projection| projection["projection_id"].as_str() == Some(projection_id.as_str()))
        .context("projection entry missing after breakage")?;
    assert_eq!(broken_projection["state"].as_str(), Some("broken"));
    let problems = broken_projection["problems"]
        .as_array()
        .context("broken projection problems missing")?;
    assert!(problems
        .iter()
        .any(|problem| problem.as_str() == Some("derived_app_missing")));

    let unproject = run_ato_with_home(
        &ato,
        &["unproject", &projection_id, "--json"],
        tmp.path(),
        &home_dir,
    )?;
    let unproject = require_success(unproject, "unproject broken projection")?;
    let unproject_json: serde_json::Value =
        serde_json::from_slice(&unproject.stdout).context("parse unproject json")?;
    assert_eq!(
        unproject_json["projection_id"].as_str(),
        Some(projection_id.as_str())
    );
    assert!(!projected_path.exists());
    assert!(!metadata_path.exists());
    assert_eq!(
        compute_tree_digest(&fetched_artifact_dir)?,
        fetched_digest_before
    );

    let final_ls = run_ato_with_home(&ato, &["project", "ls", "--json"], tmp.path(), &home_dir)?;
    let final_ls = require_success(final_ls, "list projections after unproject")?;
    let final_ls_json: serde_json::Value =
        serde_json::from_slice(&final_ls.stdout).context("parse final project ls json")?;
    assert_eq!(final_ls_json["total"].as_u64(), Some(0));

    Ok(())
}
