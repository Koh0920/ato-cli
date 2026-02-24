use anyhow::{Context, Result};
use ctrlc;
use goblin::elf::dynamic::DT_VERNEED;
use goblin::elf::Elf;
use goblin::mach::load_command::CommandVariant;
use goblin::mach::{Mach, SingleArch};
use regex::Regex;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tracing::debug;

use crate::executors::source::ExecuteMode;
use crate::ipc::inject::IpcContext;
use crate::reporters::CliReporter;
use capsule_core::execution_plan::error::AtoExecutionError;
use capsule_core::execution_plan::guard::{self, ExecutorKind};
use capsule_core::{lockfile, router, CapsuleReporter};

mod watch;

const DEFAULT_DEBOUNCE_MS: u64 = 300;

pub struct OpenArgs {
    pub target: PathBuf,
    pub target_label: Option<String>,
    pub watch: bool,
    pub background: bool,
    pub nacelle: Option<PathBuf>,
    pub enforcement: String,
    pub unsafe_mode: bool,
    pub assume_yes: bool,
    pub reporter: Arc<CliReporter>,
}

pub async fn execute(args: OpenArgs) -> Result<()> {
    let target = args.target.clone();
    let target_name = target
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string());

    if target
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        == Some("capsule".to_string())
    {
        execute_capsule_file(&args, &target).await
    } else if target.is_dir() && target.join("capsule.toml").exists() {
        if args.watch {
            execute_watch_mode(args)
        } else {
            execute_normal_mode(args).await
        }
    } else if target.is_file() && target_name == Some("capsule.toml".to_string()) {
        if args.watch {
            execute_watch_mode(args)
        } else {
            execute_normal_mode(args).await
        }
    } else {
        anyhow::bail!(
            "Target is not a valid capsule: {} (expected .capsule file or directory with capsule.toml)",
            target.display()
        );
    }
}

async fn execute_capsule_file(args: &OpenArgs, capsule_path: &PathBuf) -> Result<()> {
    debug!(capsule = %capsule_path.display(), "Extracting capsule archive");

    let extract_dir = capsule_path
        .parent()
        .map(|p| {
            p.join(format!(
                "{}-extracted",
                capsule_path.file_stem().unwrap().to_string_lossy()
            ))
        })
        .context("Failed to determine extraction directory")?;

    if extract_dir.exists() {
        debug!(
            extract_dir = %extract_dir.display(),
            "Removing existing extracted directory before extraction"
        );
        fs::remove_dir_all(&extract_dir)?;
    }

    fs::create_dir_all(&extract_dir).with_context(|| {
        format!(
            "Failed to create extraction directory: {}",
            extract_dir.display()
        )
    })?;

    let mut archive = fs::File::open(capsule_path)
        .with_context(|| format!("Failed to open capsule file: {}", capsule_path.display()))?;

    let mut ar = tar::Archive::new(&mut archive);
    ar.unpack(&extract_dir)
        .with_context(|| format!("Failed to extract capsule to: {}", extract_dir.display()))?;

    debug!(extract_dir = %extract_dir.display(), "Capsule extracted");

    let payload_zst = extract_dir.join("payload.tar.zst");

    if payload_zst.exists() {
        debug!("Extracting payload bundle");

        let payload_tar = extract_dir.join("payload.tar");
        let decoder = zstd::stream::Decoder::new(
            fs::File::open(&payload_zst).with_context(|| "Failed to open payload.tar.zst")?,
        )
        .with_context(|| "Failed to create zstd decoder")?;

        let mut tar_reader = tar::Archive::new(decoder);
        tar_reader
            .unpack(&extract_dir)
            .with_context(|| "Failed to extract payload.tar.zst")?;

        fs::remove_file(&payload_zst).ok();
        fs::remove_file(&payload_tar).ok();

        debug!("Payload extracted");
    }

    let manifest_path = extract_dir.join("capsule.toml");
    if !manifest_path.exists() {
        anyhow::bail!("Extracted capsule does not contain capsule.toml");
    }

    let original_dir = {
        let parent = capsule_path.parent();
        if parent.map(|p| p.as_os_str().is_empty()).unwrap_or(true) {
            std::env::current_dir().context("Failed to get current directory")?
        } else if parent == Some(std::path::Path::new(".")) {
            std::env::current_dir().context("Failed to get current directory")?
        } else {
            parent.unwrap().to_path_buf()
        }
    };

    let has_source_files = check_has_source_files(&extract_dir);
    let original_has_source = check_has_source_files(&original_dir);

    if !has_source_files && original_has_source {
        debug!("Copying source files to extracted directory");

        copy_source_files(&original_dir, &extract_dir, &args.reporter).await?;

        debug!("Source files copied");
    }

    debug!(extract_dir = %extract_dir.display(), "Running extracted capsule");

    let open_args = OpenArgs {
        target: manifest_path,
        target_label: args.target_label.clone(),
        watch: args.watch,
        background: args.background,
        nacelle: args.nacelle.clone(),
        enforcement: args.enforcement.clone(),
        unsafe_mode: args.unsafe_mode,
        assume_yes: args.assume_yes,
        reporter: args.reporter.clone(),
    };

    execute_normal_mode(open_args).await
}

async fn copy_source_files(
    original_dir: &Path,
    extract_dir: &Path,
    _reporter: &Arc<CliReporter>,
) -> Result<()> {
    let entries = fs::read_dir(original_dir).with_context(|| {
        format!(
            "Failed to read original directory: {}",
            original_dir.display()
        )
    })?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();

        if path == extract_dir || path.starts_with(extract_dir) {
            continue;
        }

        if file_name == "capsule.toml" || file_name == "capsule.lock" || file_name == "config.json"
        {
            continue;
        }

        if path.is_dir() && file_name.to_string_lossy().ends_with("-extracted") {
            continue;
        }

        if path.is_file() {
            let should_skip_artifact = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| {
                    matches!(
                        ext.to_ascii_lowercase().as_str(),
                        "capsule" | "sig" | "bundle" | "zst" | "tar"
                    )
                })
                .unwrap_or(false);
            if should_skip_artifact {
                continue;
            }
        }

        if file_name == "source" && path.is_dir() {
            let dest = extract_dir.join("source");
            copy_dir_recursive(&path, &dest)?;
            debug!("Copied source/");
        } else if path.is_file() {
            let dest = extract_dir.join(&file_name);
            fs::copy(&path, &dest)?;
            debug!(file = %file_name.to_string_lossy(), "Copied file into extracted capsule");
        } else if path.is_dir() && !is_hidden(&file_name) {
            let dest = extract_dir.join(&file_name);
            copy_dir_recursive(&path, &dest)?;
            debug!(dir = %file_name.to_string_lossy(), "Copied directory into extracted capsule");
        }
    }

    Ok(())
}

fn check_has_source_files(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };

    let mut file_count = 0usize;
    let mut has_actual_source_files = false;

    for entry in entries {
        if let Ok(entry) = entry {
            file_count += 1;
            let file_name = entry.file_name();
            let path = entry.path();

            if file_name == "capsule.toml"
                || file_name == "capsule.lock"
                || file_name == "config.json"
                || file_name == "signature.json"
            {
                continue;
            }

            if path.is_file() {
                let name = file_name.to_string_lossy();
                if name == "package.json"
                    || name == "pyproject.toml"
                    || name == "requirements.txt"
                    || name == "go.mod"
                    || name == "Cargo.toml"
                {
                    return true;
                }
                if is_source_file(&file_name) {
                    return true;
                }
                has_actual_source_files = true;
            }

            if path.is_dir() && !is_hidden(&file_name) {
                if file_name == "source" {
                    if fs::read_dir(&path)
                        .ok()
                        .and_then(|mut it| it.next())
                        .is_some()
                    {
                        return true;
                    }
                }

                if path.join("package.json").exists()
                    || path.join("pyproject.toml").exists()
                    || path.join("index.js").exists()
                    || path.join("main.py").exists()
                {
                    return true;
                }
            }
        }
    }

    has_actual_source_files || (file_count > 5)
}

fn is_source_file(file_name: &std::ffi::OsString) -> bool {
    let exts = [
        "js", "ts", "py", "go", "rs", "json", "html", "css", "mjs", "cjs",
    ];
    if let Some(ext) = file_name.to_str().and_then(|s| s.rsplit('.').next()) {
        exts.contains(&ext)
    } else {
        false
    }
}

fn is_hidden(file_name: &std::ffi::OsString) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let bytes = file_name.as_os_str().as_bytes();
    bytes.first() == Some(&b'.') && bytes.len() > 1
}

fn copy_dir_recursive(from: &Path, to: &Path) -> Result<()> {
    if !to.exists() {
        fs::create_dir_all(to)?;
    }

    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let path = entry.path();
        let dest = to.join(entry.file_name());

        if path.is_dir() {
            copy_dir_recursive(&path, &dest)?;
        } else {
            fs::copy(&path, &dest)?;
        }
    }

    Ok(())
}

async fn execute_normal_mode(args: OpenArgs) -> Result<()> {
    let manifest_path = if args.target.is_dir() {
        args.target.join("capsule.toml")
    } else {
        args.target.clone()
    };

    let compiled = capsule_core::execution_plan::derive::compile_execution_plan(
        &manifest_path,
        router::ExecutionProfile::Dev,
        args.target_label.as_deref(),
    )?;
    let execution_plan = compiled.execution_plan;
    let decision = compiled.runtime_decision;
    let tier = compiled.tier;

    let lockfile_path = manifest_path.parent().map(|p| p.join("capsule.lock"));
    if let Some(lock_path) = lockfile_path {
        if lock_path.exists() {
            lockfile::verify_lockfile_manifest(&manifest_path, &lock_path).map_err(|err| {
                if err.to_string().contains("manifest hash mismatch") {
                    AtoExecutionError::lockfile_tampered(err.to_string(), Some("capsule.lock"))
                } else {
                    AtoExecutionError::policy_violation(err.to_string())
                }
            })?;
            debug!("capsule.lock integrity verified");
        }
    }

    let guard_result = guard::evaluate(
        &execution_plan,
        &decision.plan.manifest_dir,
        &args.enforcement,
        args.unsafe_mode,
    )?;

    crate::consent_store::require_consent(&execution_plan, args.assume_yes)?;

    debug!(
        runtime = execution_plan.target.runtime.as_str(),
        driver = execution_plan.target.driver.as_str(),
        ?tier,
        executor = ?guard_result.executor_kind,
        requires_unsafe_opt_in = guard_result.requires_unsafe_opt_in,
        "ExecutionPlan resolved"
    );

    let sidecar = match crate::common::sidecar::maybe_start_sidecar() {
        Ok(Some(sidecar)) => {
            debug!("Sidecar started");
            Some(sidecar)
        }
        Ok(None) => {
            debug!("Sidecar not available (no TSNET env)");
            None
        }
        Err(err) => {
            debug!(error = %err, "Sidecar start failed");
            None
        }
    };

    let mut sidecar_cleanup = crate::SidecarCleanup::new(sidecar, args.reporter.clone());

    let mode = if args.background {
        ExecuteMode::Background
    } else {
        ExecuteMode::Foreground
    };

    // ── IPC: Resolve imports and build IPC environment context ──
    let ipc_ctx = match IpcContext::from_manifest(&decision.plan.manifest) {
        Ok(ctx) => {
            if ctx.has_ipc() {
                debug!(
                    resolved_services = ctx.resolved_count,
                    injected_env_vars = ctx.env_vars.len(),
                    "IPC resolved"
                );
            }
            for warning in &ctx.warnings {
                debug!(warning = %warning, "IPC warning");
            }
            ctx
        }
        Err(err) => {
            debug!(error = %err, "IPC resolution failed");
            IpcContext::empty()
        }
    };
    let ipc_env = if ipc_ctx.has_ipc() {
        Some(&ipc_ctx.env_vars)
    } else {
        None
    };

    match guard_result.executor_kind {
        ExecutorKind::Native => {
            preflight_native_sandbox(args.nacelle.clone(), &decision.plan)?;

            let mut process = crate::executors::source::execute(
                &decision.plan,
                args.nacelle,
                args.reporter.clone(),
                &args.enforcement,
                mode,
                ipc_env,
            )?;

            if args.background {
                let pid = process.child.id();
                let id = format!("capsule-{}", pid);

                let info = crate::process_manager::ProcessInfo {
                    id: id.clone(),
                    name: decision
                        .plan
                        .manifest_path
                        .file_stem()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    pid: pid as i32,
                    status: crate::process_manager::ProcessStatus::Running,
                    runtime: "nacelle".to_string(),
                    start_time: std::time::SystemTime::now(),
                    manifest_path: Some(decision.plan.manifest_path.clone()),
                };

                let pm = crate::process_manager::ProcessManager::new()?;
                pm.write_pid(&info)?;

                args.reporter
                    .notify(format!("🚀 Capsule started in background (ID: {})", id))
                    .await?;

                drop(process);
                sidecar_cleanup.stop_now();
                return Ok(());
            }

            let exit_code = crate::executors::source::wait_for_exit(&mut process.child).await?;
            let _ = std::fs::remove_file(&process.bundle_path);

            sidecar_cleanup.stop_now();

            if exit_code != 0 {
                std::process::exit(exit_code);
            }
        }
        ExecutorKind::Wasm => {
            let exit =
                crate::executors::wasm::execute(&decision.plan, args.reporter.clone(), ipc_env)?;
            sidecar_cleanup.stop_now();
            if exit != 0 {
                std::process::exit(exit);
            }
        }
        ExecutorKind::BrowserStatic => {
            crate::executors::open_web::execute(&decision.plan, args.reporter.clone())?;
            sidecar_cleanup.stop_now();
        }
        ExecutorKind::Deno => {
            let exit = crate::executors::deno::execute(&decision.plan, &execution_plan, ipc_env)?;
            sidecar_cleanup.stop_now();
            if exit != 0 {
                std::process::exit(exit);
            }
        }
        ExecutorKind::NodeCompat => {
            let exit =
                crate::executors::node_compat::execute(&decision.plan, &execution_plan, ipc_env)?;
            sidecar_cleanup.stop_now();
            if exit != 0 {
                std::process::exit(exit);
            }
        }
    }

    Ok(())
}

fn execute_watch_mode(args: OpenArgs) -> Result<()> {
    futures::executor::block_on(CapsuleReporter::notify(
        &*args.reporter,
        "👀 Starting watch mode (foreground)".to_string(),
    ))?;

    let config = watch::WatchConfig::default();

    futures::executor::block_on(CapsuleReporter::notify(
        &*args.reporter,
        format!(
            "📊 Watch config: patterns={}, ignore={}, debounce={}ms",
            config.watch_patterns.join(", "),
            config.ignore_patterns.join(", "),
            config.debounce_ms
        ),
    ))?;

    let (_watcher, capsule_handle) =
        watch::watch_directory(args.target.clone(), config, args.reporter.clone())?;

    let reporter_for_cleanup = args.reporter.clone();

    ctrlc::set_handler(move || {
        let _ = capsule_handle.stop();
        let _ = futures::executor::block_on(CapsuleReporter::warn(
            &*reporter_for_cleanup,
            "👋 Watch mode stopped".to_string(),
        ));
        std::process::exit(0);
    })
    .map_err(|e| anyhow::anyhow!("Failed to set Ctrl+C handler: {:?}", e))?;

    std::thread::park();

    Ok(())
}

fn preflight_native_sandbox(
    nacelle_override: Option<PathBuf>,
    plan: &capsule_core::router::ManifestData,
) -> Result<()> {
    preflight_python_uv_lock_for_source_driver(plan)?;
    preflight_python_uv_binary_for_source_driver(plan)?;
    preflight_glibc_compat(plan)?;
    preflight_macos_compat(plan)?;

    let nacelle = capsule_core::engine::discover_nacelle(capsule_core::engine::EngineRequest {
        explicit_path: nacelle_override,
        manifest_path: Some(plan.manifest_path.clone()),
    })?;
    let response = capsule_core::engine::run_internal(
        &nacelle,
        "features",
        &json!({ "spec_version": "0.1.0" }),
    )?;
    let capabilities = response
        .get("data")
        .and_then(|v| v.get("capabilities"))
        .or_else(|| response.get("capabilities"));

    let sandbox = capabilities
        .and_then(|v| v.get("sandbox"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    if sandbox.is_empty() {
        return Err(AtoExecutionError::compat_hardware(
            "No compatible native sandbox backend is available",
            Some("sandbox"),
        )
        .into());
    }

    Ok(())
}

fn preflight_macos_compat(plan: &capsule_core::router::ManifestData) -> Result<()> {
    let required_raw = match detect_required_macos_from_entrypoint(plan)? {
        Some(value) => value,
        None => return Ok(()),
    };

    let required_version = normalize_version(&required_raw).ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            format!("Invalid macOS version constraint '{}'", required_raw),
            Some("macos"),
        )
    })?;

    let host_os = std::env::consts::OS;
    if host_os != "macos" {
        return Err(AtoExecutionError::compat_hardware(
            format!(
                "macOS {} is required but host OS is {}",
                required_raw, host_os
            ),
            Some("macos"),
        )
        .into());
    }

    let host_raw = detect_host_macos_version().ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            "Unable to detect host macOS version".to_string(),
            Some("macos"),
        )
    })?;

    let host_version = normalize_version(&host_raw).ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            format!("Unable to parse host macOS version '{}'", host_raw),
            Some("macos"),
        )
    })?;

    if compare_versions(&host_version, &required_version) < 0 {
        return Err(AtoExecutionError::compat_hardware(
            format!(
                "macOS {} is required but host has {}",
                required_raw, host_raw
            ),
            Some("macos"),
        )
        .into());
    }

    Ok(())
}

fn preflight_python_uv_lock_for_source_driver(
    plan: &capsule_core::router::ManifestData,
) -> Result<()> {
    if !is_python_source_target(plan) {
        return Ok(());
    }

    if resolve_python_dependency_lock_path(&plan.manifest_dir).is_some() {
        return Ok(());
    }

    Err(AtoExecutionError::lock_incomplete(
        "source/python target requires uv.lock for fail-closed provisioning",
        Some("uv.lock"),
    )
    .into())
}

fn preflight_python_uv_binary_for_source_driver(
    plan: &capsule_core::router::ManifestData,
) -> Result<()> {
    if !is_python_source_target(plan) {
        return Ok(());
    }

    let status = std::process::Command::new("uv")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(exit) if exit.success() => Ok(()),
        _ => Err(AtoExecutionError::lock_incomplete(
            "source/python target requires uv CLI (uv run --offline)",
            Some("uv"),
        )
        .into()),
    }
}

fn is_python_source_target(plan: &capsule_core::router::ManifestData) -> bool {
    let runtime = plan.execution_runtime().unwrap_or_default();
    if !runtime.eq_ignore_ascii_case("source") {
        return false;
    }

    let driver = plan.execution_driver().unwrap_or_default();
    if !driver.eq_ignore_ascii_case("native") && !driver.eq_ignore_ascii_case("python") {
        return false;
    }

    plan.execution_entrypoint()
        .map(|entry| entry.trim().to_ascii_lowercase().ends_with(".py"))
        .unwrap_or(false)
}

fn preflight_glibc_compat(plan: &capsule_core::router::ManifestData) -> Result<()> {
    let required_from_elf = detect_required_glibc_from_entrypoint(plan)?;

    let lock_path = match plan.manifest_path.parent() {
        Some(parent) => parent.join("capsule.lock"),
        None => {
            if required_from_elf.is_none() {
                return Ok(());
            }
            PathBuf::from("capsule.lock")
        }
    };

    let required_from_lock = detect_required_glibc_from_lock(&lock_path)?;
    let required_raw = match required_from_elf.or(required_from_lock) {
        Some(value) => value,
        None => return Ok(()),
    };

    let required_version = normalize_version(&required_raw).ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            format!("Invalid glibc version constraint '{}'", required_raw),
            Some("glibc"),
        )
    })?;

    let host_os = std::env::consts::OS;
    if host_os != "linux" {
        return Err(AtoExecutionError::compat_hardware(
            format!(
                "glibc {} is required but host OS is {}",
                required_raw, host_os
            ),
            Some("glibc"),
        )
        .into());
    }

    let host_raw = detect_host_glibc_version().ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            "Unable to detect host glibc version".to_string(),
            Some("glibc"),
        )
    })?;

    let host_version = normalize_version(&host_raw).ok_or_else(|| {
        AtoExecutionError::compat_hardware(
            format!("Unable to parse host glibc version '{}'", host_raw),
            Some("glibc"),
        )
    })?;

    if compare_versions(&host_version, &required_version) < 0 {
        return Err(AtoExecutionError::compat_hardware(
            format!(
                "glibc {} is required but host has {}",
                required_raw, host_raw
            ),
            Some("glibc"),
        )
        .into());
    }

    Ok(())
}

fn detect_required_glibc_from_lock(lock_path: &Path) -> Result<Option<String>> {
    if !lock_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(lock_path)
        .with_context(|| format!("Failed to read {}", lock_path.display()))?;
    let lockfile: capsule_core::lockfile::CapsuleLock =
        toml::from_str(&raw).with_context(|| format!("Failed to parse {}", lock_path.display()))?;

    Ok(lockfile
        .targets
        .values()
        .find_map(|target| target.constraints.as_ref().and_then(|c| c.glibc.clone())))
}

fn detect_required_glibc_from_entrypoint(
    plan: &capsule_core::router::ManifestData,
) -> Result<Option<String>> {
    let entrypoint = match plan
        .execution_entrypoint()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None => return Ok(None),
    };

    let path = {
        let candidate = PathBuf::from(entrypoint);
        if candidate.is_absolute() {
            candidate
        } else {
            plan.manifest_dir.join(candidate)
        }
    };

    if !path.exists() || !path.is_file() {
        return Ok(None);
    }

    let bytes = fs::read(&path)
        .with_context(|| format!("Failed to read native entrypoint {}", path.display()))?;
    if bytes.len() < 4 || &bytes[0..4] != b"\x7FELF" {
        return Ok(None);
    }

    let elf = Elf::parse(&bytes).map_err(|err| {
        AtoExecutionError::compat_hardware(
            format!(
                "Failed to parse ELF entrypoint '{}': {}",
                path.display(),
                err
            ),
            Some("glibc"),
        )
    })?;

    let has_verneed = elf
        .dynamic
        .as_ref()
        .map(|dynamic| {
            dynamic
                .dyns
                .iter()
                .any(|entry| entry.d_tag == DT_VERNEED as u64)
        })
        .unwrap_or(false);
    if !has_verneed {
        return Ok(None);
    }

    let regex =
        Regex::new(r"GLIBC_[0-9]+(?:\.[0-9]+)+").expect("failed to compile GLIBC version regex");
    let corpus = String::from_utf8_lossy(&bytes);

    let mut best_raw: Option<String> = None;
    let mut best_parts: Option<Vec<u32>> = None;
    for matched in regex.find_iter(&corpus).map(|m| m.as_str().to_string()) {
        let Some(parts) = normalize_version(&matched) else {
            continue;
        };
        if best_parts
            .as_ref()
            .map(|current| compare_versions(current, &parts) < 0)
            .unwrap_or(true)
        {
            best_raw = Some(matched);
            best_parts = Some(parts);
        }
    }

    Ok(best_raw)
}

fn detect_required_macos_from_entrypoint(
    plan: &capsule_core::router::ManifestData,
) -> Result<Option<String>> {
    let entrypoint = match plan
        .execution_entrypoint()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None => return Ok(None),
    };

    let path = {
        let candidate = PathBuf::from(entrypoint);
        if candidate.is_absolute() {
            candidate
        } else {
            plan.manifest_dir.join(candidate)
        }
    };

    if !path.exists() || !path.is_file() {
        return Ok(None);
    }

    let bytes = fs::read(&path)
        .with_context(|| format!("Failed to read native entrypoint {}", path.display()))?;
    let mach = match Mach::parse(&bytes) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };

    let mut best_raw: Option<String> = None;
    let mut best_parts: Option<Vec<u32>> = None;

    let mut update_best = |candidate: String| {
        let Some(parts) = normalize_version(&candidate) else {
            return;
        };
        if best_parts
            .as_ref()
            .map(|current| compare_versions(current, &parts) < 0)
            .unwrap_or(true)
        {
            best_raw = Some(candidate);
            best_parts = Some(parts);
        }
    };

    match mach {
        Mach::Binary(binary) => {
            if let Some(ver) = extract_min_macos_from_macho(&binary) {
                update_best(ver);
            }
        }
        Mach::Fat(fat) => {
            for entry in fat.into_iter() {
                let Ok(entry) = entry else {
                    continue;
                };
                if let SingleArch::MachO(binary) = entry {
                    if let Some(ver) = extract_min_macos_from_macho(&binary) {
                        update_best(ver);
                    }
                }
            }
        }
    }

    Ok(best_raw)
}

fn extract_min_macos_from_macho(binary: &goblin::mach::MachO<'_>) -> Option<String> {
    let mut best_raw: Option<String> = None;
    let mut best_parts: Option<Vec<u32>> = None;

    for cmd in &binary.load_commands {
        let raw = match &cmd.command {
            CommandVariant::BuildVersion(build) => decode_macho_version(build.minos),
            CommandVariant::VersionMinMacosx(min) => decode_macho_version(min.version),
            _ => None,
        };

        let Some(candidate) = raw else {
            continue;
        };
        let Some(parts) = normalize_version(&candidate) else {
            continue;
        };

        if best_parts
            .as_ref()
            .map(|current| compare_versions(current, &parts) < 0)
            .unwrap_or(true)
        {
            best_parts = Some(parts);
            best_raw = Some(candidate);
        }
    }

    best_raw
}

fn decode_macho_version(encoded: u32) -> Option<String> {
    let major = (encoded >> 16) & 0xffff;
    let minor = (encoded >> 8) & 0xff;
    let patch = encoded & 0xff;
    if major == 0 {
        return None;
    }
    Some(format!("{}.{}.{}", major, minor, patch))
}

fn normalize_version(value: &str) -> Option<Vec<u32>> {
    let normalized = value
        .trim()
        .trim_start_matches("GLIBC_")
        .trim_start_matches("GLIBC")
        .trim_start_matches("glibc")
        .trim_start_matches('-')
        .trim_start_matches('=')
        .trim();
    if normalized.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    for segment in normalized.split('.') {
        if segment.is_empty() {
            continue;
        }
        let digits = segment
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>();
        if digits.is_empty() {
            break;
        }
        let parsed = digits.parse::<u32>().ok()?;
        out.push(parsed);
    }

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn compare_versions(left: &[u32], right: &[u32]) -> i32 {
    let max_len = left.len().max(right.len());
    for idx in 0..max_len {
        let l = *left.get(idx).unwrap_or(&0);
        let r = *right.get(idx).unwrap_or(&0);
        if l < r {
            return -1;
        }
        if l > r {
            return 1;
        }
    }
    0
}

fn detect_host_glibc_version() -> Option<String> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let ptr = unsafe { libc::gnu_get_libc_version() };
        if ptr.is_null() {
            return None;
        }
        let cstr = unsafe { std::ffi::CStr::from_ptr(ptr) };
        return Some(cstr.to_string_lossy().to_string());
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        None
    }
}

fn detect_host_macos_version() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sw_vers")
            .arg("-productVersion")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if version.is_empty() {
            None
        } else {
            Some(version)
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

fn resolve_uv_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        manifest_dir.join("uv.lock"),
        manifest_dir.join("source").join("uv.lock"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn resolve_python_dependency_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    resolve_uv_lock_path(manifest_dir)
}

#[cfg(test)]
mod tests {
    use super::resolve_python_dependency_lock_path;

    #[test]
    fn resolve_python_dependency_detects_uv_lock() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        std::fs::create_dir_all(tmp.path().join("source")).expect("create source dir");
        std::fs::write(tmp.path().join("source").join("uv.lock"), "").expect("write uv.lock");

        let found = resolve_python_dependency_lock_path(tmp.path()).expect("must resolve uv.lock");
        assert_eq!(found, tmp.path().join("source").join("uv.lock"));
    }
}
