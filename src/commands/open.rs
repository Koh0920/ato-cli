use anyhow::{Context, Result};
use ctrlc;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::executors::source::ExecuteMode;
use crate::ipc::inject::IpcContext;
use crate::reporters::CliReporter;
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
    args.reporter
        .notify(format!(
            "📦 Extracting capsule archive: {}",
            capsule_path.display()
        ))
        .await?;

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
        args.reporter
            .warn(format!(
                "⚠️  Removing existing extracted directory: {}",
                extract_dir.display()
            ))
            .await?;
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

    args.reporter
        .notify(format!("✅ Extracted to: {}", extract_dir.display()))
        .await?;

    let payload_zst = extract_dir.join("payload.tar.zst");

    if payload_zst.exists() {
        args.reporter
            .notify("📦 Extracting payload bundle...".to_string())
            .await?;

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

        args.reporter
            .notify("✅ Payload extracted".to_string())
            .await?;
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
        args.reporter
            .notify("📦 Copying source files to extracted directory...".to_string())
            .await?;

        copy_source_files(&original_dir, &extract_dir, &args.reporter).await?;

        args.reporter
            .notify("✅ Source files copied".to_string())
            .await?;
    }

    args.reporter
        .notify(format!(
            "🚀 Running extracted capsule from: {}",
            extract_dir.display()
        ))
        .await?;

    let open_args = OpenArgs {
        target: manifest_path,
        target_label: args.target_label.clone(),
        watch: args.watch,
        background: args.background,
        nacelle: args.nacelle.clone(),
        enforcement: args.enforcement.clone(),
        reporter: args.reporter.clone(),
    };

    execute_normal_mode(open_args).await
}

async fn copy_source_files(
    original_dir: &Path,
    extract_dir: &Path,
    reporter: &Arc<CliReporter>,
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

        if file_name == "capsule.toml" || file_name == "capsule.lock" || file_name == "config.json"
        {
            continue;
        }

        if file_name == "source" && path.is_dir() {
            let dest = extract_dir.join("source");
            copy_dir_recursive(&path, &dest)?;
            reporter.notify(format!("   ✅ Copied source/")).await?;
        } else if path.is_file() {
            let dest = extract_dir.join(&file_name);
            fs::copy(&path, &dest)?;
            reporter
                .notify(format!("   ✅ Copied {}", file_name.to_string_lossy()))
                .await?;
        } else if path.is_dir() && !is_hidden(&file_name) {
            let dest = extract_dir.join(&file_name);
            copy_dir_recursive(&path, &dest)?;
            reporter
                .notify(format!("   ✅ Copied {}/", file_name.to_string_lossy()))
                .await?;
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
    let lockfile_path = manifest_path.parent().map(|p| p.join("capsule.lock"));

    if let Some(lock_path) = lockfile_path {
        if lock_path.exists() {
            lockfile::verify_lockfile_manifest(&manifest_path, &lock_path)?;
            args.reporter
                .notify("✅ capsule.lock integrity verified".to_string())
                .await?;
        } else {
            args.reporter
                .warn("⚠️  capsule.lock not found; integrity check skipped".to_string())
                .await?;
        }
    }

    let decision = router::route_manifest(
        &args.target,
        router::ExecutionProfile::Dev,
        args.target_label.as_deref(),
    )?;

    args.reporter
        .notify("🧭 RuntimeRouter: running in normal mode".to_string())
        .await?;

    let sidecar = match crate::common::sidecar::maybe_start_sidecar() {
        Ok(Some(sidecar)) => {
            args.reporter
                .notify("✅ Sidecar started".to_string())
                .await?;
            Some(sidecar)
        }
        Ok(None) => {
            args.reporter
                .warn("⚠️  Sidecar not available (no TSNET env)".to_string())
                .await?;
            None
        }
        Err(err) => {
            args.reporter
                .warn(format!("⚠️  Sidecar start failed: {}", err).to_string())
                .await?;
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
                args.reporter
                    .notify(format!(
                        "🔌 IPC: {} service(s) resolved, {} env var(s) injected",
                        ctx.resolved_count,
                        ctx.env_vars.len()
                    ))
                    .await?;
            }
            for warning in &ctx.warnings {
                args.reporter.warn(format!("⚠️  {}", warning)).await?;
            }
            ctx
        }
        Err(err) => {
            args.reporter
                .warn(format!("⚠️  IPC resolution failed: {}", err))
                .await?;
            IpcContext::empty()
        }
    };
    let ipc_env = if ipc_ctx.has_ipc() {
        Some(&ipc_ctx.env_vars)
    } else {
        None
    };

    match decision.kind {
        capsule_core::router::RuntimeKind::Source => {
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
        capsule_core::router::RuntimeKind::Oci => {
            crate::executors::oci::execute(&decision.plan, args.reporter.clone(), ipc_env)?;
        }
        capsule_core::router::RuntimeKind::Wasm => {
            crate::executors::wasm::execute(&decision.plan, args.reporter.clone(), ipc_env)?;
        }
        capsule_core::router::RuntimeKind::Web => {
            crate::executors::open_web::execute(&decision.plan, args.reporter.clone())?;
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
