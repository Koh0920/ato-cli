use anyhow::Result;
use ctrlc;
use std::path::PathBuf;
use std::sync::Arc;

use crate::executors::source::ExecuteMode;
use crate::reporters::CliReporter;
use capsule_core::{router, CapsuleReporter};

mod watch;

const DEFAULT_DEBOUNCE_MS: u64 = 300;

pub struct OpenArgs {
    pub target: PathBuf,
    pub watch: bool,
    pub background: bool,
    pub nacelle: Option<PathBuf>,
    pub enforcement: String,
    pub reporter: Arc<CliReporter>,
}

pub async fn execute(args: OpenArgs) -> Result<()> {
    if args.watch {
        execute_watch_mode(args)
    } else {
        execute_normal_mode(args).await
    }
}

async fn execute_normal_mode(args: OpenArgs) -> Result<()> {
    let decision = router::route_manifest(&args.target, router::ExecutionProfile::Dev)?;

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

    match decision.kind {
        capsule_core::router::RuntimeKind::Source => {
            let mut process = crate::executors::source::execute(
                &decision.plan,
                args.nacelle,
                args.reporter.clone(),
                &args.enforcement,
                mode,
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
            crate::executors::oci::execute(&decision.plan, args.reporter.clone())?;
        }
        capsule_core::router::RuntimeKind::Wasm => {
            crate::executors::wasm::execute(&decision.plan, args.reporter.clone())?;
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
