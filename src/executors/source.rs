use anyhow::{Context, Result};
use rand::Rng;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use capsule_core::runtime::native::NativeHandle;
use capsule_core::{RuntimeMetadata, SessionRunner, SessionRunnerConfig};

use crate::reporters::CliReporter;

use capsule_core::engine;
use capsule_core::packers::bundle::{build_bundle, PackBundleArgs};
use capsule_core::r3_config;
use capsule_core::router::ManifestData;

use crate::common::proxy;

pub struct CapsuleProcess {
    pub child: Child,
    pub bundle_path: PathBuf,
}

pub enum ExecuteMode {
    Foreground,
    Background,
}

pub fn execute(
    plan: &ManifestData,
    nacelle_override: Option<PathBuf>,
    reporter: std::sync::Arc<CliReporter>,
    enforcement: &str,
    mode: ExecuteMode,
) -> Result<CapsuleProcess> {
    let nacelle = engine::discover_nacelle(engine::EngineRequest {
        explicit_path: nacelle_override.clone(),
        manifest_path: Some(plan.manifest_path.clone()),
    })?;

    r3_config::generate_and_write_config(&plan.manifest_path, Some(enforcement.to_string()))?;

    // Create a Tokio runtime for async pack/bundle operations.
    // Note: this function is sync, but it needs to run async code.
    // - If we're already inside a Tokio runtime, we must NOT create another runtime.
    // - Otherwise, create a fresh runtime.
    enum Rt<'a> {
        Handle(tokio::runtime::Handle),
        Owned(&'a tokio::runtime::Runtime),
    }

    let owned_rt;
    let rt = if let Ok(handle) = tokio::runtime::Handle::try_current() {
        Rt::Handle(handle)
    } else {
        owned_rt = tokio::runtime::Runtime::new()?;
        Rt::Owned(&owned_rt)
    };

    let bundle_path = {
        let mut rng = rand::thread_rng();
        let suffix: u64 = rng.gen();
        let output = std::env::temp_dir().join(format!("capsule-dev-{}.bundle", suffix));

        let reporter = reporter.clone();
        match &rt {
            Rt::Handle(h) => tokio::task::block_in_place(|| {
                h.block_on(build_bundle(
                    PackBundleArgs {
                        manifest_path: plan.manifest_path.clone(),
                        runtime_path: None,
                        output: Some(output),
                        nacelle_path: Some(nacelle),
                    },
                    reporter,
                ))
            })?,
            Rt::Owned(runtime) => runtime.block_on(build_bundle(
                PackBundleArgs {
                    manifest_path: plan.manifest_path.clone(),
                    runtime_path: None,
                    output: Some(output),
                    nacelle_path: Some(nacelle),
                },
                reporter,
            ))?,
        }
    };

    let child = match &rt {
        Rt::Handle(h) => tokio::task::block_in_place(|| h.block_on(run_bundle(
            &bundle_path,
            &plan.manifest_dir,
            reporter.clone(),
            mode,
        )))?,
        Rt::Owned(runtime) => runtime.block_on(run_bundle(
            &bundle_path,
            &plan.manifest_dir,
            reporter.clone(),
            mode,
        ))?,
    };

    Ok(CapsuleProcess { child, bundle_path })
}

async fn run_bundle(
    bundle_path: &Path,
    manifest_dir: &Path,
    reporter: std::sync::Arc<CliReporter>,
    mode: ExecuteMode,
) -> Result<Child> {
    let mut cmd = Command::new(bundle_path);
    cmd.current_dir(manifest_dir);
    if let Some(proxy_env) = proxy::proxy_env_from_env(&[])? {
        proxy::apply_proxy_env(&mut cmd, &proxy_env);
    }

    match mode {
        ExecuteMode::Foreground => {
            cmd.stdin(std::process::Stdio::inherit());
            cmd.stdout(std::process::Stdio::inherit());
            cmd.stderr(std::process::Stdio::inherit());
        }
        ExecuteMode::Background => {
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
        }
    }

    let child = cmd
        .spawn()
        .with_context(|| format!("Failed to execute bundle: {}", bundle_path.display()))?;

    Ok(child)
}

pub async fn wait_for_exit(child: &mut Child) -> Result<i32> {
    let pid = child.id();

    let session_id = format!("dev-{}", rand::thread_rng().gen::<u64>());
    let handle = NativeHandle::new(session_id, pid);
    let config = SessionRunnerConfig::default();

    let reporter = crate::reporters::CliReporter::new(false);
    let metrics = SessionRunner::new(handle, reporter)
        .with_config(config)
        .run()
        .await?;

    Ok(extract_exit_code(&metrics))
}

fn extract_exit_code(metrics: &capsule_core::UnifiedMetrics) -> i32 {
    match &metrics.metadata {
        RuntimeMetadata::Nacelle { exit_code, .. } => (*exit_code).unwrap_or(1),
        _ => 1,
    }
}
