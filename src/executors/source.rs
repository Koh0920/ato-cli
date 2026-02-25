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

/// Additional IPC environment variables to inject into the child process.
pub type IpcEnvVars = std::collections::HashMap<String, String>;

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
    ipc_env: Option<&IpcEnvVars>,
) -> Result<CapsuleProcess> {
    let force_python_no_bytecode = plan
        .execution_entrypoint()
        .map(|entry| entry.trim().to_ascii_lowercase().ends_with(".py"))
        .unwrap_or(false);
    let injected_port = plan
        .execution_runtime()
        .map(|runtime| runtime.trim().eq_ignore_ascii_case("web"))
        .unwrap_or(false)
        .then(|| plan.execution_port().map(|port| port.to_string()))
        .flatten();

    let nacelle = engine::discover_nacelle(engine::EngineRequest {
        explicit_path: nacelle_override.clone(),
        manifest_path: Some(plan.manifest_path.clone()),
    })?;

    r3_config::generate_and_write_config(
        &plan.manifest_path,
        Some(enforcement.to_string()),
        false,
    )?;

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
        Rt::Handle(h) => tokio::task::block_in_place(|| {
            h.block_on(run_bundle(
                &bundle_path,
                &plan.manifest_dir,
                reporter.clone(),
                mode,
                ipc_env,
                force_python_no_bytecode,
                injected_port.as_deref(),
            ))
        })?,
        Rt::Owned(runtime) => runtime.block_on(run_bundle(
            &bundle_path,
            &plan.manifest_dir,
            reporter.clone(),
            mode,
            ipc_env,
            force_python_no_bytecode,
            injected_port.as_deref(),
        ))?,
    };

    Ok(CapsuleProcess { child, bundle_path })
}

pub fn execute_host(
    plan: &ManifestData,
    _reporter: std::sync::Arc<CliReporter>,
    mode: ExecuteMode,
    ipc_env: Option<&IpcEnvVars>,
) -> Result<CapsuleProcess> {
    let entrypoint = plan
        .execution_entrypoint()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| {
            capsule_core::execution_plan::error::AtoExecutionError::policy_violation(
                "source/native target requires entrypoint",
            )
        })?;

    let runtime_dir = resolve_runtime_dir(&plan.manifest_dir, &entrypoint);
    let entrypoint_path = if Path::new(&entrypoint).is_absolute() {
        PathBuf::from(&entrypoint)
    } else {
        runtime_dir.join(&entrypoint)
    };
    let force_python_no_bytecode = is_python_entrypoint(plan, &entrypoint);
    let injected_port = plan
        .execution_runtime()
        .map(|runtime| runtime.trim().eq_ignore_ascii_case("web"))
        .unwrap_or(false)
        .then(|| plan.execution_port().map(|port| port.to_string()))
        .flatten();

    let mut cmd = if force_python_no_bytecode {
        let mut python = Command::new("python");
        python.arg(&entrypoint);
        python
    } else {
        Command::new(entrypoint_path)
    };

    cmd.current_dir(&runtime_dir);
    if let Some(proxy_env) = proxy::proxy_env_from_env(&[])? {
        proxy::apply_proxy_env(&mut cmd, &proxy_env);
    }
    apply_allowlisted_session_env(&mut cmd, ipc_env)?;
    apply_python_runtime_hardening(&mut cmd, force_python_no_bytecode);

    for (key, value) in plan.execution_env() {
        cmd.env(key, value);
    }
    if let Some(port) = injected_port {
        cmd.env("PORT", port);
    }

    let args = plan.targets_oci_cmd();
    if !args.is_empty() {
        cmd.args(args);
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
        .context("Failed to execute host process with --dangerously-skip-permissions")?;

    Ok(CapsuleProcess {
        child,
        bundle_path: PathBuf::new(),
    })
}

fn resolve_runtime_dir(manifest_dir: &Path, entrypoint: &str) -> PathBuf {
    let source_dir = manifest_dir.join("source");
    if source_dir.is_dir() && source_dir.join(entrypoint).exists() {
        return source_dir;
    }
    manifest_dir.to_path_buf()
}

fn is_python_entrypoint(plan: &ManifestData, entrypoint: &str) -> bool {
    if plan
        .execution_driver()
        .map(|driver| driver.trim().eq_ignore_ascii_case("python"))
        .unwrap_or(false)
    {
        return true;
    }

    entrypoint.trim().to_ascii_lowercase().ends_with(".py")
}

async fn run_bundle(
    bundle_path: &Path,
    manifest_dir: &Path,
    reporter: std::sync::Arc<CliReporter>,
    mode: ExecuteMode,
    ipc_env: Option<&IpcEnvVars>,
    force_python_no_bytecode: bool,
    injected_port: Option<&str>,
) -> Result<Child> {
    let mut cmd = Command::new(bundle_path);
    cmd.current_dir(manifest_dir);
    if let Some(proxy_env) = proxy::proxy_env_from_env(&[])? {
        proxy::apply_proxy_env(&mut cmd, &proxy_env);
    }

    apply_allowlisted_session_env(&mut cmd, ipc_env)?;
    apply_python_runtime_hardening(&mut cmd, force_python_no_bytecode);
    if let Some(port) = injected_port {
        cmd.env("PORT", port);
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

fn apply_python_runtime_hardening(cmd: &mut Command, force_python_no_bytecode: bool) {
    if force_python_no_bytecode {
        cmd.env("PYTHONDONTWRITEBYTECODE", "1");
    }
}

pub fn apply_allowlisted_session_env(
    cmd: &mut Command,
    ipc_env: Option<&IpcEnvVars>,
) -> Result<()> {
    let Some(env) = ipc_env else {
        return Ok(());
    };

    for (key, value) in env {
        if key.starts_with("CAPSULE_IPC_") || key == "ATO_BRIDGE_TOKEN" {
            cmd.env(key, value);
            continue;
        }

        return Err(
            capsule_core::execution_plan::error::AtoExecutionError::policy_violation(format!(
                "session_token env '{}' is not allowlisted",
                key
            ))
            .into(),
        );
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_python_runtime_hardening_sets_env() {
        let mut cmd = Command::new("echo");
        apply_python_runtime_hardening(&mut cmd, true);

        let value = cmd
            .get_envs()
            .find_map(|(key, value)| {
                if key == "PYTHONDONTWRITEBYTECODE" {
                    value.map(|v| v.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .expect("PYTHONDONTWRITEBYTECODE must be set");

        assert_eq!(value, "1");
    }

    #[test]
    fn test_apply_python_runtime_hardening_noop_when_disabled() {
        let mut cmd = Command::new("echo");
        apply_python_runtime_hardening(&mut cmd, false);

        let has_var = cmd
            .get_envs()
            .any(|(key, _)| key == "PYTHONDONTWRITEBYTECODE");

        assert!(!has_var, "PYTHONDONTWRITEBYTECODE must not be set");
    }
}
