use anyhow::{Context, Result};
use rand::Rng;
use std::process::{Command, Stdio};
use tracing::warn;

use crate::hardware;
use crate::runtime_router::ManifestData;

#[derive(Debug, Clone)]
enum OciEngine {
    Docker,
    Podman,
}

pub fn execute(plan: &ManifestData) -> Result<i32> {
    let engine = detect_engine()?;
    let image = resolve_image(plan)?;
    let mut env = plan.execution_env();
    env.extend(plan.targets_oci_env());

    let mut cmd = Command::new(engine_binary(&engine));
    cmd.arg("run").arg("--rm");

    let name = format!("capsule-{}", rand::thread_rng().gen::<u32>());
    cmd.arg("--name").arg(name);

    if let Some(port) = plan.execution_port() {
        cmd.arg("-p").arg(format!("{port}:{port}"));
    }

    if let Some(workdir) = plan
        .targets_oci_working_dir()
        .or_else(|| plan.execution_working_dir())
    {
        cmd.arg("-w").arg(workdir);
    }

    if hardware::requires_gpu(&plan.manifest) {
        if let Some(report) = hardware::detect_nvidia_gpus()? {
            if report.count > 0 {
                cmd.arg("--gpus").arg("all");
            } else {
                warn!("GPU requested but none detected; continuing without --gpus");
            }
        } else {
            warn!("GPU requested but nvidia-smi unavailable; continuing without --gpus");
        }
    }

    for (k, v) in env {
        cmd.arg("--env").arg(format!("{}={}", k, v));
    }

    cmd.arg(image);

    let mut args = plan.targets_oci_cmd();
    if args.is_empty() {
        if let Some(entrypoint) = plan.execution_entrypoint() {
            if let Ok(parsed) = shell_words::split(&entrypoint) {
                args = parsed;
            }
        }
    }

    if !args.is_empty() {
        cmd.args(args);
    }

    let status = cmd
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("Failed to run OCI engine: {}", engine_binary(&engine)))?;

    Ok(status.code().unwrap_or(1))
}

fn detect_engine() -> Result<OciEngine> {
    if which::which("docker").is_ok() {
        return Ok(OciEngine::Docker);
    }
    if which::which("podman").is_ok() {
        return Ok(OciEngine::Podman);
    }
    anyhow::bail!("No OCI engine found (docker/podman)");
}

fn engine_binary(engine: &OciEngine) -> &'static str {
    match engine {
        OciEngine::Docker => "docker",
        OciEngine::Podman => "podman",
    }
}

fn resolve_image(plan: &ManifestData) -> Result<String> {
    if let Some(image) = plan.targets_oci_image() {
        return Ok(image);
    }
    if let Some(image) = plan.execution_image() {
        return Ok(image);
    }

    if let Some(runtime) = plan.execution_runtime() {
        if runtime.eq_ignore_ascii_case("oci") || runtime.eq_ignore_ascii_case("docker") {
            if let Some(entrypoint) = plan.execution_entrypoint() {
                return Ok(entrypoint);
            }
        }
    }

    anyhow::bail!("OCI runtime selected but no image specified")
}

