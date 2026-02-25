use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::{
    collections::{BTreeMap, BTreeSet},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    },
};

use anyhow::{Context, Result};

use capsule_core::execution_plan::canonical::{
    compute_policy_segment_hash, compute_provisioning_policy_hash,
};
use capsule_core::execution_plan::error::AtoExecutionError;
use capsule_core::execution_plan::model::{ExecutionPlan, ExecutionRuntime};
use capsule_core::router::ManifestData;

use crate::common::proxy;

use super::source::IpcEnvVars;

enum DependencyLock {
    Deno(PathBuf),
    PackageJson(PathBuf),
}

pub fn execute(
    plan: &ManifestData,
    execution_plan: &ExecutionPlan,
    ipc_env: Option<&IpcEnvVars>,
    dangerously_skip_permissions: bool,
) -> Result<i32> {
    if which::which("deno").is_err() {
        anyhow::bail!("deno is not installed or not on PATH");
    }

    verify_execution_plan_hashes(execution_plan)?;

    let entrypoint = plan
        .execution_entrypoint()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| {
            AtoExecutionError::policy_violation("source/deno target requires entrypoint")
        })?;

    let runtime_dir = resolve_deno_runtime_dir(&plan.manifest_dir, &entrypoint);
    let lock = resolve_dependency_lock(&plan.manifest_dir, &runtime_dir);
    let Some(lock) = lock else {
        return Err(AtoExecutionError::lock_incomplete(
            "deno.lock or package-lock.json is required for source/deno execution",
            Some("deno.lock"),
        )
        .into());
    };

    run_provisioning(plan, &runtime_dir, &entrypoint, &lock, ipc_env)?;
    run_runtime(
        plan,
        execution_plan,
        &runtime_dir,
        &entrypoint,
        &lock,
        ipc_env,
        dangerously_skip_permissions,
    )
}

fn run_provisioning(
    _plan: &ManifestData,
    runtime_dir: &Path,
    entrypoint: &str,
    lock: &DependencyLock,
    ipc_env: Option<&IpcEnvVars>,
) -> Result<()> {
    let mut cmd = Command::new("deno");
    cmd.current_dir(runtime_dir).arg("cache");
    match lock {
        DependencyLock::Deno(lock_path) => {
            cmd.arg("--lock").arg(lock_path).arg("--frozen");
        }
        DependencyLock::PackageJson(_) => {
            cmd.arg("--node-modules-dir=auto");
        }
    }
    cmd.arg(entrypoint)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    apply_session_tokens(&mut cmd, ipc_env)?;
    if let Some(proxy_env) = proxy::proxy_env_from_env(&[])? {
        proxy::apply_proxy_env(&mut cmd, &proxy_env);
    }

    let status = cmd.status().context("Failed to execute deno cache")?;
    if status.success() {
        Ok(())
    } else {
        let message = match lock {
            DependencyLock::Deno(_) => format!(
                "deno cache --lock --frozen failed with exit code {}",
                status.code().unwrap_or(1)
            ),
            DependencyLock::PackageJson(lock_path) => format!(
                "deno cache with package-lock.json fallback failed ({}): exit code {}",
                lock_path.display(),
                status.code().unwrap_or(1)
            ),
        };
        Err(AtoExecutionError::lock_incomplete(message, Some("deno.lock")).into())
    }
}

fn run_runtime(
    plan: &ManifestData,
    execution_plan: &ExecutionPlan,
    runtime_dir: &Path,
    entrypoint: &str,
    lock: &DependencyLock,
    ipc_env: Option<&IpcEnvVars>,
    dangerously_skip_permissions: bool,
) -> Result<i32> {
    let mut cmd = Command::new("deno");
    cmd.current_dir(runtime_dir).arg("run").arg("--no-prompt");
    if !dangerously_skip_permissions {
        cmd.arg("--cached-only");
    }

    match lock {
        DependencyLock::Deno(lock_path) => {
            cmd.arg("--lock").arg(lock_path).arg("--frozen");
        }
        DependencyLock::PackageJson(_) => {
            cmd.arg("--node-modules-dir=auto");
        }
    }

    if dangerously_skip_permissions {
        cmd.arg("-A");
    } else {
        if !execution_plan.runtime.policy.network.allow_hosts.is_empty() {
            cmd.arg(format!(
                "--allow-net={}",
                execution_plan.runtime.policy.network.allow_hosts.join(",")
            ));
        }

        let mut allow_read = execution_plan.runtime.policy.filesystem.read_only.clone();
        allow_read.extend(execution_plan.runtime.policy.filesystem.read_write.clone());
        if !allow_read.is_empty() {
            cmd.arg(format!("--allow-read={}", allow_read.join(",")));
        }

        if !execution_plan
            .runtime
            .policy
            .filesystem
            .read_write
            .is_empty()
        {
            cmd.arg(format!(
                "--allow-write={}",
                execution_plan
                    .runtime
                    .policy
                    .filesystem
                    .read_write
                    .join(",")
            ));
        }
    }

    for (key, value) in plan.execution_env() {
        cmd.env(key, value);
    }
    if execution_plan.target.runtime == ExecutionRuntime::Web {
        if let Some(port) = plan.execution_port() {
            cmd.env("PORT", port.to_string());
            if !dangerously_skip_permissions {
                cmd.arg("--allow-env=PORT");
            }
        }
    }

    #[cfg(unix)]
    let mut _secret_fd_guard: Option<std::fs::File> = None;

    #[cfg(unix)]
    {
        let secrets = collect_runtime_secrets(execution_plan);
        if !secrets.is_empty() {
            _secret_fd_guard = Some(inject_secrets_via_fd3(&mut cmd, &secrets)?);
            cmd.arg("--allow-env");
        }
    }

    apply_session_tokens(&mut cmd, ipc_env)?;

    if let Some(proxy_env) = proxy::proxy_env_from_env(&[])? {
        proxy::apply_proxy_env(&mut cmd, &proxy_env);
    }

    cmd.arg(entrypoint);
    let args = plan.targets_oci_cmd();
    if !args.is_empty() {
        cmd.args(args);
    }

    let output = cmd
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("Failed to execute deno run")?;

    if !output.stdout.is_empty() {
        let _ = std::io::stdout().write_all(&output.stdout);
        let _ = std::io::stdout().flush();
    }
    if !output.stderr.is_empty() {
        let _ = std::io::stderr().write_all(&output.stderr);
        let _ = std::io::stderr().flush();
    }

    let exit_code = output.status.code().unwrap_or(1);
    if exit_code != 0 {
        if let Some(err) = map_deno_permission_error(&output.stderr) {
            return Err(err.into());
        }
    }

    Ok(exit_code)
}

fn resolve_deno_runtime_dir(manifest_dir: &Path, entrypoint: &str) -> PathBuf {
    let source_dir = manifest_dir.join("source");
    if source_dir.is_dir() && source_dir.join(entrypoint).exists() {
        return source_dir;
    }
    manifest_dir.to_path_buf()
}

fn resolve_deno_lock_path(manifest_dir: &Path, runtime_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        runtime_dir.join("deno.lock"),
        manifest_dir.join("deno.lock"),
        manifest_dir.join("source").join("deno.lock"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn resolve_package_lock_path(manifest_dir: &Path, runtime_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        runtime_dir.join("package-lock.json"),
        manifest_dir.join("package-lock.json"),
        manifest_dir.join("source").join("package-lock.json"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn resolve_dependency_lock(manifest_dir: &Path, runtime_dir: &Path) -> Option<DependencyLock> {
    if let Some(lock_path) = resolve_deno_lock_path(manifest_dir, runtime_dir) {
        return Some(DependencyLock::Deno(lock_path));
    }

    resolve_package_lock_path(manifest_dir, runtime_dir).map(DependencyLock::PackageJson)
}

fn map_deno_permission_error(stderr: &[u8]) -> Option<AtoExecutionError> {
    let text = String::from_utf8_lossy(stderr);
    let lower = text.to_ascii_lowercase();

    if !lower.contains("notcapable") && !lower.contains("requires net access") {
        return None;
    }

    let target = extract_deno_net_target(&text);
    let message = if let Some(ref host) = target {
        format!("network policy violation: blocked egress to {}", host)
    } else {
        "network policy violation: blocked egress".to_string()
    };

    Some(AtoExecutionError::new(
        capsule_core::execution_plan::error::AtoErrorCode::AtoErrPolicyViolation,
        message,
        Some("network"),
        target.as_deref(),
        None,
    ))
}

fn extract_deno_net_target(stderr: &str) -> Option<String> {
    let marker = "Requires net access to \"";
    let start = stderr.find(marker)? + marker.len();
    let tail = &stderr[start..];
    let end = tail.find('"')?;
    let host_port = &tail[..end];
    let host = host_port.split(':').next().unwrap_or(host_port).trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

#[cfg(unix)]
fn collect_runtime_secrets(execution_plan: &ExecutionPlan) -> BTreeMap<String, String> {
    let mut keys = BTreeSet::new();

    for key in &execution_plan.runtime.policy.secrets.allow_secret_ids {
        if !key.trim().is_empty() {
            keys.insert(key.trim().to_string());
        }
    }

    if std::env::var_os("OPENAI_API_KEY").is_some() {
        keys.insert("OPENAI_API_KEY".to_string());
    }

    let mut secrets = BTreeMap::new();
    for key in keys {
        if let Ok(value) = std::env::var(&key) {
            if !value.is_empty() {
                secrets.insert(key, value);
            }
        }
    }

    secrets
}

#[cfg(unix)]
fn inject_secrets_via_fd3(
    cmd: &mut Command,
    secrets: &BTreeMap<String, String>,
) -> Result<std::fs::File> {
    let mut fds = [0; 2];
    let pipe_result = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if pipe_result != 0 {
        return Err(anyhow::anyhow!("failed to create secret pipe"));
    }

    let read_fd = fds[0];
    let write_fd = fds[1];

    let mut writer = unsafe { std::fs::File::from_raw_fd(write_fd) };
    let payload = serde_json::to_vec(secrets)
        .context("failed to serialize secret payload for fd injection")?;
    writer
        .write_all(&payload)
        .context("failed to write secret payload into fd pipe")?;
    drop(writer);

    let reader = unsafe { std::fs::File::from_raw_fd(read_fd) };
    let dup_from_fd = reader.as_raw_fd();

    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(dup_from_fd, 3) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if dup_from_fd != 3 {
                libc::close(dup_from_fd);
            }
            Ok(())
        });
    }

    cmd.env("ATO_SECRET_FD", "3");
    for key in secrets.keys() {
        cmd.env(format!("ATO_SECRET_FD_{key}"), "3");
        cmd.env_remove(key);
    }

    Ok(reader)
}

fn verify_execution_plan_hashes(execution_plan: &ExecutionPlan) -> Result<()> {
    let expected_policy_hash = compute_policy_segment_hash(
        &execution_plan.runtime,
        &execution_plan.consent.mount_set_algo_id,
        execution_plan.consent.mount_set_algo_version,
    )?;
    if expected_policy_hash != execution_plan.consent.policy_segment_hash {
        return Err(AtoExecutionError::lockfile_tampered(
            "policy_segment_hash mismatch detected before deno runtime",
            Some("policy_segment_hash"),
        )
        .into());
    }

    let expected_provisioning_hash =
        compute_provisioning_policy_hash(&execution_plan.provisioning)?;
    if expected_provisioning_hash != execution_plan.consent.provisioning_policy_hash {
        return Err(AtoExecutionError::lockfile_tampered(
            "provisioning_policy_hash mismatch detected before deno runtime",
            Some("provisioning_policy_hash"),
        )
        .into());
    }

    Ok(())
}

fn apply_session_tokens(cmd: &mut Command, ipc_env: Option<&IpcEnvVars>) -> Result<()> {
    let Some(ipc_env) = ipc_env else {
        return Ok(());
    };

    for (key, value) in ipc_env {
        if key.starts_with("CAPSULE_IPC_") || key == "ATO_BRIDGE_TOKEN" {
            cmd.env(key, value);
            continue;
        }

        return Err(AtoExecutionError::policy_violation(format!(
            "session_token env '{}' is not allowlisted",
            key
        ))
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{resolve_deno_lock_path, resolve_deno_runtime_dir, resolve_package_lock_path};

    #[test]
    fn deno_runtime_dir_uses_source_when_entrypoint_exists_only_there() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        std::fs::create_dir_all(tmp.path().join("source")).expect("create source dir");
        std::fs::write(
            tmp.path().join("source").join("main.ts"),
            "console.log('ok');",
        )
        .expect("write source entrypoint");

        let runtime_dir = resolve_deno_runtime_dir(tmp.path(), "main.ts");
        assert_eq!(runtime_dir, tmp.path().join("source"));
    }

    #[test]
    fn deno_lock_path_falls_back_to_source_dir() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        std::fs::create_dir_all(tmp.path().join("source")).expect("create source dir");
        std::fs::write(tmp.path().join("source").join("deno.lock"), "{}")
            .expect("write source deno lock");

        let runtime_dir = resolve_deno_runtime_dir(tmp.path(), "main.ts");
        let lock_path =
            resolve_deno_lock_path(tmp.path(), &runtime_dir).expect("must resolve deno.lock");
        assert_eq!(lock_path, tmp.path().join("source").join("deno.lock"));
    }

    #[test]
    fn package_lock_path_falls_back_to_source_dir() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        std::fs::create_dir_all(tmp.path().join("source")).expect("create source dir");
        std::fs::write(tmp.path().join("source").join("package-lock.json"), "{}")
            .expect("write source package-lock");

        let runtime_dir = resolve_deno_runtime_dir(tmp.path(), "main.ts");
        let lock_path = resolve_package_lock_path(tmp.path(), &runtime_dir)
            .expect("must resolve package-lock.json");
        assert_eq!(
            lock_path,
            tmp.path().join("source").join("package-lock.json")
        );
    }
}
