use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use capsule_core::router::ManifestData;

use crate::reporters::CliReporter;

pub fn execute(plan: &ManifestData, _reporter: std::sync::Arc<CliReporter>) -> Result<()> {
    let driver = plan
        .execution_driver()
        .map(|v| v.trim().to_ascii_lowercase())
        .ok_or_else(|| anyhow::anyhow!("runtime=web target requires driver"))?;
    if driver != "static" {
        anyhow::bail!(
            "open_web executor only supports runtime=web driver=static (got '{}')",
            driver
        );
    }

    if which::which("deno").is_err() {
        anyhow::bail!("deno is required for runtime=web driver=static execution");
    }

    let entrypoint = plan
        .execution_entrypoint()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("runtime=web target requires entrypoint"))?;
    let port = plan.execution_port().ok_or_else(|| {
        anyhow::anyhow!(
            "runtime=web target '{}' requires targets.<label>.port",
            plan.selected_target_label()
        )
    })?;

    let serve_dir = resolve_static_serve_dir(&plan.manifest_dir, &entrypoint)?;
    let args = build_deno_file_server_args(&serve_dir, port);

    let status = Command::new("deno")
        .args(&args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("Failed to launch deno file server for runtime=web static target")?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "deno file server exited with status {}",
            status
        ))
    }
}

fn resolve_static_serve_dir(manifest_dir: &Path, entrypoint: &str) -> Result<PathBuf> {
    let path = manifest_dir.join(entrypoint.trim());
    if !path.exists() || !path.is_dir() {
        anyhow::bail!(
            "runtime=web static entrypoint '{}' must be an existing directory",
            entrypoint
        );
    }

    let root = manifest_dir
        .canonicalize()
        .unwrap_or_else(|_| manifest_dir.to_path_buf());
    let canonical_path = path
        .canonicalize()
        .with_context(|| format!("Failed to resolve static entrypoint path '{}'", entrypoint))?;

    if !canonical_path.starts_with(&root) {
        anyhow::bail!(
            "runtime=web static entrypoint '{}' resolves outside manifest directory",
            entrypoint
        );
    }

    Ok(canonical_path)
}

fn build_deno_file_server_args(serve_dir: &Path, port: u16) -> Vec<String> {
    vec![
        "run".to_string(),
        "--no-prompt".to_string(),
        format!("--allow-read={}", serve_dir.to_string_lossy()),
        format!("--allow-net=127.0.0.1:{port},localhost:{port}"),
        "jsr:@std/http/file-server".to_string(),
        serve_dir.to_string_lossy().to_string(),
        "--host".to_string(),
        "127.0.0.1".to_string(),
        "--port".to_string(),
        port.to_string(),
        "--no-dir-listing".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deno_file_server_args_are_hardened_for_loopback_only() {
        let args = build_deno_file_server_args(Path::new("/tmp/site"), 61357);
        let rendered = args.join(" ");
        assert!(rendered.contains("--allow-read=/tmp/site"));
        assert!(rendered.contains("--allow-net=127.0.0.1:61357,localhost:61357"));
        assert!(rendered.contains("--host 127.0.0.1"));
        assert!(rendered.contains("--port 61357"));
        assert!(rendered.contains("--no-dir-listing"));
    }
}
