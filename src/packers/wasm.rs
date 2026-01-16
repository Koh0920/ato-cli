use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tracing::warn;

use crate::runtime_router::ManifestData;
use crate::signing;

#[derive(Debug, Clone)]
pub struct WasmPackResult {
    pub artifact: PathBuf,
    pub signature: Option<PathBuf>,
}

pub fn pack(
    plan: &ManifestData,
    output: Option<PathBuf>,
    key_path: Option<PathBuf>,
) -> Result<WasmPackResult> {
    let component = resolve_component(plan)?;
    let source_path = plan.resolve_path(&component);

    let output_path = match output {
        Some(path) => path,
        None => {
            // Default: emit a copy alongside manifest to avoid mutating source
            let name = plan
                .manifest_name()
                .unwrap_or_else(|| "capsule".to_string());
            plan.manifest_dir.join(format!("{}.wasm", name))
        }
    };

    if output_path == source_path {
        warn!("Output path equals source; skipping optimization");
        let signature = if let Some(key) = key_path.as_ref() {
            Some(signing::sign_artifact(&source_path, key, "capsule-cli", None)?)
        } else {
            None
        };
        return Ok(WasmPackResult {
            artifact: source_path,
            signature,
        });
    }

    if which::which("wasm-opt").is_ok() {
        let status = Command::new("wasm-opt")
            .arg("-Oz")
            .arg(&source_path)
            .arg("-o")
            .arg(&output_path)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .context("Failed to run wasm-opt")?;
        if !status.success() {
            anyhow::bail!("wasm-opt failed");
        }
    } else {
        fs::copy(&source_path, &output_path)
            .with_context(|| format!("Failed to copy wasm: {}", source_path.display()))?;
    }

    println!("✅ Wasm artifact ready: {}", output_path.display());

    let signature = if let Some(key) = key_path {
        Some(signing::sign_artifact(&output_path, &key, "capsule-cli", None)?)
    } else {
        None
    };

    Ok(WasmPackResult {
        artifact: output_path,
        signature,
    })
}

fn resolve_component(plan: &ManifestData) -> Result<String> {
    if let Some(component) = plan.targets_wasm_component() {
        return Ok(component);
    }

    if let Some(entrypoint) = plan.execution_entrypoint() {
        if is_wasm_path(&entrypoint) {
            return Ok(entrypoint);
        }

        if let Ok(parsed) = shell_words::split(&entrypoint) {
            if let Some(first) = parsed.first() {
                if is_wasm_path(first) {
                    return Ok(first.to_string());
                }
            }
        }
    }

    anyhow::bail!("Wasm runtime selected but no component path found")
}

fn is_wasm_path(value: &str) -> bool {
    value.ends_with(".wasm") || value.ends_with(".component")
}
