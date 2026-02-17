use anyhow::{Context, Result};
use capsule_core::CapsuleReporter;
use std::io::IsTerminal;
use std::path::PathBuf;

use crate::init;
use crate::reporters;

pub fn execute_pack_command(
    dir: PathBuf,
    init_if_missing: bool,
    key: Option<PathBuf>,
    standalone: bool,
    keep_failed_artifacts: bool,
    enforcement: String,
    reporter: std::sync::Arc<reporters::CliReporter>,
    cli_json: bool,
    nacelle_override: Option<PathBuf>,
) -> Result<()> {
    let dir = dir
        .canonicalize()
        .with_context(|| format!("Failed to resolve directory: {}", dir.display()))?;
    if !dir.is_dir() {
        anyhow::bail!("Target is not a directory: {}", dir.display());
    }

    let manifest = dir.join("capsule.toml");
    if !manifest.exists() {
        let stdin_is_tty = std::io::stdin().is_terminal();
        if init_if_missing {
            if !stdin_is_tty {
                anyhow::bail!("--init requires an interactive TTY");
            }
            if cli_json {
                anyhow::bail!("--init cannot be used with --json output");
            }
            init::execute(
                init::InitArgs {
                    path: Some(dir.clone()),
                    yes: false,
                },
                reporter.clone(),
            )?;
        } else {
            anyhow::bail!(
                "capsule.toml not found. Use --init to create one, or specify a directory with capsule.toml."
            );
        }
    }

    if !manifest.exists() {
        anyhow::bail!("capsule.toml not found after initialization");
    }

    let decision = capsule_core::router::route_manifest(
        &manifest,
        capsule_core::router::ExecutionProfile::Release,
        None,
    )?;
    capsule_core::diagnostics::manifest::validate_manifest_for_build(
        &manifest,
        decision.plan.selected_target_label(),
    )?;

    futures::executor::block_on(
        reporter.notify("📦 Capsule Pack - Pure Runtime Architecture v3.0".to_string()),
    )?;
    futures::executor::block_on(
        reporter.notify("   Performing build-time validations...\n".to_string()),
    )?;

    futures::executor::block_on(reporter.notify(format!(
        "🧭 RuntimeRouter: {:?} ({})",
        decision.kind, decision.reason
    )))?;

    let manifest_dir = manifest
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    match decision.kind {
        capsule_core::router::RuntimeKind::Source => {
            let artifact_path = capsule_core::packers::source::pack(
                &decision.plan,
                capsule_core::packers::source::SourcePackOptions {
                    manifest_path: manifest.clone(),
                    manifest_dir: manifest_dir.clone(),
                    output: None,
                    runtime: None,
                    skip_l1: false,
                    skip_validation: false,
                    enforcement: enforcement.clone(),
                    nacelle_override,
                    standalone,
                },
                reporter.clone(),
            )?;

            if standalone {
                futures::executor::block_on(
                    reporter.warn(
                        "⚠️  Phase 1: --standalone build is not smoke-tested yet (planned in next phase)"
                            .to_string(),
                    ),
                )?;
            } else {
                futures::executor::block_on(
                    reporter.notify("🧪 Phase 4: Running smoke test".to_string()),
                )?;
                match capsule_core::smoke::run_capsule_smoke(
                    &artifact_path,
                    decision.plan.selected_target_label(),
                ) {
                    Ok(summary) => {
                        futures::executor::block_on(reporter.notify(format!(
                            "   ✅ Smoke passed (timeout={}ms, port={:?}, checks={})",
                            summary.startup_timeout_ms,
                            summary.required_port,
                            summary.checked_commands
                        )))?;
                    }
                    Err(err) => {
                        handle_failed_artifact(
                            &artifact_path,
                            keep_failed_artifacts,
                            reporter.clone(),
                            format!("Smoke test failed: {err}"),
                        )?;
                    }
                }
            }

            let _ = sign_if_requested(&artifact_path, key.as_ref(), reporter.clone())?;
            futures::executor::block_on(
                reporter.notify(format!("✅ Pack complete: {}", artifact_path.display())),
            )?;
        }
        capsule_core::router::RuntimeKind::Oci => {
            let result = capsule_core::packers::oci::pack(&decision.plan, None, reporter.as_ref())?;
            if let Some(path) = result.archive {
                let _ = sign_if_requested(&path, key.as_ref(), reporter.clone())?;
                futures::executor::block_on(
                    reporter.notify(format!("✅ Pack complete: {}", path.display())),
                )?;
            } else if key.is_some() {
                futures::executor::block_on(
                    reporter.warn(
                        "ℹ️  Signature skipped: OCI pack produced no archive file".to_string(),
                    ),
                )?;
            } else {
                futures::executor::block_on(
                    reporter.notify(format!("✅ Pack complete: {}", result.image)),
                )?;
            }
        }
        capsule_core::router::RuntimeKind::Wasm => {
            let result =
                capsule_core::packers::wasm::pack(&decision.plan, None, None, reporter.as_ref())?;
            futures::executor::block_on(
                reporter.notify(format!("✅ Pack complete: {}", result.artifact.display())),
            )?;
            let _ = sign_if_requested(&result.artifact, key.as_ref(), reporter.clone())?;
        }
        capsule_core::router::RuntimeKind::Web => {
            anyhow::bail!("runtime=web targets are not packable as executable runtime artifacts");
        }
    }

    Ok(())
}

fn handle_failed_artifact(
    artifact_path: &PathBuf,
    keep_failed_artifacts: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
    message: String,
) -> Result<()> {
    if keep_failed_artifacts {
        futures::executor::block_on(reporter.warn(format!(
            "⚠️  Build failed but artifact kept for debugging: {}",
            artifact_path.display()
        )))?;
        anyhow::bail!("{message}");
    }

    if artifact_path.exists() {
        if let Err(err) = std::fs::remove_file(artifact_path) {
            futures::executor::block_on(reporter.warn(format!(
                "⚠️  Failed to remove artifact after smoke failure: {} ({err})",
                artifact_path.display()
            )))?;
        }
    }

    anyhow::bail!("{message}");
}

fn sign_if_requested(
    target: &std::path::Path,
    key: Option<&PathBuf>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<Option<PathBuf>> {
    if let Some(key_path) = key {
        futures::executor::block_on(
            reporter.notify("🔐 Generating detached signature...".to_string()),
        )?;
        let sig_path = capsule_core::signing::sign_artifact(target, key_path, "ato-cli", None)?;
        futures::executor::block_on(
            reporter.notify(format!("✅ Signature: {}", sig_path.display())),
        )?;
        return Ok(Some(sig_path));
    }
    Ok(None)
}
