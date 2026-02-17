use std::path::PathBuf;

use crate::engine;
use crate::error::{CapsuleError, Result};
use crate::lockfile;
use crate::manifest;
use crate::packers::bundle::{build_bundle, PackBundleArgs};
use crate::packers::capsule as capsule_packer;
use crate::r3_config;
use crate::reporter::CapsuleReporter;
use crate::resource::cas::create_cas_client_from_env;
use crate::router::ManifestData;
use crate::validation;

#[derive(Debug, Clone)]
pub struct SourcePackOptions {
    pub manifest_path: PathBuf,
    pub manifest_dir: PathBuf,
    pub output: Option<PathBuf>,
    pub runtime: Option<PathBuf>,
    pub skip_l1: bool,
    pub skip_validation: bool,
    pub enforcement: String,
    pub nacelle_override: Option<PathBuf>,
    pub standalone: bool,
}

pub fn pack(
    plan: &ManifestData,
    opts: SourcePackOptions,
    reporter: std::sync::Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    let rt = tokio::runtime::Runtime::new()?;

    let loaded = manifest::load_manifest(&opts.manifest_path)?;
    if let Some(targets) = loaded.model.targets.as_ref() {
        if let Some(digest) = targets.source_digest.as_ref() {
            futures::executor::block_on(
                reporter.notify("🧩 Phase 0: Checking CAS for source_digest".to_string()),
            )?;
            let cas = create_cas_client_from_env()?;
            let exists = rt.block_on(cas.exists(digest))?;
            if !exists {
                return Err(CapsuleError::NotFound(format!(
                    "CAS blob not found for source_digest: {}",
                    digest
                )));
            }
        }
    }

    if !opts.skip_validation && !opts.skip_l1 {
        futures::executor::block_on(
            reporter.notify("🔍 Phase 1: L1 Source Policy Scan".to_string()),
        )?;
        let source_dir = opts.manifest_dir.join("source");
        if source_dir.exists() {
            let scan_extensions = &["py", "sh", "js", "ts", "go", "rs"];
            match validation::source_policy::scan_source_directory(&source_dir, scan_extensions) {
                Ok(()) => {
                    futures::executor::block_on(
                        reporter.notify("   ✅ No dangerous patterns detected\n".to_string()),
                    )?;
                }
                Err(e) => {
                    futures::executor::block_on(
                        reporter.warn(format!("   ❌ L1 Policy violation: {}", e)),
                    )?;
                    futures::executor::block_on(
                        reporter.warn(
                            "\n💡 Tip: Fix the security issue or use --skip-l1 (not recommended)"
                                .to_string(),
                        ),
                    )?;
                    return Err(CapsuleError::Pack(
                        "L1 Source Policy check failed".to_string(),
                    ));
                }
            }
        } else {
            futures::executor::block_on(
                reporter.warn("   ⚠️  No source/ directory found, skipping scan\n".to_string()),
            )?;
        }
    } else if opts.skip_l1 {
        futures::executor::block_on(
            reporter.warn("⚠️  Phase 1: L1 Source Policy Scan SKIPPED (--skip-l1)\n".to_string()),
        )?;
    }

    if !opts.skip_validation {
        futures::executor::block_on(
            reporter.notify("🔍 Phase 1b: Entrypoint Validation".to_string()),
        )?;
        validate_entrypoint(&opts.manifest_path, &opts.manifest_dir)?;
        futures::executor::block_on(reporter.notify("   ✅ Entrypoint file exists\n".to_string()))?;
    }

    futures::executor::block_on(
        reporter.notify("🧭 Phase 2: Generating R3 config.json".to_string()),
    )?;
    let enforcement = opts.enforcement.clone();
    let config_path = r3_config::generate_and_write_config(
        &opts.manifest_path,
        Some(enforcement.clone()),
        opts.standalone,
    )?;

    let config_reporter = reporter.clone();

    futures::executor::block_on(config_reporter.notify(format!(
        "   ✅ config.json generated: {}",
        config_path.display()
    )));

    let lockfile_path = rt.block_on(lockfile::generate_and_write_lockfile(
        &opts.manifest_path,
        &loaded.raw,
        &loaded.raw_text,
        config_reporter,
    ))?;

    let lockfile_reporter = reporter.clone();
    futures::executor::block_on(lockfile_reporter.notify(format!(
        "   ✅ capsule.lock generated: {}",
        lockfile_path.display()
    )));

    if opts.standalone {
        futures::executor::block_on(
            reporter.notify(
                "📦 Phase 3: Building self-extracting bundle (embedded runtime)".to_string(),
            ),
        )?;
        let nacelle = engine::discover_nacelle(engine::EngineRequest {
            explicit_path: opts.nacelle_override,
            manifest_path: Some(opts.manifest_path.clone()),
        })?;

        let bundle_path = rt.block_on(build_bundle(
            PackBundleArgs {
                manifest_path: opts.manifest_path.clone(),
                runtime_path: opts.runtime.clone(),
                output: opts.output.clone(),
                nacelle_path: Some(nacelle),
            },
            reporter.clone(),
        ))?;

        futures::executor::block_on(reporter.notify(format!(
            "   ✅ Self-extracting bundle created: {}\n",
            bundle_path.display()
        )))?;
        Ok(bundle_path)
    } else {
        futures::executor::block_on(
            reporter.notify("📦 Phase 3: Creating Capsule Archive (.capsule format)".to_string()),
        )?;

        let artifact_path = rt.block_on(capsule_packer::pack(
            plan,
            capsule_packer::CapsulePackOptions {
                manifest_path: opts.manifest_path.clone(),
                manifest_dir: opts.manifest_dir.clone(),
                output: opts.output.clone(),
                enforcement: enforcement.clone(),
                standalone: false,
            },
            reporter.clone(),
        ))?;

        futures::executor::block_on(reporter.notify(format!(
            "   ✅ Capsule created: {}\n",
            artifact_path.display()
        )))?;
        Ok(artifact_path)
    }
}

fn validate_entrypoint(manifest_path: &PathBuf, manifest_dir: &PathBuf) -> Result<()> {
    use std::fs;

    let manifest_content = fs::read_to_string(manifest_path)?;
    let manifest: toml::Value = manifest_content
        .parse()
        .map_err(|e| CapsuleError::Pack(format!("Failed to parse capsule.toml: {}", e)))?;

    let default_target = manifest
        .get("default_target")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CapsuleError::Pack("default_target is required".to_string()))?;

    let entrypoint = manifest
        .get("targets")
        .and_then(|t| t.as_table())
        .and_then(|t| t.get(default_target))
        .and_then(|s| s.get("entrypoint"))
        .and_then(|e| e.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CapsuleError::Pack("No entrypoint defined in capsule.toml".to_string()))?;

    let clean_entrypoint = entrypoint.trim_start_matches("./");

    if !clean_entrypoint.contains('/') && !clean_entrypoint.contains('\\') {
        if clean_entrypoint.contains(' ') || clean_entrypoint.contains('\t') {
            return Err(CapsuleError::Pack(format!(
                "Entrypoint '{}' contains whitespace. Use entrypoint for the command and command for arguments.",
                entrypoint
            )));
        }
        return Ok(());
    }

    let entrypoint_path = manifest_dir.join(clean_entrypoint);
    let source_entrypoint_path = manifest_dir.join("source").join(clean_entrypoint);

    if !entrypoint_path.exists() && !source_entrypoint_path.exists() {
        return Err(CapsuleError::Pack(format!(
            r#"Entrypoint not found

  The entrypoint defined in capsule.toml does not exist:
    Path: {}

  Checked locations:
    - Project root: {}
    - Source directory: {}

  Please ensure the file exists in the project root or source/ directory,
  or update the 'entrypoint' field in capsule.toml.
"#,
            entrypoint,
            entrypoint_path.display(),
            source_entrypoint_path.display()
        )));
    }

    Ok(())
}
