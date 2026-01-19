use std::path::PathBuf;

use crate::engine;
use crate::error::{CapsuleError, Result};
use crate::manifest;
use crate::packers::bundle::{build_bundle, PackBundleArgs};
use crate::r3_config;
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
}

pub fn pack(
    _plan: &ManifestData,
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

    futures::executor::block_on(
        reporter.notify("🧭 Phase 2: Generating R3 config.json".to_string()),
    )?;
    let config_path =
        r3_config::generate_and_write_config(&opts.manifest_path, Some(opts.enforcement))?;
    futures::executor::block_on(reporter.notify(format!(
        "   ✅ config.json generated: {}\n",
        config_path.display()
    )))?;

    futures::executor::block_on(
        reporter.notify("📦 Phase 3: Building bundle (embedded runtime)".to_string()),
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

    futures::executor::block_on(
        reporter.notify(format!("   ✅ Bundle created: {}\n", bundle_path.display())),
    )?;
    Ok(bundle_path)
}
