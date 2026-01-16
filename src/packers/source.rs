use anyhow::Result;
use std::path::PathBuf;

use crate::engine;
use crate::manifest;
use crate::packers::bundle::{build_bundle, PackBundleArgs};
use crate::r3_config;
use crate::resource::cas::create_cas_client_from_env;
use crate::runtime_router::ManifestData;
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

pub fn pack(_plan: &ManifestData, opts: SourcePackOptions) -> Result<PathBuf> {
    let rt = tokio::runtime::Runtime::new()?;

    let loaded = manifest::load_manifest(&opts.manifest_path)?;
    if let Some(targets) = loaded.model.targets.as_ref() {
        if let Some(digest) = targets.source_digest.as_ref() {
            println!("🧩 Phase 0: Checking CAS for source_digest");
            let cas = create_cas_client_from_env().map_err(|e| anyhow::anyhow!(e))?;
            let exists = rt
                .block_on(cas.exists(digest))
                .map_err(|e| anyhow::anyhow!(e))?;
            if !exists {
                anyhow::bail!("CAS blob not found for source_digest: {}", digest);
            }
        }
    }

    if !opts.skip_validation && !opts.skip_l1 {
        println!("🔍 Phase 1: L1 Source Policy Scan");
        let source_dir = opts.manifest_dir.join("source");
        if source_dir.exists() {
            let scan_extensions = &["py", "sh", "js", "ts", "go", "rs"];
            match validation::source_policy::scan_source_directory(&source_dir, scan_extensions) {
                Ok(()) => println!("   ✅ No dangerous patterns detected\n"),
                Err(e) => {
                    eprintln!("   ❌ L1 Policy violation: {}", e);
                    eprintln!("\n💡 Tip: Fix the security issue or use --skip-l1 (not recommended)");
                    anyhow::bail!("L1 Source Policy check failed");
                }
            }
        } else {
            println!("   ⚠️  No source/ directory found, skipping scan\n");
        }
    } else if opts.skip_l1 {
        println!("⚠️  Phase 1: L1 Source Policy Scan SKIPPED (--skip-l1)\n");
    }

    println!("🧭 Phase 2: Generating R3 config.json");
    let config_path =
        r3_config::generate_and_write_config(&opts.manifest_path, Some(opts.enforcement))?;
    println!("   ✅ config.json generated: {}\n", config_path.display());

    println!("📦 Phase 3: Building bundle (embedded runtime)");
    let nacelle = engine::discover_nacelle(engine::EngineRequest {
        explicit_path: opts.nacelle_override,
        manifest_path: Some(opts.manifest_path.clone()),
    })?;

    let bundle_path = rt.block_on(build_bundle(PackBundleArgs {
        manifest_path: opts.manifest_path.clone(),
        runtime_path: opts.runtime.clone(),
        output: opts.output.clone(),
        nacelle_path: Some(nacelle),
    }))?;

    println!("   ✅ Bundle created: {}\n", bundle_path.display());
    Ok(bundle_path)
}
