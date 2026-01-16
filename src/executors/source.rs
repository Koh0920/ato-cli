use anyhow::{Context, Result};
use rand::Rng;
use std::path::PathBuf;
use std::process::Command;

use crate::engine;
use crate::packers::bundle::{build_bundle, PackBundleArgs};
use crate::r3_config;
use crate::runtime_router::ManifestData;

pub fn execute(plan: &ManifestData, nacelle_override: Option<PathBuf>) -> Result<i32> {
    let nacelle = engine::discover_nacelle(engine::EngineRequest {
        explicit_path: nacelle_override.clone(),
        manifest_path: Some(plan.manifest_path.clone()),
    })?;

    r3_config::generate_and_write_config(&plan.manifest_path, Some("best_effort".to_string()))?;

    let bundle_path = {
        let mut rng = rand::thread_rng();
        let suffix: u64 = rng.gen();
        let output = std::env::temp_dir().join(format!("capsule-dev-{}.bundle", suffix));

        tokio::runtime::Runtime::new()?.block_on(build_bundle(PackBundleArgs {
            manifest_path: plan.manifest_path.clone(),
            runtime_path: None,
            output: Some(output),
            nacelle_path: Some(nacelle),
        }))?
    };

    let status = Command::new(&bundle_path)
        .current_dir(&plan.manifest_dir)
        .status()
        .with_context(|| format!("Failed to execute bundle: {}", bundle_path.display()))?;

    let _ = std::fs::remove_file(&bundle_path);

    Ok(status.code().unwrap_or(1))
}
