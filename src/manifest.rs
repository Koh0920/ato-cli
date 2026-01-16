use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::capsule_types::capsule_v1::{CapsuleManifestV1, TargetsConfig};

pub struct LoadedManifest {
    pub raw: toml::Value,
    pub model: CapsuleManifestV1,
    pub raw_text: String,
    pub path: PathBuf,
    pub dir: PathBuf,
}

pub fn load_manifest(path: &Path) -> Result<LoadedManifest> {
    let raw_text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read manifest: {}", path.display()))?;

    let raw: toml::Value = toml::from_str(&raw_text)
        .with_context(|| format!("Failed to parse manifest TOML: {}", path.display()))?;

    let mut model = CapsuleManifestV1::from_toml(&raw_text)
        .with_context(|| format!("Failed to parse manifest into schema: {}", path.display()))?;

    if let Err(errors) = model.validate() {
        let details = errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("Manifest validation failed: {}", details);
    }

    if let Some(targets) = model.targets.as_ref() {
        targets
            .validate_source_digest()
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    // Ensure schema_version is set for downstream consumers.
    if model.schema_version.trim().is_empty() {
        model.schema_version = "1.0".to_string();
    }

    let dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    Ok(LoadedManifest {
        raw,
        model,
        raw_text,
        path: path.to_path_buf(),
        dir,
    })
}

#[allow(dead_code)]
pub fn manifest_requires_cas_source(targets: &TargetsConfig) -> bool {
    targets.source.is_some() && targets.source_digest.is_some()
}
