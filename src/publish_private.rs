use std::fs;

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct PublishPrivateArgs {
    pub registry_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishPrivateResult {
    pub scoped_id: String,
    pub version: String,
    pub artifact_url: String,
    pub file_name: String,
    pub sha256: String,
    pub blake3: String,
    pub size_bytes: u64,
    pub registry_url: String,
}

pub fn execute(args: PublishPrivateArgs) -> Result<PublishPrivateResult> {
    let cwd = std::env::current_dir().context("Failed to resolve current directory")?;
    let manifest_path = cwd.join("capsule.toml");
    let manifest_raw = fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;

    let manifest = capsule_core::types::capsule_v1::CapsuleManifestV1::from_toml(&manifest_raw)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml: {}", err))?;

    let slug = manifest_slug(&manifest.name)?;
    let publisher = resolve_private_publisher(&manifest_raw);
    let scoped_id = format!("{}/{}", publisher, slug);

    let artifact_path = crate::publish_ci::build_capsule_artifact(
        &manifest_path,
        &manifest.name,
        &manifest.version,
    )
    .with_context(|| "Failed to build artifact for private registry publish")?;

    let uploaded = crate::publish_artifact::publish_artifact(
        crate::publish_artifact::PublishArtifactArgs {
            artifact_path,
            scoped_id,
            registry_url: args.registry_url.clone(),
        },
    )?;

    Ok(PublishPrivateResult {
        scoped_id: uploaded.scoped_id,
        version: uploaded.version,
        artifact_url: uploaded.artifact_url,
        file_name: uploaded.file_name,
        sha256: uploaded.sha256,
        blake3: uploaded.blake3,
        size_bytes: uploaded.size_bytes,
        registry_url: args.registry_url,
    })
}

fn resolve_private_publisher(manifest_raw: &str) -> String {
    if let Some(repo_owner) = manifest_repository_owner(manifest_raw) {
        return repo_owner;
    }

    if let Ok(origin) = crate::publish_preflight::run_git(&["remote", "get-url", "origin"]) {
        if let Some(repo) = crate::publish_preflight::normalize_origin_to_repo(&origin) {
            if let Some((owner, _)) = repo.split_once('/') {
                let normalized = normalize_segment(owner);
                if !normalized.is_empty() {
                    return normalized;
                }
            }
        }
    }

    "local".to_string()
}

fn manifest_repository_owner(manifest_raw: &str) -> Option<String> {
    let raw = crate::publish_preflight::find_manifest_repository(manifest_raw)?;
    let normalized = crate::publish_preflight::normalize_repository_value(&raw).ok()?;
    let (owner, _) = normalized.split_once('/')?;
    let owner = normalize_segment(owner);
    if owner.is_empty() {
        None
    } else {
        Some(owner)
    }
}

fn manifest_slug(raw: &str) -> Result<String> {
    let slug = raw.trim();
    if slug.is_empty() {
        anyhow::bail!("capsule.toml name is empty");
    }
    let parsed = crate::install::parse_capsule_ref(&format!("local/{}", slug))
        .with_context(|| "capsule.toml name must be lowercase kebab-case")?;
    if parsed.slug != slug {
        anyhow::bail!("capsule.toml name must be lowercase kebab-case");
    }
    Ok(slug.to_string())
}

fn normalize_segment(input: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;

    for ch in input.trim().to_ascii_lowercase().chars() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            out.push(ch);
            prev_dash = false;
            continue;
        }

        if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }

    out.trim_matches('-').to_string()
}
