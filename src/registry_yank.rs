#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RegistryYankArgs {
    pub scoped_id: String,
    pub manifest_hash: String,
    pub registry_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryYankResult {
    pub scoped_id: String,
    pub target_manifest_hash: String,
    pub yanked: bool,
}

pub fn yank_manifest(args: RegistryYankArgs) -> Result<RegistryYankResult> {
    let scoped = crate::install::parse_capsule_ref(&args.scoped_id)?;
    let base_url = crate::registry_http::normalize_registry_url(&args.registry_url, "--registry")?;
    let endpoint = format!("{}/v1/manifest/yank", base_url);
    let payload = serde_json::json!({
        "scoped_id": scoped.scoped_id,
        "target_manifest_hash": args.manifest_hash,
    });

    let request = crate::registry_http::blocking_client_builder(&base_url)
        .build()
        .context("Failed to create registry yank client")?
        .post(&endpoint)
        .json(&payload);
    let request = crate::registry_http::with_blocking_ato_token(request);

    let response = request
        .send()
        .map_err(|err| anyhow::anyhow!("Failed to yank manifest via {}: {}", endpoint, err))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        let message = crate::registry_http::parse_error_message(status, &body);
        bail!("Registry yank failed ({}): {}", status.as_u16(), message);
    }

    response
        .json::<RegistryYankResult>()
        .context("Invalid local registry yank response")
}
