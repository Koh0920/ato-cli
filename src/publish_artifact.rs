use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct PublishArtifactArgs {
    pub artifact_path: PathBuf,
    pub scoped_id: String,
    pub registry_url: String,
    pub force_large_payload: bool,
    pub allow_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishArtifactResult {
    pub scoped_id: String,
    pub version: String,
    pub artifact_url: String,
    pub file_name: String,
    pub sha256: String,
    pub blake3: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub already_existed: bool,
}

#[derive(Debug)]
struct ArtifactPayload {
    publisher: String,
    slug: String,
    version: String,
    file_name: String,
    bytes: Vec<u8>,
    sha256: String,
    blake3: String,
}

#[derive(Debug, Clone)]
pub struct ArtifactManifestInfo {
    pub name: String,
    pub version: String,
    pub repository_owner: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegistryErrorPayload {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Error)]
pub enum PublishArtifactError {
    #[error("Artifact upload conflict (409 version_exists): {message}")]
    VersionExists { message: String },
    #[error("Artifact upload failed ({status}): {message}")]
    UploadFailed { status: u16, message: String },
}

pub fn publish_artifact(args: PublishArtifactArgs) -> Result<PublishArtifactResult> {
    let base_url = normalize_registry_url(&args.registry_url)?;
    crate::payload_guard::ensure_payload_size(
        &args.artifact_path,
        args.force_large_payload,
        "--force-large-payload",
    )?;
    let payload = load_artifact_payload(&args.artifact_path, &args.scoped_id)?;
    let endpoint = build_upload_endpoint(
        &base_url,
        &payload.publisher,
        &payload.slug,
        &payload.version,
        &payload.file_name,
        args.allow_existing,
    );

    let request = reqwest::blocking::Client::new()
        .put(&endpoint)
        .header("content-type", "application/octet-stream")
        .header("x-ato-sha256", &payload.sha256)
        .header("x-ato-blake3", &payload.blake3);

    let request = if let Some(token) = read_ato_token() {
        request.header("authorization", format!("Bearer {}", token))
    } else {
        request
    };

    let response = request
        .body(payload.bytes)
        .send()
        .with_context(|| format!("Failed to upload artifact to {}", endpoint))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        let error = classify_upload_failure(status, &body);
        return Err(error.into());
    }

    let result = response
        .json::<PublishArtifactResult>()
        .context("Invalid local registry upload response")?;
    Ok(result)
}

pub fn inspect_artifact_manifest(path: &Path) -> Result<ArtifactManifestInfo> {
    if !path.exists() {
        bail!("Artifact not found: {}", path.display());
    }
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| !ext.eq_ignore_ascii_case("capsule"))
        .unwrap_or(true)
    {
        bail!("--artifact must point to a .capsule file");
    }

    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read artifact: {}", path.display()))?;
    let manifest = extract_manifest_from_capsule(&bytes)?;
    let parsed = capsule_core::types::CapsuleManifest::from_toml(&manifest)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml from artifact: {}", err))?;

    Ok(ArtifactManifestInfo {
        name: parsed.name,
        version: parsed.version,
        repository_owner: extract_repository_owner(&manifest),
    })
}

fn build_upload_endpoint(
    base_url: &str,
    publisher: &str,
    slug: &str,
    version: &str,
    file_name: &str,
    allow_existing: bool,
) -> String {
    let mut endpoint = format!(
        "{}/v1/local/capsules/{}/{}/{}?file_name={}",
        base_url,
        urlencoding::encode(publisher),
        urlencoding::encode(slug),
        urlencoding::encode(version),
        urlencoding::encode(file_name)
    );
    if allow_existing {
        endpoint.push_str("&allow_existing=true");
    }
    endpoint
}

fn load_artifact_payload(path: &Path, scoped_id: &str) -> Result<ArtifactPayload> {
    if !path.exists() {
        bail!("Artifact not found: {}", path.display());
    }
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| !ext.eq_ignore_ascii_case("capsule"))
        .unwrap_or(true)
    {
        bail!("--artifact must point to a .capsule file");
    }

    let scoped = crate::install::parse_capsule_ref(scoped_id)?;
    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read artifact: {}", path.display()))?;
    let manifest = extract_manifest_from_capsule(&bytes)?;
    let parsed = capsule_core::types::CapsuleManifest::from_toml(&manifest)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml from artifact: {}", err))?;

    if parsed.name != scoped.slug {
        bail!(
            "--scoped-id slug '{}' must match artifact manifest.name '{}'",
            scoped.slug,
            parsed.name
        );
    }

    let file_name = format!("{}-{}.capsule", scoped.slug, parsed.version);

    Ok(ArtifactPayload {
        publisher: scoped.publisher,
        slug: scoped.slug,
        version: parsed.version,
        file_name,
        sha256: compute_sha256(&bytes),
        blake3: compute_blake3(&bytes),
        bytes,
    })
}

fn extract_manifest_from_capsule(bytes: &[u8]) -> Result<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let entries = archive
        .entries()
        .context("Failed to read .capsule archive entries")?;

    for entry in entries {
        let mut entry = entry.context("Invalid .capsule entry")?;
        let entry_path = entry
            .path()
            .context("Failed to read archive entry path")?
            .to_string_lossy()
            .to_string();
        if entry_path == "capsule.toml" {
            let mut manifest = String::new();
            entry
                .read_to_string(&mut manifest)
                .context("Failed to read capsule.toml from artifact")?;
            return Ok(manifest);
        }
    }

    bail!("Invalid artifact: capsule.toml not found in .capsule archive")
}

fn normalize_registry_url(raw: &str) -> Result<String> {
    let url = reqwest::Url::parse(raw)
        .with_context(|| format!("Invalid --registry URL for artifact publish: {}", raw))?;
    let scheme = url.scheme().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        bail!(
            "Registry URL must use http or https scheme (got '{}')",
            url.scheme()
        );
    }
    Ok(raw.trim().trim_end_matches('/').to_string())
}

fn classify_upload_failure(status: StatusCode, body: &str) -> PublishArtifactError {
    let parsed = serde_json::from_str::<RegistryErrorPayload>(body).ok();
    if is_version_exists_conflict(status, parsed.as_ref(), body) {
        let message = parsed
            .as_ref()
            .and_then(|v| v.message.as_deref())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("same version is already published")
            .to_string();
        return PublishArtifactError::VersionExists { message };
    }

    let message = parsed
        .as_ref()
        .and_then(|v| v.message.as_deref())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| body.trim())
        .to_string();

    PublishArtifactError::UploadFailed {
        status: status.as_u16(),
        message,
    }
}

fn is_version_exists_conflict(
    status: StatusCode,
    parsed: Option<&RegistryErrorPayload>,
    raw_body: &str,
) -> bool {
    if status != StatusCode::CONFLICT {
        return false;
    }

    if parsed
        .and_then(|v| v.error.as_deref())
        .map(|v| v.eq_ignore_ascii_case("version_exists"))
        .unwrap_or(false)
    {
        return true;
    }

    let message = parsed
        .and_then(|v| v.message.as_deref())
        .unwrap_or(raw_body)
        .to_ascii_lowercase();
    message.contains("same version is already published")
        || message.contains("version_exists")
        || message.contains("sha256 mismatch")
}

fn extract_repository_owner(manifest_raw: &str) -> Option<String> {
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

fn read_ato_token() -> Option<String> {
    std::env::var("ATO_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn compute_blake3(data: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    let hash = hasher.finalize();
    format!("blake3:{}", hex::encode(hash.as_bytes()))
}

fn compute_sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tar::Builder;

    fn test_capsule_bytes(name: &str, version: &str) -> Vec<u8> {
        let manifest = format!(
            r#"schema_version = "0.2"
name = "{name}"
version = "{version}"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "deno"
entrypoint = "main.ts"
"#
        );
        let mut buf = Vec::<u8>::new();
        {
            let mut builder = Builder::new(&mut buf);
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(manifest.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, "capsule.toml", manifest.as_bytes())
                .expect("append manifest");

            let sig = r#"{"signed":false}"#;
            let mut sig_header = tar::Header::new_gnu();
            sig_header.set_mode(0o644);
            sig_header.set_size(sig.len() as u64);
            sig_header.set_cksum();
            builder
                .append_data(&mut sig_header, "signature.json", sig.as_bytes())
                .expect("append signature");
            builder.finish().expect("finish tar");
        }
        buf
    }

    #[test]
    fn extract_manifest_from_capsule_succeeds() {
        let bytes = test_capsule_bytes("sample-capsule", "1.0.0");
        let manifest = extract_manifest_from_capsule(&bytes).expect("extract manifest");
        assert!(manifest.contains("name = \"sample-capsule\""));
        assert!(manifest.contains("version = \"1.0.0\""));
    }

    #[test]
    fn slug_mismatch_is_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("sample-capsule.capsule");
        std::fs::write(&path, test_capsule_bytes("sample-capsule", "1.0.0")).expect("write");

        let err = load_artifact_payload(&path, "koh0920/another-slug").expect_err("must fail");
        assert!(err
            .to_string()
            .contains("must match artifact manifest.name"));
    }

    #[test]
    fn hash_generation_is_stable() {
        let data = b"capsule-bytes";
        let s1 = compute_sha256(data);
        let s2 = compute_sha256(data);
        let b1 = compute_blake3(data);
        let b2 = compute_blake3(data);
        assert_eq!(s1, s2);
        assert_eq!(b1, b2);
        assert!(s1.starts_with("sha256:"));
        assert!(b1.starts_with("blake3:"));
    }

    #[test]
    fn build_upload_endpoint_appends_allow_existing() {
        let endpoint = build_upload_endpoint(
            "http://127.0.0.1:8787",
            "local",
            "demo-app",
            "1.0.0",
            "demo-app-1.0.0.capsule",
            true,
        );
        assert!(endpoint.contains("allow_existing=true"));
        assert!(endpoint.contains("file_name=demo-app-1.0.0.capsule"));
    }

    #[test]
    fn build_upload_endpoint_omits_allow_existing_by_default() {
        let endpoint = build_upload_endpoint(
            "http://127.0.0.1:8787",
            "local",
            "demo-app",
            "1.0.0",
            "demo-app-1.0.0.capsule",
            false,
        );
        assert!(!endpoint.contains("allow_existing="));
    }

    #[test]
    fn classify_upload_failure_detects_version_exists_from_status_and_message() {
        let err = classify_upload_failure(
            StatusCode::CONFLICT,
            r#"{"error":"other","message":"same version is already published"}"#,
        );
        assert!(matches!(err, PublishArtifactError::VersionExists { .. }));
    }
}
