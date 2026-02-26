//! Install command implementation
//!
//! Downloads and installs capsules from the Store.
//! Primary path: `/v1/capsules/by/:publisher/:slug/distributions` (.capsule contract)
//! Legacy fallback: `/v1/capsules/by/:publisher/:slug/download`

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::PathBuf;

use crate::registry::RegistryResolver;

const DEFAULT_STORE_DIR: &str = ".capsule/store";
const SEGMENT_MAX_LEN: usize = 63;

#[derive(Debug, Serialize)]
pub struct InstallResult {
    pub capsule_id: String,
    pub scoped_id: String,
    pub publisher: String,
    pub slug: String,
    pub version: String,
    pub path: PathBuf,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapsuleDetailSummary {
    pub scoped_id: String,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub latest_version: Option<String>,
    pub permissions: Option<CapsulePermissions>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CapsulePermissions {
    #[serde(default)]
    pub network: Option<CapsuleNetworkPermissions>,
    #[serde(default)]
    pub isolation: Option<CapsuleIsolationPermissions>,
    #[serde(default)]
    pub filesystem: Option<CapsuleFilesystemPermissions>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CapsuleNetworkPermissions {
    #[serde(default)]
    pub egress_allow: Vec<String>,
    #[serde(default)]
    pub connect_allowlist: Vec<String>,
}

impl CapsuleNetworkPermissions {
    pub fn merged_endpoints(&self) -> Vec<String> {
        let mut merged = self.egress_allow.clone();
        for endpoint in &self.connect_allowlist {
            if !merged.iter().any(|existing| existing == endpoint) {
                merged.push(endpoint.clone());
            }
        }
        merged
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CapsuleIsolationPermissions {
    #[serde(default)]
    pub allow_env: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CapsuleFilesystemPermissions {
    #[serde(default, alias = "read")]
    pub read_only: Vec<String>,
    #[serde(default, alias = "write")]
    pub read_write: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CapsuleDetail {
    id: String,
    #[serde(default, alias = "scopedId", alias = "scoped_id")]
    scoped_id: Option<String>,
    slug: String,
    name: String,
    description: String,
    price: u64,
    currency: String,
    #[serde(rename = "latestVersion", alias = "latest_version", default)]
    latest_version: Option<String>,
    releases: Vec<ReleaseInfo>,
    #[serde(default)]
    permissions: Option<CapsulePermissions>,
}

#[derive(Debug, Deserialize)]
struct ReleaseInfo {
    version: String,
    content_hash: String,
    #[serde(default)]
    signature_status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DistributionResponse {
    version: String,
    artifact_url: String,
    sha256: Option<String>,
    blake3: Option<String>,
    file_name: String,
    #[serde(default)]
    signature_status: Option<String>,
    #[serde(default)]
    publisher_verified: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScopedCapsuleRef {
    pub publisher: String,
    pub slug: String,
    pub scoped_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScopedSuggestion {
    pub scoped_id: String,
    pub downloads: u64,
}

#[derive(Debug, Deserialize)]
struct SuggestionCapsulesResponse {
    capsules: Vec<SuggestionCapsuleRow>,
}

#[derive(Debug, Deserialize)]
struct SuggestionCapsuleRow {
    slug: String,
    #[serde(default, alias = "scopedId", alias = "scoped_id")]
    scoped_id: Option<String>,
    #[serde(default)]
    downloads: Option<u64>,
    #[serde(default)]
    publisher: Option<SuggestionPublisher>,
}

#[derive(Debug, Deserialize)]
struct SuggestionPublisher {
    handle: String,
}

fn is_valid_segment(value: &str) -> bool {
    if value.is_empty() || value.len() > SEGMENT_MAX_LEN {
        return false;
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    let mut prev_hyphen = false;
    for ch in chars {
        let is_valid = ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-';
        if !is_valid {
            return false;
        }
        if ch == '-' && prev_hyphen {
            return false;
        }
        prev_hyphen = ch == '-';
    }
    !value.ends_with('-')
}

pub fn parse_capsule_ref(input: &str) -> Result<ScopedCapsuleRef> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
    }
    let normalized = trimmed.strip_prefix('@').unwrap_or(trimmed);
    let mut parts = normalized.split('/');
    let publisher = parts.next().unwrap_or_default().trim().to_lowercase();
    let slug = parts.next().unwrap_or_default().trim().to_lowercase();
    if parts.next().is_some() {
        bail!("invalid_capsule_ref: use publisher/slug (optionally @publisher/slug)");
    }
    if publisher.is_empty() || slug.is_empty() {
        bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
    }
    if !is_valid_segment(&publisher) || !is_valid_segment(&slug) {
        bail!("invalid_capsule_ref: publisher/slug must be lowercase kebab-case");
    }
    Ok(ScopedCapsuleRef {
        publisher: publisher.clone(),
        slug: slug.clone(),
        scoped_id: format!("{}/{}", publisher, slug),
    })
}

pub fn is_slug_only_ref(input: &str) -> bool {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return false;
    }
    let normalized = trimmed.strip_prefix('@').unwrap_or(trimmed);
    !normalized.contains('/')
}

pub async fn install_app(
    capsule_ref: &str,
    registry_url: Option<&str>,
    version: Option<&str>,
    output_dir: Option<PathBuf>,
    _set_default: bool,
    allow_unverified: bool,
    json_output: bool,
) -> Result<InstallResult> {
    let scoped_ref = parse_capsule_ref(capsule_ref)?;
    let registry = resolve_registry_url(registry_url, !json_output).await?;

    let client = reqwest::Client::new();
    let capsule_url = format!(
        "{}/v1/capsules/by/{}/{}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    let capsule: CapsuleDetail = client
        .get(&capsule_url)
        .send()
        .await
        .with_context(|| format!("Failed to connect to registry: {}", registry))?
        .json()
        .await
        .with_context(|| format!("Capsule not found: {}", scoped_ref.scoped_id))?;

    if capsule.price > 0 {
        bail!(
            "This capsule costs {} {}. Beta only supports free apps.",
            capsule.price,
            capsule.currency
        );
    }

    if !json_output {
        let latest_display = capsule
            .latest_version
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        eprintln!("📦 Found: {} v{}", capsule.name, latest_display);
        if !capsule.description.is_empty() {
            eprintln!("   {}", capsule.description);
        }
    }

    let target_version_owned = match version.map(str::trim).filter(|value| !value.is_empty()) {
        Some(explicit) => explicit.to_string(),
        None => capsule
            .latest_version
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No installable version available for '{}'. This capsule has no published release version.",
                    scoped_ref.scoped_id
                )
            })?,
    };
    let target_version = target_version_owned.as_str();
    let release = capsule
        .releases
        .iter()
        .find(|r| r.version == target_version)
        .with_context(|| format!("Version {} not found", target_version))?;

    let (download_url, sha256, blake3, file_name, signature_status) = match resolve_distribution(
        &client,
        &registry,
        &scoped_ref,
        target_version,
        allow_unverified,
    )
    .await
    {
        Ok(d) => (
            d.artifact_url,
            d.sha256,
            d.blake3,
            d.file_name,
            d.signature_status.unwrap_or_else(|| "unknown".to_string()),
        ),
        Err(_) => {
            if !json_output {
                eprintln!(
                    "ℹ️  Distribution API unavailable, falling back to legacy download endpoint"
                );
            }
            let legacy_download_url = resolve_legacy_download_url(
                &client,
                &registry,
                &scoped_ref,
                target_version,
                allow_unverified,
            )
            .await?;
            (
                legacy_download_url,
                None,
                if release.content_hash.starts_with("blake3:") {
                    Some(release.content_hash.clone())
                } else {
                    None
                },
                format!("{}-{}.capsule", scoped_ref.slug, target_version),
                release
                    .signature_status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
            )
        }
    };

    if !json_output {
        eprintln!("⬇️  Downloading {}", file_name);
    }

    let bytes = client
        .get(&download_url)
        .send()
        .await
        .with_context(|| "Failed to download artifact")?
        .bytes()
        .await
        .with_context(|| "Failed to read downloaded artifact")?;

    let computed_blake3 = compute_blake3(&bytes);
    ensure_signature_verified(&signature_status, allow_unverified)?;

    match (sha256.as_ref(), blake3.as_ref()) {
        (None, None) => {
            bail!("No hash metadata available for verification");
        }
        (Some(expected_sha), _) => {
            let got_sha = compute_sha256(&bytes);
            if !equals_hash(expected_sha, &got_sha) {
                bail!(
                    "SHA256 mismatch!\n  Expected: {}\n  Got: {}",
                    expected_sha,
                    got_sha
                );
            }
        }
        (None, Some(_)) => {}
    }

    if let Some(expected_blake3) = blake3.as_ref() {
        if !equals_hash(expected_blake3, &computed_blake3) {
            bail!(
                "BLAKE3 mismatch!\n  Expected: {}\n  Got: {}",
                expected_blake3,
                computed_blake3
            );
        }
    }

    let store_root = output_dir.unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(DEFAULT_STORE_DIR)
    });
    let install_dir = store_root
        .join(&scoped_ref.publisher)
        .join(&scoped_ref.slug)
        .join(target_version);
    std::fs::create_dir_all(&install_dir).with_context(|| {
        format!(
            "Failed to create store directory: {}",
            install_dir.display()
        )
    })?;

    let normalized_file_name = if file_name.to_lowercase().ends_with(".capsule") {
        file_name
    } else {
        format!("{}-{}.capsule", scoped_ref.slug, target_version)
    };

    let output_path = install_dir.join(normalized_file_name);
    let mut file = std::fs::File::create(&output_path)
        .with_context(|| format!("Failed to create file: {}", output_path.display()))?;
    file.write_all(&bytes)?;

    if !json_output {
        eprintln!("✅ Installed to: {}", output_path.display());
        eprintln!("   To run: ato run {}", output_path.display());
    }

    Ok(InstallResult {
        capsule_id: capsule.id,
        scoped_id: scoped_ref.scoped_id.clone(),
        publisher: scoped_ref.publisher,
        slug: capsule.slug,
        version: release.version.clone(),
        path: output_path,
        content_hash: computed_blake3,
    })
}

pub async fn fetch_capsule_detail(
    capsule_ref: &str,
    registry_url: Option<&str>,
) -> Result<CapsuleDetailSummary> {
    let scoped_ref = parse_capsule_ref(capsule_ref)?;
    let registry = resolve_registry_url(registry_url, false).await?;
    let client = reqwest::Client::new();
    let capsule_url = format!(
        "{}/v1/capsules/by/{}/{}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    let capsule: CapsuleDetail = client
        .get(&capsule_url)
        .send()
        .await
        .with_context(|| format!("Failed to connect to registry: {}", registry))?
        .json()
        .await
        .with_context(|| format!("Capsule not found: {}", scoped_ref.scoped_id))?;

    Ok(CapsuleDetailSummary {
        scoped_id: capsule
            .scoped_id
            .unwrap_or_else(|| scoped_ref.scoped_id.clone()),
        slug: capsule.slug,
        name: capsule.name,
        description: capsule.description,
        latest_version: capsule
            .latest_version
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        permissions: capsule.permissions,
    })
}

async fn resolve_registry_url(registry_url: Option<&str>, emit_log: bool) -> Result<String> {
    if let Some(url) = registry_url {
        return Ok(url.to_string());
    }

    let resolver = RegistryResolver::default();
    let info = resolver.resolve("localhost").await?;
    if emit_log {
        eprintln!(
            "📡 Using registry: {} ({})",
            info.url,
            format!("{:?}", info.source).to_lowercase()
        );
    }
    Ok(info.url)
}

async fn resolve_distribution(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    version: &str,
    allow_unverified: bool,
) -> Result<DistributionResponse> {
    let os = normalize_os(std::env::consts::OS);
    let arch = normalize_arch(std::env::consts::ARCH);
    let url = format!(
        "{}/v1/capsules/by/{}/{}/distributions?os={}&arch={}&channel=stable&version={}&allow_unverified={}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug),
        urlencoding::encode(os),
        urlencoding::encode(arch),
        urlencoding::encode(version),
        if allow_unverified { "true" } else { "false" },
    );

    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| "Failed to resolve distribution")?;
    if !response.status().is_success() {
        bail!("Distribution resolution failed with {}", response.status());
    }
    let payload = response
        .json::<DistributionResponse>()
        .await
        .with_context(|| "Invalid distribution response")?;
    Ok(payload)
}

async fn resolve_legacy_download_url(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    version: &str,
    allow_unverified: bool,
) -> Result<String> {
    let download_url = format!(
        "{}/v1/capsules/by/{}/{}/download?version={}&allow_unverified={}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug),
        urlencoding::encode(version),
        if allow_unverified { "true" } else { "false" },
    );

    let response = client
        .get(&download_url)
        .send()
        .await
        .with_context(|| "Failed to get legacy download URL")?;

    if response.status().is_redirection() {
        let location = response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .with_context(|| "Missing Location header in redirect")?;
        return Ok(location.to_string());
    }

    if response.status().is_success() {
        return Ok(download_url);
    }

    bail!("Legacy download URL failed: {}", response.status())
}

fn normalize_os(raw: &str) -> &str {
    match raw {
        "macos" => "macos",
        "windows" => "windows",
        "linux" => "linux",
        _ => "any",
    }
}

fn normalize_arch(raw: &str) -> &str {
    match raw {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        _ => "any",
    }
}

fn compute_blake3(data: &[u8]) -> String {
    use blake3::Hasher;
    let mut hasher = Hasher::new();
    hasher.update(data);
    let hash = hasher.finalize();
    format!("blake3:{}", hex::encode(hash.as_bytes()))
}

fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn equals_hash(expected: &str, got_raw: &str) -> bool {
    let normalized_expected = expected
        .strip_prefix("sha256:")
        .or_else(|| expected.strip_prefix("blake3:"))
        .unwrap_or(expected)
        .to_lowercase();
    let normalized_got = got_raw
        .strip_prefix("sha256:")
        .or_else(|| got_raw.strip_prefix("blake3:"))
        .unwrap_or(got_raw)
        .to_lowercase();
    normalized_expected == normalized_got
}

fn ensure_signature_verified(signature_status: &str, allow_unverified: bool) -> Result<()> {
    if signature_status == "verified" || (allow_unverified && signature_status == "unverified") {
        return Ok(());
    }
    if allow_unverified {
        bail!(
            "Signature verification failed even with --allow-unverified: signature_status={}",
            signature_status
        );
    }
    bail!(
        "Signature verification failed: signature_status={}",
        signature_status
    );
}

pub async fn suggest_scoped_capsules(
    slug: &str,
    registry_url: Option<&str>,
    limit: usize,
) -> Result<Vec<ScopedSuggestion>> {
    let registry = resolve_registry_url(registry_url, false).await?;
    let client = reqwest::Client::new();
    let url = format!(
        "{}/v1/capsules?q={}&limit={}",
        registry,
        urlencoding::encode(slug),
        limit.clamp(1, 10)
    );
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| "Failed to fetch capsule suggestions")?;
    if !response.status().is_success() {
        return Ok(vec![]);
    }

    let payload = response
        .json::<SuggestionCapsulesResponse>()
        .await
        .with_context(|| "Invalid suggestions response")?;

    let needle = slug.trim().to_lowercase();
    let mut suggestions: Vec<ScopedSuggestion> = payload
        .capsules
        .into_iter()
        .filter_map(|capsule| {
            let scoped_id = capsule.scoped_id.or_else(|| {
                capsule
                    .publisher
                    .as_ref()
                    .map(|publisher| format!("{}/{}", publisher.handle, capsule.slug))
            })?;
            let capsule_slug = capsule.slug.to_lowercase();
            if capsule_slug != needle && !capsule_slug.ends_with(&needle) {
                return None;
            }
            Some(ScopedSuggestion {
                scoped_id,
                downloads: capsule.downloads.unwrap_or(0),
            })
        })
        .collect();
    suggestions.sort_by(|a, b| b.downloads.cmp(&a.downloads));
    suggestions.truncate(3);
    Ok(suggestions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_blake3() {
        let data = b"hello world";
        let hash = compute_blake3(data);
        assert!(hash.starts_with("blake3:"));
        assert_eq!(hash.len(), 7 + 64);
    }

    #[test]
    fn test_compute_sha256() {
        let data = b"hello world";
        let hash = compute_sha256(data);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_equals_hash() {
        let value = "b94d27b9934d3e08a52e52d7da7dabfade4f3e9e64c94f4db5d4ef7d6df4f6f6";
        assert!(equals_hash(value, value));
        assert!(equals_hash(&format!("sha256:{}", value), value));
        assert!(equals_hash(&format!("blake3:{}", value), value));
    }

    #[test]
    fn test_ensure_signature_verified() {
        assert!(ensure_signature_verified("verified", false).is_ok());
        assert!(ensure_signature_verified("unverified", false).is_err());
        assert!(ensure_signature_verified("unverified", true).is_ok());
        assert!(ensure_signature_verified("unknown", true).is_err());
    }

    #[test]
    fn test_permissions_deserialization_with_aliases() {
        let payload = r#"{
            "network": {
                "egress_allow": ["api.example.com"],
                "connect_allowlist": ["wss://ws.example.com"]
            },
            "isolation": {
                "allow_env": ["OPENAI_API_KEY"]
            },
            "filesystem": {
                "read": ["/opt/data"],
                "write": ["/tmp"]
            }
        }"#;

        let permissions: CapsulePermissions = serde_json::from_str(payload).unwrap();
        let network = permissions.network.unwrap();
        assert_eq!(network.merged_endpoints().len(), 2);
        assert_eq!(
            permissions.isolation.unwrap().allow_env,
            vec!["OPENAI_API_KEY".to_string()]
        );
        let filesystem = permissions.filesystem.unwrap();
        assert_eq!(filesystem.read_only, vec!["/opt/data".to_string()]);
        assert_eq!(filesystem.read_write, vec!["/tmp".to_string()]);
    }

    #[test]
    fn test_permissions_deserialization_missing_fields() {
        let payload = r#"{}"#;
        let permissions: CapsulePermissions = serde_json::from_str(payload).unwrap();
        assert!(permissions.network.is_none());
        assert!(permissions.isolation.is_none());
        assert!(permissions.filesystem.is_none());
    }

    #[test]
    fn test_parse_capsule_ref_accepts_scoped_and_at_scoped() {
        let plain = parse_capsule_ref("koh0920/sample-capsule").unwrap();
        assert_eq!(plain.publisher, "koh0920");
        assert_eq!(plain.slug, "sample-capsule");
        assert_eq!(plain.scoped_id, "koh0920/sample-capsule");

        let with_at = parse_capsule_ref("@koh0920/sample-capsule").unwrap();
        assert_eq!(with_at.scoped_id, "koh0920/sample-capsule");
    }

    #[test]
    fn test_parse_capsule_ref_rejects_slug_only() {
        assert!(parse_capsule_ref("sample-capsule").is_err());
        assert!(is_slug_only_ref("sample-capsule"));
    }
}
