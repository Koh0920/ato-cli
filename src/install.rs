//! Install command implementation
//!
//! Downloads and installs capsules from the Store.
//! Primary path: `/v1/capsules/:id/distributions` (.capsule contract)
//! Legacy fallback: `/v1/capsules/:id/download`

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::PathBuf;

use crate::registry::RegistryResolver;

const DEFAULT_STORE_DIR: &str = ".capsule/store";

#[derive(Debug, Serialize)]
pub struct InstallResult {
    pub capsule_id: String,
    pub slug: String,
    pub version: String,
    pub path: PathBuf,
    pub content_hash: String,
}

#[derive(Debug, Deserialize)]
struct CapsuleDetail {
    id: String,
    slug: String,
    name: String,
    description: String,
    price: u64,
    currency: String,
    latest_version: String,
    releases: Vec<ReleaseInfo>,
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

pub async fn install_app(
    slug: &str,
    registry_url: Option<&str>,
    version: Option<&str>,
    output_dir: Option<PathBuf>,
    _set_default: bool,
    json_output: bool,
) -> Result<InstallResult> {
    let registry = if let Some(url) = registry_url {
        url.to_string()
    } else {
        let resolver = RegistryResolver::default();
        let info = resolver.resolve("localhost").await?;
        if !json_output {
            eprintln!(
                "📡 Using registry: {} ({})",
                info.url,
                format!("{:?}", info.source).to_lowercase()
            );
        }
        info.url
    };

    let client = reqwest::Client::new();
    let capsule_url = format!("{}/v1/capsules/{}", registry, urlencoding::encode(slug));
    let capsule: CapsuleDetail = client
        .get(&capsule_url)
        .send()
        .await
        .with_context(|| format!("Failed to connect to registry: {}", registry))?
        .json()
        .await
        .with_context(|| format!("Capsule not found: {}", slug))?;

    if capsule.price > 0 {
        bail!(
            "This capsule costs {} {}. Beta only supports free apps.",
            capsule.price,
            capsule.currency
        );
    }

    if !json_output {
        eprintln!("📦 Found: {} v{}", capsule.name, capsule.latest_version);
        if !capsule.description.is_empty() {
            eprintln!("   {}", capsule.description);
        }
    }

    let target_version = version.unwrap_or(&capsule.latest_version);
    let release = capsule
        .releases
        .iter()
        .find(|r| r.version == target_version)
        .with_context(|| format!("Version {} not found", target_version))?;

    let (download_url, sha256, blake3, file_name, signature_status) =
        match resolve_distribution(&client, &registry, slug, target_version).await {
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
                let legacy_download_url =
                    resolve_legacy_download_url(&client, &registry, slug, target_version).await?;
                (
                    legacy_download_url,
                    None,
                    if release.content_hash.starts_with("blake3:") {
                        Some(release.content_hash.clone())
                    } else {
                        None
                    },
                    format!("{}-{}.capsule", slug, target_version),
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
    ensure_signature_verified(&signature_status)?;

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
    let install_dir = store_root.join(slug).join(target_version);
    std::fs::create_dir_all(&install_dir).with_context(|| {
        format!(
            "Failed to create store directory: {}",
            install_dir.display()
        )
    })?;

    let normalized_file_name = if file_name.to_lowercase().ends_with(".capsule") {
        file_name
    } else {
        format!("{}-{}.capsule", slug, target_version)
    };

    let output_path = install_dir.join(normalized_file_name);
    let mut file = std::fs::File::create(&output_path)
        .with_context(|| format!("Failed to create file: {}", output_path.display()))?;
    file.write_all(&bytes)?;

    if !json_output {
        eprintln!("✅ Installed to: {}", output_path.display());
        eprintln!("   To run: ato open {}", output_path.display());
    }

    Ok(InstallResult {
        capsule_id: capsule.id,
        slug: capsule.slug,
        version: release.version.clone(),
        path: output_path,
        content_hash: computed_blake3,
    })
}

async fn resolve_distribution(
    client: &reqwest::Client,
    registry: &str,
    slug: &str,
    version: &str,
) -> Result<DistributionResponse> {
    let os = normalize_os(std::env::consts::OS);
    let arch = normalize_arch(std::env::consts::ARCH);
    let url = format!(
        "{}/v1/capsules/{}/distributions?os={}&arch={}&channel=stable&version={}",
        registry,
        urlencoding::encode(slug),
        urlencoding::encode(os),
        urlencoding::encode(arch),
        urlencoding::encode(version),
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
    slug: &str,
    version: &str,
) -> Result<String> {
    let download_url = format!(
        "{}/v1/capsules/{}/download?version={}",
        registry,
        urlencoding::encode(slug),
        urlencoding::encode(version),
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

fn ensure_signature_verified(signature_status: &str) -> Result<()> {
    if signature_status == "verified" {
        return Ok(());
    }
    bail!(
        "Signature verification failed: signature_status={}",
        signature_status
    );
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
        assert!(ensure_signature_verified("verified").is_ok());
        assert!(ensure_signature_verified("unverified").is_err());
        assert!(ensure_signature_verified("unknown").is_err());
    }
}
