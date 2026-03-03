//! Install command implementation
//!
//! Downloads and installs capsules from the Store.
//! Primary path: `/v1/capsules/by/:publisher/:slug/distributions` (.capsule contract)
//! Legacy fallback: `/v1/capsules/by/:publisher/:slug/download`

use anyhow::{bail, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::sync::Semaphore;

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum V3SyncOutcome {
    Synced,
    SkippedUnsupportedRegistry,
    SkippedDisabledCas(capsule_core::capsule_v3::CasDisableReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkDownloadOutcome {
    Stored,
    UnsupportedRegistry,
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

    if let Some(v3_manifest) = extract_payload_v3_manifest_from_capsule(&bytes)? {
        let sync_result = sync_v3_chunks_from_manifest(&client, &registry, &v3_manifest).await?;
        match sync_result {
            V3SyncOutcome::Synced => {}
            V3SyncOutcome::SkippedUnsupportedRegistry => {
                if !json_output {
                    eprintln!(
                        "ℹ️  Registry does not expose v3 chunk sync endpoint; falling back to embedded payload"
                    );
                }
            }
            V3SyncOutcome::SkippedDisabledCas(reason) => {
                emit_cas_disabled_performance_warning_once(&reason, json_output);
            }
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

fn extract_payload_v3_manifest_from_capsule(
    bytes: &[u8],
) -> Result<Option<capsule_core::capsule_v3::CapsuleManifestV3>> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
    let mut entries = archive
        .entries()
        .context("Failed to read .capsule archive entries")?;

    while let Some(entry) = entries.next() {
        let mut entry = entry.context("Invalid .capsule entry")?;
        let entry_path = entry
            .path()
            .context("Failed to read archive entry path")?
            .to_string_lossy()
            .to_string();
        if entry_path != capsule_core::capsule_v3::V3_PAYLOAD_MANIFEST_PATH {
            continue;
        }

        let mut manifest_bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut manifest_bytes)
            .context("Failed to read payload.v3.manifest.json from artifact")?;
        let manifest: capsule_core::capsule_v3::CapsuleManifestV3 =
            serde_json::from_slice(&manifest_bytes)
                .context("Failed to parse payload.v3.manifest.json from artifact")?;
        capsule_core::capsule_v3::verify_artifact_hash(&manifest)
            .context("Invalid payload.v3.manifest.json artifact_hash")?;
        return Ok(Some(manifest));
    }

    Ok(None)
}

async fn sync_v3_chunks_from_manifest(
    client: &reqwest::Client,
    registry: &str,
    manifest: &capsule_core::capsule_v3::CapsuleManifestV3,
) -> Result<V3SyncOutcome> {
    let cas = match capsule_core::capsule_v3::CasProvider::from_env() {
        capsule_core::capsule_v3::CasProvider::Enabled(store) => store,
        capsule_core::capsule_v3::CasProvider::Disabled(reason) => {
            capsule_core::capsule_v3::CasProvider::log_disabled_once(
                "install_v3_chunk_sync",
                &reason,
            );
            return Ok(V3SyncOutcome::SkippedDisabledCas(reason));
        }
    };
    let token = read_ato_token();
    let concurrency = sync_concurrency_limit();
    sync_v3_chunks_from_manifest_with_options(client, registry, manifest, cas, token, concurrency)
        .await
}

fn emit_cas_disabled_performance_warning_once(
    reason: &capsule_core::capsule_v3::CasDisableReason,
    json_output: bool,
) {
    if json_output {
        return;
    }
    static STDERR_WARN_ONCE: Once = Once::new();
    STDERR_WARN_ONCE.call_once(|| {
        eprintln!(
            "⚠️  Performance warning: CAS is disabled (reason: {}). Falling back to v2 legacy mode.",
            reason
        );
    });
}

async fn sync_v3_chunks_from_manifest_with_options(
    client: &reqwest::Client,
    registry: &str,
    manifest: &capsule_core::capsule_v3::CapsuleManifestV3,
    cas: capsule_core::capsule_v3::CasStore,
    token: Option<String>,
    concurrency: usize,
) -> Result<V3SyncOutcome> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut downloads = FuturesUnordered::new();

    for chunk in &manifest.chunks {
        if cas
            .has_chunk(&chunk.raw_hash)
            .with_context(|| format!("Failed to check local CAS chunk {}", chunk.raw_hash))?
        {
            continue;
        }
        let client = client.clone();
        let cas = cas.clone();
        let registry = registry.to_string();
        let token = token.clone();
        let raw_hash = chunk.raw_hash.clone();
        let raw_size = chunk.raw_size;
        let semaphore = Arc::clone(&semaphore);

        downloads.push(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("v3 pull semaphore was closed"))?;
            download_chunk_to_cas_with_retry(
                &client,
                &registry,
                &cas,
                &raw_hash,
                raw_size,
                token.as_deref(),
            )
            .await
        });
    }

    while let Some(result) = downloads.next().await {
        match result? {
            ChunkDownloadOutcome::Stored => {}
            ChunkDownloadOutcome::UnsupportedRegistry => {
                return Ok(V3SyncOutcome::SkippedUnsupportedRegistry);
            }
        }
    }

    Ok(V3SyncOutcome::Synced)
}

async fn download_chunk_to_cas_with_retry(
    client: &reqwest::Client,
    registry: &str,
    cas: &capsule_core::capsule_v3::CasStore,
    raw_hash: &str,
    raw_size: u32,
    token: Option<&str>,
) -> Result<ChunkDownloadOutcome> {
    let endpoint = format!("{}/v1/chunks/{}", registry, urlencoding::encode(raw_hash));
    const MAX_RETRIES: usize = 4;

    for attempt in 0..=MAX_RETRIES {
        let mut req = client.get(&endpoint);
        if let Some(token) = token {
            req = req.bearer_auth(token);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                let bytes = resp.bytes().await.with_context(|| {
                    format!("Failed to read downloaded chunk body {}", raw_hash)
                })?;
                verify_downloaded_chunk(raw_hash, raw_size, bytes.as_ref())?;
                cas.put_chunk_zstd(raw_hash, bytes.as_ref())
                    .with_context(|| format!("Failed to store downloaded chunk {}", raw_hash))?;
                return Ok(ChunkDownloadOutcome::Stored);
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if is_sync_not_supported_status(status) {
                    return Ok(ChunkDownloadOutcome::UnsupportedRegistry);
                }
                if is_transient_status(status) && attempt < MAX_RETRIES {
                    tokio::time::sleep(backoff_duration(attempt)).await;
                    continue;
                }
                bail!(
                    "v3 chunk download failed for {} ({}): {}",
                    raw_hash,
                    status.as_u16(),
                    body.trim()
                );
            }
            Err(err) => {
                if is_transient_reqwest_error(&err) && attempt < MAX_RETRIES {
                    tokio::time::sleep(backoff_duration(attempt)).await;
                    continue;
                }
                return Err(err).with_context(|| {
                    format!(
                        "v3 chunk download request failed for {} via {}",
                        raw_hash, endpoint
                    )
                });
            }
        }
    }

    bail!("v3 chunk download exhausted retries for {}", raw_hash)
}

fn verify_downloaded_chunk(raw_hash: &str, raw_size: u32, zstd_bytes: &[u8]) -> Result<()> {
    let cursor = std::io::Cursor::new(zstd_bytes);
    let mut decoder = zstd::Decoder::new(cursor)
        .with_context(|| format!("Failed to decode downloaded chunk {}", raw_hash))?;
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = std::io::Read::read(&mut decoder, &mut buf)
            .with_context(|| format!("Failed to read decoded bytes for {}", raw_hash))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total = total.saturating_add(n as u64);
    }

    if total != raw_size as u64 {
        bail!(
            "downloaded chunk raw_size mismatch for {}: expected {} got {}",
            raw_hash,
            raw_size,
            total
        );
    }

    let got = format!("blake3:{}", hex::encode(hasher.finalize().as_bytes()));
    if !equals_hash(raw_hash, &got) {
        bail!(
            "downloaded chunk hash mismatch for {}: expected {} got {}",
            raw_hash,
            raw_hash,
            got
        );
    }

    Ok(())
}

fn sync_concurrency_limit() -> usize {
    std::env::var("ATO_SYNC_CONCURRENCY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|v| v.clamp(1, 128))
        .unwrap_or(8)
}

fn is_sync_not_supported_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::METHOD_NOT_ALLOWED
            | reqwest::StatusCode::NOT_IMPLEMENTED
    )
}

fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn is_transient_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

fn backoff_duration(attempt: usize) -> Duration {
    let base_ms = 200u64.saturating_mul(1u64 << attempt.min(4));
    Duration::from_millis(base_ms.min(2_000))
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

fn read_ato_token() -> Option<String> {
    std::env::var("ATO_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
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
    use axum::extract::{Path as AxumPath, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::Router;
    use capsule_core::capsule_v3::{set_artifact_hash, CapsuleManifestV3, ChunkMeta};
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;

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

    #[derive(Clone)]
    struct PullMockState {
        zstd_by_hash: Arc<HashMap<String, Vec<u8>>>,
        calls_by_hash: Arc<AsyncMutex<HashMap<String, usize>>>,
        fail_once_hashes: Arc<AsyncMutex<HashSet<String>>>,
        fail_n_times_by_hash: Arc<AsyncMutex<HashMap<String, usize>>>,
        corrupt_hashes: Arc<HashSet<String>>,
        total_gets: Arc<AtomicUsize>,
    }

    impl PullMockState {
        fn new(zstd_by_hash: HashMap<String, Vec<u8>>) -> Self {
            Self {
                zstd_by_hash: Arc::new(zstd_by_hash),
                calls_by_hash: Arc::new(AsyncMutex::new(HashMap::new())),
                fail_once_hashes: Arc::new(AsyncMutex::new(HashSet::new())),
                fail_n_times_by_hash: Arc::new(AsyncMutex::new(HashMap::new())),
                corrupt_hashes: Arc::new(HashSet::new()),
                total_gets: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    async fn pull_chunk_handler(
        State(state): State<PullMockState>,
        AxumPath(raw_hash): AxumPath<String>,
    ) -> impl IntoResponse {
        state.total_gets.fetch_add(1, Ordering::SeqCst);
        {
            let mut calls = state.calls_by_hash.lock().await;
            let entry = calls.entry(raw_hash.clone()).or_insert(0);
            *entry += 1;
        }

        {
            let mut fail_once = state.fail_once_hashes.lock().await;
            if fail_once.remove(&raw_hash) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::body::Bytes::from_static(b"transient"),
                )
                    .into_response();
            }
        }
        {
            let mut fail_n = state.fail_n_times_by_hash.lock().await;
            if let Some(remaining) = fail_n.get_mut(&raw_hash) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        axum::body::Bytes::from_static(b"transient"),
                    )
                        .into_response();
                }
            }
        }

        let Some(mut bytes) = state.zstd_by_hash.get(&raw_hash).cloned() else {
            return (StatusCode::NOT_FOUND, axum::body::Bytes::new()).into_response();
        };
        if state.corrupt_hashes.contains(&raw_hash) && !bytes.is_empty() {
            bytes[0] = bytes[0].wrapping_add(1);
        }
        (
            StatusCode::OK,
            [("cache-control", "public, max-age=31536000, immutable")],
            axum::body::Bytes::from(bytes),
        )
            .into_response()
    }

    async fn start_pull_mock_server(state: PullMockState) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/v1/chunks/:raw_hash", get(pull_chunk_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        (format!("http://{}", addr), handle)
    }

    fn compress_zstd(raw: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::Encoder::new(Vec::new(), 3).expect("encoder");
        encoder.write_all(raw).expect("write");
        encoder.finish().expect("finish")
    }

    fn build_manifest_and_chunks(
        count: usize,
    ) -> (
        CapsuleManifestV3,
        HashMap<String, Vec<u8>>,
        Vec<(String, Vec<u8>)>,
    ) {
        let mut chunks = Vec::new();
        let mut map = HashMap::new();
        let mut ordered_raw = Vec::new();
        for i in 0..count {
            let raw = vec![(i % 251) as u8; 1024 + (i * 17)];
            let raw_hash = capsule_core::capsule_v3::manifest::blake3_digest(&raw);
            let zstd = compress_zstd(&raw);
            chunks.push(ChunkMeta {
                raw_hash: raw_hash.clone(),
                raw_size: raw.len() as u32,
                zstd_size_hint: Some(zstd.len() as u32),
            });
            map.insert(raw_hash.clone(), zstd);
            ordered_raw.push((raw_hash, raw));
        }
        let mut manifest = CapsuleManifestV3::new(chunks);
        set_artifact_hash(&mut manifest).expect("artifact hash");
        (manifest, map, ordered_raw)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_v3_sync_discards_corrupted_download() {
        let (manifest, zstd_by_hash, ordered_raw) = build_manifest_and_chunks(1);
        let corrupted_hash = ordered_raw[0].0.clone();
        let mut state = PullMockState::new(zstd_by_hash);
        state.corrupt_hashes = Arc::new(HashSet::from([corrupted_hash.clone()]));
        let (base_url, handle) = start_pull_mock_server(state.clone()).await;
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas = capsule_core::capsule_v3::CasStore::new(tmp.path()).expect("cas");
        let client = reqwest::Client::new();

        let err = sync_v3_chunks_from_manifest_with_options(
            &client,
            &base_url,
            &manifest,
            cas.clone(),
            None,
            1,
        )
        .await
        .expect_err("must fail on corruption");
        assert!(
            err.to_string().contains("hash mismatch")
                || err.to_string().contains("decode")
                || err.to_string().contains("raw_size mismatch")
        );
        assert!(!cas.has_chunk(&corrupted_hash).expect("has_chunk"));

        // Ensure no temporary artifacts remain after failure.
        let mut stack = vec![cas.root().to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let entry = entry.expect("entry");
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                assert!(
                    !name.starts_with(".tmp-"),
                    "temporary file leaked: {}",
                    path.display()
                );
            }
        }

        handle.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_v3_sync_pull_resume_after_interruption() {
        let (manifest, zstd_by_hash, ordered_raw) = build_manifest_and_chunks(2);
        let first_hash = ordered_raw[0].0.clone();
        let second_hash = ordered_raw[1].0.clone();

        let state = PullMockState::new(zstd_by_hash);
        {
            let mut fail_n = state.fail_n_times_by_hash.lock().await;
            // Exceed MAX_RETRIES (=4) so first sync fails; second sync resumes and succeeds.
            fail_n.insert(second_hash.clone(), 5);
        }
        let (base_url, handle) = start_pull_mock_server(state.clone()).await;
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas = capsule_core::capsule_v3::CasStore::new(tmp.path()).expect("cas");
        let client = reqwest::Client::new();

        let first_attempt = sync_v3_chunks_from_manifest_with_options(
            &client,
            &base_url,
            &manifest,
            cas.clone(),
            None,
            1,
        )
        .await;
        assert!(first_attempt.is_err(), "first attempt should fail");
        assert!(cas.has_chunk(&first_hash).expect("has first"));
        assert!(!cas.has_chunk(&second_hash).expect("has second"));

        let second_outcome = sync_v3_chunks_from_manifest_with_options(
            &client,
            &base_url,
            &manifest,
            cas.clone(),
            None,
            1,
        )
        .await
        .expect("second attempt should succeed");
        assert_eq!(second_outcome, V3SyncOutcome::Synced);
        assert!(cas.has_chunk(&first_hash).expect("has first"));
        assert!(cas.has_chunk(&second_hash).expect("has second"));

        let calls = state.calls_by_hash.lock().await;
        // first chunk should be fetched only once (then skipped on resume).
        assert_eq!(calls.get(&first_hash).copied().unwrap_or(0), 1);
        // second chunk should have at least one failed attempt before succeeding on resume.
        assert!(calls.get(&second_hash).copied().unwrap_or(0) >= 2);

        handle.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_v3_sync_zero_transfer_on_full_hit() {
        let (manifest, zstd_by_hash, ordered_raw) = build_manifest_and_chunks(3);
        let expected_chunk_gets = ordered_raw.len();
        let state = PullMockState::new(zstd_by_hash);
        let (base_url, handle) = start_pull_mock_server(state.clone()).await;
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas = capsule_core::capsule_v3::CasStore::new(tmp.path()).expect("cas");
        let client = reqwest::Client::new();

        let first_outcome = sync_v3_chunks_from_manifest_with_options(
            &client,
            &base_url,
            &manifest,
            cas.clone(),
            None,
            3,
        )
        .await
        .expect("first sync should succeed");
        assert_eq!(first_outcome, V3SyncOutcome::Synced);
        let gets_after_first = state.total_gets.load(Ordering::SeqCst);
        assert_eq!(
            gets_after_first, expected_chunk_gets,
            "first sync should fetch each chunk exactly once"
        );

        let second_outcome =
            sync_v3_chunks_from_manifest_with_options(&client, &base_url, &manifest, cas, None, 3)
                .await
                .expect("second sync should succeed");
        assert_eq!(second_outcome, V3SyncOutcome::Synced);
        let gets_after_second = state.total_gets.load(Ordering::SeqCst);
        assert_eq!(
            gets_after_second, gets_after_first,
            "second sync should not download any chunk on full local hit"
        );

        handle.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_v3_sync_falls_back_when_registry_lacks_chunk_endpoint() {
        let (manifest, _zstd_by_hash, _ordered_raw) = build_manifest_and_chunks(1);
        let state = PullMockState::new(HashMap::new());
        let (base_url, handle) = start_pull_mock_server(state).await;
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas = capsule_core::capsule_v3::CasStore::new(tmp.path()).expect("cas");
        let client = reqwest::Client::new();

        let outcome =
            sync_v3_chunks_from_manifest_with_options(&client, &base_url, &manifest, cas, None, 1)
                .await
                .expect("must skip unsupported chunk endpoint");
        assert_eq!(outcome, V3SyncOutcome::SkippedUnsupportedRegistry);

        handle.abort();
    }
}
