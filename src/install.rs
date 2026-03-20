//! Install command implementation
//!
//! Downloads and installs capsules from the Store.
//! Primary path: `/v1/capsules/by/:publisher/:slug/distributions` (.capsule contract)

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::RngCore;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::debug;

use capsule_core::packers::payload as manifest_payload;
use capsule_core::resource::cas::LocalCasIndex;
use capsule_core::types::identity::public_key_to_did;
use capsule_core::types::CapsuleManifest;

use crate::artifact_hash::{
    compute_blake3_label as compute_blake3, compute_sha256_hex as compute_sha256, equals_hash,
    normalize_hash_for_compare,
};
use crate::capsule_archive::extract_payload_tar_from_capsule;
use crate::registry::RegistryResolver;
use crate::runtime_tree;

#[path = "install/github_inference.rs"]
mod github_inference;
use github_inference::*;

const DEFAULT_STORE_DIR: &str = ".ato/store";
const DEFAULT_STORE_API_URL: &str = "https://api.ato.run";
const ENV_STORE_API_URL: &str = "ATO_STORE_API_URL";
const SEGMENT_MAX_LEN: usize = 63;
const LEASE_REFRESH_INTERVAL_SECS: u64 = 300;
const NEGOTIATE_DEFAULT_MAX_BYTES: u64 = 16 * 1024 * 1024;
const DELTA_RECONSTRUCT_ZSTD_LEVEL: i32 = 3;
const DEFAULT_GITHUB_DRAFT_NODE_RUNTIME_VERSION: &str = "20.12.0";
const DEFAULT_GITHUB_DRAFT_PYTHON_RUNTIME_VERSION: &str = "3.11.10";

#[derive(Debug, Serialize)]
pub struct InstallResult {
    pub capsule_id: String,
    pub scoped_id: String,
    pub publisher: String,
    pub slug: String,
    pub version: String,
    pub path: PathBuf,
    pub content_hash: String,
    pub install_kind: InstallKind,
    pub launchable: Option<LaunchableTarget>,
    pub local_derivation: Option<LocalDerivationInfo>,
    pub projection: Option<ProjectionInfo>,
    pub promotion: Option<PromotionInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub enum InstallKind {
    Standard,
    NativeRequiresLocalDerivation,
}

#[derive(Debug, Clone, Serialize)]
pub enum LaunchableTarget {
    CapsuleArchive { path: PathBuf },
    DerivedApp { path: PathBuf },
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalDerivationInfo {
    pub schema_version: String,
    pub performed: bool,
    pub fetched_dir: PathBuf,
    pub derived_app_path: Option<PathBuf>,
    pub provenance_path: Option<PathBuf>,
    pub parent_digest: Option<String>,
    pub derived_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionInfo {
    pub performed: bool,
    pub projection_id: Option<String>,
    pub projected_path: Option<PathBuf>,
    pub state: Option<String>,
    pub schema_version: Option<String>,
    pub metadata_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromotionInfo {
    pub performed: bool,
    pub preview_id: Option<String>,
    pub source_reference: Option<String>,
    pub source_metadata_path: Option<PathBuf>,
    pub source_manifest_path: Option<PathBuf>,
    pub manifest_source: Option<String>,
    pub inference_mode: Option<String>,
    pub resolved_ref: Option<GitHubInstallDraftResolvedRef>,
    pub derived_plan: Option<PromotionDerivedPlanSnapshot>,
    pub promotion_metadata_path: Option<PathBuf>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromotionDerivedPlanSnapshot {
    pub runtime: Option<String>,
    pub driver: Option<String>,
    pub resolved_runtime_version: Option<String>,
    pub resolved_port: Option<u16>,
    pub resolved_lock_files: Vec<PathBuf>,
    pub resolved_pack_include: Vec<String>,
    pub warnings: Vec<String>,
    pub deferred_constraints: Vec<String>,
    pub promotion_eligibility: String,
}

#[derive(Debug, Clone)]
pub struct PromotionSourceInfo {
    pub preview_id: String,
    pub source_reference: String,
    pub source_metadata_path: PathBuf,
    pub source_manifest_path: PathBuf,
    pub manifest_source: Option<String>,
    pub inference_mode: Option<String>,
    pub resolved_ref: Option<GitHubInstallDraftResolvedRef>,
    pub derived_plan: PromotionDerivedPlanSnapshot,
}

#[derive(Debug)]
pub struct GitHubCheckout {
    pub repository: String,
    pub publisher: String,
    pub checkout_dir: PathBuf,
    temp_dir: Option<tempfile::TempDir>,
}

impl GitHubCheckout {
    pub fn preserve_for_debugging(&mut self) -> PathBuf {
        if let Some(temp_dir) = self.temp_dir.take() {
            std::mem::forget(temp_dir);
        }
        self.checkout_dir.clone()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallDraftResponse {
    pub repo: GitHubInstallDraftRepo,
    #[serde(rename = "capsuleToml")]
    pub capsule_toml: GitHubInstallDraftCapsuleToml,
    #[serde(rename = "repoRef")]
    pub repo_ref: String,
    #[serde(rename = "proposedRunCommand")]
    pub proposed_run_command: Option<String>,
    #[serde(rename = "proposedInstallCommand")]
    pub proposed_install_command: String,
    #[serde(rename = "resolvedRef")]
    pub resolved_ref: GitHubInstallDraftResolvedRef,
    #[serde(rename = "manifestSource")]
    pub manifest_source: String,
    #[serde(rename = "previewToml")]
    pub preview_toml: Option<String>,
    #[serde(rename = "capsuleHint")]
    pub capsule_hint: Option<GitHubInstallDraftHint>,
    #[serde(rename = "inferenceMode")]
    pub inference_mode: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallDraftRepo {
    pub owner: String,
    pub repo: String,
    #[serde(rename = "fullName")]
    pub full_name: String,
    #[serde(rename = "defaultBranch")]
    pub default_branch: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallDraftCapsuleToml {
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitHubInstallDraftResolvedRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub sha: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallDraftHint {
    pub confidence: String,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub launchability: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GitHubStoreErrorPayload {
    error: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitHubInstallDraftRetryRequest {
    pub attempt_id: Option<String>,
    pub resolved_ref_sha: String,
    pub previous_toml: String,
    pub smoke_error_class: String,
    pub smoke_error_excerpt: String,
    pub retry_ordinal: u8,
}

impl GitHubInstallDraftResponse {
    pub fn normalize_preview_toml_for_checkout(&self, checkout_dir: &Path) -> Result<Self> {
        let mut normalized = self.clone();
        normalized.preview_toml = self
            .preview_toml
            .as_deref()
            .map(|raw| normalize_github_install_preview_toml(checkout_dir, raw))
            .transpose()?;
        Ok(normalized)
    }
}

pub struct InstallExecutionOptions {
    pub output_dir: Option<PathBuf>,
    pub yes: bool,
    pub projection_preference: ProjectionPreference,
    pub json_output: bool,
    pub can_prompt_interactively: bool,
    pub promotion_source: Option<PromotionSourceInfo>,
    pub keep_progressive_flow_open: bool,
}

enum InstallSource {
    Registry(String),
    Local(String),
}

impl InstallSource {
    fn registry_url(&self) -> Option<&str> {
        match self {
            Self::Registry(url) => Some(url),
            Self::Local(_) => None,
        }
    }

    fn cache_label(&self) -> &str {
        match self {
            Self::Registry(url) | Self::Local(url) => url,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPreference {
    Prompt,
    Force,
    Skip,
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
    manifest_toml: Option<String>,
    #[serde(default)]
    capsule_lock: Option<String>,
    #[serde(default)]
    permissions: Option<CapsulePermissions>,
}

#[derive(Debug, Deserialize)]
struct ReleaseInfo {
    version: String,
}

#[derive(Debug, Deserialize)]
struct ManifestEpochResolveResponse {
    pointer: ManifestEpochPointer,
    public_key: String,
}

#[derive(Debug, Deserialize)]
struct ManifestEpochPointer {
    scoped_id: String,
    epoch: u64,
    manifest_hash: String,
    #[serde(default)]
    prev_epoch_hash: Option<String>,
    issued_at: String,
    signer_did: String,
    key_id: String,
    signature: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ManifestChunkBloomRequest {
    m_bits: u64,
    k_hashes: u32,
    seed: u64,
    bitset_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ManifestNegotiateRequest {
    scoped_id: String,
    target_manifest_hash: String,
    #[serde(default)]
    have_chunks: Vec<String>,
    #[serde(default)]
    have_chunks_bloom: Option<ManifestChunkBloomRequest>,
    #[serde(default)]
    reuse_lease_id: Option<String>,
    #[serde(default)]
    max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ManifestNegotiateResponse {
    required_chunks: Vec<String>,
    #[serde(default)]
    yanked: Option<bool>,
    #[serde(default)]
    lease_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ManifestLeaseRefreshRequest {
    lease_id: String,
    ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ManifestLeaseRefreshResponse {
    lease_id: String,
}

#[derive(Debug, Serialize)]
struct ManifestLeaseReleaseRequest {
    lease_id: String,
}

#[derive(Debug)]
enum DeltaInstallResult {
    Artifact(Vec<u8>),
    DownloadedArtifact { bytes: Vec<u8>, file_name: String },
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

#[derive(Debug, Default, Deserialize, Serialize)]
struct EpochGuardState {
    #[serde(default)]
    capsules: HashMap<String, EpochGuardEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EpochGuardEntry {
    max_epoch: u64,
    manifest_hash: String,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScopedCapsuleRef {
    pub publisher: String,
    pub slug: String,
    pub scoped_id: String,
}

#[derive(Debug, Clone)]
pub struct ParsedCapsuleRequest {
    pub scoped_ref: ScopedCapsuleRef,
    pub version: Option<String>,
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

#[derive(Debug, Deserialize)]
struct VersionManifestResolveResponse {
    scoped_id: String,
    version: String,
    manifest_hash: String,
    #[serde(default)]
    yanked_at: Option<String>,
}

#[derive(Debug)]
enum ManifestResolution {
    Current(ManifestEpochResolveResponse),
    Version(VersionManifestResolveResponse),
}

impl ManifestResolution {
    fn manifest_hash(&self) -> &str {
        match self {
            Self::Current(response) => &response.pointer.manifest_hash,
            Self::Version(response) => &response.manifest_hash,
        }
    }
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

fn split_capsule_request(input: &str) -> Result<(String, Option<String>)> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
    }

    let normalized = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if normalized.is_empty() {
        bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
    }

    if let Some((base, version)) = normalized.rsplit_once('@') {
        let base = base.trim();
        let version = version.trim();
        if base.is_empty() {
            bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
        }
        if version.is_empty() {
            bail!("version_required: use publisher/slug@version");
        }
        return Ok((base.to_string(), Some(version.to_string())));
    }

    Ok((normalized.to_string(), None))
}

pub fn parse_capsule_request(input: &str) -> Result<ParsedCapsuleRequest> {
    let (scoped_input, version) = split_capsule_request(input)?;
    let normalized = scoped_input.strip_prefix('@').unwrap_or(&scoped_input);
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
    Ok(ParsedCapsuleRequest {
        scoped_ref: ScopedCapsuleRef {
            publisher: publisher.clone(),
            slug: slug.clone(),
            scoped_id: format!("{}/{}", publisher, slug),
        },
        version,
    })
}

pub fn parse_capsule_ref(input: &str) -> Result<ScopedCapsuleRef> {
    Ok(parse_capsule_request(input)?.scoped_ref)
}

pub fn is_slug_only_ref(input: &str) -> bool {
    let Ok((scoped_input, _)) = split_capsule_request(input) else {
        return false;
    };
    !scoped_input.contains('/')
}

pub fn normalize_github_repository(repository: &str) -> Result<String> {
    crate::publish_preflight::normalize_repository_value(repository)
}

pub fn parse_github_run_ref(input: &str) -> Result<Option<String>> {
    let raw = input.trim();
    if raw.is_empty() {
        return Ok(None);
    }

    if raw.starts_with("github.com/") {
        return normalize_github_repository(raw).map(Some);
    }

    let is_noncanonical_github_ref = raw.starts_with("www.github.com/")
        || raw.starts_with("http://github.com/")
        || raw.starts_with("https://github.com/")
        || raw.starts_with("http://www.github.com/")
        || raw.starts_with("https://www.github.com/");

    if !is_noncanonical_github_ref {
        return Ok(None);
    }

    let normalized = normalize_github_repository(raw).with_context(|| {
        "GitHub repository inputs for `ato run` must use `github.com/owner/repo`"
    })?;
    bail!(
        "GitHub repository inputs for `ato run` must use `github.com/owner/repo`. Re-run with: ato run github.com/{}",
        normalized
    );
}

pub async fn fetch_github_install_draft(repository: &str) -> Result<GitHubInstallDraftResponse> {
    let normalized = normalize_github_repository(repository)?;
    let (owner, repo) = normalized
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("repository must include owner/repo"))?;
    let client = reqwest::Client::new();
    let endpoint = format!(
        "{}/v1/github/repos/{}/{}/install-draft",
        resolve_store_api_base_url(),
        urlencoding::encode(owner),
        urlencoding::encode(repo)
    );
    let response = client
        .get(&endpoint)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .send()
        .await
        .with_context(|| format!("Failed to fetch GitHub install draft: {normalized}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!(
            "Failed to fetch GitHub install draft (status={}): {}",
            status,
            body
        );
    }

    response
        .json::<GitHubInstallDraftResponse>()
        .await
        .with_context(|| format!("Failed to parse GitHub install draft: {normalized}"))
}

pub async fn retry_github_install_draft(
    repository: &str,
    request: &GitHubInstallDraftRetryRequest,
) -> Result<GitHubInstallDraftResponse> {
    let normalized = normalize_github_repository(repository)?;
    let (owner, repo) = normalized
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("repository must include owner/repo"))?;
    let client = reqwest::Client::new();
    let endpoint = format!(
        "{}/v1/github/repos/{}/{}/install-draft/retry",
        resolve_store_api_base_url(),
        urlencoding::encode(owner),
        urlencoding::encode(repo)
    );
    let response = client
        .post(&endpoint)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to retry GitHub install draft: {normalized}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!(
            "Failed to retry GitHub install draft (status={}): {}",
            status,
            body
        );
    }

    response
        .json::<GitHubInstallDraftResponse>()
        .await
        .with_context(|| format!("Failed to parse retried GitHub install draft: {normalized}"))
}

pub async fn download_github_repository_at_ref(
    repository: &str,
    resolved_ref: Option<&str>,
) -> Result<GitHubCheckout> {
    let normalized = normalize_github_repository(repository)?;
    let (owner, repo) = normalized
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("repository must include owner/repo"))?;
    let publisher = normalize_install_segment(owner)?;
    let client = reqwest::Client::new();
    let archive_url = match resolved_ref.filter(|value| !value.trim().is_empty()) {
        Some(reference) => format!(
            "{}/repos/{owner}/{repo}/tarball/{}",
            github_api_base_url(),
            urlencoding::encode(reference)
        ),
        None => format!("{}/repos/{owner}/{repo}/tarball", github_api_base_url()),
    };
    let response = client
        .get(&archive_url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .send()
        .await
        .with_context(|| format!("Failed to fetch GitHub repository archive: {normalized}"))?;
    let session_token = crate::auth::current_session_token();
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        if let Some(token) = session_token.as_deref() {
            let archive_bytes = download_private_github_repository_archive_via_store(
                &client,
                &normalized,
                resolved_ref,
                token,
            )
            .await?;
            let temp_root = github_checkout_root()?;
            let temp_dir = tempfile::Builder::new()
                .prefix("gh-install-")
                .tempdir_in(temp_root)
                .with_context(|| "Failed to create GitHub checkout directory")?;
            let checkout_dir = normalize_github_checkout_dir(
                unpack_github_tarball(&archive_bytes, temp_dir.path())?,
                repo,
            )?;
            return Ok(GitHubCheckout {
                repository: normalized,
                publisher,
                checkout_dir,
                temp_dir: Some(temp_dir),
            });
        }

        bail!(
            "GitHub repository archive returned 404 Not Found for '{}'. If this is a private repository, run `ato login` and ensure the ato GitHub App is installed on the repository owner account.",
            normalized
        );
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!(
            "Failed to fetch GitHub repository archive (status={}): {}",
            status,
            body
        );
    }
    let archive_bytes = response
        .bytes()
        .await
        .with_context(|| format!("Failed to read GitHub repository archive: {normalized}"))?;
    let temp_root = github_checkout_root()?;
    let temp_dir = tempfile::Builder::new()
        .prefix("gh-install-")
        .tempdir_in(temp_root)
        .with_context(|| "Failed to create GitHub checkout directory")?;
    let checkout_dir = normalize_github_checkout_dir(
        unpack_github_tarball(&archive_bytes, temp_dir.path())?,
        repo,
    )?;
    Ok(GitHubCheckout {
        repository: normalized,
        publisher,
        checkout_dir,
        temp_dir: Some(temp_dir),
    })
}

async fn download_private_github_repository_archive_via_store(
    client: &reqwest::Client,
    normalized_repository: &str,
    resolved_ref: Option<&str>,
    session_token: &str,
) -> Result<Vec<u8>> {
    let (owner, repo) = normalized_repository
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("repository must include owner/repo"))?;
    let endpoint = format!(
        "{}/v1/github/repos/{}/{}/authed/archive",
        resolve_store_api_base_url(),
        urlencoding::encode(owner),
        urlencoding::encode(repo)
    );
    let response = client
        .get(&endpoint)
        .query(&[("ref", resolved_ref.unwrap_or_default())])
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", session_token),
        )
        .send()
        .await
        .with_context(|| {
            format!(
                "Failed to fetch private GitHub repository archive via ato store: {}",
                normalized_repository
            )
        })?;

    if response.status().is_success() {
        return response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .with_context(|| {
                format!(
                    "Failed to read private GitHub repository archive via ato store: {}",
                    normalized_repository
                )
            });
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let payload = serde_json::from_str::<GitHubStoreErrorPayload>(&body).ok();
    let message = match payload.as_ref().map(|value| value.error.as_str()) {
        Some("auth_required") => {
            "Private GitHub repository access requires an ato store session. Run `ato login` and retry.".to_string()
        }
        Some("publisher_required") => {
            "Private GitHub repository access requires a publisher profile. Complete publisher setup, then retry.".to_string()
        }
        Some("github_app_required") => payload
            .as_ref()
            .map(|value| {
                format!(
                    "{} Re-run after installing or reconnecting the ato GitHub App for this owner.",
                    value.message
                )
            })
            .unwrap_or_else(|| {
                "Install or reconnect the ato GitHub App for this repository owner, then retry.".to_string()
            }),
        Some("repo_not_found") => format!(
            "GitHub repository '{}' could not be found, or the connected GitHub App installation cannot access it.",
            normalized_repository
        ),
        Some("github_archive_not_found") => payload
            .as_ref()
            .map(|value| value.message.clone())
            .unwrap_or_else(|| "GitHub archive could not be fetched for the requested ref.".to_string()),
        _ => payload
            .as_ref()
            .map(|value| value.message.clone())
            .unwrap_or(body),
    };

    bail!(
        "Failed to fetch private GitHub repository archive via ato store (status={}): {}",
        status,
        message
    );
}

pub(crate) fn resolve_store_api_base_url() -> String {
    std::env::var(ENV_STORE_API_URL)
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_STORE_API_URL.to_string())
}

pub async fn install_built_github_artifact(
    artifact_path: &Path,
    publisher: &str,
    repository: &str,
    options: InstallExecutionOptions,
) -> Result<InstallResult> {
    let artifact_bytes = std::fs::read(artifact_path)
        .with_context(|| format!("Failed to read built artifact: {}", artifact_path.display()))?;
    let manifest_toml = extract_manifest_toml_from_capsule(&artifact_bytes)
        .with_context(|| "Built artifact is missing capsule.toml")?;
    let manifest: CapsuleManifest = toml::from_str(&manifest_toml)
        .with_context(|| "Built artifact has invalid capsule.toml")?;
    let slug = normalize_install_segment(&manifest.name)?;
    let version = manifest.version.trim();
    if version.is_empty() {
        bail!("Built artifact capsule.toml is missing version");
    }
    let scoped_ref = parse_capsule_ref(&format!("{publisher}/{slug}"))?;
    let display_slug = scoped_ref.slug.clone();
    let normalized_file_name = format!("{}-{}.capsule", scoped_ref.slug, version);
    complete_install_from_bytes(
        format!("github:{repository}"),
        scoped_ref,
        display_slug,
        version.to_string(),
        artifact_bytes,
        normalized_file_name,
        options,
        InstallSource::Local(format!("github:{repository}")),
    )
    .await
}

pub fn merge_requested_version(
    embedded_version: Option<&str>,
    explicit_version: Option<&str>,
) -> Result<Option<String>> {
    match (
        embedded_version
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        explicit_version
            .map(str::trim)
            .filter(|value| !value.is_empty()),
    ) {
        (Some(left), Some(right)) if left != right => {
            bail!(
                "conflicting_version_request: ref specifies version '{}' but --version requested '{}'",
                left,
                right
            );
        }
        (Some(left), _) => Ok(Some(left.to_string())),
        (None, Some(right)) => Ok(Some(right.to_string())),
        (None, None) => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn install_app(
    capsule_ref: &str,
    registry_url: Option<&str>,
    version: Option<&str>,
    output_dir: Option<PathBuf>,
    _set_default: bool,
    yes: bool,
    projection_preference: ProjectionPreference,
    allow_unverified: bool,
    allow_downgrade: bool,
    json_output: bool,
    can_prompt_interactively: bool,
) -> Result<InstallResult> {
    let request = parse_capsule_request(capsule_ref)?;
    let scoped_ref = request.scoped_ref;
    let requested_version = merge_requested_version(request.version.as_deref(), version)?;
    let registry = resolve_registry_url(registry_url, !json_output).await?;

    let client = reqwest::Client::new();
    let capsule_url = format!(
        "{}/v1/capsules/by/{}/{}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    let capsule: CapsuleDetail = crate::registry_http::with_ato_token(client.get(&capsule_url))
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

    let target_version_owned = match requested_version.as_deref() {
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
    capsule
        .releases
        .iter()
        .find(|r| r.version == target_version)
        .with_context(|| format!("Version {} not found", target_version))?;
    let (bytes, normalized_file_name) = match install_manifest_delta_path(
        &client,
        &registry,
        &scoped_ref,
        requested_version.as_deref(),
        capsule.manifest_toml.as_deref(),
        capsule.capsule_lock.as_deref(),
    )
    .await?
    {
        DeltaInstallResult::Artifact(bytes) => {
            verify_manifest_supply_chain(
                &client,
                &registry,
                &scoped_ref,
                requested_version.as_deref(),
                &bytes,
                allow_unverified,
                allow_downgrade,
            )
            .await?;
            (
                bytes,
                format!("{}-{}.capsule", scoped_ref.slug, target_version),
            )
        }
        DeltaInstallResult::DownloadedArtifact { bytes, file_name } => {
            if !json_output {
                eprintln!(
                    "ℹ️  Registry does not expose manifest delta APIs; falling back to direct artifact download"
                );
            }
            (bytes, file_name)
        }
    };

    complete_install_from_bytes(
        capsule.id,
        scoped_ref,
        capsule.slug,
        target_version_owned,
        bytes,
        normalized_file_name,
        InstallExecutionOptions {
            output_dir,
            yes,
            projection_preference,
            json_output,
            can_prompt_interactively,
            promotion_source: None,
            keep_progressive_flow_open: false,
        },
        InstallSource::Registry(registry),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn complete_install_from_bytes(
    capsule_id: String,
    scoped_ref: ScopedCapsuleRef,
    display_slug: String,
    version: String,
    bytes: Vec<u8>,
    normalized_file_name: String,
    options: InstallExecutionOptions,
    source: InstallSource,
) -> Result<InstallResult> {
    let InstallExecutionOptions {
        output_dir,
        yes,
        projection_preference,
        json_output,
        can_prompt_interactively,
        promotion_source,
        keep_progressive_flow_open,
    } = options;
    let computed_blake3 = compute_blake3(&bytes);
    if let Some(v3_manifest) = extract_payload_v3_manifest_from_capsule(&bytes)? {
        if let Some(registry_url) = source.registry_url() {
            match sync_v3_chunks_from_manifest(&reqwest::Client::new(), registry_url, &v3_manifest)
                .await?
            {
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
    }

    let target_version = version.as_str();
    let native_spec = crate::native_delivery::detect_install_requires_local_derivation(&bytes)?;
    if let Some(_native_spec) = native_spec {
        if !crate::native_delivery::host_supports_finalize() {
            bail!(
                "This app requires local finalize, but this host does not support native finalize (macOS hosts only)."
            );
        }

        let finalize_allowed = if yes {
            true
        } else if can_prompt_interactively && !json_output {
            prompt_for_confirmation(
                "This app requires local setup to run on this machine.\nRun local finalize now? [Y/n] ",
                true,
            )?
        } else {
            false
        };

        if !finalize_allowed {
            bail!(
                "This app requires local finalize, but no interactive consent is available. Re-run with --yes."
            );
        }

        let fetch_result = crate::native_delivery::materialize_fetch_cache_from_artifact(
            &scoped_ref.scoped_id,
            target_version,
            source.cache_label(),
            &bytes,
        )?;

        if !json_output {
            eprintln!("Running local finalize...");
        }
        let finalize_result =
            crate::native_delivery::finalize_fetched_artifact(&fetch_result.cache_dir)?;

        let output_path = persist_installed_artifact(
            output_dir.clone(),
            &scoped_ref.publisher,
            &scoped_ref.slug,
            target_version,
            &normalized_file_name,
            &bytes,
            &computed_blake3,
        )?;
        let promotion =
            persist_promotion_info(&output_path, promotion_source.as_ref(), &computed_blake3)?;
        if promotion.is_some() {
            let _ = runtime_tree::prepare_promoted_runtime_for_capsule(&output_path)?;
        }

        let projection = match projection_preference {
            ProjectionPreference::Skip => {
                if !json_output {
                    eprintln!("Launcher projection skipped.");
                }
                ProjectionInfo {
                    performed: false,
                    projection_id: None,
                    projected_path: None,
                    state: Some("skipped".to_string()),
                    schema_version: Some(
                        crate::native_delivery::delivery_schema_version().to_string(),
                    ),
                    metadata_path: None,
                }
            }
            ProjectionPreference::Force => {
                match crate::native_delivery::execute_project(
                    &finalize_result.derived_app_path,
                    None,
                ) {
                    Ok(result) => ProjectionInfo {
                        performed: true,
                        projection_id: Some(result.projection_id),
                        projected_path: Some(result.projected_path),
                        state: Some(result.state),
                        schema_version: Some(
                            crate::native_delivery::delivery_schema_version().to_string(),
                        ),
                        metadata_path: Some(result.metadata_path),
                    },
                    Err(err) => {
                        if !json_output {
                            eprintln!("Launcher projection failed: {err}");
                            eprintln!(
                                "Run `ato project {}` to try again later.",
                                finalize_result.derived_app_path.display()
                            );
                        }
                        ProjectionInfo {
                            performed: false,
                            projection_id: None,
                            projected_path: None,
                            state: Some("failed".to_string()),
                            schema_version: Some(
                                crate::native_delivery::delivery_schema_version().to_string(),
                            ),
                            metadata_path: None,
                        }
                    }
                }
            }
            ProjectionPreference::Prompt => {
                let should_project = if yes {
                    true
                } else if can_prompt_interactively && !json_output {
                    prompt_for_confirmation(
                        "This app can also be added to your Applications launcher.\nCreate a launcher projection? [y/N] ",
                        false,
                    )?
                } else {
                    false
                };
                if should_project {
                    match crate::native_delivery::execute_project(
                        &finalize_result.derived_app_path,
                        None,
                    ) {
                        Ok(result) => ProjectionInfo {
                            performed: true,
                            projection_id: Some(result.projection_id),
                            projected_path: Some(result.projected_path),
                            state: Some(result.state),
                            schema_version: Some(
                                crate::native_delivery::delivery_schema_version().to_string(),
                            ),
                            metadata_path: Some(result.metadata_path),
                        },
                        Err(err) => {
                            if !json_output {
                                eprintln!("Launcher projection failed: {err}");
                                eprintln!(
                                    "Run `ato project {}` to try again later.",
                                    finalize_result.derived_app_path.display()
                                );
                            }
                            ProjectionInfo {
                                performed: false,
                                projection_id: None,
                                projected_path: None,
                                state: Some("failed".to_string()),
                                schema_version: Some(
                                    crate::native_delivery::delivery_schema_version().to_string(),
                                ),
                                metadata_path: None,
                            }
                        }
                    }
                } else {
                    if !json_output {
                        eprintln!("Launcher projection skipped.");
                    }
                    ProjectionInfo {
                        performed: false,
                        projection_id: None,
                        projected_path: None,
                        state: Some("skipped".to_string()),
                        schema_version: Some(
                            crate::native_delivery::delivery_schema_version().to_string(),
                        ),
                        metadata_path: None,
                    }
                }
            }
        };

        return Ok(InstallResult {
            capsule_id,
            scoped_id: scoped_ref.scoped_id.clone(),
            publisher: scoped_ref.publisher,
            slug: display_slug,
            version,
            path: output_path,
            content_hash: computed_blake3,
            install_kind: InstallKind::NativeRequiresLocalDerivation,
            launchable: Some(LaunchableTarget::DerivedApp {
                path: finalize_result.derived_app_path.clone(),
            }),
            local_derivation: Some(LocalDerivationInfo {
                schema_version: crate::native_delivery::delivery_schema_version().to_string(),
                performed: true,
                fetched_dir: fetch_result.cache_dir,
                derived_app_path: Some(finalize_result.derived_app_path),
                provenance_path: Some(finalize_result.provenance_path),
                parent_digest: Some(finalize_result.parent_digest),
                derived_digest: Some(finalize_result.derived_digest),
            }),
            projection: Some(projection),
            promotion,
        });
    }

    let output_path = persist_installed_artifact(
        output_dir,
        &scoped_ref.publisher,
        &scoped_ref.slug,
        target_version,
        &normalized_file_name,
        &bytes,
        &computed_blake3,
    )?;
    let promotion =
        persist_promotion_info(&output_path, promotion_source.as_ref(), &computed_blake3)?;
    if promotion.is_some() {
        let _ = runtime_tree::prepare_promoted_runtime_for_capsule(&output_path)?;
    }

    if !json_output {
        if crate::progressive_ui::can_use_progressive_ui(false) {
            crate::progressive_ui::show_note(
                "Installed 1 capsule",
                format!(
                    "{}\nSaved to    :\n{}\nRun with    :\n  ato run {}",
                    scoped_ref.scoped_id,
                    crate::progressive_ui::format_path_for_note(&output_path),
                    output_path.display()
                ),
            )?;
            if keep_progressive_flow_open && crate::progressive_ui::is_flow_active() {
                crate::progressive_ui::show_step(format!(
                    "Installed and linked: {}",
                    output_path.display()
                ))?;
            } else {
                crate::progressive_ui::show_outro(format!(
                    "Done! Run persistently with: ato run {}",
                    output_path.display()
                ))?;
            }
        } else {
            eprintln!("✅ Installed to: {}", output_path.display());
            eprintln!("   To run: ato run {}", output_path.display());
        }
    }

    Ok(InstallResult {
        capsule_id,
        scoped_id: scoped_ref.scoped_id.clone(),
        publisher: scoped_ref.publisher,
        slug: display_slug,
        version,
        path: output_path.clone(),
        content_hash: computed_blake3,
        install_kind: InstallKind::Standard,
        launchable: Some(LaunchableTarget::CapsuleArchive {
            path: output_path.clone(),
        }),
        local_derivation: None,
        projection: None,
        promotion,
    })
}

fn persist_promotion_info(
    artifact_path: &Path,
    promotion_source: Option<&PromotionSourceInfo>,
    content_hash: &str,
) -> Result<Option<PromotionInfo>> {
    let Some(source) = promotion_source else {
        return Ok(None);
    };

    let install_dir = artifact_path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "installed artifact must have a parent directory: {}",
            artifact_path.display()
        )
    })?;
    let metadata_path = install_dir.join("promotion.json");
    let promotion = PromotionInfo {
        performed: true,
        preview_id: Some(source.preview_id.clone()),
        source_reference: Some(source.source_reference.clone()),
        source_metadata_path: Some(source.source_metadata_path.clone()),
        source_manifest_path: Some(source.source_manifest_path.clone()),
        manifest_source: source.manifest_source.clone(),
        inference_mode: source.inference_mode.clone(),
        resolved_ref: source.resolved_ref.clone(),
        derived_plan: Some(source.derived_plan.clone()),
        promotion_metadata_path: Some(metadata_path.clone()),
        content_hash: Some(content_hash.to_string()),
    };
    let serialized =
        serde_json::to_vec_pretty(&promotion).context("Failed to serialize promotion metadata")?;
    std::fs::write(&metadata_path, serialized).with_context(|| {
        format!(
            "Failed to write promotion metadata: {}",
            metadata_path.display()
        )
    })?;
    Ok(Some(promotion))
}

fn persist_installed_artifact(
    output_dir: Option<PathBuf>,
    publisher: &str,
    slug: &str,
    version: &str,
    normalized_file_name: &str,
    bytes: &[u8],
    content_hash: &str,
) -> Result<PathBuf> {
    let store_root = output_dir.unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(DEFAULT_STORE_DIR)
    });
    let install_dir = store_root.join(publisher).join(slug).join(version);
    std::fs::create_dir_all(&install_dir).with_context(|| {
        format!(
            "Failed to create store directory: {}",
            install_dir.display()
        )
    })?;

    let output_path = install_dir.join(normalized_file_name);
    sweep_stale_tmp_capsules(&install_dir)?;
    write_capsule_atomic(&output_path, bytes, content_hash)?;
    runtime_tree::prepare_runtime_tree(publisher, slug, version, bytes)?;
    Ok(output_path)
}

fn prompt_for_confirmation(prompt: &str, default_yes: bool) -> Result<bool> {
    crate::progressive_ui::confirm_with_fallback(
        prompt,
        default_yes,
        crate::progressive_ui::can_use_progressive_ui(false),
    )
}

fn normalize_install_segment(value: &str) -> Result<String> {
    let mut normalized = String::new();
    let mut prev_hyphen = false;
    for ch in value.trim().chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            normalized.push(ch);
            prev_hyphen = false;
        } else if !prev_hyphen && !normalized.is_empty() {
            normalized.push('-');
            prev_hyphen = true;
        }
    }
    while normalized.ends_with('-') {
        normalized.pop();
    }
    if !is_valid_segment(&normalized) {
        bail!(
            "invalid install identifier segment '{}': must contain lowercase letters or digits and may include single hyphens between them",
            value
        );
    }
    Ok(normalized)
}

fn github_checkout_root() -> Result<PathBuf> {
    let root = std::env::current_dir()
        .with_context(|| "Failed to resolve current directory for temporary checkout")?
        .join(".tmp")
        .join("ato")
        .join("gh-install");
    std::fs::create_dir_all(&root).with_context(|| {
        format!(
            "Failed to create temporary checkout root: {}",
            root.display()
        )
    })?;
    Ok(root)
}

/// Returns the GitHub API base URL for repository archive downloads.
///
/// `ATO_GITHUB_API_BASE_URL` is intended for local/mock CLI tests so the
/// `--from-gh-repo` flow can be exercised without real GitHub network access.
fn github_api_base_url() -> String {
    std::env::var("ATO_GITHUB_API_BASE_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_string())
}

fn unpack_github_tarball(bytes: &[u8], destination: &Path) -> Result<PathBuf> {
    let decoder = flate2::read::GzDecoder::new(Cursor::new(bytes));
    let mut archive = tar::Archive::new(decoder);
    let mut root_dir: Option<PathBuf> = None;
    for entry in archive
        .entries()
        .context("Failed to read GitHub repository archive")?
    {
        let mut entry = entry.context("Invalid GitHub repository archive entry")?;
        if !matches!(
            entry.header().entry_type(),
            tar::EntryType::Regular
                | tar::EntryType::Directory
                | tar::EntryType::Symlink
                | tar::EntryType::Link
        ) {
            // Ignore tar metadata entries like PAX/GNU headers so valid GitHub
            // archives with a single repository root are not rejected.
            continue;
        }
        let path = entry
            .path()
            .context("Failed to read GitHub archive entry path")?;
        let mut components = path.components();
        let first = components
            .next()
            .ok_or_else(|| anyhow::anyhow!("GitHub archive entry path is empty or invalid"))?;
        let Component::Normal(root_component) = first else {
            bail!(
                "GitHub archive entry must start with a top-level directory before repository files; found non-standard leading path component"
            );
        };
        // The first component is the expected top-level repository directory. Remaining
        // components must stay within that directory and must not traverse outward.
        if components.any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            bail!(
                "GitHub archive entry contains unsafe path traversal components (`..`, absolute paths, or prefixes)"
            );
        }
        let root_path = PathBuf::from(root_component);
        match &root_dir {
            Some(existing) if existing != &root_path => {
                bail!("GitHub archive contains multiple top-level directories")
            }
            None => root_dir = Some(root_path),
            _ => {}
        }
        entry
            .unpack_in(destination)
            .context("Failed to unpack GitHub repository archive")?;
    }
    let root_dir = root_dir.ok_or_else(|| anyhow::anyhow!("GitHub archive is empty"))?;
    Ok(destination.join(root_dir))
}

fn normalize_github_checkout_dir(extracted_root: PathBuf, repo: &str) -> Result<PathBuf> {
    let parent = extracted_root
        .parent()
        .ok_or_else(|| anyhow::anyhow!("GitHub checkout root is missing a parent directory"))?;
    let normalized = parent.join(repo.trim());
    if normalized == extracted_root {
        return Ok(extracted_root);
    }
    if normalized.exists() {
        bail!(
            "GitHub checkout directory already exists: {}",
            normalized.display()
        );
    }
    std::fs::rename(&extracted_root, &normalized).with_context(|| {
        format!(
            "Failed to normalize GitHub checkout directory {} -> {}",
            extracted_root.display(),
            normalized.display()
        )
    })?;
    Ok(normalized)
}

fn extract_payload_v3_manifest_from_capsule(
    bytes: &[u8],
) -> Result<Option<capsule_core::capsule_v3::CapsuleManifestV3>> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
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
    let token = crate::registry_http::current_ato_token();
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
    let capsule: CapsuleDetail = crate::registry_http::with_ato_token(client.get(&capsule_url))
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

pub async fn fetch_capsule_manifest_toml(
    capsule_ref: &str,
    registry_url: Option<&str>,
) -> Result<String> {
    let scoped_ref = parse_capsule_ref(capsule_ref)?;
    let registry = resolve_registry_url(registry_url, false).await?;
    let client = reqwest::Client::new();
    let capsule_url = format!(
        "{}/v1/capsules/by/{}/{}",
        registry,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    let response = crate::registry_http::with_ato_token(client.get(&capsule_url))
        .send()
        .await
        .with_context(|| format!("Failed to connect to registry: {}", registry))?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        bail!("Capsule not found: {}", scoped_ref.scoped_id);
    }

    let capsule: CapsuleDetail = response
        .error_for_status()
        .with_context(|| format!("Failed to fetch capsule detail: {}", scoped_ref.scoped_id))?
        .json()
        .await
        .with_context(|| format!("Invalid registry response for {}", scoped_ref.scoped_id))?;

    capsule
        .manifest_toml
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("capsule.toml was not returned by registry"))
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

async fn resolve_manifest_target(
    client: &reqwest::Client,
    base: &str,
    scoped_ref: &ScopedCapsuleRef,
    requested_version: Option<&str>,
    has_token: bool,
    require_current_epoch: bool,
) -> Result<ManifestResolution> {
    if let Some(version) = requested_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let endpoint = format!(
            "{}/v1/manifest/resolve/{}/{}/{}",
            base,
            urlencoding::encode(&scoped_ref.publisher),
            urlencoding::encode(&scoped_ref.slug),
            urlencoding::encode(version)
        );
        let response = crate::registry_http::with_ato_token(client.get(&endpoint))
            .send()
            .await
            .with_context(|| "Failed to resolve versioned manifest hash")?;
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED && !has_token {
            bail!(
                "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
                crate::error_codes::ATO_ERR_AUTH_REQUIRED
            );
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            if let Some(message) = parse_yanked_message(&body) {
                bail!(
                    "{}: {}",
                    crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                    message
                );
            }
            bail!(
                "Failed to resolve manifest for {}@{} (status={}): {}",
                scoped_ref.scoped_id,
                version,
                status,
                body
            );
        }
        let payload = response
            .json::<VersionManifestResolveResponse>()
            .await
            .with_context(|| "Invalid version resolve response")?;
        if payload.scoped_id != scoped_ref.scoped_id {
            bail!(
                "version resolve scoped_id mismatch (expected {}, got {})",
                scoped_ref.scoped_id,
                payload.scoped_id
            );
        }
        if payload.version != version {
            bail!(
                "version resolve mismatch (expected {}, got {})",
                version,
                payload.version
            );
        }
        if let Some(yanked_at) = payload.yanked_at.as_deref() {
            bail!(
                "{}: manifest has been yanked by the publisher at {}",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                yanked_at
            );
        }
        return Ok(ManifestResolution::Version(payload));
    }

    let epoch_endpoint = format!("{}/v1/manifest/epoch/resolve", base);
    let epoch_response = crate::registry_http::with_ato_token(
        client
            .post(&epoch_endpoint)
            .json(&serde_json::json!({ "scoped_id": scoped_ref.scoped_id })),
    )
    .send()
    .await
    .with_context(|| "Failed to fetch manifest epoch pointer")?;
    if !epoch_response.status().is_success() {
        if epoch_response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
            bail!(
                "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
                crate::error_codes::ATO_ERR_AUTH_REQUIRED
            );
        }
        if require_current_epoch {
            bail!(
                "manifest epoch pointer is required for delta install (status={})",
                epoch_response.status()
            );
        }
        bail!(
            "manifest epoch pointer is required for verified install (status={})",
            epoch_response.status()
        );
    }
    let epoch = epoch_response
        .json::<ManifestEpochResolveResponse>()
        .await
        .with_context(|| "Invalid manifest epoch response")?;
    verify_epoch_signature(&epoch).with_context(|| "Epoch signature verification failed")?;
    Ok(ManifestResolution::Current(epoch))
}

async fn install_manifest_delta_path(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    requested_version: Option<&str>,
    capsule_toml: Option<&str>,
    capsule_lock: Option<&str>,
) -> Result<DeltaInstallResult> {
    let mut lease_id: Option<String> = None;
    let result = install_manifest_delta_path_inner(
        client,
        registry,
        scoped_ref,
        requested_version,
        capsule_toml,
        capsule_lock,
        &mut lease_id,
    )
    .await;
    if let Some(lease_id) = lease_id {
        let _ = release_lease_best_effort(client, registry, &lease_id).await;
    }
    match result {
        Ok(result) => Ok(result),
        Err(err) if is_manifest_api_unsupported_error(&err) => {
            download_capsule_artifact_via_distribution(
                client,
                registry,
                scoped_ref,
                requested_version,
            )
            .await
        }
        Err(err) => Err(err),
    }
}

#[derive(Debug, Deserialize)]
struct RegistryDistributionResponse {
    version: String,
    artifact_url: String,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    blake3: Option<String>,
}

fn is_manifest_api_unsupported_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    (message.contains("endpoint not found") || message.contains("status=404"))
        && (message.contains("manifest") || message.contains("epoch pointer"))
}

async fn download_capsule_artifact_via_distribution(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    requested_version: Option<&str>,
) -> Result<DeltaInstallResult> {
    let base = registry.trim_end_matches('/');
    let mut distribution_url = format!(
        "{}/v1/capsules/by/{}/{}/distributions",
        base,
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    if let Some(version) = requested_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        distribution_url.push_str(&format!("?version={}", urlencoding::encode(version)));
    }

    let has_token = has_ato_token();
    let distribution_response = crate::registry_http::with_ato_token(client.get(&distribution_url))
        .send()
        .await
        .with_context(|| "Failed to resolve distribution fallback for install")?;
    if distribution_response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
        bail!(
            "{}: registry requires authentication for capsule download APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
            crate::error_codes::ATO_ERR_AUTH_REQUIRED
        );
    }
    let distribution = distribution_response
        .error_for_status()
        .with_context(|| {
            format!(
                "Failed to resolve direct download fallback for {}",
                scoped_ref.scoped_id
            )
        })?
        .json::<RegistryDistributionResponse>()
        .await
        .with_context(|| "Invalid distribution fallback response")?;

    let artifact_request = artifact_request_builder(client, registry, &distribution.artifact_url);
    let artifact_response = artifact_request
        .send()
        .await
        .with_context(|| "Failed to download artifact for direct install fallback")?;
    if artifact_response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
        bail!(
            "{}: registry requires authentication for capsule download APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
            crate::error_codes::ATO_ERR_AUTH_REQUIRED
        );
    }
    let artifact_status = artifact_response.status();
    if !artifact_status.is_success() {
        let body = artifact_response
            .text()
            .await
            .unwrap_or_else(|_| String::new());
        let body = body.trim();
        if body.is_empty() {
            bail!(
                "Artifact download fallback failed (status={})",
                artifact_status
            );
        }
        bail!(
            "Artifact download fallback failed (status={}): {}",
            artifact_status,
            body
        );
    }
    let bytes = artifact_response
        .bytes()
        .await
        .with_context(|| "Failed to read downloaded artifact body")?
        .to_vec();

    if let Some(expected_sha256) = distribution.sha256.as_deref() {
        let computed_sha256 = compute_sha256(&bytes);
        if !equals_hash(expected_sha256, &computed_sha256) {
            bail!(
                "{}: downloaded artifact sha256 mismatch during install fallback",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
            );
        }
    }
    if let Some(expected_blake3) = distribution.blake3.as_deref() {
        let computed_blake3 = compute_blake3(&bytes);
        if !equals_hash(expected_blake3, &computed_blake3) {
            bail!(
                "{}: downloaded artifact blake3 mismatch during install fallback",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
            );
        }
    }

    Ok(DeltaInstallResult::DownloadedArtifact {
        bytes,
        file_name: distribution
            .file_name
            .unwrap_or_else(|| format!("{}-{}.capsule", scoped_ref.slug, distribution.version)),
    })
}

async fn install_manifest_delta_path_inner(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    requested_version: Option<&str>,
    _capsule_toml: Option<&str>,
    capsule_lock: Option<&str>,
    lease_id: &mut Option<String>,
) -> Result<DeltaInstallResult> {
    let base = registry.trim_end_matches('/');
    let has_token = has_ato_token();
    let resolution =
        resolve_manifest_target(client, base, scoped_ref, requested_version, has_token, true)
            .await?;
    let target_manifest_hash = resolution.manifest_hash().to_string();

    let manifest_endpoint = format!(
        "{}/v1/manifest/documents/{}",
        base,
        urlencoding::encode(&target_manifest_hash)
    );
    let manifest_response = crate::registry_http::with_ato_token(client.get(&manifest_endpoint))
        .send()
        .await
        .with_context(|| "Failed to fetch manifest document for delta install")?;
    let manifest_status = manifest_response.status();
    if !manifest_status.is_success() {
        if manifest_status == reqwest::StatusCode::UNAUTHORIZED && !has_token {
            bail!(
                "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
                crate::error_codes::ATO_ERR_AUTH_REQUIRED
            );
        }
        let body = manifest_response.text().await.unwrap_or_default();
        if let Some(message) = parse_yanked_message(&body) {
            bail!(
                "{}: {}",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                message
            );
        }
        bail!(
            "Failed to fetch registry manifest for delta install (status={})",
            manifest_status
        );
    }

    let manifest_bytes = manifest_response
        .bytes()
        .await
        .with_context(|| "Failed to read manifest payload for delta install")?
        .to_vec();
    let manifest_toml = String::from_utf8(manifest_bytes)
        .with_context(|| "Remote manifest payload must be UTF-8 TOML")?;
    let manifest: CapsuleManifest = toml::from_str(&manifest_toml)
        .with_context(|| "Invalid remote capsule.toml for delta install")?;
    let manifest_hash = compute_manifest_hash_without_signatures(&manifest)?;
    if normalize_hash_for_compare(&manifest_hash)
        != normalize_hash_for_compare(&target_manifest_hash)
    {
        bail!(
            "Manifest hash mismatch for delta install (expected {}, got {})",
            target_manifest_hash,
            manifest_hash
        );
    }

    let cas_index =
        LocalCasIndex::open_default().with_context(|| "Failed to open local CAS index")?;
    let bloom_wire = cas_index.build_bloom(Some(0.01))?.to_wire();
    let negotiate_request = ManifestNegotiateRequest {
        scoped_id: scoped_ref.scoped_id.clone(),
        target_manifest_hash: target_manifest_hash.clone(),
        have_chunks: Vec::new(),
        have_chunks_bloom: Some(ManifestChunkBloomRequest {
            m_bits: bloom_wire.m_bits,
            k_hashes: bloom_wire.k_hashes,
            seed: bloom_wire.seed,
            bitset_base64: bloom_wire.bitset_base64,
        }),
        reuse_lease_id: None,
        max_bytes: Some(NEGOTIATE_DEFAULT_MAX_BYTES),
    };

    let first_payload = negotiate_manifest(client, base, &negotiate_request, has_token).await?;
    if let Some(id) = first_payload.lease_id.clone() {
        *lease_id = Some(id);
    }

    download_required_chunks(
        client,
        base,
        &cas_index,
        &first_payload.required_chunks,
        lease_id,
        has_token,
    )
    .await?;

    let mut reconstruction = reconstruct_payload_from_local_chunks(&cas_index, &manifest)?;
    if !reconstruction.missing_chunks.is_empty() {
        let reuse_lease = lease_id.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "{}: delta negotiate returned no lease_id; cannot retry exact chunk list.",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
            )
        })?;
        let have_exact = cas_index.available_hashes_for_manifest(
            &manifest_distribution(&manifest)?
                .chunk_list
                .iter()
                .map(|chunk| chunk.chunk_hash.clone())
                .collect::<Vec<_>>(),
        )?;
        let second_request = ManifestNegotiateRequest {
            scoped_id: scoped_ref.scoped_id.clone(),
            target_manifest_hash: target_manifest_hash.clone(),
            have_chunks: have_exact,
            have_chunks_bloom: None,
            reuse_lease_id: Some(reuse_lease),
            max_bytes: Some(NEGOTIATE_DEFAULT_MAX_BYTES),
        };
        let second_payload = negotiate_manifest(client, base, &second_request, has_token).await?;
        if let Some(id) = second_payload.lease_id.clone() {
            *lease_id = Some(id);
        }
        download_required_chunks(
            client,
            base,
            &cas_index,
            &second_payload.required_chunks,
            lease_id,
            has_token,
        )
        .await?;
        reconstruction = reconstruct_payload_from_local_chunks(&cas_index, &manifest)?;
        if !reconstruction.missing_chunks.is_empty() {
            bail!(
                "{}: missing chunks after retry negotiate: {}",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                reconstruction.missing_chunks.join(",")
            );
        }
    }

    verify_payload_chunks(&manifest, &reconstruction.payload_tar)?;
    verify_manifest_merkle_root(&manifest)?;

    let payload_tar_zst = {
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), DELTA_RECONSTRUCT_ZSTD_LEVEL)
            .with_context(|| "Failed to create zstd encoder for reconstructed payload")?;
        encoder
            .write_all(&reconstruction.payload_tar)
            .with_context(|| "Failed to encode reconstructed payload.tar.zst")?;
        encoder
            .finish()
            .with_context(|| "Failed to finalize reconstructed payload.tar.zst")?
    };
    let artifact = build_capsule_artifact(Some(&manifest_toml), capsule_lock, &payload_tar_zst)?;
    Ok(DeltaInstallResult::Artifact(artifact))
}

async fn negotiate_manifest(
    client: &reqwest::Client,
    base: &str,
    request: &ManifestNegotiateRequest,
    has_token: bool,
) -> Result<ManifestNegotiateResponse> {
    let endpoint = format!("{}/v1/manifest/negotiate", base);
    let response = crate::registry_http::with_ato_token(client.post(&endpoint).json(request))
        .send()
        .await
        .with_context(|| "Failed to call manifest negotiate")?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::NOT_IMPLEMENTED {
        bail!("Registry does not support the manifest negotiate API");
    }
    if status == reqwest::StatusCode::UNAUTHORIZED && !has_token {
        bail!(
            "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
            crate::error_codes::ATO_ERR_AUTH_REQUIRED
        );
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if let Some(message) = parse_yanked_message(&body) {
            bail!(
                "{}: {}",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                message
            );
        }
        bail!("manifest negotiate failed (status={}): {}", status, body);
    }
    let payload = response
        .json::<ManifestNegotiateResponse>()
        .await
        .with_context(|| "Invalid manifest negotiate response payload")?;
    if payload.yanked.unwrap_or(false) {
        bail!(
            "{}: manifest has been yanked by the publisher.",
            crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
        );
    }
    Ok(payload)
}

async fn download_required_chunks(
    client: &reqwest::Client,
    base: &str,
    cas_index: &LocalCasIndex,
    required_chunks: &[String],
    lease_id: &mut Option<String>,
    has_token: bool,
) -> Result<()> {
    let mut last_refresh = Instant::now();
    for chunk_hash in required_chunks {
        if cas_index.load_chunk_bytes(chunk_hash)?.is_some() {
            continue;
        }
        if lease_id.is_some()
            && last_refresh.elapsed() >= Duration::from_secs(LEASE_REFRESH_INTERVAL_SECS)
        {
            let refreshed =
                refresh_lease(client, base, lease_id.as_deref().unwrap(), has_token).await?;
            *lease_id = Some(refreshed.lease_id);
            last_refresh = Instant::now();
        }
        let endpoint = format!(
            "{}/v1/manifest/chunks/{}",
            base,
            urlencoding::encode(chunk_hash)
        );
        let response = crate::registry_http::with_ato_token(client.get(&endpoint))
            .send()
            .await
            .with_context(|| format!("Failed to fetch required chunk {}", chunk_hash))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
            bail!(
                "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
                crate::error_codes::ATO_ERR_AUTH_REQUIRED
            );
        }
        if !response.status().is_success() {
            bail!(
                "{}: failed to fetch chunk {} (status={})",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                chunk_hash,
                response.status()
            );
        }
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("Failed to read required chunk {}", chunk_hash))?;
        cas_index.put_verified_chunk(chunk_hash, &bytes)?;
    }
    Ok(())
}

async fn refresh_lease(
    client: &reqwest::Client,
    base: &str,
    lease_id: &str,
    has_token: bool,
) -> Result<ManifestLeaseRefreshResponse> {
    let endpoint = format!("{}/v1/manifest/leases/refresh", base);
    let response = crate::registry_http::with_ato_token(client.post(&endpoint).json(
        &ManifestLeaseRefreshRequest {
            lease_id: lease_id.to_string(),
            ttl_secs: Some(LEASE_REFRESH_INTERVAL_SECS),
        },
    ))
    .send()
    .await
    .with_context(|| "Failed to refresh manifest lease")?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
        bail!(
            "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
            crate::error_codes::ATO_ERR_AUTH_REQUIRED
        );
    }
    if !response.status().is_success() {
        bail!(
            "manifest lease refresh failed (status={})",
            response.status()
        );
    }
    response
        .json::<ManifestLeaseRefreshResponse>()
        .await
        .with_context(|| "Invalid manifest lease refresh response")
}

async fn release_lease_best_effort(
    client: &reqwest::Client,
    registry: &str,
    lease_id: &str,
) -> Result<()> {
    let endpoint = format!(
        "{}/v1/manifest/leases/release",
        registry.trim_end_matches('/')
    );
    let _ = crate::registry_http::with_ato_token(client.post(&endpoint).json(
        &ManifestLeaseReleaseRequest {
            lease_id: lease_id.to_string(),
        },
    ))
    .send()
    .await;
    Ok(())
}

#[derive(Debug, Default)]
struct ReconstructResult {
    payload_tar: Vec<u8>,
    missing_chunks: Vec<String>,
}

fn manifest_distribution(
    manifest: &CapsuleManifest,
) -> Result<&capsule_core::types::DistributionInfo> {
    manifest.distribution.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "{}: distribution metadata is missing from capsule.toml",
            crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
        )
    })
}

fn reconstruct_payload_from_local_chunks(
    cas_index: &LocalCasIndex,
    manifest: &CapsuleManifest,
) -> Result<ReconstructResult> {
    let mut payload = Vec::new();
    let mut missing = Vec::new();
    for chunk in &manifest_distribution(manifest)?.chunk_list {
        match cas_index.load_chunk_bytes(&chunk.chunk_hash)? {
            Some(bytes) => {
                if bytes.len() as u64 != chunk.length {
                    bail!(
                        "{}: chunk length mismatch for {} (expected {}, got {})",
                        crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                        chunk.chunk_hash,
                        chunk.length,
                        bytes.len()
                    );
                }
                payload.extend_from_slice(&bytes);
            }
            None => {
                missing.push(chunk.chunk_hash.clone());
            }
        }
    }
    Ok(ReconstructResult {
        payload_tar: payload,
        missing_chunks: missing,
    })
}

fn build_capsule_artifact(
    capsule_toml: Option<&str>,
    capsule_lock: Option<&str>,
    payload_tar_zst: &[u8],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut out);
        if let Some(manifest_toml) = capsule_toml {
            if !manifest_toml.is_empty() {
                append_capsule_entry(&mut builder, "capsule.toml", manifest_toml.as_bytes())?;
            }
        }
        if let Some(lockfile) = capsule_lock {
            if !lockfile.is_empty() {
                append_capsule_entry(&mut builder, "capsule.lock.json", lockfile.as_bytes())?;
            }
        }
        append_capsule_entry(&mut builder, "payload.tar.zst", payload_tar_zst)?;
        builder
            .finish()
            .with_context(|| "Failed to finalize reconstructed .capsule archive")?;
    }
    Ok(out)
}

fn append_capsule_entry(
    builder: &mut tar::Builder<&mut Vec<u8>>,
    path: &str,
    bytes: &[u8],
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_cksum();
    builder
        .append_data(&mut header, path, Cursor::new(bytes))
        .with_context(|| format!("Failed to append {} to reconstructed artifact", path))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct YankedResponsePayload {
    #[serde(default)]
    yanked: Option<bool>,
    #[serde(default)]
    message: Option<String>,
}

fn parse_yanked_message(body: &str) -> Option<String> {
    let parsed: YankedResponsePayload = serde_json::from_str(body).ok()?;
    if parsed.yanked.unwrap_or(false) {
        return Some(
            parsed
                .message
                .unwrap_or_else(|| "Manifest has been yanked by the publisher.".to_string()),
        );
    }
    None
}

fn sweep_stale_tmp_capsules(install_dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(install_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "Failed to read install directory {}: {}",
                install_dir.display(),
                err
            ))
        }
    };
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "Failed to enumerate install directory {}",
                install_dir.display()
            )
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(".capsule.tmp.") {
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

fn write_capsule_atomic(path: &Path, bytes: &[u8], expected_blake3: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid install path without parent directory: {}",
            path.display()
        )
    })?;
    let mut nonce = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut nonce);
    let tmp_path = parent.join(format!(".capsule.tmp.{}", hex::encode(nonce)));

    let result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&tmp_path)
            .with_context(|| format!("Failed to create file: {}", tmp_path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("Failed to write file: {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("Failed to sync file: {}", tmp_path.display()))?;

        let computed = compute_blake3(bytes);
        if !equals_hash(expected_blake3, &computed) {
            bail!(
                "{}: computed artifact hash changed during atomic install write",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE
            );
        }

        std::fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "Failed to atomically move {} -> {}",
                tmp_path.display(),
                path.display()
            )
        })?;
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

async fn verify_manifest_supply_chain(
    client: &reqwest::Client,
    registry: &str,
    scoped_ref: &ScopedCapsuleRef,
    requested_version: Option<&str>,
    artifact_bytes: &[u8],
    allow_unverified: bool,
    allow_downgrade: bool,
) -> Result<()> {
    let base = registry.trim_end_matches('/');
    let endpoint = format!("{}/v1/manifest/epoch/resolve", base);
    let has_token = has_ato_token();
    let resolution = if requested_version.is_some() {
        resolve_manifest_target(
            client,
            base,
            scoped_ref,
            requested_version,
            has_token,
            false,
        )
        .await?
    } else {
        let response = crate::registry_http::with_ato_token(
            client
                .post(&endpoint)
                .json(&serde_json::json!({ "scoped_id": scoped_ref.scoped_id })),
        )
        .send()
        .await
        .with_context(|| "Failed to fetch manifest epoch pointer")?;
        if !response.status().is_success() {
            if response.status() == reqwest::StatusCode::UNAUTHORIZED && !has_token {
                bail!(
                    "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
                    crate::error_codes::ATO_ERR_AUTH_REQUIRED
                );
            }
            if allow_unverified {
                eprintln!(
                    "⚠️  manifest epoch pointer unavailable (status={}): continuing due to --allow-unverified",
                    response.status()
                );
                return Ok(());
            }
            bail!(
                "manifest epoch pointer is required for verified install (status={})",
                response.status()
            );
        }
        let epoch = response
            .json::<ManifestEpochResolveResponse>()
            .await
            .with_context(|| "Invalid manifest epoch response")?;
        verify_epoch_signature(&epoch).with_context(|| "Epoch signature verification failed")?;
        ManifestResolution::Current(epoch)
    };
    let target_manifest_hash = resolution.manifest_hash().to_string();

    let local_manifest_bytes = extract_manifest_toml_from_capsule(artifact_bytes)
        .with_context(|| "capsule.toml is required in artifact")?;
    let local_manifest: CapsuleManifest =
        toml::from_str(&local_manifest_bytes).with_context(|| "Invalid local capsule.toml")?;
    let local_manifest_hash = compute_manifest_hash_without_signatures(&local_manifest)?;
    if normalize_hash_for_compare(&local_manifest_hash)
        != normalize_hash_for_compare(&target_manifest_hash)
    {
        bail!(
            "Artifact manifest hash mismatch against resolved manifest (expected {}, got {})",
            target_manifest_hash,
            local_manifest_hash
        );
    }

    let manifest_endpoint = format!(
        "{}/v1/manifest/documents/{}",
        base,
        urlencoding::encode(&target_manifest_hash)
    );
    let manifest_response = crate::registry_http::with_ato_token(client.get(&manifest_endpoint))
        .send()
        .await
        .with_context(|| "Failed to fetch manifest payload")?;
    let manifest_status = manifest_response.status();
    if manifest_status.is_success() {
        let remote_manifest_bytes = manifest_response
            .bytes()
            .await
            .with_context(|| "Failed to read remote manifest payload")?;
        let remote_manifest_toml = String::from_utf8(remote_manifest_bytes.to_vec())
            .with_context(|| "Remote manifest payload must be UTF-8 TOML")?;
        let remote_manifest: CapsuleManifest =
            toml::from_str(&remote_manifest_toml).with_context(|| "Invalid remote capsule.toml")?;
        let remote_manifest_hash = compute_manifest_hash_without_signatures(&remote_manifest)?;
        if normalize_hash_for_compare(&remote_manifest_hash)
            != normalize_hash_for_compare(&target_manifest_hash)
        {
            bail!(
                "Remote manifest hash mismatch against resolved manifest (expected {}, got {})",
                target_manifest_hash,
                remote_manifest_hash
            );
        }
    } else if manifest_status == reqwest::StatusCode::UNAUTHORIZED && !has_token {
        bail!(
            "{}: registry requires authentication for manifest read APIs. Run `ato login` or set `ATO_TOKEN=<token>`.",
            crate::error_codes::ATO_ERR_AUTH_REQUIRED
        );
    } else {
        let body = manifest_response.text().await.unwrap_or_default();
        if let Some(message) = parse_yanked_message(&body) {
            bail!(
                "{}: {}",
                crate::error_codes::ATO_ERR_INTEGRITY_FAILURE,
                message
            );
        }
    }
    if !manifest_status.is_success() && !allow_unverified {
        bail!(
            "Failed to fetch registry manifest (status={})",
            manifest_status
        );
    }

    let payload_tar_bytes = extract_payload_tar_from_capsule(artifact_bytes)?;
    verify_payload_chunks(&local_manifest, &payload_tar_bytes)?;
    verify_manifest_merkle_root(&local_manifest)?;

    if let ManifestResolution::Current(epoch) = resolution {
        enforce_epoch_monotonicity(
            &scoped_ref.scoped_id,
            epoch.pointer.epoch,
            &epoch.pointer.manifest_hash,
            allow_downgrade,
        )?;
    }

    Ok(())
}

fn artifact_request_builder(
    client: &reqwest::Client,
    registry: &str,
    artifact_url: &str,
) -> reqwest::RequestBuilder {
    let request = client.get(artifact_url);
    if should_attach_ato_token_to_artifact_url(registry, artifact_url) {
        crate::registry_http::with_ato_token(request)
    } else {
        request
    }
}

fn should_attach_ato_token_to_artifact_url(registry: &str, artifact_url: &str) -> bool {
    let Ok(registry_url) = reqwest::Url::parse(registry) else {
        return false;
    };
    let Ok(artifact) = reqwest::Url::parse(artifact_url) else {
        return false;
    };
    registry_url.scheme() == artifact.scheme()
        && registry_url.host_str() == artifact.host_str()
        && registry_url.port_or_known_default() == artifact.port_or_known_default()
        && artifact.path().starts_with("/v1/capsules/")
}

fn has_ato_token() -> bool {
    crate::registry_http::current_ato_token().is_some()
}

fn verify_epoch_signature(epoch: &ManifestEpochResolveResponse) -> Result<()> {
    let pub_bytes = BASE64
        .decode(epoch.public_key.as_bytes())
        .with_context(|| "Invalid base64 public key")?;
    if pub_bytes.len() != 32 {
        bail!(
            "Invalid manifest epoch public key length: {}",
            pub_bytes.len()
        );
    }
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&pub_bytes);
    let did = public_key_to_did(&pubkey);
    if did != epoch.pointer.signer_did {
        bail!(
            "Epoch signer DID mismatch (expected {}, got {})",
            epoch.pointer.signer_did,
            did
        );
    }
    let verifying_key =
        VerifyingKey::from_bytes(&pubkey).with_context(|| "Invalid manifest epoch public key")?;
    let signature_bytes = BASE64
        .decode(epoch.pointer.signature.as_bytes())
        .with_context(|| "Invalid base64 epoch signature")?;
    if signature_bytes.len() != 64 {
        bail!(
            "Invalid manifest epoch signature length: {}",
            signature_bytes.len()
        );
    }
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&signature_bytes);
    let signature = Signature::from_bytes(&sig);
    let unsigned = serde_json::json!({
        "scoped_id": epoch.pointer.scoped_id,
        "epoch": epoch.pointer.epoch,
        "manifest_hash": epoch.pointer.manifest_hash,
        "prev_epoch_hash": epoch.pointer.prev_epoch_hash,
        "issued_at": epoch.pointer.issued_at,
        "signer_did": epoch.pointer.signer_did,
        "key_id": epoch.pointer.key_id,
    });
    let canonical = serde_jcs::to_vec(&unsigned)?;
    verifying_key
        .verify(&canonical, &signature)
        .with_context(|| "ed25519 verification failed")?;
    Ok(())
}

fn extract_manifest_toml_from_capsule(bytes: &[u8]) -> Result<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let entries = archive
        .entries()
        .context("Failed to read .capsule archive entries")?;
    for entry in entries {
        let mut entry = entry.context("Invalid .capsule entry")?;
        let path = entry.path().context("Failed to read archive entry path")?;
        if path.to_string_lossy() == "capsule.toml" {
            let mut manifest = Vec::new();
            entry
                .read_to_end(&mut manifest)
                .context("Failed to read capsule.toml from artifact")?;
            return String::from_utf8(manifest).with_context(|| "capsule.toml must be UTF-8");
        }
    }
    bail!("Invalid artifact: capsule.toml not found in .capsule archive")
}

fn compute_manifest_hash_without_signatures(manifest: &CapsuleManifest) -> Result<String> {
    manifest_payload::compute_manifest_hash_without_signatures(manifest)
        .map_err(anyhow::Error::from)
}

fn verify_payload_chunks(manifest: &CapsuleManifest, payload_tar: &[u8]) -> Result<()> {
    let distribution = manifest_distribution(manifest)?;
    let mut next_offset = 0u64;
    for chunk in &distribution.chunk_list {
        if chunk.offset != next_offset {
            bail!(
                "manifest chunk_list offset mismatch: expected {}, got {}",
                next_offset,
                chunk.offset
            );
        }
        let start = chunk.offset as usize;
        let end = start.saturating_add(chunk.length as usize);
        if end > payload_tar.len() {
            bail!(
                "manifest chunk range out of bounds: {}..{} (payload={})",
                start,
                end,
                payload_tar.len()
            );
        }
        let actual = format!("blake3:{}", blake3::hash(&payload_tar[start..end]).to_hex());
        if normalize_hash_for_compare(&actual) != normalize_hash_for_compare(&chunk.chunk_hash) {
            bail!(
                "manifest chunk hash mismatch at offset {}: expected {}, got {}",
                chunk.offset,
                chunk.chunk_hash,
                actual
            );
        }
        next_offset = chunk.offset.saturating_add(chunk.length);
    }
    if next_offset != payload_tar.len() as u64 {
        bail!(
            "manifest chunk coverage mismatch: covered {}, payload {}",
            next_offset,
            payload_tar.len()
        );
    }
    Ok(())
}

fn verify_manifest_merkle_root(manifest: &CapsuleManifest) -> Result<()> {
    let distribution = manifest_distribution(manifest)?;
    let mut level: Vec<[u8; 32]> = manifest
        .distribution
        .as_ref()
        .expect("distribution metadata")
        .chunk_list
        .iter()
        .map(|chunk| {
            let normalized = normalize_hash_for_compare(&chunk.chunk_hash);
            let decoded = hex::decode(normalized).unwrap_or_default();
            let mut out = [0u8; 32];
            if decoded.len() == 32 {
                out.copy_from_slice(&decoded);
            }
            out
        })
        .collect();
    let actual_merkle = if level.is_empty() {
        format!("blake3:{}", blake3::hash(b"").to_hex())
    } else {
        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            let mut idx = 0usize;
            while idx < level.len() {
                let left = level[idx];
                let right = if idx + 1 < level.len() {
                    level[idx + 1]
                } else {
                    level[idx]
                };
                let mut hasher = blake3::Hasher::new();
                hasher.update(&left);
                hasher.update(&right);
                let digest = hasher.finalize();
                let mut out = [0u8; 32];
                out.copy_from_slice(digest.as_bytes());
                next.push(out);
                idx += 2;
            }
            level = next;
        }
        format!("blake3:{}", hex::encode(level[0]))
    };
    if normalize_hash_for_compare(&actual_merkle)
        != normalize_hash_for_compare(&distribution.merkle_root)
    {
        bail!(
            "manifest merkle_root mismatch: expected {}, got {}",
            distribution.merkle_root,
            actual_merkle
        );
    }
    Ok(())
}

fn epoch_guard_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ato")
        .join("state")
        .join("epoch-guard.json")
}

fn enforce_epoch_monotonicity(
    scoped_id: &str,
    epoch: u64,
    manifest_hash: &str,
    allow_downgrade: bool,
) -> Result<()> {
    enforce_epoch_monotonicity_at(
        &epoch_guard_path(),
        scoped_id,
        epoch,
        manifest_hash,
        allow_downgrade,
    )
}

fn enforce_epoch_monotonicity_at(
    state_path: &Path,
    scoped_id: &str,
    epoch: u64,
    manifest_hash: &str,
    allow_downgrade: bool,
) -> Result<()> {
    let mut state = load_epoch_guard_state(state_path)?;
    let manifest_norm = normalize_hash_for_compare(manifest_hash);
    let now = chrono::Utc::now().to_rfc3339();

    if let Some(previous) = state.capsules.get(scoped_id) {
        if epoch == previous.max_epoch
            && normalize_hash_for_compare(&previous.manifest_hash) != manifest_norm
        {
            bail!(
                "Epoch replay mismatch for {} at epoch {}: manifest differs from previously trusted value",
                scoped_id,
                epoch
            );
        }
        if epoch < previous.max_epoch && !allow_downgrade {
            bail!(
                "Downgrade detected for {}: remote epoch {} is older than trusted epoch {}. Re-run with --allow-downgrade to proceed.",
                scoped_id,
                epoch,
                previous.max_epoch
            );
        }
    }

    let mut should_persist = false;
    match state.capsules.get_mut(scoped_id) {
        Some(entry) => {
            if epoch > entry.max_epoch {
                entry.max_epoch = epoch;
                entry.manifest_hash = manifest_hash.to_string();
                entry.updated_at = now;
                should_persist = true;
            }
        }
        None => {
            state.capsules.insert(
                scoped_id.to_string(),
                EpochGuardEntry {
                    max_epoch: epoch,
                    manifest_hash: manifest_hash.to_string(),
                    updated_at: now,
                },
            );
            should_persist = true;
        }
    }

    if should_persist {
        write_epoch_guard_state_atomic(state_path, &state)?;
    }
    Ok(())
}

fn load_epoch_guard_state(path: &Path) -> Result<EpochGuardState> {
    if !path.exists() {
        return Ok(EpochGuardState::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read epoch guard state: {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(EpochGuardState::default());
    }
    serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse epoch guard state: {}", path.display()))
}

fn write_epoch_guard_state_atomic(path: &Path, state: &EpochGuardState) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "Failed to create epoch guard state directory: {}",
                parent.display()
            )
        })?;
    }

    let payload = serde_json::to_vec_pretty(state).context("Failed to serialize epoch guard")?;
    let mut nonce = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut nonce);
    let tmp_name = format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("epoch-guard"),
        hex::encode(nonce)
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        let mut file = std::fs::File::create(&tmp_path)
            .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
        file.write_all(&payload)
            .with_context(|| format!("Failed to write {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("Failed to flush {}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "Failed to atomically replace epoch guard state at {}",
            path.display()
        )
    })?;
    Ok(())
}

pub async fn suggest_scoped_capsules(
    slug: &str,
    registry_url: Option<&str>,
    limit: usize,
) -> Result<Vec<ScopedSuggestion>> {
    let registry = resolve_registry_url(registry_url, false).await?;
    let client = reqwest::Client::new();
    let url = format!(
        "{}/v1/manifest/capsules?q={}&limit={}",
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
#[path = "install_tests.rs"]
mod tests;
