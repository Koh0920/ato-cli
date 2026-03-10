use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use crate::install;
use crate::registry::RegistryResolver;

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};
#[cfg(windows)]
use std::os::windows::fs::symlink_dir;

const DEFAULT_FETCHES_DIR: &str = ".ato/fetches";
const FETCH_ARTIFACT_DIR: &str = "artifact";
const FETCH_METADATA_FILE: &str = "fetch.json";
const FETCH_SOURCE_ARTIFACT_FILE: &str = "artifact.capsule";
const DELIVERY_CONFIG_FILE: &str = "ato.delivery.toml";
const PROVENANCE_FILE: &str = "local-derivation.json";
const DELIVERY_SCHEMA_VERSION: &str = "exp-0.1";
const DELIVERY_FRAMEWORK: &str = "tauri";
const DELIVERY_STAGE: &str = "unsigned";
const DELIVERY_TARGET: &str = "darwin/arm64";
const FINALIZE_TOOL: &str = "codesign";
const DEFAULT_LAUNCHER_DIR: &str = "Applications";
const PROJECTIONS_DIR: &str = ".ato/native-delivery/projections";
const PROJECTION_KIND: &str = "symlink";

#[derive(Debug, Serialize)]
pub struct FetchResult {
    pub scoped_id: String,
    pub version: String,
    pub cache_dir: PathBuf,
    pub artifact_dir: PathBuf,
    pub parent_digest: String,
    pub registry: String,
}

#[derive(Debug, Serialize)]
pub struct FinalizeResult {
    pub fetched_dir: PathBuf,
    pub output_dir: PathBuf,
    pub derived_app_path: PathBuf,
    pub provenance_path: PathBuf,
    pub parent_digest: String,
    pub derived_digest: String,
}

#[derive(Debug, Serialize)]
pub struct ProjectResult {
    pub projection_id: String,
    pub metadata_path: PathBuf,
    pub launcher_dir: PathBuf,
    pub projected_path: PathBuf,
    pub derived_app_path: PathBuf,
    pub parent_digest: String,
    pub derived_digest: String,
    pub state: String,
    pub problems: Vec<String>,
    pub created: bool,
}

#[derive(Debug, Serialize)]
pub struct UnprojectResult {
    pub projection_id: String,
    pub metadata_path: PathBuf,
    pub projected_path: PathBuf,
    pub removed_projected_path: bool,
    pub removed_metadata: bool,
    pub state_before: String,
    pub problems_before: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ProjectionListResult {
    pub projections: Vec<ProjectionStatus>,
    pub total: usize,
    pub broken: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct ProjectionStatus {
    pub projection_id: String,
    pub metadata_path: PathBuf,
    pub launcher_dir: PathBuf,
    pub projected_path: PathBuf,
    pub derived_app_path: PathBuf,
    pub parent_digest: String,
    pub derived_digest: String,
    pub state: String,
    pub problems: Vec<String>,
    pub projected_at: String,
    pub projection_kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FetchMetadata {
    schema_version: String,
    scoped_id: String,
    version: String,
    registry: String,
    fetched_at: String,
    parent_digest: String,
    artifact_blake3: String,
}

#[derive(Debug, Deserialize)]
struct CapsuleDetail {
    #[serde(rename = "latestVersion", alias = "latest_version", default)]
    latest_version: Option<String>,
    releases: Vec<ReleaseInfo>,
}

#[derive(Debug, Deserialize)]
struct ReleaseInfo {
    version: String,
}

#[derive(Debug, Deserialize)]
struct DeliveryConfig {
    schema_version: String,
    artifact: DeliveryArtifact,
    finalize: DeliveryFinalize,
}

#[derive(Debug, Deserialize)]
struct DeliveryArtifact {
    framework: String,
    stage: String,
    target: String,
    input: String,
}

#[derive(Debug, Deserialize)]
struct DeliveryFinalize {
    tool: String,
    args: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LocalDerivationProvenance {
    parent_digest: String,
    derived_digest: String,
    framework: String,
    target: String,
    finalized_locally: bool,
    finalize_tool: String,
    finalized_at: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ResolvedFetchRequest {
    capsule_ref: String,
    registry_url: Option<String>,
    version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ProjectionMetadata {
    schema_version: String,
    projection_id: String,
    projection_kind: String,
    projected_at: String,
    launcher_dir: PathBuf,
    projected_path: PathBuf,
    derived_app_path: PathBuf,
    provenance_path: PathBuf,
    parent_digest: String,
    derived_digest: String,
    framework: String,
    target: String,
    finalized_at: String,
}

#[derive(Debug, Clone)]
struct ProjectionSource {
    derived_app_path: PathBuf,
    provenance_path: PathBuf,
    parent_digest: String,
    derived_digest: String,
    framework: String,
    target: String,
    finalized_at: String,
}

#[derive(Debug)]
struct StoredProjection {
    metadata_path: PathBuf,
    metadata: ProjectionMetadata,
}

pub async fn execute_fetch(
    capsule_ref: &str,
    registry_url: Option<&str>,
    version: Option<&str>,
) -> Result<FetchResult> {
    let resolved = resolve_fetch_request(capsule_ref, registry_url, version)?;
    let request = install::parse_capsule_request(&resolved.capsule_ref)?;
    let scoped_ref = request.scoped_ref;
    let requested_version =
        install::merge_requested_version(request.version.as_deref(), resolved.version.as_deref())?;
    let registry = resolve_registry_url(resolved.registry_url.as_deref()).await?;
    let client = reqwest::Client::new();

    let detail_url = format!(
        "{}/v1/manifest/capsules/by/{}/{}",
        registry.trim_end_matches('/'),
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug)
    );
    let detail: CapsuleDetail = with_ato_token(client.get(&detail_url))
        .send()
        .await
        .with_context(|| format!("Failed to connect to registry: {}", registry))?
        .error_for_status()
        .with_context(|| format!("Capsule not found: {}", scoped_ref.scoped_id))?
        .json()
        .await
        .with_context(|| {
            format!(
                "Invalid capsule detail payload for {}",
                scoped_ref.scoped_id
            )
        })?;

    let target_version = match requested_version.as_deref() {
        Some(explicit) => explicit.to_string(),
        None => detail
            .latest_version
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!(
                "No fetchable version available for '{}'. This capsule has no published release version.",
                scoped_ref.scoped_id
            ))?,
    };
    detail
        .releases
        .iter()
        .find(|release| release.version == target_version)
        .with_context(|| format!("Version {} not found", target_version))?;

    let download_url = format!(
        "{}/v1/manifest/capsules/by/{}/{}/download?version={}",
        registry.trim_end_matches('/'),
        urlencoding::encode(&scoped_ref.publisher),
        urlencoding::encode(&scoped_ref.slug),
        urlencoding::encode(&target_version)
    );
    let artifact_bytes = with_ato_token(client.get(&download_url))
        .send()
        .await
        .with_context(|| {
            format!(
                "Failed to download artifact for {}@{}",
                scoped_ref.scoped_id, target_version
            )
        })?
        .error_for_status()
        .with_context(|| {
            format!(
                "Artifact download failed for {}@{}",
                scoped_ref.scoped_id, target_version
            )
        })?
        .bytes()
        .await
        .with_context(|| {
            format!(
                "Failed to read artifact body for {}@{}",
                scoped_ref.scoped_id, target_version
            )
        })?
        .to_vec();

    materialize_fetch_cache(
        &scoped_ref.scoped_id,
        &target_version,
        &registry,
        &artifact_bytes,
    )
}

fn resolve_fetch_request(
    input: &str,
    registry_override: Option<&str>,
    version_override: Option<&str>,
) -> Result<ResolvedFetchRequest> {
    if let Some((inline_registry, inline_capsule_ref, inline_version)) =
        parse_inline_fetch_ref(input)?
    {
        let version =
            install::merge_requested_version(inline_version.as_deref(), version_override)?;
        let registry_url = merge_registry_override(registry_override, Some(&inline_registry))?;
        return Ok(ResolvedFetchRequest {
            capsule_ref: inline_capsule_ref,
            registry_url,
            version,
        });
    }

    Ok(ResolvedFetchRequest {
        capsule_ref: input.trim().to_string(),
        registry_url: registry_override.map(|value| value.trim().to_string()),
        version: version_override.map(|value| value.trim().to_string()),
    })
}

fn parse_inline_fetch_ref(input: &str) -> Result<Option<(String, String, Option<String>)>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("scoped_id_required: use publisher/slug (for example: koh0920/sample-capsule)");
    }

    let (registry_part, path_part) = if let Some(rest) = trimmed.strip_prefix("http://") {
        let Some((host_and_port, path)) = rest.split_once('/') else {
            return Ok(None);
        };
        (format!("http://{}", host_and_port), path)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        let Some((host_and_port, path)) = rest.split_once('/') else {
            return Ok(None);
        };
        (format!("https://{}", host_and_port), path)
    } else {
        let Some((host_and_port, path)) = trimmed.split_once('/') else {
            return Ok(None);
        };
        if !(host_and_port.eq_ignore_ascii_case("localhost")
            || host_and_port.contains(':')
            || host_and_port.contains('.'))
        {
            return Ok(None);
        }
        (format!("http://{}", host_and_port), path)
    };

    let path = path_part.trim().trim_matches('/');
    if path.is_empty() {
        bail!("invalid_fetch_ref: missing capsule path after registry host");
    }

    let mut segments = path.split('/').collect::<Vec<_>>();
    if segments.len() > 2 {
        bail!(
            "invalid_fetch_ref: use <registry>/<slug>:<version> or <registry>/<publisher>/<slug>:<version>"
        );
    }
    let last = segments
        .pop()
        .ok_or_else(|| anyhow::anyhow!("invalid_fetch_ref: missing capsule slug"))?;
    let (slug, version) = split_inline_fetch_slug(last)?;

    let capsule_ref = match segments.as_slice() {
        [] => format!("local/{}", slug),
        [publisher] => format!("{}/{}", publisher.trim().to_ascii_lowercase(), slug),
        _ => unreachable!(),
    };

    Ok(Some((registry_part, capsule_ref, version)))
}

fn split_inline_fetch_slug(input: &str) -> Result<(String, Option<String>)> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("invalid_fetch_ref: missing capsule slug");
    }
    if let Some((slug, version)) = trimmed.rsplit_once(':') {
        let slug = slug.trim();
        let version = version.trim();
        if slug.is_empty() {
            bail!("invalid_fetch_ref: missing capsule slug before version");
        }
        if version.is_empty() {
            bail!("version_required: use <registry>/<slug>:<version>");
        }
        return Ok((slug.to_ascii_lowercase(), Some(version.to_string())));
    }
    if let Some((slug, version)) = trimmed.rsplit_once('@') {
        let slug = slug.trim();
        let version = version.trim();
        if slug.is_empty() {
            bail!("invalid_fetch_ref: missing capsule slug before version");
        }
        if version.is_empty() {
            bail!("version_required: use <registry>/<slug>@<version>");
        }
        return Ok((slug.to_ascii_lowercase(), Some(version.to_string())));
    }
    Ok((trimmed.to_ascii_lowercase(), None))
}

fn merge_registry_override(
    registry_override: Option<&str>,
    inline_registry: Option<&str>,
) -> Result<Option<String>> {
    let explicit = registry_override
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let inline = inline_registry
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (explicit, inline) {
        (Some(left), Some(right))
            if normalize_registry_url(left) != normalize_registry_url(right) =>
        {
            bail!(
                "conflicting_registry_request: ref specifies registry '{}' but --registry requested '{}'",
                right,
                left
            );
        }
        (Some(left), _) => Ok(Some(left.to_string())),
        (None, Some(right)) => Ok(Some(right.to_string())),
        (None, None) => Ok(None),
    }
}

fn normalize_registry_url(input: &str) -> String {
    input.trim().trim_end_matches('/').to_ascii_lowercase()
}

pub fn execute_finalize(
    fetched_dir: &Path,
    output_dir: &Path,
    allow_external_finalize: bool,
) -> Result<FinalizeResult> {
    if !allow_external_finalize {
        bail!("finalize requires --allow-external-finalize for any external signing step");
    }

    if !host_supports_finalize() {
        bail!("ato finalize PoC currently supports macOS darwin/arm64 only");
    }

    finalize_with_runner(fetched_dir, output_dir, run_codesign_command)
}

pub fn execute_project(
    derived_app_path: &Path,
    launcher_dir: Option<&Path>,
) -> Result<ProjectResult> {
    if !host_supports_finalize() {
        bail!("ato project PoC currently supports macOS darwin/arm64 only");
    }

    let launcher_dir = resolve_launcher_dir(launcher_dir)?;
    let metadata_root = projections_root()?;
    project_with_roots(derived_app_path, &launcher_dir, &metadata_root)
}

pub fn execute_project_ls() -> Result<ProjectionListResult> {
    if !host_supports_finalize() {
        bail!("ato project ls PoC currently supports macOS darwin/arm64 only");
    }

    list_projections(&projections_root()?)
}

pub fn execute_unproject(reference: &str) -> Result<UnprojectResult> {
    if !host_supports_finalize() {
        bail!("ato unproject PoC currently supports macOS darwin/arm64 only");
    }

    unproject_with_metadata_root(reference, &projections_root()?)
}

fn finalize_with_runner<F>(
    fetched_dir: &Path,
    output_dir: &Path,
    runner: F,
) -> Result<FinalizeResult>
where
    F: Fn(&Path, &DeliveryConfig) -> Result<()>,
{
    let metadata = load_fetch_metadata(fetched_dir)?;
    let artifact_root = fetched_dir.join(FETCH_ARTIFACT_DIR);
    if !artifact_root.is_dir() {
        bail!(
            "Fetched artifact directory is missing: {}",
            artifact_root.display()
        );
    }

    let config_path = artifact_root.join(DELIVERY_CONFIG_FILE);
    let config = load_delivery_config(&config_path)?;
    let parent_digest = compute_tree_digest(&artifact_root)?;
    if metadata.parent_digest != parent_digest {
        bail!(
            "Fetched artifact integrity mismatch: expected {}, got {}",
            metadata.parent_digest,
            parent_digest
        );
    }

    let input_relative = PathBuf::from(config.artifact.input.trim());
    validate_relative_input_path(&input_relative)?;
    let input_app_path = artifact_root.join(&input_relative);
    if !input_app_path.is_dir() {
        bail!(
            "Finalize input is not a .app directory: {}",
            input_app_path.display()
        );
    }
    if input_app_path.extension().and_then(|ext| ext.to_str()) != Some("app") {
        bail!(
            "Finalize input must be a .app bundle: {}",
            input_app_path.display()
        );
    }

    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            output_dir.display()
        )
    })?;
    let derived_dir = create_unique_output_dir(output_dir)?;
    let input_name = input_app_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Finalize input path has no terminal name"))?;
    let derived_app_path = derived_dir.join(input_name);

    let result = (|| -> Result<FinalizeResult> {
        validate_minimal_macos_app_permissions(&input_app_path)?;
        copy_recursively(&input_app_path, &derived_app_path)?;
        validate_minimal_macos_app_permissions(&derived_app_path)?;
        runner(&derived_dir, &config)?;
        validate_minimal_macos_app_permissions(&derived_app_path)?;
        let derived_digest = compute_tree_digest(&derived_app_path)?;
        let provenance = LocalDerivationProvenance {
            parent_digest: parent_digest.clone(),
            derived_digest: derived_digest.clone(),
            framework: config.artifact.framework.clone(),
            target: config.artifact.target.clone(),
            finalized_locally: true,
            finalize_tool: config.finalize.tool.clone(),
            finalized_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        };
        let provenance_path = derived_dir.join(PROVENANCE_FILE);
        write_json_pretty(&provenance_path, &provenance)?;
        Ok(FinalizeResult {
            fetched_dir: fetched_dir.to_path_buf(),
            output_dir: derived_dir.clone(),
            derived_app_path,
            provenance_path,
            parent_digest,
            derived_digest,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&derived_dir);
    }
    result
}

fn project_with_roots(
    derived_app_path: &Path,
    launcher_dir: &Path,
    metadata_root: &Path,
) -> Result<ProjectResult> {
    let source = load_projection_source(derived_app_path)?;
    fs::create_dir_all(launcher_dir).with_context(|| {
        format!(
            "Failed to create launcher directory: {}",
            launcher_dir.display()
        )
    })?;
    fs::create_dir_all(metadata_root).with_context(|| {
        format!(
            "Failed to create projection metadata directory: {}",
            metadata_root.display()
        )
    })?;

    let launcher_dir = absolute_path(launcher_dir)?;
    let app_name = source
        .derived_app_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Derived app path has no terminal name"))?
        .to_os_string();
    let projected_path = launcher_dir.join(&app_name);
    let projection_id = build_projection_id(
        &source.derived_app_path,
        &projected_path,
        &source.derived_digest,
    );
    let metadata_path = metadata_root.join(format!("{}.json", projection_id));

    let existing = load_projection_records(metadata_root)?;
    for record in &existing {
        if record.metadata.projection_id == projection_id {
            let status = inspect_projection(&record.metadata, &record.metadata_path)?;
            if status.state == "ok" {
                return Ok(ProjectResult {
                    projection_id,
                    metadata_path,
                    launcher_dir,
                    projected_path,
                    derived_app_path: source.derived_app_path.clone(),
                    parent_digest: source.parent_digest.clone(),
                    derived_digest: source.derived_digest.clone(),
                    state: status.state,
                    problems: status.problems,
                    created: false,
                });
            }
            break;
        }

        if paths_match(&record.metadata.derived_app_path, &source.derived_app_path)? {
            bail!(
                "Derived app is already projected via '{}' (id {}). Use 'ato unproject' first.",
                record.metadata.projected_path.display(),
                record.metadata.projection_id
            );
        }
        if paths_match(&record.metadata.projected_path, &projected_path)? {
            bail!(
                "Projection name conflict: '{}' is already managed by projection {}",
                projected_path.display(),
                record.metadata.projection_id
            );
        }
    }

    if projected_path.exists() || fs::symlink_metadata(&projected_path).is_ok() {
        if is_managed_symlink_to(&projected_path, &source.derived_app_path)? {
            return Ok(ProjectResult {
                projection_id,
                metadata_path,
                launcher_dir,
                projected_path,
                derived_app_path: source.derived_app_path.clone(),
                parent_digest: source.parent_digest.clone(),
                derived_digest: source.derived_digest.clone(),
                state: "ok".to_string(),
                problems: Vec::new(),
                created: false,
            });
        }
        bail!(
            "Projection name conflict: launcher path already exists: {}",
            projected_path.display()
        );
    }

    let metadata = ProjectionMetadata {
        schema_version: DELIVERY_SCHEMA_VERSION.to_string(),
        projection_id: projection_id.clone(),
        projection_kind: PROJECTION_KIND.to_string(),
        projected_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        launcher_dir: launcher_dir.clone(),
        projected_path: projected_path.clone(),
        derived_app_path: source.derived_app_path.clone(),
        provenance_path: source.provenance_path.clone(),
        parent_digest: source.parent_digest.clone(),
        derived_digest: source.derived_digest.clone(),
        framework: source.framework.clone(),
        target: source.target.clone(),
        finalized_at: source.finalized_at.clone(),
    };

    let result = (|| -> Result<ProjectResult> {
        create_projection_symlink(&source.derived_app_path, &projected_path).with_context(
            || {
                format!(
                    "Failed to create symlink {} -> {}",
                    projected_path.display(),
                    source.derived_app_path.display()
                )
            },
        )?;
        write_json_pretty(&metadata_path, &metadata)?;
        let status = inspect_projection(&metadata, &metadata_path)?;
        Ok(ProjectResult {
            projection_id,
            metadata_path: metadata_path.clone(),
            launcher_dir,
            projected_path: projected_path.clone(),
            derived_app_path: source.derived_app_path,
            parent_digest: source.parent_digest,
            derived_digest: source.derived_digest,
            state: status.state,
            problems: status.problems,
            created: true,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_file(&projected_path);
        let _ = fs::remove_file(&metadata_path);
    }
    result
}

fn list_projections(metadata_root: &Path) -> Result<ProjectionListResult> {
    let projections = load_projection_records(metadata_root)?
        .into_iter()
        .map(|record| inspect_projection(&record.metadata, &record.metadata_path))
        .collect::<Result<Vec<_>>>()?;
    let broken = projections
        .iter()
        .filter(|item| item.state == "broken")
        .count();
    Ok(ProjectionListResult {
        total: projections.len(),
        broken,
        projections,
    })
}

fn unproject_with_metadata_root(reference: &str, metadata_root: &Path) -> Result<UnprojectResult> {
    let record = find_projection_record(reference, metadata_root)?;
    let status = inspect_projection(&record.metadata, &record.metadata_path)?;

    let mut removed_projected_path = false;
    match fs::symlink_metadata(&record.metadata.projected_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            fs::remove_file(&record.metadata.projected_path).with_context(|| {
                format!(
                    "Failed to remove projection symlink: {}",
                    record.metadata.projected_path.display()
                )
            })?;
            removed_projected_path = true;
        }
        Ok(_) => {
            bail!(
                "Refusing to remove non-symlink projected path: {}",
                record.metadata.projected_path.display()
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "Failed to inspect projected path: {}",
                    record.metadata.projected_path.display()
                )
            })
        }
    }

    fs::remove_file(&record.metadata_path).with_context(|| {
        format!(
            "Failed to remove projection metadata: {}",
            record.metadata_path.display()
        )
    })?;

    Ok(UnprojectResult {
        projection_id: record.metadata.projection_id,
        metadata_path: record.metadata_path,
        projected_path: record.metadata.projected_path,
        removed_projected_path,
        removed_metadata: true,
        state_before: status.state,
        problems_before: status.problems,
    })
}

fn materialize_fetch_cache(
    scoped_id: &str,
    version: &str,
    registry: &str,
    artifact_bytes: &[u8],
) -> Result<FetchResult> {
    let fetches_root = fetches_root()?;
    fs::create_dir_all(&fetches_root).with_context(|| {
        format!(
            "Failed to create fetch cache root: {}",
            fetches_root.display()
        )
    })?;

    let temp_dir = create_temp_subdir(&fetches_root, ".tmp-fetch")?;
    let artifact_root = temp_dir.join(FETCH_ARTIFACT_DIR);
    fs::create_dir_all(&artifact_root).with_context(|| {
        format!(
            "Failed to create fetch artifact dir: {}",
            artifact_root.display()
        )
    })?;

    let result = (|| -> Result<FetchResult> {
        let payload_tar = extract_payload_tar_from_capsule(artifact_bytes)?;
        unpack_payload_tar(&payload_tar, &artifact_root)?;
        let parent_digest = compute_tree_digest(&artifact_root)?;
        let digest_dir_name = digest_dir_name(&parent_digest)?;
        let final_dir = fetches_root.join(digest_dir_name);
        let final_artifact_dir = final_dir.join(FETCH_ARTIFACT_DIR);

        if final_dir.exists() {
            let existing = load_fetch_metadata(&final_dir).ok();
            let existing_version = existing
                .as_ref()
                .map(|value| value.version.clone())
                .unwrap_or_else(|| version.to_string());
            return Ok(FetchResult {
                scoped_id: scoped_id.to_string(),
                version: existing_version,
                cache_dir: final_dir,
                artifact_dir: final_artifact_dir,
                parent_digest,
                registry: registry.to_string(),
            });
        }

        fs::write(temp_dir.join(FETCH_SOURCE_ARTIFACT_FILE), artifact_bytes).with_context(
            || {
                format!(
                    "Failed to write fetched artifact: {}",
                    temp_dir.join(FETCH_SOURCE_ARTIFACT_FILE).display()
                )
            },
        )?;
        let metadata = FetchMetadata {
            schema_version: DELIVERY_SCHEMA_VERSION.to_string(),
            scoped_id: scoped_id.to_string(),
            version: version.to_string(),
            registry: registry.to_string(),
            fetched_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            parent_digest: parent_digest.clone(),
            artifact_blake3: compute_blake3(artifact_bytes),
        };
        write_json_pretty(&temp_dir.join(FETCH_METADATA_FILE), &metadata)?;

        match fs::rename(&temp_dir, &final_dir) {
            Ok(()) => {}
            Err(_err) if final_dir.exists() => {
                let _ = fs::remove_dir_all(&temp_dir);
                return Ok(FetchResult {
                    scoped_id: scoped_id.to_string(),
                    version: version.to_string(),
                    cache_dir: final_dir,
                    artifact_dir: final_artifact_dir,
                    parent_digest,
                    registry: registry.to_string(),
                });
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "Failed to finalize fetch cache {} -> {}",
                        temp_dir.display(),
                        final_dir.display()
                    )
                })
            }
        }

        Ok(FetchResult {
            scoped_id: scoped_id.to_string(),
            version: version.to_string(),
            cache_dir: final_dir,
            artifact_dir: final_artifact_dir,
            parent_digest,
            registry: registry.to_string(),
        })
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&temp_dir);
    }
    result
}

fn load_fetch_metadata(fetched_dir: &Path) -> Result<FetchMetadata> {
    let metadata_path = fetched_dir.join(FETCH_METADATA_FILE);
    let raw = fs::read_to_string(&metadata_path)
        .with_context(|| format!("Failed to read fetch metadata: {}", metadata_path.display()))?;
    serde_json::from_str(&raw).with_context(|| {
        format!(
            "Failed to parse fetch metadata: {}",
            metadata_path.display()
        )
    })
}

fn load_projection_source(derived_app_path: &Path) -> Result<ProjectionSource> {
    let absolute_path = absolute_path(derived_app_path)?;
    if !absolute_path.is_dir() {
        bail!(
            "Projection input must be a finalized .app directory: {}",
            absolute_path.display()
        );
    }
    if absolute_path.extension().and_then(|ext| ext.to_str()) != Some("app") {
        bail!(
            "Projection input must be a .app bundle: {}",
            absolute_path.display()
        );
    }
    let derived_app_path = fs::canonicalize(&absolute_path).with_context(|| {
        format!(
            "Failed to canonicalize finalized app path: {}",
            absolute_path.display()
        )
    })?;
    let derived_dir = derived_app_path.parent().ok_or_else(|| {
        anyhow::anyhow!("Projection input must be an ato finalize output with a parent directory")
    })?;
    let provenance_path = derived_dir.join(PROVENANCE_FILE);
    let raw = fs::read_to_string(&provenance_path).with_context(|| {
        format!(
            "ato project requires an ato finalize output containing {} next to the .app: {}",
            PROVENANCE_FILE,
            provenance_path.display()
        )
    })?;
    let provenance: LocalDerivationProvenance = serde_json::from_str(&raw).with_context(|| {
        format!(
            "Failed to parse finalize provenance: {}",
            provenance_path.display()
        )
    })?;
    if !provenance.finalized_locally {
        bail!("Projection input must be finalized locally via `ato finalize`");
    }
    if provenance.finalize_tool != FINALIZE_TOOL {
        bail!(
            "Projection input requires finalize_tool '{}' but found '{}'",
            FINALIZE_TOOL,
            provenance.finalize_tool
        );
    }
    if provenance.framework != DELIVERY_FRAMEWORK {
        bail!(
            "Projection input framework '{}' is unsupported; expected '{}'",
            provenance.framework,
            DELIVERY_FRAMEWORK
        );
    }
    if provenance.target != DELIVERY_TARGET {
        bail!(
            "Projection input target '{}' is unsupported; expected '{}'",
            provenance.target,
            DELIVERY_TARGET
        );
    }

    let actual_digest = compute_tree_digest(&derived_app_path)?;
    if actual_digest != provenance.derived_digest {
        bail!(
            "Derived artifact digest mismatch: expected {}, got {}",
            provenance.derived_digest,
            actual_digest
        );
    }

    Ok(ProjectionSource {
        derived_app_path,
        provenance_path,
        parent_digest: provenance.parent_digest,
        derived_digest: provenance.derived_digest,
        framework: provenance.framework,
        target: provenance.target,
        finalized_at: provenance.finalized_at,
    })
}

fn load_projection_records(metadata_root: &Path) -> Result<Vec<StoredProjection>> {
    if !metadata_root.exists() {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(metadata_root)
        .with_context(|| format!("Failed to read {}", metadata_root.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("Failed to enumerate {}", metadata_root.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    let mut out = Vec::new();
    for entry in entries {
        let metadata_path = entry.path();
        if metadata_path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let raw = fs::read_to_string(&metadata_path)
            .with_context(|| format!("Failed to read {}", metadata_path.display()))?;
        let metadata: ProjectionMetadata = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse {}", metadata_path.display()))?;
        out.push(StoredProjection {
            metadata_path,
            metadata,
        });
    }
    Ok(out)
}

fn find_projection_record(reference: &str, metadata_root: &Path) -> Result<StoredProjection> {
    let records = load_projection_records(metadata_root)?;
    if records.is_empty() {
        bail!(
            "No projection metadata found in {}",
            metadata_root.display()
        );
    }

    let mut matches = Vec::new();
    let reference_path = PathBuf::from(reference);
    let reference_abs = absolute_path(&reference_path).ok();
    for record in records {
        if record.metadata.projection_id == reference {
            matches.push(record);
            continue;
        }
        if let Some(reference_abs) = reference_abs.as_ref() {
            if paths_match(reference_abs, &record.metadata.projected_path)?
                || paths_match(reference_abs, &record.metadata.derived_app_path)?
                || paths_match(reference_abs, &record.metadata_path)?
            {
                matches.push(record);
            }
        }
    }

    match matches.len() {
        0 => bail!("Projection not found: {}", reference),
        1 => Ok(matches.remove(0)),
        _ => bail!("Projection reference is ambiguous: {}", reference),
    }
}

fn inspect_projection(
    metadata: &ProjectionMetadata,
    metadata_path: &Path,
) -> Result<ProjectionStatus> {
    let mut problems = Vec::new();
    if metadata.schema_version != DELIVERY_SCHEMA_VERSION {
        problems.push(format!(
            "unsupported_schema_version:{}",
            metadata.schema_version
        ));
    }
    if metadata.projection_kind != PROJECTION_KIND {
        problems.push(format!(
            "unsupported_projection_kind:{}",
            metadata.projection_kind
        ));
    }
    if metadata.framework != DELIVERY_FRAMEWORK {
        problems.push(format!("unsupported_framework:{}", metadata.framework));
    }
    if metadata.target != DELIVERY_TARGET {
        problems.push(format!("unsupported_target:{}", metadata.target));
    }

    match fs::symlink_metadata(&metadata.projected_path) {
        Ok(projected_meta) if projected_meta.file_type().is_symlink() => {
            let link_target = fs::read_link(&metadata.projected_path).with_context(|| {
                format!(
                    "Failed to read projection symlink: {}",
                    metadata.projected_path.display()
                )
            })?;
            let resolved_target = if link_target.is_absolute() {
                link_target
            } else {
                metadata
                    .projected_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(link_target)
            };
            if !paths_match(&resolved_target, &metadata.derived_app_path)? {
                problems.push("projected_symlink_target_mismatch".to_string());
            }
        }
        Ok(_) => problems.push("projected_path_replaced".to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            problems.push("projected_path_missing".to_string())
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "Failed to inspect projected path: {}",
                    metadata.projected_path.display()
                )
            })
        }
    }

    if !metadata.derived_app_path.exists() {
        problems.push("derived_app_missing".to_string());
    } else if !metadata.derived_app_path.is_dir() {
        problems.push("derived_app_replaced".to_string());
    } else {
        let digest = compute_tree_digest(&metadata.derived_app_path)?;
        if digest != metadata.derived_digest {
            problems.push("derived_digest_mismatch".to_string());
        }
    }

    Ok(ProjectionStatus {
        projection_id: metadata.projection_id.clone(),
        metadata_path: metadata_path.to_path_buf(),
        launcher_dir: metadata.launcher_dir.clone(),
        projected_path: metadata.projected_path.clone(),
        derived_app_path: metadata.derived_app_path.clone(),
        parent_digest: metadata.parent_digest.clone(),
        derived_digest: metadata.derived_digest.clone(),
        state: if problems.is_empty() {
            "ok".to_string()
        } else {
            "broken".to_string()
        },
        problems,
        projected_at: metadata.projected_at.clone(),
        projection_kind: metadata.projection_kind.clone(),
    })
}

fn load_delivery_config(path: &Path) -> Result<DeliveryConfig> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let config: DeliveryConfig =
        toml::from_str(&raw).with_context(|| format!("Failed to parse {}", path.display()))?;
    validate_delivery_config(&config)?;
    Ok(config)
}

fn validate_delivery_config(config: &DeliveryConfig) -> Result<()> {
    if config.schema_version != DELIVERY_SCHEMA_VERSION {
        bail!(
            "Unsupported ato.delivery.toml schema_version '{}'; expected '{}'",
            config.schema_version,
            DELIVERY_SCHEMA_VERSION
        );
    }
    if config.artifact.framework != DELIVERY_FRAMEWORK {
        bail!(
            "Unsupported artifact.framework '{}'; expected '{}'",
            config.artifact.framework,
            DELIVERY_FRAMEWORK
        );
    }
    if config.artifact.stage != DELIVERY_STAGE {
        bail!(
            "Unsupported artifact.stage '{}'; expected '{}'",
            config.artifact.stage,
            DELIVERY_STAGE
        );
    }
    if config.artifact.target != DELIVERY_TARGET {
        bail!(
            "Unsupported artifact.target '{}'; expected '{}'",
            config.artifact.target,
            DELIVERY_TARGET
        );
    }
    if config.finalize.tool != FINALIZE_TOOL {
        bail!(
            "Unsupported finalize.tool '{}'; PoC requires '{}'",
            config.finalize.tool,
            FINALIZE_TOOL
        );
    }
    let input = config.artifact.input.trim();
    if input.is_empty() {
        bail!("artifact.input must not be empty");
    }
    let expected_args = ["--deep", "--force", "--sign", "-"];
    if config.finalize.args.len() != 5 {
        bail!("finalize.args must be exactly [\"--deep\", \"--force\", \"--sign\", \"-\", <input>] for this PoC");
    }
    for (idx, expected) in expected_args.iter().enumerate() {
        if config.finalize.args[idx] != *expected {
            bail!("finalize.args[{}] must be '{}' for this PoC", idx, expected);
        }
    }
    if config.finalize.args[4] != input {
        bail!(
            "finalize.args[4] must exactly match artifact.input ('{}')",
            input
        );
    }
    Ok(())
}

fn validate_relative_input_path(path: &Path) -> Result<()> {
    if path.is_absolute() {
        bail!("artifact.input must be a relative path inside fetched artifact");
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("artifact.input must not escape fetched artifact root");
    }
    Ok(())
}

fn run_codesign_command(derived_dir: &Path, config: &DeliveryConfig) -> Result<()> {
    let mut command = Command::new(FINALIZE_TOOL);
    command
        .args(&config.finalize.args)
        .current_dir(derived_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = command.output().with_context(|| {
        format!(
            "Failed to execute {} in {}",
            FINALIZE_TOOL,
            derived_dir.display()
        )
    })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let details = if !stderr.trim().is_empty() {
        stderr.trim().to_string()
    } else {
        stdout.trim().to_string()
    };
    bail!(
        "codesign failed with status {}{}",
        output
            .status
            .code()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "signal".to_string()),
        if details.is_empty() {
            String::new()
        } else {
            format!(": {}", details)
        }
    )
}

async fn resolve_registry_url(registry_url: Option<&str>) -> Result<String> {
    if let Some(url) = registry_url {
        return Ok(url.to_string());
    }
    let resolver = RegistryResolver::default();
    let info = resolver.resolve("localhost").await?;
    Ok(info.url)
}

fn with_ato_token(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(token) = current_ato_token() {
        request.header("authorization", format!("Bearer {}", token))
    } else {
        request
    }
}

fn current_ato_token() -> Option<String> {
    std::env::var("ATO_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn extract_payload_tar_from_capsule(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let entries = archive
        .entries()
        .context("Failed to read .capsule archive entries")?;
    for entry in entries {
        let mut entry = entry.context("Invalid .capsule entry")?;
        let path = entry.path().context("Failed to read .capsule entry path")?;
        if path.to_string_lossy() != "payload.tar.zst" {
            continue;
        }
        let mut payload_zst = Vec::new();
        entry
            .read_to_end(&mut payload_zst)
            .context("Failed to read payload.tar.zst from artifact")?;
        let mut decoder = zstd::stream::Decoder::new(Cursor::new(payload_zst))
            .context("Failed to decode payload.tar.zst")?;
        let mut payload_tar = Vec::new();
        decoder
            .read_to_end(&mut payload_tar)
            .context("Failed to read payload.tar bytes")?;
        return Ok(payload_tar);
    }
    bail!("Invalid artifact: payload.tar.zst not found in .capsule archive")
}

fn unpack_payload_tar(payload_tar: &[u8], destination: &Path) -> Result<()> {
    let mut archive = tar::Archive::new(Cursor::new(payload_tar));
    let entries = archive
        .entries()
        .context("Failed to read payload.tar entries")?;
    for entry in entries {
        let mut entry = entry.context("Invalid payload.tar entry")?;
        let path = entry.path().context("Failed to read payload entry path")?;
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            bail!("Refusing to unpack unsafe payload path: {}", path.display());
        }
        entry.unpack_in(destination).with_context(|| {
            format!(
                "Failed to unpack payload entry into {}",
                destination.display()
            )
        })?;
    }
    Ok(())
}

fn compute_tree_digest(root: &Path) -> Result<String> {
    if !root.exists() {
        bail!("Digest root does not exist: {}", root.display());
    }
    let mut hasher = blake3::Hasher::new();
    hash_tree_node(root, Path::new(""), &mut hasher)?;
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn hash_tree_node(path: &Path, relative: &Path, hasher: &mut blake3::Hasher) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("Failed to stat {}", path.display()))?;
    let file_type = metadata.file_type();

    if file_type.is_dir() {
        if !relative.as_os_str().is_empty() {
            update_tree_header(hasher, b"dir", relative, mode_bits(&metadata));
        }
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {}", path.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("Failed to enumerate directory {}", path.display()))?;
        entries.sort_by_key(|left| left.file_name());
        for entry in entries {
            let child_path = entry.path();
            let child_relative = if relative.as_os_str().is_empty() {
                PathBuf::from(entry.file_name())
            } else {
                relative.join(entry.file_name())
            };
            hash_tree_node(&child_path, &child_relative, hasher)?;
        }
        return Ok(());
    }

    if file_type.is_symlink() {
        update_tree_header(hasher, b"symlink", relative, 0);
        let target = fs::read_link(path)
            .with_context(|| format!("Failed to read symlink {}", path.display()))?;
        hasher.update(target.as_os_str().to_string_lossy().as_bytes());
        hasher.update(b"\0");
        return Ok(());
    }

    if file_type.is_file() {
        update_tree_header(hasher, b"file", relative, mode_bits(&metadata));
        hasher.update(format!("{}\0", metadata.len()).as_bytes());
        let mut file =
            fs::File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
        let mut buf = [0u8; 16 * 1024];
        loop {
            let n = file
                .read(&mut buf)
                .with_context(|| format!("Failed to read {}", path.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        hasher.update(b"\0");
        return Ok(());
    }

    bail!(
        "Unsupported filesystem entry in digest walk: {}",
        path.display()
    )
}

fn update_tree_header(hasher: &mut blake3::Hasher, kind: &[u8], relative: &Path, mode: u32) {
    hasher.update(kind);
    hasher.update(b"\0");
    hasher.update(relative.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hasher.update(format!("{:o}", mode).as_bytes());
    hasher.update(b"\0");
}

fn copy_recursively(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to stat {}", source.display()))?;
    let file_type = metadata.file_type();

    if file_type.is_dir() {
        fs::create_dir_all(destination)
            .with_context(|| format!("Failed to create directory {}", destination.display()))?;
        fs::set_permissions(destination, metadata.permissions())
            .with_context(|| format!("Failed to set permissions on {}", destination.display()))?;
        let mut entries = fs::read_dir(source)
            .with_context(|| format!("Failed to read directory {}", source.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("Failed to enumerate directory {}", source.display()))?;
        entries.sort_by_key(|left| left.file_name());
        for entry in entries {
            copy_recursively(&entry.path(), &destination.join(entry.file_name()))?;
        }
        return Ok(());
    }

    if file_type.is_symlink() {
        #[cfg(unix)]
        {
            let target = fs::read_link(source)
                .with_context(|| format!("Failed to read symlink {}", source.display()))?;
            symlink(&target, destination).with_context(|| {
                format!(
                    "Failed to recreate symlink {} -> {}",
                    destination.display(),
                    target.display()
                )
            })?;
            return Ok(());
        }
        #[cfg(not(unix))]
        {
            let _ = destination;
            bail!("Symlink copy is not supported on this platform")
        }
    }

    if file_type.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create parent directory {}", parent.display())
            })?;
        }
        fs::copy(source, destination).with_context(|| {
            format!(
                "Failed to copy file {} -> {}",
                source.display(),
                destination.display()
            )
        })?;
        fs::set_permissions(destination, metadata.permissions())
            .with_context(|| format!("Failed to set permissions on {}", destination.display()))?;
        return Ok(());
    }

    bail!(
        "Unsupported filesystem entry for copy: {}",
        source.display()
    )
}

fn validate_minimal_macos_app_permissions(app_dir: &Path) -> Result<()> {
    let macos_dir = app_dir.join("Contents").join("MacOS");
    if !macos_dir.is_dir() {
        return Ok(());
    }

    let mut found_regular_file = false;
    for entry in fs::read_dir(&macos_dir)
        .with_context(|| format!("Failed to read directory {}", macos_dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("Failed to enumerate directory {}", macos_dir.display()))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("Failed to stat {}", path.display()))?;
        if !metadata.is_file() {
            continue;
        }
        found_regular_file = true;
        #[cfg(unix)]
        {
            let mode = metadata.permissions().mode();
            if mode & 0o111 == 0 {
                bail!(
                    "Executable bit is missing for {} (mode {:o})",
                    path.display(),
                    mode & 0o777
                );
            }
        }
    }

    if !found_regular_file {
        bail!(
            "Finalize input is missing a regular executable in {}",
            macos_dir.display()
        );
    }

    Ok(())
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("Failed to serialize JSON")?;
    let mut file =
        fs::File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("Failed to finalize {}", path.display()))?;
    Ok(())
}

fn create_unique_output_dir(output_root: &Path) -> Result<PathBuf> {
    for _ in 0..32 {
        let candidate = output_root.join(format!(
            "derived-{}-{}",
            Utc::now().format("%Y%m%dT%H%M%SZ"),
            random_hex(4)
        ));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Failed to create {}", candidate.display()))
            }
        }
    }
    bail!("Failed to allocate unique finalize output directory")
}

fn create_temp_subdir(root: &Path, prefix: &str) -> Result<PathBuf> {
    for _ in 0..32 {
        let candidate = root.join(format!("{}-{}", prefix, random_hex(8)));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Failed to create {}", candidate.display()))
            }
        }
    }
    bail!(
        "Failed to allocate temporary directory in {}",
        root.display()
    )
}

fn digest_dir_name(digest: &str) -> Result<String> {
    let normalized = digest
        .trim()
        .trim_start_matches("blake3:")
        .trim_start_matches("sha256:")
        .to_ascii_lowercase();
    if normalized.is_empty() {
        bail!("Digest label is empty");
    }
    Ok(normalized)
}

fn compute_blake3(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn fetches_root() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(DEFAULT_FETCHES_DIR))
}

fn projections_root() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(PROJECTIONS_DIR))
}

fn resolve_launcher_dir(launcher_dir: Option<&Path>) -> Result<PathBuf> {
    match launcher_dir {
        Some(path) => absolute_path(path),
        None => Ok(dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(DEFAULT_LAUNCHER_DIR)),
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("Failed to read current working directory")?
            .join(path))
    }
}

fn build_projection_id(
    derived_app_path: &Path,
    projected_path: &Path,
    derived_digest: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(derived_app_path.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hasher.update(projected_path.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hasher.update(derived_digest.as_bytes());
    hex::encode(&hasher.finalize().as_bytes()[..8])
}

fn paths_match(left: &Path, right: &Path) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    let left_canon = fs::canonicalize(left).ok();
    let right_canon = fs::canonicalize(right).ok();
    if let (Some(left_canon), Some(right_canon)) = (left_canon, right_canon) {
        return Ok(left_canon == right_canon);
    }
    Ok(absolute_path(left)? == absolute_path(right)?)
}

fn is_managed_symlink_to(path: &Path, target: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("Failed to stat {}", path.display())),
    };
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let link_target = fs::read_link(path)
        .with_context(|| format!("Failed to read symlink {}", path.display()))?;
    let resolved_target = if link_target.is_absolute() {
        link_target
    } else {
        path.parent()
            .unwrap_or_else(|| Path::new("."))
            .join(link_target)
    };
    paths_match(&resolved_target, target)
}

#[cfg(unix)]
fn create_projection_symlink(target: &Path, destination: &Path) -> std::io::Result<()> {
    symlink(target, destination)
}

#[cfg(windows)]
fn create_projection_symlink(target: &Path, destination: &Path) -> std::io::Result<()> {
    symlink_dir(target, destination)
}

fn host_supports_finalize() -> bool {
    cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64"
}

fn random_hex(len_bytes: usize) -> String {
    let mut bytes = vec![0u8; len_bytes];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    hex::encode(bytes)
}

#[cfg(unix)]
fn mode_bits(metadata: &fs::Metadata) -> u32 {
    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn mode_bits(_metadata: &fs::Metadata) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fn sample_delivery_toml() -> &'static str {
        r#"schema_version = "exp-0.1"
[artifact]
framework = "tauri"
stage = "unsigned"
target = "darwin/arm64"
input = "MyApp.app"
[finalize]
tool = "codesign"
args = ["--deep", "--force", "--sign", "-", "MyApp.app"]
"#
    }

    fn sample_fetch_dir(root: &Path) -> Result<PathBuf> {
        sample_fetch_dir_with_mode(root, 0o755)
    }

    fn sample_fetch_dir_with_mode(root: &Path, mode: u32) -> Result<PathBuf> {
        let fetched_dir = root.join("fetched");
        let artifact_dir = fetched_dir.join(FETCH_ARTIFACT_DIR);
        fs::create_dir_all(artifact_dir.join("MyApp.app/Contents/MacOS"))?;
        fs::write(
            artifact_dir.join(DELIVERY_CONFIG_FILE),
            sample_delivery_toml(),
        )?;
        fs::write(
            artifact_dir.join("MyApp.app/Contents/MacOS/MyApp"),
            b"unsigned-app",
        )?;
        #[cfg(unix)]
        {
            let binary = artifact_dir.join("MyApp.app/Contents/MacOS/MyApp");
            let mut permissions = fs::metadata(&binary)?.permissions();
            permissions.set_mode(mode);
            fs::set_permissions(&binary, permissions)?;
        }
        let metadata = FetchMetadata {
            schema_version: DELIVERY_SCHEMA_VERSION.to_string(),
            scoped_id: "local/my-app".to_string(),
            version: "0.1.0".to_string(),
            registry: "http://127.0.0.1:8787".to_string(),
            fetched_at: "2026-03-09T00:00:00Z".to_string(),
            parent_digest: compute_tree_digest(&artifact_dir)?,
            artifact_blake3: compute_blake3(b"artifact"),
        };
        fs::create_dir_all(&fetched_dir)?;
        write_json_pretty(&fetched_dir.join(FETCH_METADATA_FILE), &metadata)?;
        Ok(fetched_dir)
    }

    fn sample_finalized_app(root: &Path) -> Result<(PathBuf, PathBuf)> {
        let derived_dir = root.join("derived-output");
        let derived_app = derived_dir.join("MyApp.app");
        fs::create_dir_all(derived_app.join("Contents/MacOS"))?;
        fs::write(derived_app.join("Contents/MacOS/MyApp"), b"signed-app")?;
        #[cfg(unix)]
        {
            let binary = derived_app.join("Contents/MacOS/MyApp");
            let mut permissions = fs::metadata(&binary)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&binary, permissions)?;
        }
        let provenance = LocalDerivationProvenance {
            parent_digest: "blake3:parent-digest".to_string(),
            derived_digest: compute_tree_digest(&derived_app)?,
            framework: DELIVERY_FRAMEWORK.to_string(),
            target: DELIVERY_TARGET.to_string(),
            finalized_locally: true,
            finalize_tool: FINALIZE_TOOL.to_string(),
            finalized_at: "2026-03-09T00:00:00Z".to_string(),
        };
        write_json_pretty(&derived_dir.join(PROVENANCE_FILE), &provenance)?;
        Ok((derived_dir, derived_app))
    }

    #[test]
    fn delivery_config_rejects_non_codesign_tool() {
        let config: DeliveryConfig = toml::from_str(
            r#"schema_version = "exp-0.1"
[artifact]
    framework = "tauri"
    stage = "unsigned"
    target = "darwin/arm64"
    input = "MyApp.app"
[finalize]
    tool = "bash"
    args = ["--deep", "--force", "--sign", "-", "MyApp.app"]
"#,
        )
        .expect("config parse");
        let err = validate_delivery_config(&config).expect_err("config must fail");
        assert!(err.to_string().contains("PoC requires 'codesign'"));
    }

    #[test]
    fn resolve_fetch_request_accepts_issue_style_inline_registry_ref() -> Result<()> {
        let resolved =
            resolve_fetch_request("localhost:8080/my-tauri-app:unsigned-0.1.0", None, None)?;
        assert_eq!(
            resolved,
            ResolvedFetchRequest {
                capsule_ref: "local/my-tauri-app".to_string(),
                registry_url: Some("http://localhost:8080".to_string()),
                version: Some("unsigned-0.1.0".to_string()),
            }
        );
        Ok(())
    }

    #[test]
    fn resolve_fetch_request_accepts_inline_registry_with_explicit_scope() -> Result<()> {
        let resolved = resolve_fetch_request(
            "https://127.0.0.1:8787/koh0920/sample-native-capsule:0.1.0",
            None,
            None,
        )?;
        assert_eq!(
            resolved,
            ResolvedFetchRequest {
                capsule_ref: "koh0920/sample-native-capsule".to_string(),
                registry_url: Some("https://127.0.0.1:8787".to_string()),
                version: Some("0.1.0".to_string()),
            }
        );
        Ok(())
    }

    #[test]
    fn resolve_fetch_request_rejects_conflicting_registry_override() {
        let err = resolve_fetch_request(
            "localhost:8080/my-tauri-app:unsigned-0.1.0",
            Some("http://127.0.0.1:8787"),
            None,
        )
        .expect_err("registry conflict must fail");
        assert!(err.to_string().contains("conflicting_registry_request"));
    }

    #[test]
    fn tree_digest_is_stable_for_identical_trees() -> Result<()> {
        let tmp = tempdir()?;
        let left = tmp.path().join("left");
        let right = tmp.path().join("right");
        fs::create_dir_all(left.join("a/b"))?;
        fs::create_dir_all(right.join("a/b"))?;
        fs::write(left.join("a/b/file.txt"), b"hello")?;
        fs::write(right.join("a/b/file.txt"), b"hello")?;
        assert_eq!(compute_tree_digest(&left)?, compute_tree_digest(&right)?);
        Ok(())
    }

    #[test]
    fn finalize_creates_derived_copy_without_mutating_parent() -> Result<()> {
        let tmp = tempdir()?;
        let fetched_dir = sample_fetch_dir(tmp.path())?;
        let artifact_dir = fetched_dir.join(FETCH_ARTIFACT_DIR);
        let parent_before = compute_tree_digest(&artifact_dir)?;
        let output_root = tmp.path().join("dist");

        let result = finalize_with_runner(&fetched_dir, &output_root, |derived_dir, _config| {
            let app_binary = derived_dir.join("MyApp.app/Contents/MacOS/MyApp");
            fs::write(&app_binary, b"signed-app")?;
            Ok(())
        })?;

        assert_eq!(parent_before, result.parent_digest);
        assert_eq!(compute_tree_digest(&artifact_dir)?, parent_before);
        assert!(result.derived_app_path.exists());
        assert!(result.provenance_path.exists());
        assert_ne!(result.parent_digest, result.derived_digest);
        #[cfg(unix)]
        {
            let derived_binary = result.derived_app_path.join("Contents/MacOS/MyApp");
            assert_ne!(
                fs::metadata(&derived_binary)?.permissions().mode() & 0o111,
                0
            );
        }
        let sidecar: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&result.provenance_path)?)?;
        assert_eq!(sidecar["parent_digest"], result.parent_digest);
        assert_eq!(sidecar["derived_digest"], result.derived_digest);
        assert_eq!(sidecar["finalize_tool"], FINALIZE_TOOL);
        Ok(())
    }

    #[test]
    fn finalize_rejects_missing_executable_bit() -> Result<()> {
        let tmp = tempdir()?;
        let fetched_dir = sample_fetch_dir_with_mode(tmp.path(), 0o644)?;
        let output_root = tmp.path().join("dist");

        let err = finalize_with_runner(&fetched_dir, &output_root, |_derived_dir, _config| Ok(()))
            .expect_err("finalize must fail closed when executable bit is missing");
        assert!(err.to_string().contains("Executable bit is missing"));
        Ok(())
    }

    #[test]
    fn copy_recursively_preserves_executable_mode() -> Result<()> {
        let tmp = tempdir()?;
        let source = tmp.path().join("source.bin");
        let destination = tmp.path().join("nested/destination.bin");
        fs::write(&source, b"hello")?;
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&source)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&source, permissions)?;
        }

        copy_recursively(&source, &destination)?;

        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(&destination)?.permissions().mode() & 0o777,
                0o755
            );
        }
        Ok(())
    }

    #[test]
    fn materialize_fetch_cache_extracts_payload_tree() -> Result<()> {
        let tmp_home = tempdir()?;
        std::env::set_var("HOME", tmp_home.path());

        let payload_tar = {
            let mut out = Vec::new();
            {
                let mut builder = tar::Builder::new(&mut out);
                append_tar_entry(
                    &mut builder,
                    DELIVERY_CONFIG_FILE,
                    sample_delivery_toml().as_bytes(),
                    0o644,
                )?;
                append_tar_entry(
                    &mut builder,
                    "MyApp.app/Contents/MacOS/MyApp",
                    b"unsigned-app",
                    0o644,
                )?;
                builder.finish()?;
            }
            out
        };
        let artifact = build_capsule_bytes(&payload_tar)?;
        let result =
            materialize_fetch_cache("local/my-app", "0.1.0", "http://127.0.0.1:8787", &artifact)?;

        assert!(result.cache_dir.exists());
        assert!(result.artifact_dir.join(DELIVERY_CONFIG_FILE).exists());
        assert!(result
            .artifact_dir
            .join("MyApp.app/Contents/MacOS/MyApp")
            .exists());
        let metadata = load_fetch_metadata(&result.cache_dir)?;
        assert_eq!(metadata.parent_digest, result.parent_digest);
        Ok(())
    }

    #[test]
    fn materialize_fetch_cache_preserves_executable_mode_from_payload() -> Result<()> {
        let tmp_home = tempdir()?;
        std::env::set_var("HOME", tmp_home.path());

        let payload_tar = {
            let mut out = Vec::new();
            {
                let mut builder = tar::Builder::new(&mut out);
                append_tar_entry(
                    &mut builder,
                    DELIVERY_CONFIG_FILE,
                    sample_delivery_toml().as_bytes(),
                    0o644,
                )?;
                append_tar_entry(
                    &mut builder,
                    "MyApp.app/Contents/MacOS/MyApp",
                    b"unsigned-app",
                    0o755,
                )?;
                builder.finish()?;
            }
            out
        };
        let artifact = build_capsule_bytes(&payload_tar)?;
        let result =
            materialize_fetch_cache("local/my-app", "0.1.0", "http://127.0.0.1:8787", &artifact)?;

        #[cfg(unix)]
        {
            let binary = result.artifact_dir.join("MyApp.app/Contents/MacOS/MyApp");
            assert_ne!(fs::metadata(binary)?.permissions().mode() & 0o111, 0);
        }
        Ok(())
    }

    #[test]
    fn project_creates_symlink_metadata_without_mutating_derived_artifact() -> Result<()> {
        let tmp = tempdir()?;
        let metadata_root = tmp.path().join("projection-metadata");
        let launcher_dir = tmp.path().join("Applications");
        let (_derived_dir, derived_app) = sample_finalized_app(tmp.path())?;
        let digest_before = compute_tree_digest(&derived_app)?;

        let result = project_with_roots(&derived_app, &launcher_dir, &metadata_root)?;

        assert!(result.created);
        assert_eq!(result.state, "ok");
        assert_eq!(compute_tree_digest(&derived_app)?, digest_before);
        let symlink_meta = fs::symlink_metadata(&result.projected_path)?;
        assert!(symlink_meta.file_type().is_symlink());
        assert!(result.metadata_path.exists());
        Ok(())
    }

    #[test]
    fn project_rejects_name_conflict() -> Result<()> {
        let tmp = tempdir()?;
        let metadata_root = tmp.path().join("projection-metadata");
        let launcher_dir = tmp.path().join("Applications");
        let (_derived_dir, derived_app) = sample_finalized_app(tmp.path())?;
        fs::create_dir_all(&launcher_dir)?;
        fs::write(launcher_dir.join("MyApp.app"), b"occupied")?;

        let err = project_with_roots(&derived_app, &launcher_dir, &metadata_root)
            .expect_err("projection must reject name conflicts");
        assert!(err.to_string().contains("Projection name conflict"));
        Ok(())
    }

    #[test]
    fn project_list_reports_broken_projection_when_target_missing() -> Result<()> {
        let tmp = tempdir()?;
        let metadata_root = tmp.path().join("projection-metadata");
        let launcher_dir = tmp.path().join("Applications");
        let (_derived_dir, derived_app) = sample_finalized_app(tmp.path())?;
        let result = project_with_roots(&derived_app, &launcher_dir, &metadata_root)?;
        fs::rename(&derived_app, tmp.path().join("MyApp-orphaned.app"))?;

        let listing = list_projections(&metadata_root)?;
        assert_eq!(listing.total, 1);
        assert_eq!(listing.broken, 1);
        assert_eq!(listing.projections[0].projection_id, result.projection_id);
        assert!(listing.projections[0]
            .problems
            .iter()
            .any(|problem| problem == "derived_app_missing"));
        Ok(())
    }

    #[test]
    fn unproject_removes_symlink_and_metadata_even_when_target_missing() -> Result<()> {
        let tmp = tempdir()?;
        let metadata_root = tmp.path().join("projection-metadata");
        let launcher_dir = tmp.path().join("Applications");
        let (_derived_dir, derived_app) = sample_finalized_app(tmp.path())?;
        let result = project_with_roots(&derived_app, &launcher_dir, &metadata_root)?;
        fs::rename(&derived_app, tmp.path().join("MyApp-orphaned.app"))?;

        let unprojected = unproject_with_metadata_root(&result.projection_id, &metadata_root)?;
        assert!(unprojected.removed_projected_path);
        assert!(unprojected.removed_metadata);
        assert!(!result.projected_path.exists());
        assert!(!result.metadata_path.exists());
        Ok(())
    }

    #[test]
    fn project_rejects_digest_mismatch() -> Result<()> {
        let tmp = tempdir()?;
        let metadata_root = tmp.path().join("projection-metadata");
        let launcher_dir = tmp.path().join("Applications");
        let (derived_dir, derived_app) = sample_finalized_app(tmp.path())?;
        fs::write(derived_app.join("Contents/MacOS/MyApp"), b"tampered-app")?;

        let err = project_with_roots(&derived_app, &launcher_dir, &metadata_root)
            .expect_err("projection must reject digest mismatches");
        assert!(err.to_string().contains("Derived artifact digest mismatch"));
        assert!(derived_dir.join(PROVENANCE_FILE).exists());
        Ok(())
    }

    fn append_tar_entry(
        builder: &mut tar::Builder<&mut Vec<u8>>,
        path: &str,
        bytes: &[u8],
        mode: u32,
    ) -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(mode);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        header.set_cksum();
        builder.append_data(&mut header, path, Cursor::new(bytes))?;
        Ok(())
    }

    fn build_capsule_bytes(payload_tar: &[u8]) -> Result<Vec<u8>> {
        let payload_tar_zst = zstd::stream::encode_all(Cursor::new(payload_tar), 3)?;
        let mut out = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut out);
            append_tar_entry(&mut builder, "capsule.toml", b"schema_version = \"0.2\"\nname = \"demo\"\nversion = \"0.1.0\"\ntype = \"app\"\ndefault_target = \"cli\"\n[targets.cli]\nruntime = \"static\"\npath = \"MyApp.app\"\n", 0o644)?;
            append_tar_entry(&mut builder, "payload.tar.zst", &payload_tar_zst, 0o644)?;
            builder.finish()?;
        }
        Ok(out)
    }
}
