use std::collections::HashMap;
use std::io::ErrorKind;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::net::{SocketAddr, TcpListener};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use capsule_core::capsule_v3::manifest::validate_blake3_digest;
use capsule_core::capsule_v3::{verify_artifact_hash, CasStore};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use crate::artifact_hash::{
    compute_blake3_label as compute_blake3, compute_sha256_label as compute_sha256, equals_hash,
};
use crate::binding;
use crate::process_manager::{ProcessInfo, ProcessManager, ProcessStatus};
use crate::publish_artifact::{
    ChunkUploadResponse, SyncCommitRequest, SyncCommitResponse, SyncNegotiateRequest,
    SyncNegotiateResponse,
};
use crate::registry_store::{
    EpochResolveRequest, KeyRevokeRequest, KeyRotateRequest, LeaseRefreshRequest,
    LeaseReleaseRequest, NegotiateRequest, RegistryStore, RollbackRequest, YankRequest,
};
use crate::state::{ensure_registered_state_binding_in_store, load_manifest};

mod auth;
mod http;
mod local_api;
mod local_service;
mod routes;
mod ui;

use auth::*;
use http::*;
use local_api::*;
use local_service::*;
use routes::*;
use ui::*;

const README_CANDIDATES: [&str; 4] = ["README.md", "README.mdx", "README.txt", "README"];
const README_MAX_BYTES: usize = 512 * 1024;
const LOCAL_REGISTRY_DISABLE_UI_ENV: &str = "ATO_LOCAL_REGISTRY_DISABLE_UI";

#[derive(RustEmbed)]
#[folder = "apps/ato-store-local/dist"]
struct LocalRegistryUiAssets;

#[derive(Debug, Clone)]
pub struct RegistryServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub auth_token: Option<String>,
}

#[derive(Clone)]
struct AppState {
    listen_url: String,
    data_dir: PathBuf,
    auth_token: Option<String>,
    lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryIndex {
    schema_version: String,
    capsules: Vec<StoredCapsule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCapsule {
    id: String,
    publisher: String,
    slug: String,
    name: String,
    description: String,
    category: String,
    #[serde(rename = "type")]
    capsule_type: String,
    price: u64,
    currency: String,
    latest_version: String,
    releases: Vec<StoredRelease>,
    downloads: u64,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRelease {
    version: String,
    file_name: String,
    sha256: String,
    blake3: String,
    size_bytes: u64,
    signature_status: String,
    created_at: String,
    #[serde(default)]
    payload_v3: Option<StoredPayloadV3>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPayloadV3 {
    artifact_hash: String,
    chunk_count: usize,
    total_raw_size: u64,
    manifest_rel_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreMetadataEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    icon_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoreMetadataIndex {
    #[serde(default)]
    entries: HashMap<String, StoreMetadataEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeConfigIndex {
    #[serde(default)]
    entries: HashMap<String, CapsuleRuntimeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CapsuleRuntimeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    selected_target: Option<String>,
    #[serde(default)]
    targets: HashMap<String, RuntimeTargetConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PersistentStateListQuery {
    owner_scope: Option<String>,
    state_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ServiceBindingListQuery {
    owner_scope: Option<String>,
    service_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ServiceBindingResolveQuery {
    owner_scope: Option<String>,
    service_name: Option<String>,
    binding_kind: Option<String>,
    caller_service: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RegisterPersistentStateRequest {
    manifest: String,
    state_name: String,
    path: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegisterServiceBindingRequest {
    manifest: String,
    service_name: String,
    url: Option<String>,
    binding_kind: Option<String>,
    process_id: Option<String>,
    port: Option<u16>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RunPermissionMode {
    Sandbox,
    Dangerous,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeTargetConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    permission_mode: Option<RunPermissionMode>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: Option<String>,
    category: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DistributionQuery {
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UploadQuery {
    file_name: Option<String>,
    allow_existing: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UpsertStoreMetadataRequest {
    confirmed: bool,
    icon_path: Option<String>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpsertRuntimeConfigRequest {
    selected_target: Option<String>,
    targets: Option<HashMap<String, RuntimeTargetConfigRequest>>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeTargetConfigRequest {
    port: Option<u16>,
    env: Option<HashMap<String, String>>,
    permission_mode: Option<RunPermissionMode>,
}

#[derive(Debug, Deserialize)]
struct DeleteCapsuleQuery {
    version: Option<String>,
    confirmed: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ProcessLogsQuery {
    tail: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct UrlReadyQuery {
    url: Option<String>,
}

#[derive(Debug, Serialize)]
struct UploadResponse {
    scoped_id: String,
    version: String,
    artifact_url: String,
    file_name: String,
    sha256: String,
    blake3: String,
    size_bytes: u64,
    already_existed: bool,
}

#[derive(Debug, Serialize)]
struct DeleteCapsuleResponse {
    deleted: bool,
    scoped_id: String,
    removed_capsule: bool,
    removed_versions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    removed_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    removed_service_binding_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreMetadataPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    icon_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunCapsuleRequest {
    confirmed: bool,
    target: Option<String>,
    port: Option<u16>,
    env: Option<HashMap<String, String>>,
    permission_mode: Option<RunPermissionMode>,
}

#[derive(Debug, Serialize)]
struct RunCapsuleResponse {
    accepted: bool,
    scoped_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    registered_service_binding_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RollbackResponsePayload {
    scoped_id: String,
    target_manifest_hash: String,
    manifest_hash: String,
    to_epoch: u64,
    pointer: capsule_core::types::EpochPointer,
    public_key: String,
}

#[derive(Debug, Deserialize)]
struct StopProcessRequest {
    confirmed: bool,
    force: Option<bool>,
}

#[derive(Debug, Serialize)]
struct StopProcessResponse {
    stopped: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    removed_service_binding_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProcessRowResponse {
    id: String,
    name: String,
    pid: i32,
    status: String,
    active: bool,
    runtime: String,
    started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scoped_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_port: Option<u16>,
}

#[derive(Debug, Serialize)]
struct ProcessLogsResponse {
    lines: Vec<String>,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct ClearLogsResponse {
    cleared: bool,
}

#[derive(Debug, Serialize)]
struct UrlReadyResponse {
    ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PublisherInfo {
    handle: String,
    #[serde(rename = "authorDid")]
    author_did: String,
    verified: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SearchCapsuleRow {
    id: String,
    slug: String,
    scoped_id: String,
    #[serde(rename = "scopedId")]
    scoped_id_camel: String,
    name: String,
    description: String,
    category: String,
    #[serde(rename = "type")]
    capsule_type: String,
    price: u64,
    currency: String,
    publisher: PublisherInfo,
    #[serde(rename = "latestVersion")]
    latest_version: String,
    #[serde(rename = "latestSizeBytes")]
    latest_size_bytes: u64,
    downloads: u64,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_metadata: Option<StoreMetadataPayload>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    capsules: Vec<SearchCapsuleRow>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct CapsuleDetailResponse {
    id: String,
    scoped_id: String,
    slug: String,
    name: String,
    description: String,
    price: u64,
    currency: String,
    #[serde(rename = "latestVersion")]
    latest_version: String,
    releases: Vec<CapsuleReleaseRow>,
    publisher: PublisherInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest_toml: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capsule_lock: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readme_markdown: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readme_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_metadata: Option<StoreMetadataPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_config: Option<CapsuleRuntimeConfig>,
}

#[derive(Debug, Serialize)]
struct CapsuleReleaseRow {
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest_hash: Option<String>,
    content_hash: String,
    signature_status: String,
    is_current: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    yanked_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct DistributionResponse {
    version: String,
    artifact_url: String,
    sha256: String,
    blake3: String,
    file_name: String,
    signature_status: String,
    publisher_verified: bool,
}

#[derive(Debug)]
struct ArtifactMeta {
    name: String,
    version: String,
    description: String,
}

pub async fn serve(config: RegistryServerConfig) -> Result<()> {
    let host = config.host;
    let access_host = display_access_host(&host);
    let auth_token = config
        .auth_token
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());
    if host != "127.0.0.1" && auth_token.is_none() {
        bail!("--auth-token is required when binding local registry to non-loopback host");
    }
    let data_dir = expand_data_dir(&config.data_dir)?;
    initialize_storage(&data_dir)?;
    let listen_url = format!("http://{}:{}", host, config.port);
    let state = AppState {
        listen_url: listen_url.clone(),
        data_dir,
        auth_token,
        lock: Arc::new(Mutex::new(())),
    };
    spawn_registry_gc_worker(state.data_dir.clone());

    let ui_enabled = std::env::var_os(LOCAL_REGISTRY_DISABLE_UI_ENV).is_none();

    let mut app = build_app_router(ui_enabled).with_state(state);

    if std::env::var_os("ATO_LOCAL_REGISTRY_DEV_CORS").is_some() {
        app = app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::PUT,
                    Method::POST,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any),
        );
    }

    app = app.layer(DefaultBodyLimit::max(512 * 1024 * 1024));

    let addr: SocketAddr = format!("{}:{}", host, config.port)
        .parse()
        .context("Invalid listen address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!(format_bind_error(addr, &err)))?;
    let access_base_url = format!("http://{}:{}", access_host, config.port);
    println!("🚀 Local registry serving at {}", listen_url);
    println!("🔌 API: {}/v1/...", access_base_url);
    if ui_enabled {
        println!("🌐 Web UI: {}/", access_base_url);
    }
    if ui_enabled && LocalRegistryUiAssets::get("index.html").is_none() {
        println!("⚠️  Web UI assets are missing. Rebuild with `cargo build` after installing npm deps in apps/ato-store-local.");
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Local registry server failed")?;
    Ok(())
}

fn format_bind_error(addr: SocketAddr, err: &std::io::Error) -> String {
    let mut message = format!("Failed to bind {}: {}", addr, err);
    match err.kind() {
        ErrorKind::AddrInUse => {
            message.push_str(". Another process is already listening on that port. Try a different `--port` or inspect listeners with `lsof -nP -iTCP:<port> -sTCP:LISTEN`.");
        }
        ErrorKind::AddrNotAvailable => {
            message.push_str(". The requested host is not available on this machine.");
        }
        ErrorKind::PermissionDenied => {
            message.push_str(". Permission was denied while opening the socket.");
        }
        _ => {}
    }
    message
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn spawn_registry_gc_worker(data_dir: PathBuf) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut ticks: u64 = 0;
        loop {
            interval.tick().await;
            let store = match RegistryStore::open(&data_dir) {
                Ok(store) => store,
                Err(err) => {
                    tracing::warn!(
                        "registry gc worker failed to open store path={} error={}",
                        data_dir.display(),
                        err
                    );
                    continue;
                }
            };

            let now = Utc::now().to_rfc3339();
            match store.gc_tick(&now, 32) {
                Ok(tick) => {
                    if tick.deleted > 0 {
                        let vacuum_pages = (tick.deleted.saturating_mul(2)).max(1);
                        if let Err(err) = store.incremental_vacuum(vacuum_pages) {
                            tracing::warn!(
                                "registry gc incremental vacuum failed path={} error={}",
                                data_dir.display(),
                                err
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "registry gc tick failed path={} error={}",
                        data_dir.display(),
                        err
                    );
                }
            }

            ticks = ticks.saturating_add(1);
            if ticks.is_multiple_of(60) {
                if let Err(err) = store.checkpoint_wal_truncate() {
                    tracing::warn!(
                        "registry gc checkpoint failed path={} error={}",
                        data_dir.display(),
                        err
                    );
                }
            }
        }
    });
}

async fn handle_well_known(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let write_auth_required = state.auth_token.is_some();
    Json(json!({
        "url": public_base_url,
        "name": "Ato Local Registry",
        "version": "1",
        "write_auth_required": write_auth_required
    }))
}

fn display_access_host(bind_host: &str) -> &str {
    match bind_host {
        "0.0.0.0" => "127.0.0.1",
        "::" | "[::]" => "::1",
        _ => bind_host,
    }
}

async fn handle_search_capsules(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let store_metadata = load_store_metadata(&state.data_dir).unwrap_or_default();
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);

    let limit = query.limit.unwrap_or(20).clamp(1, 50);
    let cursor = query
        .cursor
        .as_deref()
        .unwrap_or("0")
        .parse::<usize>()
        .unwrap_or(0);
    let needle = query.q.as_deref().unwrap_or("").trim().to_lowercase();
    let category = query.category.as_deref().map(str::to_lowercase);

    let mut rows = index
        .capsules
        .iter()
        .filter(|capsule| {
            let metadata_text =
                get_store_metadata_entry(&store_metadata, &capsule.publisher, &capsule.slug)
                    .and_then(|entry| entry.text.as_deref())
                    .unwrap_or(capsule.description.as_str());
            if needle.is_empty() {
                true
            } else {
                capsule.slug.to_lowercase().contains(&needle)
                    || capsule.name.to_lowercase().contains(&needle)
                    || metadata_text.to_lowercase().contains(&needle)
            }
        })
        .filter(|capsule| {
            category
                .as_ref()
                .map(|cat| capsule.category.to_lowercase() == *cat)
                .unwrap_or(true)
        })
        .map(|capsule| {
            let metadata =
                get_store_metadata_entry(&store_metadata, &capsule.publisher, &capsule.slug);
            stored_to_search_row(capsule, metadata, &public_base_url)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

    let total = rows.len();
    let start = cursor.min(total);
    let end = (start + limit).min(total);
    let page = rows[start..end].to_vec();
    let next_cursor = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (
        StatusCode::OK,
        Json(SearchResponse {
            capsules: page,
            next_cursor,
        }),
    )
        .into_response()
}

async fn handle_get_capsule(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let Some(capsule) = index
        .capsules
        .iter()
        .find(|c| c.publisher == publisher && c.slug == slug)
    else {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    };
    let store_metadata = load_store_metadata(&state.data_dir).unwrap_or_default();
    let metadata = get_store_metadata_entry(&store_metadata, &publisher, &slug);
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let store_metadata_payload = metadata_to_payload(metadata, &public_base_url, &publisher, &slug);
    let runtime_config = load_runtime_config(&state.data_dir)
        .ok()
        .and_then(|index| get_runtime_config_entry(&index, &publisher, &slug).cloned());
    let scoped_id = format!("{}/{}", capsule.publisher, capsule.slug);
    let store = RegistryStore::open(&state.data_dir).ok();
    let current_manifest_hash = store.as_ref().and_then(|store| {
        store
            .resolve_epoch_pointer(&scoped_id)
            .ok()
            .flatten()
            .map(|response| response.pointer.manifest_hash)
    });

    let (manifest, repository, manifest_toml, capsule_lock, readme_markdown, readme_source) =
        load_capsule_detail_manifest(&state.data_dir, capsule);
    let readme_markdown = append_store_metadata_section(readme_markdown, metadata);
    let detail = CapsuleDetailResponse {
        id: capsule.id.clone(),
        scoped_id: scoped_id.clone(),
        slug: capsule.slug.clone(),
        name: capsule.name.clone(),
        description: metadata
            .and_then(|entry| entry.text.as_ref())
            .map(String::as_str)
            .unwrap_or(capsule.description.as_str())
            .to_string(),
        price: capsule.price,
        currency: capsule.currency.clone(),
        latest_version: capsule.latest_version.clone(),
        releases: capsule
            .releases
            .iter()
            .map(|release| {
                let resolved = store.as_ref().and_then(|store| {
                    store
                        .resolve_release_version(&publisher, &slug, &release.version)
                        .ok()
                        .flatten()
                });
                let manifest_hash = resolved.as_ref().map(|record| record.manifest_hash.clone());
                let is_current = manifest_hash.as_deref() == current_manifest_hash.as_deref();
                let yanked_at = resolved.and_then(|record| record.yanked_at);
                CapsuleReleaseRow {
                    version: release.version.clone(),
                    manifest_hash,
                    content_hash: release.blake3.clone(),
                    signature_status: release.signature_status.clone(),
                    is_current,
                    yanked_at,
                }
            })
            .collect(),
        publisher: publisher_info(&capsule.publisher),
        manifest,
        manifest_toml,
        capsule_lock,
        repository,
        readme_markdown,
        readme_source,
        store_metadata: store_metadata_payload,
        runtime_config,
    };
    (StatusCode::OK, Json(detail)).into_response()
}

async fn handle_distributions(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    Query(query): Query<DistributionQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let Some(capsule) = index
        .capsules
        .iter()
        .find(|c| c.publisher == publisher && c.slug == slug)
    else {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    };

    let requested = query
        .version
        .unwrap_or_else(|| capsule.latest_version.clone());
    let Some(release) = capsule.releases.iter().find(|r| r.version == requested) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "version_not_found",
            "Version not found",
        );
    };
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let artifact_url = format!(
        "{}/v1/artifacts/{}/{}/{}/{}",
        public_base_url, capsule.publisher, capsule.slug, release.version, release.file_name
    );
    let response = DistributionResponse {
        version: release.version.clone(),
        artifact_url,
        sha256: release.sha256.clone(),
        blake3: release.blake3.clone(),
        file_name: release.file_name.clone(),
        signature_status: release.signature_status.clone(),
        publisher_verified: true,
    };
    (StatusCode::OK, Json(response)).into_response()
}

async fn handle_download(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    Query(query): Query<DistributionQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let Some(capsule) = index
        .capsules
        .iter()
        .find(|c| c.publisher == publisher && c.slug == slug)
    else {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    };

    let requested = query
        .version
        .unwrap_or_else(|| capsule.latest_version.clone());
    let Some(release) = capsule.releases.iter().find(|r| r.version == requested) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "version_not_found",
            "Version not found",
        );
    };

    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let artifact_url = format!(
        "{}/v1/artifacts/{}/{}/{}/{}",
        public_base_url, capsule.publisher, capsule.slug, release.version, release.file_name
    );
    (
        StatusCode::FOUND,
        [(header::LOCATION, artifact_url.as_str())],
    )
        .into_response()
}

async fn handle_manifest_negotiate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<NegotiateRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.negotiate(&request) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            let message = err.to_string();
            if message.contains("manifest yanked:") {
                return (
                    StatusCode::GONE,
                    Json(json!({
                        "error": "manifest_yanked",
                        "message": message,
                        "yanked": true
                    })),
                )
                    .into_response();
            }
            json_error(StatusCode::BAD_REQUEST, "negotiate_failed", &message)
        }
    }
}

async fn handle_manifest_get_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(manifest_hash): AxumPath<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.load_manifest_document(&manifest_hash) {
        Ok(Some(document)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/toml; charset=utf-8")],
            document,
        )
            .into_response(),
        Ok(None) => json_error(StatusCode::NOT_FOUND, "not_found", "Manifest not found"),
        Err(err) => {
            let message = err.to_string();
            if message.contains("manifest yanked:") {
                return (
                    StatusCode::GONE,
                    Json(json!({
                        "error": "manifest_yanked",
                        "message": message,
                        "yanked": true
                    })),
                )
                    .into_response();
            }
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "manifest_read_failed",
                &message,
            )
        }
    }
}

async fn handle_manifest_resolve_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((publisher, slug, version)): AxumPath<(String, String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.resolve_release_version(&publisher, &slug, &version) {
        Ok(Some(record)) => {
            if let Some(yanked_at) = record.yanked_at.clone() {
                return (
                    StatusCode::GONE,
                    Json(json!({
                        "error": "manifest_yanked",
                        "message": format!(
                            "manifest yanked: scoped_id={} manifest_hash={} yanked_at={}",
                            record.scoped_id,
                            record.manifest_hash,
                            yanked_at
                        ),
                        "yanked": true,
                        "yanked_at": yanked_at
                    })),
                )
                    .into_response();
            }
            (StatusCode::OK, Json(record)).into_response()
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, "not_found", "Version not found"),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "resolve_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_get_chunk(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(chunk_hash): AxumPath<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.load_chunk_bytes(&chunk_hash) {
        Ok(Some(bytes)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Ok(None) => json_error(StatusCode::NOT_FOUND, "not_found", "Chunk not found"),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "chunk_read_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_epoch_resolve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EpochResolveRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.resolve_epoch_pointer(&request.scoped_id) {
        Ok(Some(response)) => (StatusCode::OK, Json(response)).into_response(),
        Ok(None) => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Epoch pointer not found",
        ),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "epoch_resolve_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_lease_refresh(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LeaseRefreshRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    let ttl_secs = request.ttl_secs.unwrap_or(300).max(1);
    match store.refresh_lease(&request.lease_id, ttl_secs) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => json_error(
            StatusCode::BAD_REQUEST,
            "lease_refresh_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_lease_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LeaseReleaseRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_read_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.release_lease(&request.lease_id) {
        Ok(removed) => (StatusCode::OK, Json(json!({ "released": removed }))).into_response(),
        Err(err) => json_error(
            StatusCode::BAD_REQUEST,
            "lease_release_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_key_rotate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<KeyRotateRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.rotate_signing_key(request.overlap_hours.unwrap_or(24)) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "key_rotate_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_key_revoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<KeyRevokeRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.revoke_key(&request.key_id, request.did.as_deref()) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => json_error(
            StatusCode::BAD_REQUEST,
            "key_revoke_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_manifest_rollback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RollbackRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.rollback_to_manifest(&request.scoped_id, &request.target_manifest_hash) {
        Ok(Some(response)) => (
            StatusCode::OK,
            Json(RollbackResponsePayload {
                scoped_id: request.scoped_id.clone(),
                target_manifest_hash: request.target_manifest_hash.clone(),
                manifest_hash: response.pointer.manifest_hash.clone(),
                to_epoch: response.pointer.epoch,
                pointer: response.pointer,
                public_key: response.public_key,
            }),
        )
            .into_response(),
        Ok(None) => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Rollback target not found",
        ),
        Err(err) => {
            let message = err.to_string();
            if message.contains(crate::error_codes::ATO_ERR_INTEGRITY_FAILURE)
                || message.contains("rollback target is yanked")
            {
                return json_error(StatusCode::CONFLICT, "rollback_failed", &message);
            }
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "rollback_failed",
                &message,
            )
        }
    }
}

async fn handle_manifest_yank(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<YankRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };
    match store.yank_manifest(&request.scoped_id, &request.target_manifest_hash) {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({
                "scoped_id": request.scoped_id,
                "target_manifest_hash": request.target_manifest_hash,
                "yanked": true,
            })),
        )
            .into_response(),
        Ok(false) => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Yank target not found in capsule history",
        ),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "yank_failed",
            &err.to_string(),
        ),
    }
}

async fn handle_get_artifact(
    State(state): State<AppState>,
    AxumPath((publisher, slug, version, file_name)): AxumPath<(String, String, String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    if let Err(err) = validate_version(&version) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_version", &err.to_string());
    }
    if let Err(err) = validate_file_name(&file_name) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_file_name",
            &err.to_string(),
        );
    }

    let path = artifact_path(&state.data_dir, &publisher, &slug, &version, &file_name);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(_) => return json_error(StatusCode::NOT_FOUND, "not_found", "Artifact not found"),
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    )
        .into_response()
}

async fn handle_sync_negotiate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SyncNegotiateRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }

    if request.schema_version != 3 {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_schema_version",
            "schema_version must be 3",
        );
    }
    if let Err(err) = validate_blake3_digest("artifact_hash", &request.artifact_hash) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_artifact_hash",
            &err.to_string(),
        );
    }

    let cas = match registry_cas_store(&state.data_dir) {
        Ok(cas) => cas,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas_init_failed",
                &err.to_string(),
            );
        }
    };

    let mut missing_chunks = Vec::new();
    for chunk in &request.chunks {
        if let Err(err) = validate_blake3_digest("chunk.raw_hash", &chunk.raw_hash) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_chunk_hash",
                &err.to_string(),
            );
        }
        if chunk.raw_size == 0 {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_chunk_size",
                "chunk.raw_size must be greater than zero",
            );
        }

        match cas.has_chunk(&chunk.raw_hash) {
            Ok(true) => {}
            Ok(false) => missing_chunks.push(chunk.raw_hash.clone()),
            Err(err) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "cas_lookup_failed",
                    &err.to_string(),
                );
            }
        }
    }

    (
        StatusCode::OK,
        Json(SyncNegotiateResponse {
            missing_chunks,
            total_chunks: request.chunks.len(),
        }),
    )
        .into_response()
}

async fn handle_put_chunk(
    State(state): State<AppState>,
    AxumPath(raw_hash): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    if let Err(err) = validate_blake3_digest("raw_hash", &raw_hash) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_chunk_hash",
            &err.to_string(),
        );
    }

    let raw_size = match parse_required_u32_header(&headers, "x-raw-size") {
        Ok(v) if v > 0 => v,
        Ok(_) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_chunk_size",
                "x-raw-size must be greater than zero",
            );
        }
        Err(err) => {
            return json_error(StatusCode::BAD_REQUEST, "missing_header", &err.to_string());
        }
    };

    if let Err(err) = verify_uploaded_chunk(&raw_hash, raw_size, &body) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "chunk_validation_failed",
            &err,
        );
    }

    let cas = match registry_cas_store(&state.data_dir) {
        Ok(cas) => cas,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas_init_failed",
                &err.to_string(),
            );
        }
    };
    let put = match cas.put_chunk_zstd(&raw_hash, &body) {
        Ok(result) => result,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "chunk_store_failed",
                &err.to_string(),
            );
        }
    };

    let status = if put.inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(ChunkUploadResponse {
            raw_hash,
            inserted: put.inserted,
            zstd_size: put.zstd_size,
        }),
    )
        .into_response()
}

async fn handle_get_chunk(
    State(state): State<AppState>,
    AxumPath(raw_hash): AxumPath<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_blake3_digest("raw_hash", &raw_hash) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_chunk_hash",
            &err.to_string(),
        );
    }

    let cas = match registry_cas_store(&state.data_dir) {
        Ok(cas) => cas,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas_init_failed",
                &err.to_string(),
            );
        }
    };
    let path = match cas.chunk_path(&raw_hash) {
        Ok(path) => path,
        Err(err) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_chunk_hash",
                &err.to_string(),
            );
        }
    };
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(_) => return json_error(StatusCode::NOT_FOUND, "not_found", "Chunk not found"),
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/zstd"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response()
}

async fn handle_sync_commit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SyncCommitRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    if let Err(err) = validate_capsule_segments(&request.publisher, &request.slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    if let Err(err) = validate_version(&request.version) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_version", &err.to_string());
    }
    if let Err(err) = request.manifest.validate() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_manifest",
            &err.to_string(),
        );
    }
    if let Err(err) = verify_artifact_hash(&request.manifest) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "hash_mismatch",
            &err.to_string(),
        );
    }

    let cas = match registry_cas_store(&state.data_dir) {
        Ok(cas) => cas,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas_init_failed",
                &err.to_string(),
            );
        }
    };
    let fsck = match cas.fsck_manifest(&request.manifest) {
        Ok(report) => report,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "fsck_failed",
                &err.to_string(),
            );
        }
    };
    if !fsck.is_ok() {
        let message = if fsck.hard_errors.is_empty() {
            "manifest references invalid chunks".to_string()
        } else {
            fsck.hard_errors.join("; ")
        };
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "manifest_chunks_invalid",
            &message,
        );
    }

    let canonical_manifest = match serde_jcs::to_vec(&request.manifest) {
        Ok(bytes) => bytes,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "manifest_serialize_failed",
                &err.to_string(),
            );
        }
    };
    let rel_path = release_manifest_rel_path(&request.publisher, &request.slug, &request.version);
    let abs_path = state.data_dir.join(&rel_path);
    if let Err(err) = atomic_write_bytes(&abs_path, &canonical_manifest) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "manifest_write_failed",
            &err.to_string(),
        );
    }

    (
        StatusCode::CREATED,
        Json(SyncCommitResponse {
            scoped_id: format!("{}/{}", request.publisher, request.slug),
            version: request.version,
            artifact_hash: request.manifest.artifact_hash,
            chunk_count: request.manifest.chunks.len(),
            total_raw_size: request.manifest.total_raw_size,
        }),
    )
        .into_response()
}

async fn handle_get_release_manifest(
    State(state): State<AppState>,
    AxumPath((publisher, slug, version)): AxumPath<(String, String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    if let Err(err) = validate_version(&version) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_version", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let mut manifest_path = state
        .data_dir
        .join(release_manifest_rel_path(&publisher, &slug, &version));
    if let Ok(index) = load_index(&state.data_dir) {
        if let Some(rel_path) = index
            .capsules
            .iter()
            .find(|c| c.publisher == publisher && c.slug == slug)
            .and_then(|c| c.releases.iter().find(|r| r.version == version))
            .and_then(|r| r.payload_v3.as_ref())
            .map(|v| v.manifest_rel_path.clone())
        {
            manifest_path = state.data_dir.join(rel_path);
        }
    }
    let bytes = match std::fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                "payload v3 manifest not found",
            )
        }
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response()
}

async fn handle_put_local_capsule(
    State(state): State<AppState>,
    AxumPath((publisher, slug, version)): AxumPath<(String, String, String)>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }

    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    if let Err(err) = validate_version(&version) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_version", &err.to_string());
    }
    let file_name = query
        .file_name
        .unwrap_or_else(|| format!("{}-{}.capsule", slug, version));
    let allow_existing = query.allow_existing.unwrap_or(false);
    if let Err(err) = validate_file_name(&file_name) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_file_name",
            &err.to_string(),
        );
    }

    let expected_sha = match get_required_header(&headers, "x-ato-sha256") {
        Ok(v) => v,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, "missing_header", &err.to_string()),
    };
    let expected_blake3 = match get_required_header(&headers, "x-ato-blake3") {
        Ok(v) => v,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, "missing_header", &err.to_string()),
    };

    let actual_sha = compute_sha256(&body);
    if !equals_hash(&expected_sha, &actual_sha) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "hash_mismatch",
            "sha256 mismatch",
        );
    }
    let actual_blake3 = compute_blake3(&body);
    if !equals_hash(&expected_blake3, &actual_blake3) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "hash_mismatch",
            "blake3 mismatch",
        );
    }

    let artifact_meta = match parse_artifact_manifest(&body) {
        Ok(meta) => meta,
        Err(err) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_artifact",
                &format!("manifest parse failed: {}", err),
            )
        }
    };
    if artifact_meta.name != slug {
        return json_error(
            StatusCode::BAD_REQUEST,
            "scoped_id_mismatch",
            "path slug does not match artifact manifest.name",
        );
    }
    if artifact_meta.version != version {
        return json_error(
            StatusCode::BAD_REQUEST,
            "version_mismatch",
            "path version does not match artifact manifest.version",
        );
    }

    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            )
        }
    };

    if let Some(existing_release) = match store.find_registry_release(&publisher, &slug, &version) {
        Ok(release) => release,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_query_failed",
                &err.to_string(),
            )
        }
    } {
        match existing_release_outcome(&existing_release.sha256, allow_existing, &actual_sha) {
            ExistingReleaseOutcome::Reuse => {
                let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
                let artifact_url = format!(
                    "{}/v1/artifacts/{}/{}/{}/{}",
                    public_base_url, publisher, slug, version, existing_release.file_name
                );
                return (
                    StatusCode::OK,
                    Json(UploadResponse {
                        scoped_id: format!("{}/{}", publisher, slug),
                        version,
                        artifact_url,
                        file_name: existing_release.file_name.clone(),
                        sha256: format!("sha256:{}", existing_release.sha256),
                        blake3: format!("blake3:{}", existing_release.blake3),
                        size_bytes: existing_release.size_bytes,
                        already_existed: true,
                    }),
                )
                    .into_response();
            }
            ExistingReleaseOutcome::Conflict(message) => {
                return json_error(StatusCode::CONFLICT, "version_exists", message);
            }
        }
    }

    let artifact_path = artifact_path(&state.data_dir, &publisher, &slug, &version, &file_name);
    if let Some(parent) = artifact_path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                &format!("failed to create artifact dir: {}", err),
            );
        }
    }
    if let Err(err) = std::fs::write(&artifact_path, &body) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            &format!("failed to write artifact: {}", err),
        );
    }

    let now = Utc::now().to_rfc3339();
    if let Err(err) = store.publish_registry_release(
        &publisher,
        &slug,
        &artifact_meta.name,
        &artifact_meta.description,
        &version,
        &file_name,
        &actual_sha,
        &actual_blake3,
        body.len() as u64,
        &body,
        &now,
    ) {
        let message = err.to_string();
        let _ = std::fs::remove_file(&artifact_path);
        if message.contains("capsule.toml not found")
            || message.contains("payload.tar.zst not found")
        {
            return json_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "manifest_required",
                "capsule.toml and payload.tar.zst are required for upload",
            );
        }
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "registry_record_failed",
            &message,
        );
    }

    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let artifact_url = format!(
        "{}/v1/artifacts/{}/{}/{}/{}",
        public_base_url, publisher, slug, version, file_name
    );
    (
        StatusCode::CREATED,
        Json(UploadResponse {
            scoped_id: format!("{}/{}", publisher, slug),
            version,
            artifact_url,
            file_name,
            sha256: actual_sha,
            blake3: actual_blake3,
            size_bytes: body.len() as u64,
            already_existed: false,
        }),
    )
        .into_response()
}

async fn handle_get_store_metadata(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let exists = index
        .capsules
        .iter()
        .any(|capsule| capsule.publisher == publisher && capsule.slug == slug);
    if !exists {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    }

    let store_metadata = load_store_metadata(&state.data_dir).unwrap_or_default();
    let metadata = get_store_metadata_entry(&store_metadata, &publisher, &slug);
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let payload = metadata_to_payload(metadata, &public_base_url, &publisher, &slug).unwrap_or(
        StoreMetadataPayload {
            icon_path: None,
            text: None,
            icon_url: None,
        },
    );
    (StatusCode::OK, Json(payload)).into_response()
}

async fn handle_put_store_metadata(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<UpsertStoreMetadataRequest>,
) -> impl IntoResponse {
    if !request.confirmed {
        return json_error(
            StatusCode::BAD_REQUEST,
            "confirmation_required",
            "confirmed=true is required",
        );
    }
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let icon_path = normalize_optional_string(request.icon_path);
    let text = normalize_optional_string(request.text);
    let scoped_id = format!("{}/{}", publisher, slug);
    let now = Utc::now().to_rfc3339();

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let exists = index
        .capsules
        .iter()
        .any(|capsule| capsule.publisher == publisher && capsule.slug == slug);
    if !exists {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    }

    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            );
        }
    };
    if let Err(err) =
        store.upsert_store_metadata(&scoped_id, icon_path.as_deref(), text.as_deref(), &now)
    {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_metadata_upsert_failed",
            &err.to_string(),
        );
    }
    let store_metadata = load_store_metadata(&state.data_dir).unwrap_or_default();
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let payload = metadata_to_payload(
        get_store_metadata_entry(&store_metadata, &publisher, &slug),
        &public_base_url,
        &publisher,
        &slug,
    )
    .unwrap_or(StoreMetadataPayload {
        icon_path: None,
        text: None,
        icon_url: None,
    });

    (
        StatusCode::OK,
        Json(json!({
            "updated": true,
            "scoped_id": scoped_id,
            "store_metadata": payload,
        })),
    )
        .into_response()
}

async fn handle_get_store_icon(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            );
        }
    };
    let scoped_id = format!("{}/{}", publisher, slug);
    let metadata = match store.load_store_metadata_entry(&scoped_id) {
        Ok(metadata) => metadata,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "store_metadata_read_failed",
                &err.to_string(),
            );
        }
    };
    let Some(metadata) = metadata else {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Store metadata not found",
        );
    };
    let Some(raw_icon_path) = metadata.icon_path.as_deref() else {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Icon path is not set");
    };
    let icon_path = expand_user_path(raw_icon_path);
    let bytes = match std::fs::read(&icon_path) {
        Ok(bytes) => bytes,
        Err(err) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("Icon file is not readable: {}", err),
            );
        }
    };
    let content_type = mime_guess::from_path(&icon_path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
        ],
        bytes,
    )
        .into_response()
}

async fn handle_get_runtime_config(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let exists = index
        .capsules
        .iter()
        .any(|capsule| capsule.publisher == publisher && capsule.slug == slug);
    if !exists {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    }

    let runtime_config = load_runtime_config(&state.data_dir)
        .ok()
        .and_then(|cfg| get_runtime_config_entry(&cfg, &publisher, &slug).cloned())
        .unwrap_or_default();
    (StatusCode::OK, Json(runtime_config)).into_response()
}

async fn handle_put_runtime_config(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    Json(request): Json<UpsertRuntimeConfigRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let exists = index
        .capsules
        .iter()
        .any(|capsule| capsule.publisher == publisher && capsule.slug == slug);
    if !exists {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    }

    let mut runtime_index = load_runtime_config(&state.data_dir).unwrap_or_default();
    let entry_key = runtime_config_key(&publisher, &slug);
    let selected_target = normalize_optional_string(request.selected_target);
    let mut targets = HashMap::new();
    if let Some(request_targets) = request.targets {
        for (raw_label, raw_config) in request_targets {
            let label = raw_label.trim();
            if label.is_empty() {
                continue;
            }
            let env = raw_config
                .env
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(key, value)| {
                    let normalized = key.trim();
                    if normalized.is_empty() {
                        None
                    } else {
                        Some((normalized.to_string(), value))
                    }
                })
                .collect::<HashMap<_, _>>();
            if env.is_empty() && raw_config.port.is_none() && raw_config.permission_mode.is_none() {
                continue;
            }
            targets.insert(
                label.to_string(),
                RuntimeTargetConfig {
                    port: raw_config.port,
                    env,
                    permission_mode: raw_config.permission_mode,
                },
            );
        }
    }
    let next_config = CapsuleRuntimeConfig {
        selected_target,
        targets,
    };
    if next_config.selected_target.is_none() && next_config.targets.is_empty() {
        runtime_index.entries.remove(&entry_key);
    } else {
        runtime_index.entries.insert(entry_key, next_config.clone());
    }
    if let Err(err) = write_runtime_config(&state.data_dir, &runtime_index) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime_config_write_failed",
            &err.to_string(),
        );
    }
    (StatusCode::OK, Json(next_config)).into_response()
}

async fn handle_run_local_capsule(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<RunCapsuleRequest>,
) -> impl IntoResponse {
    if !request.confirmed {
        return json_error(
            StatusCode::BAD_REQUEST,
            "confirmation_required",
            "confirmed=true is required",
        );
    }
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    if request.port.is_some_and(|port| port == 0) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_port",
            "port must be between 1 and 65535",
        );
    }

    let requested_target = request
        .target
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let requested_env_overrides = request
        .env
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| {
            let trimmed = key.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some((trimmed.to_string(), value))
            }
        })
        .collect::<HashMap<_, _>>();

    let _guard = state.lock.lock().await;
    let index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let Some(capsule) = index
        .capsules
        .iter()
        .find(|capsule| capsule.publisher == publisher && capsule.slug == slug)
    else {
        return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
    };

    let local_artifact = resolve_run_artifact_path(&state.data_dir, capsule);
    let saved_runtime_config = load_runtime_config(&state.data_dir)
        .ok()
        .and_then(|cfg| get_runtime_config_entry(&cfg, &publisher, &slug).cloned());

    let mut effective_target = requested_target.clone().or_else(|| {
        saved_runtime_config
            .as_ref()
            .and_then(|cfg| normalize_optional_string(cfg.selected_target.clone()))
    });
    let mut effective_port = request.port;
    let mut effective_permission_mode = request.permission_mode;
    let mut env_overrides = HashMap::new();

    if let Some(saved) = saved_runtime_config.as_ref() {
        let saved_target_config = effective_target
            .as_deref()
            .and_then(|label| saved.targets.get(label))
            .or_else(|| {
                saved
                    .selected_target
                    .as_deref()
                    .and_then(|label| saved.targets.get(label))
            });
        if let Some(target_config) = saved_target_config {
            if effective_port.is_none() {
                effective_port = target_config.port;
            }
            if effective_permission_mode.is_none() {
                effective_permission_mode = target_config.permission_mode;
            }
            for (key, value) in &target_config.env {
                let normalized = key.trim();
                if !normalized.is_empty() {
                    env_overrides.insert(normalized.to_string(), value.clone());
                }
            }
            if effective_target.is_none() {
                effective_target = saved
                    .selected_target
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string);
            }
        }
    }
    env_overrides.extend(requested_env_overrides);

    if !env_overrides.contains_key("ATO_CONTROL_PLANE_PORT") {
        if let Some(port) = allocate_loopback_port() {
            env_overrides.insert("ATO_CONTROL_PLANE_PORT".to_string(), port.to_string());
        }
    }
    drop(_guard);
    let Some(local_artifact) = local_artifact else {
        return json_error(
            StatusCode::NOT_FOUND,
            "artifact_not_found",
            "artifact is missing in local registry storage",
        );
    };
    if !local_artifact.exists() {
        return json_error(
            StatusCode::NOT_FOUND,
            "artifact_not_found",
            "resolved artifact is missing in local registry storage",
        );
    }

    let scoped_id = format!("{}/{}", publisher, slug);
    let request_base_url = resolve_public_base_url(&headers, &state.listen_url);
    let registry_url =
        normalize_registry_base_url_for_local_run(&request_base_url, &state.listen_url);
    let ato_path = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_error",
                &format!("failed to resolve ato binary path: {}", err),
            );
        }
    };
    let run_target = local_artifact
        .canonicalize()
        .unwrap_or_else(|_| local_artifact.clone());
    let mut consent_manifest_tmpdir = None;
    let consent_manifest_path = if run_target
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("capsule"))
    {
        let bytes = match std::fs::read(&run_target) {
            Ok(bytes) => bytes,
            Err(err) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "run_plan_invalid",
                    &format!(
                        "failed to read local artifact for consent planning: {}",
                        err
                    ),
                );
            }
        };
        let manifest_raw = match extract_manifest_from_capsule(&bytes) {
            Ok(raw) => raw,
            Err(err) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "run_plan_invalid",
                    &format!("failed to extract capsule.toml from artifact: {}", err),
                );
            }
        };
        let temp_dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(err) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "run_plan_invalid",
                    &format!("failed to create consent planning workspace: {}", err),
                );
            }
        };
        let manifest_path = temp_dir.path().join("capsule.toml");
        if let Err(err) = std::fs::write(&manifest_path, manifest_raw) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_plan_invalid",
                &format!("failed to prepare consent planning manifest: {}", err),
            );
        }
        consent_manifest_tmpdir = Some(temp_dir);
        manifest_path
    } else {
        run_target.clone()
    };
    let compiled = match capsule_core::execution_plan::derive::compile_execution_plan(
        &consent_manifest_path,
        capsule_core::router::ExecutionProfile::Dev,
        effective_target.as_deref(),
    ) {
        Ok(compiled) => compiled,
        Err(err) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "run_plan_invalid",
                &format!("failed to prepare execution plan: {}", err),
            );
        }
    };
    let _ = consent_manifest_tmpdir.as_ref();
    if let Err(err) = crate::consent_store::seed_consent(&compiled.execution_plan) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "run_consent_seed_failed",
            &format!("failed to seed execution consent: {}", err),
        );
    }
    let mut cmd = std::process::Command::new(ato_path);
    cmd.arg("run")
        .arg(&run_target)
        .arg("--registry")
        .arg(&registry_url)
        .arg("--yes")
        .env("ATO_UI_SCOPED_ID", &scoped_id)
        .stdin(Stdio::null());
    if let Some(target) = effective_target.as_deref() {
        cmd.arg("--target").arg(target);
    }
    if let Some(port) = effective_port {
        cmd.env("ATO_UI_OVERRIDE_PORT", port.to_string());
    }
    match effective_permission_mode {
        Some(RunPermissionMode::Sandbox) => {
            cmd.arg("--sandbox");
        }
        Some(RunPermissionMode::Dangerous) => {
            cmd.arg("--dangerously-skip-permissions")
                .env("CAPSULE_ALLOW_UNSAFE", "1");
        }
        None => {}
    }
    if !env_overrides.is_empty() {
        let env_json = match serde_json::to_string(&env_overrides) {
            Ok(value) => value,
            Err(err) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_env",
                    &format!("failed to serialize env overrides: {}", err),
                );
            }
        };
        cmd.env("ATO_UI_OVERRIDE_ENV_JSON", env_json);
    }

    let now = Utc::now();
    let nonce = now
        .timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp_millis() * 1_000_000);
    let process_id = format!("capsule-{}-{}", nonce, std::process::id());
    let log_path = process_log_path(&process_id);
    if let Some(parent) = log_path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_spawn_failed",
                &format!("failed to prepare log directory: {}", err),
            );
        }
    }
    let log_file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(file) => file,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_spawn_failed",
                &format!("failed to open process log file: {}", err),
            );
        }
    };
    let log_file_err = match log_file.try_clone() {
        Ok(file) => file,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_spawn_failed",
                &format!("failed to clone process log handle: {}", err),
            );
        }
    };
    cmd.stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));

    #[cfg(unix)]
    unsafe {
        // Isolate spawned runtime into its own process group so stop can terminate the full tree.
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_spawn_failed",
                &format!("failed to spawn `ato run`: {}", err),
            );
        }
    };

    let pid = child.id() as i32;
    let process_info = ProcessInfo {
        id: process_id.clone(),
        name: slug.clone(),
        pid,
        workload_pid: None,
        status: ProcessStatus::Running,
        runtime: "ato-run".to_string(),
        start_time: std::time::SystemTime::now(),
        manifest_path: Some(run_target.clone()),
        scoped_id: Some(scoped_id.clone()),
        target_label: effective_target.clone(),
        requested_port: effective_port,
        log_path: Some(process_log_path(&process_id)),
        ready_at: Some(std::time::SystemTime::now()),
        last_event: Some("spawned".to_string()),
        last_error: None,
        exit_code: None,
    };
    let process_manager = match ProcessManager::new() {
        Ok(manager) => manager,
        Err(err) => {
            let _ = child.kill();
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "run_spawn_failed",
                &format!("failed to initialize process manager: {}", err),
            );
        }
    };
    if let Err(err) = process_manager.write_pid(&process_info) {
        let _ = child.kill();
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "run_spawn_failed",
            &format!("failed to persist process record: {}", err),
        );
    }
    let registered_service_binding_ids =
        match binding::sync_service_bindings_for_process(&process_id) {
            Ok(records) => records
                .into_iter()
                .map(|record| record.binding_id)
                .collect(),
            Err(err) => {
                let _ = child.kill();
                let _ = process_manager.delete_pid(&process_id);
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "service_binding_register_failed",
                    &err.to_string(),
                );
            }
        };
    std::thread::spawn(move || {
        let _ = child.wait();
    });

    (
        StatusCode::ACCEPTED,
        Json(RunCapsuleResponse {
            accepted: true,
            scoped_id,
            requested_target: effective_target,
            requested_port: effective_port,
            registered_service_binding_ids,
        }),
    )
        .into_response()
}

async fn handle_delete_local_capsule(
    State(state): State<AppState>,
    AxumPath((publisher, slug)): AxumPath<(String, String)>,
    Query(query): Query<DeleteCapsuleQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = validate_write_auth(&headers, state.auth_token.as_deref()) {
        return json_error(StatusCode::UNAUTHORIZED, "unauthorized", &err);
    }
    if !query.confirmed.unwrap_or(false) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "confirmation_required",
            "confirmed=true is required",
        );
    }
    if let Err(err) = validate_capsule_segments(&publisher, &slug) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_scope", &err.to_string());
    }
    let delete_version = query
        .version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    if let Some(version) = delete_version.as_deref() {
        if let Err(err) = validate_version(version) {
            return json_error(StatusCode::BAD_REQUEST, "invalid_version", &err.to_string());
        }
    }

    let scoped_id = format!("{}/{}", publisher, slug);
    let process_manager = match ProcessManager::new() {
        Ok(manager) => manager,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "process_manager_error",
                &err.to_string(),
            )
        }
    };
    let processes = match process_manager.list_processes() {
        Ok(processes) => processes,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "process_list_failed",
                &err.to_string(),
            )
        }
    };
    if processes.iter().any(|process| {
        process.status.is_active() && process.scoped_id.as_deref() == Some(scoped_id.as_str())
    }) {
        return json_error(
            StatusCode::CONFLICT,
            "capsule_running",
            "capsule is running; stop active process before delete",
        );
    }
    let inactive_processes = processes
        .into_iter()
        .filter(|process| process.scoped_id.as_deref() == Some(scoped_id.as_str()))
        .collect::<Vec<_>>();

    let _guard = state.lock.lock().await;
    let store = match RegistryStore::open(&state.data_dir) {
        Ok(store) => store,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry_store_error",
                &err.to_string(),
            );
        }
    };
    let now = Utc::now().to_rfc3339();
    let outcome =
        match store.delete_registry_capsule(&publisher, &slug, delete_version.as_deref(), &now) {
            Ok(crate::registry_store::RegistryDeleteOutcome::CapsuleNotFound) => {
                return json_error(StatusCode::NOT_FOUND, "not_found", "Capsule not found");
            }
            Ok(crate::registry_store::RegistryDeleteOutcome::VersionNotFound(version)) => {
                return json_error(
                    StatusCode::NOT_FOUND,
                    "version_not_found",
                    &format!("Version '{}' not found", version),
                );
            }
            Ok(crate::registry_store::RegistryDeleteOutcome::Deleted(outcome)) => outcome,
            Err(err) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "delete_failed",
                    &err.to_string(),
                );
            }
        };

    let mut removed_service_binding_ids = Vec::new();
    if outcome.removed_capsule {
        if let Err(err) = store.delete_store_metadata(&scoped_id) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "store_metadata_delete_failed",
                &err.to_string(),
            );
        }
        let mut runtime_config = load_runtime_config(&state.data_dir).unwrap_or_default();
        runtime_config
            .entries
            .remove(&runtime_config_key(&publisher, &slug));
        if let Err(err) = write_runtime_config(&state.data_dir, &runtime_config) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_config_write_failed",
                &err.to_string(),
            );
        }
        for process in &inactive_processes {
            match binding::cleanup_service_bindings_for_process_info(process) {
                Ok(records) => removed_service_binding_ids
                    .extend(records.into_iter().map(|record| record.binding_id)),
                Err(err) => {
                    return json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "service_binding_cleanup_failed",
                        &err.to_string(),
                    );
                }
            }
            if let Err(err) = process_manager.delete_pid(&process.id) {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "process_cleanup_failed",
                    &err.to_string(),
                );
            }
        }
    }

    cleanup_removed_artifacts(
        &state.data_dir,
        &publisher,
        &slug,
        &outcome.removed_releases,
    );
    (
        StatusCode::OK,
        Json(DeleteCapsuleResponse {
            deleted: true,
            scoped_id,
            removed_capsule: outcome.removed_capsule,
            removed_versions: outcome.removed_releases.len(),
            removed_version: outcome.removed_version,
            removed_service_binding_ids,
        }),
    )
        .into_response()
}

fn process_log_path(id: &str) -> PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from(".").join(".tmp"));
    home.join(".ato").join("logs").join(format!("{id}.log"))
}

fn read_process_log_lines(path: &Path, tail: usize) -> Vec<String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let all_lines = reader.lines().map_while(Result::ok).collect::<Vec<_>>();
    if all_lines.len() <= tail {
        return all_lines;
    }
    all_lines[all_lines.len() - tail..].to_vec()
}

#[cfg(test)]
#[derive(Debug)]
struct DeleteCapsuleResult {
    removed_capsule: bool,
    removed_version: Option<String>,
}

#[cfg(test)]
#[derive(Debug)]
enum DeleteCapsuleOutcome {
    CapsuleNotFound,
    VersionNotFound(String),
    Deleted(DeleteCapsuleResult),
}

#[cfg(test)]
fn delete_capsule_from_index(
    index: &mut RegistryIndex,
    publisher: &str,
    slug: &str,
    version: Option<&str>,
    now: &str,
) -> DeleteCapsuleOutcome {
    let Some(capsule_pos) = index
        .capsules
        .iter()
        .position(|capsule| capsule.publisher == publisher && capsule.slug == slug)
    else {
        return DeleteCapsuleOutcome::CapsuleNotFound;
    };

    if let Some(version) = version {
        let capsule = &mut index.capsules[capsule_pos];
        let Some(release_pos) = capsule
            .releases
            .iter()
            .position(|release| release.version == version)
        else {
            return DeleteCapsuleOutcome::VersionNotFound(version.to_string());
        };

        let removed = capsule.releases.remove(release_pos);
        if capsule.releases.is_empty() {
            index.capsules.remove(capsule_pos);
            return DeleteCapsuleOutcome::Deleted(DeleteCapsuleResult {
                removed_capsule: true,
                removed_version: Some(removed.version.clone()),
            });
        }

        if capsule.latest_version == removed.version {
            if let Some(last) = capsule.releases.last() {
                capsule.latest_version = last.version.clone();
            }
        }
        capsule.updated_at = now.to_string();
        return DeleteCapsuleOutcome::Deleted(DeleteCapsuleResult {
            removed_capsule: false,
            removed_version: Some(removed.version.clone()),
        });
    }

    index.capsules.remove(capsule_pos);
    DeleteCapsuleOutcome::Deleted(DeleteCapsuleResult {
        removed_capsule: true,
        removed_version: None,
    })
}

fn cleanup_removed_artifacts(
    data_dir: &Path,
    publisher: &str,
    slug: &str,
    releases: &[crate::registry_store::RegistryReleaseRecord],
) {
    for release in releases {
        let path = artifact_path(
            data_dir,
            publisher,
            slug,
            &release.version,
            &release.file_name,
        );
        if !path.exists() {
            continue;
        }
        if let Err(err) = std::fs::remove_file(&path) {
            tracing::warn!(
                "local registry failed to remove artifact file path={} error={}",
                path.display(),
                err
            );
        }
    }
}

#[cfg(test)]
fn truncate_for_error(message: &str, max_chars: usize) -> String {
    if message.chars().count() <= max_chars {
        return message.to_string();
    }
    let head = message.chars().take(max_chars).collect::<String>();
    format!("{}...", head)
}

fn store_metadata_key(publisher: &str, slug: &str) -> String {
    format!("{}/{}", publisher, slug)
}

fn runtime_config_key(publisher: &str, slug: &str) -> String {
    format!("{}/{}", publisher, slug)
}

fn get_store_metadata_entry<'a>(
    index: &'a StoreMetadataIndex,
    publisher: &str,
    slug: &str,
) -> Option<&'a StoreMetadataEntry> {
    index.entries.get(&store_metadata_key(publisher, slug))
}

fn get_runtime_config_entry<'a>(
    index: &'a RuntimeConfigIndex,
    publisher: &str,
    slug: &str,
) -> Option<&'a CapsuleRuntimeConfig> {
    index.entries.get(&runtime_config_key(publisher, slug))
}

fn metadata_icon_url(
    base_url: &str,
    publisher: &str,
    slug: &str,
    icon_path: Option<&str>,
) -> Option<String> {
    icon_path.map(|_| {
        format!(
            "{}/v1/local/capsules/by/{}/{}/store-icon",
            base_url.trim_end_matches('/'),
            urlencoding::encode(publisher),
            urlencoding::encode(slug),
        )
    })
}

fn metadata_to_payload(
    metadata: Option<&StoreMetadataEntry>,
    base_url: &str,
    publisher: &str,
    slug: &str,
) -> Option<StoreMetadataPayload> {
    metadata.map(|entry| {
        let icon_path = entry.icon_path.clone();
        StoreMetadataPayload {
            icon_url: metadata_icon_url(base_url, publisher, slug, icon_path.as_deref()),
            icon_path,
            text: entry.text.clone(),
        }
    })
}

fn append_store_metadata_section(
    readme_markdown: Option<String>,
    metadata: Option<&StoreMetadataEntry>,
) -> Option<String> {
    let Some(entry) = metadata else {
        return readme_markdown;
    };
    if entry.icon_path.is_none() && entry.text.is_none() {
        return readme_markdown;
    }

    let mut section_lines = vec!["## store.metadata".to_string(), "".to_string()];
    if let Some(icon_path) = entry.icon_path.as_ref() {
        section_lines.push(format!("- file_path: `{}`", icon_path));
    }
    if let Some(text) = entry.text.as_ref() {
        section_lines.push(format!("- text: {}", text));
    }
    let section = section_lines.join("\n");
    match readme_markdown {
        Some(existing) if !existing.trim().is_empty() => {
            Some(format!("{}\n\n{}", existing.trim_end(), section))
        }
        _ => Some(section),
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn publisher_info(handle: &str) -> PublisherInfo {
    PublisherInfo {
        handle: handle.to_string(),
        author_did: format!("did:key:local:{}", handle),
        verified: true,
    }
}

fn stored_to_search_row(
    capsule: &StoredCapsule,
    metadata: Option<&StoreMetadataEntry>,
    base_url: &str,
) -> SearchCapsuleRow {
    let scoped_id = format!("{}/{}", capsule.publisher, capsule.slug);
    let description = metadata
        .and_then(|entry| entry.text.as_ref())
        .map(String::as_str)
        .unwrap_or(capsule.description.as_str())
        .to_string();
    let latest_size_bytes = capsule
        .releases
        .iter()
        .find(|release| release.version == capsule.latest_version)
        .or_else(|| capsule.releases.last())
        .map(|release| release.size_bytes)
        .unwrap_or(0);
    let store_metadata = metadata_to_payload(metadata, base_url, &capsule.publisher, &capsule.slug);
    SearchCapsuleRow {
        id: capsule.id.clone(),
        slug: capsule.slug.clone(),
        scoped_id: scoped_id.clone(),
        scoped_id_camel: scoped_id,
        name: capsule.name.clone(),
        description,
        category: capsule.category.clone(),
        capsule_type: capsule.capsule_type.clone(),
        price: capsule.price,
        currency: capsule.currency.clone(),
        publisher: publisher_info(&capsule.publisher),
        latest_version: capsule.latest_version.clone(),
        latest_size_bytes,
        downloads: capsule.downloads,
        created_at: capsule.created_at.clone(),
        updated_at: capsule.updated_at.clone(),
        store_metadata,
    }
}

#[cfg(test)]
fn upsert_capsule(
    index: &mut RegistryIndex,
    publisher: &str,
    slug: &str,
    name: &str,
    description: &str,
    release: StoredRelease,
    now: &str,
) {
    if let Some(capsule) = index
        .capsules
        .iter_mut()
        .find(|c| c.publisher == publisher && c.slug == slug)
    {
        capsule.latest_version = release.version.clone();
        capsule.updated_at = now.to_string();
        capsule.releases.push(release);
        return;
    }

    index.capsules.push(StoredCapsule {
        id: format!("local-{}-{}", publisher, slug),
        publisher: publisher.to_string(),
        slug: slug.to_string(),
        name: name.to_string(),
        description: description.to_string(),
        category: "tools".to_string(),
        capsule_type: "app".to_string(),
        price: 0,
        currency: "usd".to_string(),
        latest_version: release.version.clone(),
        releases: vec![release],
        downloads: 0,
        created_at: now.to_string(),
        updated_at: now.to_string(),
    });
}

#[cfg(test)]
fn has_release_version(index: &RegistryIndex, publisher: &str, slug: &str, version: &str) -> bool {
    find_release_by_version(index, publisher, slug, version).is_some()
}

#[cfg(test)]
fn find_release_by_version<'a>(
    index: &'a RegistryIndex,
    publisher: &str,
    slug: &str,
    version: &str,
) -> Option<&'a StoredRelease> {
    index
        .capsules
        .iter()
        .find(|capsule| capsule.publisher == publisher && capsule.slug == slug)
        .and_then(|capsule| {
            capsule
                .releases
                .iter()
                .find(|release| release.version == version)
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingReleaseOutcome {
    Reuse,
    Conflict(&'static str),
}

fn existing_release_outcome(
    existing_sha256: &str,
    allow_existing: bool,
    actual_sha: &str,
) -> ExistingReleaseOutcome {
    if !allow_existing {
        return ExistingReleaseOutcome::Conflict("same version is already published");
    }

    if equals_hash(existing_sha256, actual_sha) {
        return ExistingReleaseOutcome::Reuse;
    }

    ExistingReleaseOutcome::Conflict("same version is already published (sha256 mismatch)")
}

fn verify_uploaded_chunk(
    raw_hash: &str,
    raw_size: u32,
    zstd_bytes: &[u8],
) -> std::result::Result<(), String> {
    let mut decoder = zstd::stream::Decoder::new(Cursor::new(zstd_bytes))
        .map_err(|e| format!("failed to initialize zstd decoder: {}", e))?;

    let mut hasher = blake3::Hasher::new();
    let mut total = 0u64;
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| format!("failed to decode zstd chunk: {}", e))?;
        if n == 0 {
            break;
        }
        total += n as u64;
        hasher.update(&buf[..n]);
    }

    if total != raw_size as u64 {
        return Err(format!(
            "raw size mismatch: expected {} got {}",
            raw_size, total
        ));
    }

    let computed = format!("blake3:{}", hasher.finalize().to_hex());
    if computed != raw_hash {
        return Err(format!(
            "raw hash mismatch: expected {} got {}",
            raw_hash, computed
        ));
    }
    Ok(())
}

fn registry_cas_store(data_dir: &Path) -> Result<CasStore> {
    CasStore::new(data_dir.join("cas")).map_err(|e| anyhow::anyhow!("{}", e))
}

fn parse_artifact_manifest(bytes: &[u8]) -> Result<ArtifactMeta> {
    let manifest = extract_manifest_from_capsule(bytes)?;
    let parsed = capsule_core::types::CapsuleManifest::from_toml(&manifest)
        .map_err(|err| anyhow::anyhow!("{}", err))?;
    Ok(ArtifactMeta {
        name: parsed.name,
        version: parsed.version,
        description: parsed.metadata.description.unwrap_or_default(),
    })
}

fn extract_manifest_from_capsule(bytes: &[u8]) -> Result<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let entries = archive
        .entries()
        .context("Failed to iterate artifact entries")?;
    for entry in entries {
        let mut entry = entry.context("Invalid artifact entry")?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path == "capsule.toml" {
            let mut manifest = String::new();
            entry
                .read_to_string(&mut manifest)
                .context("Failed to read capsule.toml")?;
            return Ok(manifest);
        }
    }

    bail!("capsule.toml not found in artifact")
}

fn extract_capsule_lock_from_capsule(bytes: &[u8]) -> Option<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let entries = archive.entries().ok()?;
    for entry in entries {
        let mut entry = entry.ok()?;
        let entry_path = entry.path().ok()?.to_string_lossy().to_string();
        if entry_path == "capsule.lock.json" || entry_path == "capsule.lock" {
            let mut lock = String::new();
            entry.read_to_string(&mut lock).ok()?;
            return Some(lock);
        }
    }
    None
}

fn collect_readme_candidates<R: Read>(archive: &mut tar::Archive<R>) -> HashMap<String, Vec<u8>> {
    let mut candidates = HashMap::new();
    let Ok(entries) = archive.entries() else {
        return candidates;
    };

    for entry in entries {
        let mut entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let entry_path = match entry.path() {
            Ok(path) => path.to_string_lossy().to_string(),
            Err(_) => continue,
        };
        let file_name = match entry_path.rsplit('/').next() {
            Some(name) => name.to_string(),
            None => continue,
        };
        if !README_CANDIDATES
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(file_name.as_str()))
        {
            continue;
        }

        let mut buf = Vec::new();
        if entry.read_to_end(&mut buf).is_err() {
            continue;
        }
        if buf.len() > README_MAX_BYTES {
            buf.truncate(README_MAX_BYTES);
        }
        candidates.entry(file_name).or_insert(buf);
    }

    candidates
}

fn extract_readme_from_capsule(bytes: &[u8]) -> Option<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut candidates = collect_readme_candidates(&mut archive);

    if candidates.is_empty() {
        let mut archive = tar::Archive::new(Cursor::new(bytes));
        let entries = archive.entries().ok()?;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let entry_path = match entry.path() {
                Ok(path) => path.to_string_lossy().to_string(),
                Err(_) => continue,
            };
            if entry_path != "payload.tar.zst" {
                continue;
            }

            let decoder = match zstd::stream::Decoder::new(entry) {
                Ok(decoder) => decoder,
                Err(_) => continue,
            };
            let mut payload_archive = tar::Archive::new(decoder);
            candidates = collect_readme_candidates(&mut payload_archive);
            if !candidates.is_empty() {
                break;
            }
        }
    }

    for candidate in README_CANDIDATES {
        if let Some((_, content)) = candidates
            .iter()
            .find(|(name, _)| candidate.eq_ignore_ascii_case(name.as_str()))
        {
            return Some(String::from_utf8_lossy(content).to_string());
        }
    }
    None
}

type CapsuleDetailManifestParts = (
    Option<serde_json::Value>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn load_capsule_detail_manifest(
    data_dir: &Path,
    capsule: &StoredCapsule,
) -> CapsuleDetailManifestParts {
    let Some(release) = capsule
        .releases
        .iter()
        .find(|release| release.version == capsule.latest_version)
        .or_else(|| capsule.releases.last())
    else {
        return (None, None, None, None, None, None);
    };
    let path = artifact_path(
        data_dir,
        &capsule.publisher,
        &capsule.slug,
        &release.version,
        &release.file_name,
    );

    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                "local registry failed to read artifact for detail manifest path={} error={}",
                path.display(),
                err
            );
            return (None, None, None, None, None, None);
        }
    };
    let readme_markdown = extract_readme_from_capsule(&bytes);
    let capsule_lock = extract_capsule_lock_from_capsule(&bytes);
    let readme_source = readme_markdown
        .as_ref()
        .map(|_| "artifact".to_string())
        .or_else(|| Some("none".to_string()));
    let manifest_raw = match extract_manifest_from_capsule(&bytes) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::warn!(
                "local registry failed to extract capsule.toml for {}/{}@{}: {}",
                capsule.publisher,
                capsule.slug,
                release.version,
                err
            );
            return (
                None,
                None,
                None,
                capsule_lock,
                readme_markdown,
                readme_source,
            );
        }
    };
    let parsed = toml::from_str::<toml::Value>(&manifest_raw);
    let (manifest, repository) = match parsed {
        Ok(parsed) => {
            let repository = extract_repository_from_manifest(&parsed);
            let manifest = match serde_json::to_value(parsed) {
                Ok(value) => Some(value),
                Err(err) => {
                    tracing::warn!(
                        "local registry failed to serialize manifest JSON for {}/{}@{}: {}",
                        capsule.publisher,
                        capsule.slug,
                        release.version,
                        err
                    );
                    None
                }
            };
            (manifest, repository)
        }
        Err(err) => {
            tracing::warn!(
                "local registry failed to parse capsule.toml for {}/{}@{}: {}",
                capsule.publisher,
                capsule.slug,
                release.version,
                err
            );
            (None, None)
        }
    };
    (
        manifest,
        repository,
        Some(manifest_raw),
        capsule_lock,
        readme_markdown,
        readme_source,
    )
}

fn extract_repository_from_manifest(parsed: &toml::Value) -> Option<String> {
    parsed
        .get("metadata")
        .and_then(|v| v.get("repository"))
        .and_then(toml::Value::as_str)
        .or_else(|| parsed.get("repository").and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

fn expand_data_dir(raw: &str) -> Result<PathBuf> {
    if raw == "~" {
        return dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to resolve home directory"));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        let home =
            dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to resolve home directory"))?;
        return Ok(home.join(rest));
    }
    Ok(PathBuf::from(raw))
}

fn initialize_storage(data_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("Failed to create data dir {}", data_dir.display()))?;
    std::fs::create_dir_all(data_dir.join("artifacts"))
        .with_context(|| format!("Failed to create artifact dir {}", data_dir.display()))?;
    let _ = RegistryStore::open(data_dir)?;
    let runtime_config_path = runtime_config_path(data_dir);
    if !runtime_config_path.exists() {
        write_runtime_config(data_dir, &RuntimeConfigIndex::default())?;
    }
    Ok(())
}

fn runtime_config_path(data_dir: &Path) -> PathBuf {
    data_dir.join("runtime-config.json")
}

fn load_index(data_dir: &Path) -> Result<RegistryIndex> {
    let store = RegistryStore::open(data_dir)?;
    let packages = store.list_registry_packages()?;
    Ok(RegistryIndex {
        schema_version: "local-registry-v1".to_string(),
        capsules: packages
            .into_iter()
            .map(|package| StoredCapsule {
                id: format!("local-{}-{}", package.publisher, package.slug),
                publisher: package.publisher,
                slug: package.slug,
                name: package.name,
                description: package.description,
                category: "tools".to_string(),
                capsule_type: "app".to_string(),
                price: 0,
                currency: "usd".to_string(),
                latest_version: package.latest_version,
                releases: package
                    .releases
                    .into_iter()
                    .map(|release| StoredRelease {
                        version: release.version,
                        file_name: release.file_name,
                        sha256: format!("sha256:{}", release.sha256),
                        blake3: format!("blake3:{}", release.blake3),
                        size_bytes: release.size_bytes,
                        signature_status: release.signature_status,
                        created_at: release.created_at,
                        payload_v3: None,
                    })
                    .collect(),
                downloads: 0,
                created_at: package.created_at,
                updated_at: package.updated_at,
            })
            .collect(),
    })
}

fn release_manifest_rel_path(publisher: &str, slug: &str, version: &str) -> PathBuf {
    PathBuf::from("payload-v3")
        .join(publisher)
        .join(slug)
        .join(format!("{}.json", version))
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("payload v3 manifest path must have a parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create directory {}", parent.display()))?;

    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, bytes)
        .with_context(|| format!("Failed to write temporary file {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "Failed to atomically rename {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn load_store_metadata(data_dir: &Path) -> Result<StoreMetadataIndex> {
    let store = RegistryStore::open(data_dir)?;
    let entries = store.list_store_metadata_entries()?;
    let mut index = StoreMetadataIndex::default();
    for entry in entries {
        index.entries.insert(
            entry.scoped_id,
            StoreMetadataEntry {
                icon_path: entry.icon_path,
                text: entry.text,
                updated_at: entry.updated_at,
            },
        );
    }
    Ok(index)
}

fn load_runtime_config(data_dir: &Path) -> Result<RuntimeConfigIndex> {
    let path = runtime_config_path(data_dir);
    if !path.exists() {
        return Ok(RuntimeConfigIndex::default());
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    let parsed = serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(parsed)
}

fn write_runtime_config(data_dir: &Path, config: &RuntimeConfigIndex) -> Result<()> {
    let path = runtime_config_path(data_dir);
    let json =
        serde_json::to_string_pretty(config).context("Failed to serialize runtime config")?;
    std::fs::write(&path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

fn expand_user_path(raw: &str) -> PathBuf {
    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

fn artifact_path(
    data_dir: &Path,
    publisher: &str,
    slug: &str,
    version: &str,
    file_name: &str,
) -> PathBuf {
    data_dir
        .join("artifacts")
        .join(publisher)
        .join(slug)
        .join(version)
        .join(file_name)
}

fn resolve_run_artifact_path(data_dir: &Path, capsule: &StoredCapsule) -> Option<PathBuf> {
    // Prefer the freshest on-disk artifact to avoid stale legacy index snapshots.
    find_latest_capsule_artifact_on_disk(data_dir, &capsule.publisher, &capsule.slug).or_else(
        || {
            capsule
                .releases
                .iter()
                .find(|release| release.version == capsule.latest_version)
                .map(|release| {
                    artifact_path(
                        data_dir,
                        &capsule.publisher,
                        &capsule.slug,
                        &release.version,
                        &release.file_name,
                    )
                })
        },
    )
}

fn find_latest_capsule_artifact_on_disk(
    data_dir: &Path,
    publisher: &str,
    slug: &str,
) -> Option<PathBuf> {
    let root = data_dir.join("artifacts").join(publisher).join(slug);
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;

    let versions = std::fs::read_dir(root).ok()?;
    for version_entry in versions.flatten() {
        let version_path = version_entry.path();
        if !version_path.is_dir() {
            continue;
        }
        let files = match std::fs::read_dir(&version_path) {
            Ok(files) => files,
            Err(_) => continue,
        };
        for file_entry in files.flatten() {
            let file_path = file_entry.path();
            if !file_path.is_file() {
                continue;
            }
            if file_path.extension().and_then(|ext| ext.to_str()) != Some("capsule") {
                continue;
            }
            let modified = file_entry
                .metadata()
                .and_then(|meta| meta.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let is_newer = newest
                .as_ref()
                .map(|(current, _)| modified > *current)
                .unwrap_or(true);
            if is_newer {
                newest = Some((modified, file_path));
            }
        }
    }

    newest.map(|(_, path)| path)
}

fn allocate_loopback_port() -> Option<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
    if port == 0 {
        None
    } else {
        Some(port)
    }
}

fn validate_capsule_segments(publisher: &str, slug: &str) -> Result<()> {
    let scoped = format!("{}/{}", publisher, slug);
    let _ = crate::install::parse_capsule_ref(&scoped)?;
    Ok(())
}

fn validate_version(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('/') || value.contains('\\') || value.contains("..") {
        bail!("invalid version segment");
    }
    Ok(())
}

fn validate_file_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || !value.to_ascii_lowercase().ends_with(".capsule")
    {
        bail!("file_name must be a .capsule file name");
    }
    Ok(())
}

impl Default for RegistryIndex {
    fn default() -> Self {
        Self {
            schema_version: "local-registry-v1".to_string(),
            capsules: vec![],
        }
    }
}

#[cfg(test)]
#[path = "registry_serve_tests.rs"]
mod tests;
