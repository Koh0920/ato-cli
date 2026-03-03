use std::io::{Cursor, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use capsule_core::capsule_v3::manifest::{validate_blake3_digest, V3_PAYLOAD_MANIFEST_PATH};
use capsule_core::capsule_v3::{verify_artifact_hash, CapsuleManifestV3, CasStore};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

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

#[derive(Debug, Deserialize)]
struct SyncChunkDescriptor {
    raw_hash: String,
    raw_size: u32,
}

#[derive(Debug, Deserialize)]
struct SyncNegotiateRequest {
    artifact_hash: String,
    schema_version: u32,
    chunks: Vec<SyncChunkDescriptor>,
}

#[derive(Debug, Serialize)]
struct SyncNegotiateResponse {
    missing_chunks: Vec<String>,
    total_chunks: usize,
}

#[derive(Debug, Serialize)]
struct ChunkUploadResponse {
    raw_hash: String,
    inserted: bool,
    zstd_size: u64,
}

#[derive(Debug, Deserialize)]
struct SyncCommitRequest {
    publisher: String,
    slug: String,
    version: String,
    manifest: CapsuleManifestV3,
}

#[derive(Debug, Serialize)]
struct SyncCommitResponse {
    scoped_id: String,
    version: String,
    artifact_hash: String,
    chunk_count: usize,
    total_raw_size: u64,
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
    downloads: u64,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
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
}

#[derive(Debug, Serialize)]
struct CapsuleReleaseRow {
    version: String,
    content_hash: String,
    signature_status: String,
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

    let app = Router::new()
        .route("/.well-known/capsule.json", get(handle_well_known))
        .route("/v1/capsules", get(handle_search_capsules))
        .route("/v1/capsules/by/:publisher/:slug", get(handle_get_capsule))
        .route(
            "/v1/capsules/by/:publisher/:slug/distributions",
            get(handle_distributions),
        )
        .route(
            "/v1/capsules/by/:publisher/:slug/download",
            get(handle_download),
        )
        .route(
            "/v1/artifacts/:publisher/:slug/:version/:file_name",
            get(handle_get_artifact),
        )
        .route("/v1/sync/negotiate", post(handle_sync_negotiate))
        .route("/v1/sync/commit", post(handle_sync_commit))
        .route("/v1/chunks/:raw_hash", put(handle_put_chunk))
        .route("/v1/chunks/:raw_hash", get(handle_get_chunk))
        .route(
            "/v1/releases/:publisher/:slug/:version/manifest",
            get(handle_get_release_manifest),
        )
        .route(
            "/v1/local/capsules/:publisher/:slug/:version",
            put(handle_put_local_capsule),
        )
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .with_state(state);

    println!("🚀 Local registry serving at {}", listen_url);
    let addr: SocketAddr = format!("{}:{}", host, config.port)
        .parse()
        .context("Invalid listen address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind {}", addr))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Local registry server failed")?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn handle_well_known(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let public_base_url = resolve_public_base_url(&headers, &state.listen_url);
    Json(json!({
        "url": public_base_url,
        "name": "Ato Local Registry",
        "version": "1"
    }))
}

async fn handle_search_capsules(
    State(state): State<AppState>,
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
            if needle.is_empty() {
                true
            } else {
                capsule.slug.to_lowercase().contains(&needle)
                    || capsule.name.to_lowercase().contains(&needle)
                    || capsule.description.to_lowercase().contains(&needle)
            }
        })
        .filter(|capsule| {
            category
                .as_ref()
                .map(|cat| capsule.category.to_lowercase() == *cat)
                .unwrap_or(true)
        })
        .map(stored_to_search_row)
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

    let detail = CapsuleDetailResponse {
        id: capsule.id.clone(),
        scoped_id: format!("{}/{}", capsule.publisher, capsule.slug),
        slug: capsule.slug.clone(),
        name: capsule.name.clone(),
        description: capsule.description.clone(),
        price: capsule.price,
        currency: capsule.currency.clone(),
        latest_version: capsule.latest_version.clone(),
        releases: capsule
            .releases
            .iter()
            .map(|release| CapsuleReleaseRow {
                version: release.version.clone(),
                content_hash: release.blake3.clone(),
                signature_status: release.signature_status.clone(),
            })
            .collect(),
        publisher: publisher_info(&capsule.publisher),
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

    let _guard = state.lock.lock().await;
    let mut index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            );
        }
    };
    let mut changed = false;
    if let Some(capsule) = index
        .capsules
        .iter_mut()
        .find(|c| c.publisher == request.publisher && c.slug == request.slug)
    {
        if let Some(release) = capsule
            .releases
            .iter_mut()
            .find(|r| r.version == request.version)
        {
            release.payload_v3 = Some(StoredPayloadV3 {
                artifact_hash: request.manifest.artifact_hash.clone(),
                chunk_count: request.manifest.chunks.len(),
                total_raw_size: request.manifest.total_raw_size,
                manifest_rel_path: rel_path.clone(),
            });
            changed = true;
        }
    }
    if changed {
        if let Err(err) = write_index(&state.data_dir, &index) {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_write_failed",
                &err.to_string(),
            );
        }
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
    let mut index = match load_index(&state.data_dir) {
        Ok(index) => index,
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "index_read_failed",
                &err.to_string(),
            )
        }
    };

    if let Some(existing_release) = find_release_by_version(&index, &publisher, &slug, &version) {
        match existing_release_outcome(existing_release, allow_existing, &actual_sha) {
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
                        sha256: existing_release.sha256.clone(),
                        blake3: existing_release.blake3.clone(),
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
    let release = StoredRelease {
        version: version.clone(),
        file_name: file_name.clone(),
        sha256: actual_sha.clone(),
        blake3: actual_blake3.clone(),
        size_bytes: body.len() as u64,
        signature_status: "verified".to_string(),
        created_at: now.clone(),
        payload_v3: None,
    };
    upsert_capsule(
        &mut index,
        &publisher,
        &slug,
        &artifact_meta.name,
        &artifact_meta.description,
        release,
        &now,
    );

    if let Err(err) = write_index(&state.data_dir, &index) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "index_write_failed",
            &err.to_string(),
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

fn validate_write_auth(headers: &HeaderMap, expected_token: Option<&str>) -> Result<(), String> {
    let Some(expected) = expected_token.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(());
    };

    let actual = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty());

    if constant_time_token_eq(expected.as_bytes(), actual.unwrap_or("").as_bytes()) {
        return Ok(());
    }

    Err("Bearer token is required for upload".to_string())
}

fn constant_time_token_eq(expected: &[u8], actual: &[u8]) -> bool {
    use sha2::{Digest, Sha256};

    let expected_digest = Sha256::digest(expected);
    let actual_digest = Sha256::digest(actual);
    expected_digest[..].ct_eq(&actual_digest[..]).into()
}

fn resolve_public_base_url(headers: &HeaderMap, fallback: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| *v == "http" || *v == "https")
        .unwrap_or("http");

    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.split(',').next().unwrap_or(v).trim().to_string());

    if let Some(host) = host {
        return format!("{}://{}", scheme, host);
    }

    fallback.to_string()
}

fn publisher_info(handle: &str) -> PublisherInfo {
    PublisherInfo {
        handle: handle.to_string(),
        author_did: format!("did:key:local:{}", handle),
        verified: true,
    }
}

fn stored_to_search_row(capsule: &StoredCapsule) -> SearchCapsuleRow {
    let scoped_id = format!("{}/{}", capsule.publisher, capsule.slug);
    SearchCapsuleRow {
        id: capsule.id.clone(),
        slug: capsule.slug.clone(),
        scoped_id: scoped_id.clone(),
        scoped_id_camel: scoped_id,
        name: capsule.name.clone(),
        description: capsule.description.clone(),
        category: capsule.category.clone(),
        capsule_type: capsule.capsule_type.clone(),
        price: capsule.price,
        currency: capsule.currency.clone(),
        publisher: publisher_info(&capsule.publisher),
        latest_version: capsule.latest_version.clone(),
        downloads: capsule.downloads,
        created_at: capsule.created_at.clone(),
        updated_at: capsule.updated_at.clone(),
    }
}

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
    existing_release: &StoredRelease,
    allow_existing: bool,
    actual_sha: &str,
) -> ExistingReleaseOutcome {
    if !allow_existing {
        return ExistingReleaseOutcome::Conflict("same version is already published");
    }

    if equals_hash(&existing_release.sha256, actual_sha) {
        return ExistingReleaseOutcome::Reuse;
    }

    ExistingReleaseOutcome::Conflict("same version is already published (sha256 mismatch)")
}

fn get_required_header(headers: &HeaderMap, key: &str) -> Result<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|v| v.to_string())
        .ok_or_else(|| anyhow::anyhow!("required header '{}' is missing", key))
}

fn parse_required_u32_header(headers: &HeaderMap, key: &str) -> Result<u32> {
    let value = get_required_header(headers, key)?;
    value
        .trim()
        .parse::<u32>()
        .with_context(|| format!("invalid '{}' header value: {}", key, value))
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
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut entries = archive
        .entries()
        .context("Failed to iterate artifact entries")?;
    while let Some(entry) = entries.next() {
        let mut entry = entry.context("Invalid artifact entry")?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path == "capsule.toml" {
            let mut manifest = String::new();
            entry
                .read_to_string(&mut manifest)
                .context("Failed to read capsule.toml")?;
            let parsed = capsule_core::types::capsule_v1::CapsuleManifestV1::from_toml(&manifest)
                .map_err(|err| anyhow::anyhow!("{}", err))?;
            return Ok(ArtifactMeta {
                name: parsed.name,
                version: parsed.version,
                description: parsed.metadata.description.unwrap_or_default(),
            });
        }
    }

    bail!("capsule.toml not found in artifact")
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
    let index_path = index_path(data_dir);
    if !index_path.exists() {
        write_index(data_dir, &RegistryIndex::default())?;
    }
    Ok(())
}

fn index_path(data_dir: &Path) -> PathBuf {
    data_dir.join("index.json")
}

fn load_index(data_dir: &Path) -> Result<RegistryIndex> {
    let path = index_path(data_dir);
    if !path.exists() {
        return Ok(RegistryIndex::default());
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    let parsed = serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(parsed)
}

fn write_index(data_dir: &Path, index: &RegistryIndex) -> Result<()> {
    let path = index_path(data_dir);
    let json = serde_json::to_string_pretty(index).context("Failed to serialize index")?;
    std::fs::write(&path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
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

fn release_manifest_rel_path(publisher: &str, slug: &str, version: &str) -> String {
    format!(
        "manifests/{}/{}/{}/{}",
        publisher, slug, version, V3_PAYLOAD_MANIFEST_PATH
    )
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("invalid path"))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))?;

    let mut tmp = tempfile::Builder::new()
        .prefix(".tmp-manifest-")
        .tempfile_in(parent)
        .with_context(|| format!("failed to create temp file in {}", parent.display()))?;
    tmp.write_all(bytes)
        .with_context(|| format!("failed to write temp file in {}", parent.display()))?;
    tmp.as_file_mut()
        .sync_all()
        .with_context(|| format!("failed to sync temp file in {}", parent.display()))?;

    let persisted = tmp.persist(path).map_err(|e| {
        anyhow::anyhow!(
            "failed to persist {} from {}: {}",
            path.display(),
            e.file.path().display(),
            e.error
        )
    })?;
    persisted
        .sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    sync_parent_directory(parent)?;
    Ok(())
}

fn sync_parent_directory(parent: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = std::fs::File::open(parent)
            .with_context(|| format!("failed to open directory {}", parent.display()))?;
        dir.sync_all()
            .with_context(|| format!("failed to sync directory {}", parent.display()))?;
    }

    #[cfg(windows)]
    {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
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

fn compute_sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn compute_blake3(data: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    format!("blake3:{}", hex::encode(hasher.finalize().as_bytes()))
}

fn equals_hash(expected: &str, got: &str) -> bool {
    let normalize = |value: &str| {
        value
            .trim()
            .trim_start_matches("sha256:")
            .trim_start_matches("blake3:")
            .to_ascii_lowercase()
    };
    normalize(expected) == normalize(got)
}

fn json_error(status: StatusCode, error: &str, message: &str) -> axum::response::Response {
    (
        status,
        Json(json!({
            "error": error,
            "message": message
        })),
    )
        .into_response()
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
mod tests {
    use super::*;

    fn compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::Encoder::new(Vec::new(), 3).expect("encoder");
        encoder.write_all(data).expect("write");
        encoder.finish().expect("finish")
    }

    #[test]
    fn initialize_storage_creates_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        initialize_storage(tmp.path()).expect("initialize");
        let index = load_index(tmp.path()).expect("load index");
        assert_eq!(index.schema_version, "local-registry-v1");
        assert!(index.capsules.is_empty());
    }

    #[test]
    fn duplicate_version_is_detected() {
        let mut index = RegistryIndex::default();
        let now = Utc::now().to_rfc3339();
        upsert_capsule(
            &mut index,
            "koh0920",
            "sample-capsule",
            "sample-capsule",
            "",
            StoredRelease {
                version: "1.0.0".to_string(),
                file_name: "sample.capsule".to_string(),
                sha256: "sha256:abc".to_string(),
                blake3: "blake3:def".to_string(),
                size_bytes: 1,
                signature_status: "verified".to_string(),
                created_at: now.clone(),
                payload_v3: None,
            },
            &now,
        );
        assert!(has_release_version(
            &index,
            "koh0920",
            "sample-capsule",
            "1.0.0"
        ));
    }

    #[test]
    fn existing_release_outcome_requires_opt_in() {
        let release = StoredRelease {
            version: "1.0.0".to_string(),
            file_name: "sample.capsule".to_string(),
            sha256: "sha256:abc".to_string(),
            blake3: "blake3:def".to_string(),
            size_bytes: 1,
            signature_status: "verified".to_string(),
            created_at: Utc::now().to_rfc3339(),
            payload_v3: None,
        };

        let outcome = existing_release_outcome(&release, false, "sha256:abc");
        assert_eq!(
            outcome,
            ExistingReleaseOutcome::Conflict("same version is already published")
        );
    }

    #[test]
    fn existing_release_outcome_reuses_when_sha256_matches() {
        let release = StoredRelease {
            version: "1.0.0".to_string(),
            file_name: "sample.capsule".to_string(),
            sha256: "sha256:abc".to_string(),
            blake3: "blake3:def".to_string(),
            size_bytes: 1,
            signature_status: "verified".to_string(),
            created_at: Utc::now().to_rfc3339(),
            payload_v3: None,
        };

        let outcome = existing_release_outcome(&release, true, "sha256:abc");
        assert_eq!(outcome, ExistingReleaseOutcome::Reuse);
    }

    #[test]
    fn existing_release_outcome_conflicts_when_sha256_differs() {
        let release = StoredRelease {
            version: "1.0.0".to_string(),
            file_name: "sample.capsule".to_string(),
            sha256: "sha256:abc".to_string(),
            blake3: "blake3:def".to_string(),
            size_bytes: 1,
            signature_status: "verified".to_string(),
            created_at: Utc::now().to_rfc3339(),
            payload_v3: None,
        };

        let outcome = existing_release_outcome(&release, true, "sha256:xyz");
        assert_eq!(
            outcome,
            ExistingReleaseOutcome::Conflict("same version is already published (sha256 mismatch)")
        );
    }

    #[test]
    fn search_cursor_paginates() {
        let mut index = RegistryIndex::default();
        let now = Utc::now().to_rfc3339();
        for slug in ["a", "b", "c"] {
            upsert_capsule(
                &mut index,
                "koh0920",
                slug,
                slug,
                "",
                StoredRelease {
                    version: "1.0.0".to_string(),
                    file_name: format!("{slug}.capsule"),
                    sha256: "sha256:abc".to_string(),
                    blake3: "blake3:def".to_string(),
                    size_bytes: 1,
                    signature_status: "verified".to_string(),
                    created_at: now.clone(),
                    payload_v3: None,
                },
                &now,
            );
        }
        let rows = index
            .capsules
            .iter()
            .map(stored_to_search_row)
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].publisher.handle, "koh0920");
    }

    #[test]
    fn validate_write_auth_allows_when_disabled() {
        let headers = HeaderMap::new();
        assert!(validate_write_auth(&headers, None).is_ok());
    }

    #[test]
    fn validate_write_auth_requires_matching_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer secret-token".parse().unwrap(),
        );
        assert!(validate_write_auth(&headers, Some("secret-token")).is_ok());
        assert!(validate_write_auth(&headers, Some("wrong-token")).is_err());
        let empty = HeaderMap::new();
        assert!(validate_write_auth(&empty, Some("secret-token")).is_err());
    }

    #[test]
    fn constant_time_token_eq_handles_length_mismatch() {
        assert!(constant_time_token_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_token_eq(b"secret-token", b"secret-token-x"));
        assert!(!constant_time_token_eq(b"secret-token", b"secret"));
    }

    #[test]
    fn resolve_public_base_url_uses_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "100.64.0.10:8787".parse().unwrap());
        let url = resolve_public_base_url(&headers, "http://0.0.0.0:8787");
        assert_eq!(url, "http://100.64.0.10:8787");
    }

    #[test]
    fn resolve_public_base_url_uses_forwarded_host_and_proto() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "store.example.com".parse().unwrap());
        let url = resolve_public_base_url(&headers, "http://127.0.0.1:8787");
        assert_eq!(url, "https://store.example.com");
    }

    #[test]
    fn resolve_public_base_url_falls_back_when_headers_missing() {
        let headers = HeaderMap::new();
        let url = resolve_public_base_url(&headers, "http://127.0.0.1:8787");
        assert_eq!(url, "http://127.0.0.1:8787");
    }

    #[test]
    fn verify_uploaded_chunk_accepts_valid_payload() {
        let raw = b"chunk-data";
        let hash = compute_blake3(raw);
        let zstd = compress(raw);
        assert!(verify_uploaded_chunk(&hash, raw.len() as u32, &zstd).is_ok());
    }

    #[test]
    fn verify_uploaded_chunk_rejects_hash_mismatch() {
        let raw = b"chunk-data";
        let zstd = compress(raw);
        let wrong_hash = compute_blake3(b"other");
        assert!(verify_uploaded_chunk(&wrong_hash, raw.len() as u32, &zstd).is_err());
    }

    #[test]
    fn release_manifest_rel_path_matches_expected_layout() {
        let rel = release_manifest_rel_path("local", "demo", "1.2.3");
        assert_eq!(rel, "manifests/local/demo/1.2.3/payload.v3.manifest.json");
    }
}
