//! Source registration and source lifecycle commands.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::auth::AuthManager;
use crate::registry::RegistryResolver;

const ENV_SESSION_TOKEN: &str = "ATO_SESSION_TOKEN";
const LEGACY_ENV_SESSION_TOKEN: &str = "CAPSULE_SESSION_TOKEN";

#[derive(Debug, Serialize)]
pub struct SourceRegisterResult {
    pub source_id: String,
    pub sync_run_id: Option<String>,
    pub capsule_slug: String,
    pub visibility: String,
    pub sync_status: String,
    pub auto_submit_playground: bool,
    pub auto_submit_result: Option<AutoSubmitResult>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoSubmitResult {
    pub deployment_id: String,
    pub review_status: String,
    pub gate_status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceSyncResult {
    pub sync_run_id: String,
    pub status: String,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub target_commit: Option<String>,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub signature_failure_reason: Option<String>,
    #[serde(default)]
    pub attempt_count: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceSyncRunStatus {
    pub sync_run_id: String,
    pub source_id: String,
    pub status: String,
    #[serde(default)]
    pub trigger: Option<String>,
    #[serde(default)]
    pub target_commit: Option<String>,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub signature_failure_reason: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub attempt_count: Option<u64>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceRebuildResult {
    pub source_id: String,
    pub sync_run_id: String,
    pub status: String,
    #[serde(default)]
    pub target_commit: Option<String>,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub signature_failure_reason: Option<String>,
    #[serde(default)]
    pub attempt_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RegisterResponse {
    source_id: String,
    #[serde(default)]
    sync_run_id: Option<String>,
    capsule_slug: String,
    visibility: String,
    sync_status: String,
    #[serde(default)]
    auto_submit_playground: bool,
    #[serde(default)]
    auto_submit_result: Option<AutoSubmitResult>,
}

#[derive(Debug, Deserialize)]
struct SourceErrorResponse {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    sync_run_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PreflightResponse {
    ok: bool,
}

fn read_env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_repo_slug(repo_url: &str) -> String {
    repo_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("unknown")
        .trim_end_matches(".git")
        .to_lowercase()
}

async fn resolve_registry_url(registry_url: Option<&str>) -> Result<String> {
    if let Some(url) = registry_url {
        return Ok(url.to_string());
    }
    let resolver = RegistryResolver::default();
    Ok(resolver.resolve("localhost").await?.url)
}

fn resolve_auth_tokens() -> Result<(Option<String>, Option<String>)> {
    let session_token = read_env_non_empty(ENV_SESSION_TOKEN)
        .or_else(|| read_env_non_empty(LEGACY_ENV_SESSION_TOKEN));
    if let Some(token) = session_token {
        return Ok((Some(token), None));
    }

    let auth = AuthManager::new()?;
    let creds = auth
        .require()
        .context("Source operation requires authentication")?;
    if let Some(token) = creds.session_token {
        Ok((Some(token), None))
    } else if let Some(token) = creds.github_token {
        Ok((None, Some(token)))
    } else {
        anyhow::bail!("Source operation requires authentication");
    }
}

fn with_auth(
    request: reqwest::RequestBuilder,
    session_token: Option<&str>,
    bearer_token: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(cookie_token) = session_token {
        request.header(
            "Cookie",
            format!(
                "better-auth.session_token={}; __Secure-better-auth.session_token={}",
                cookie_token, cookie_token
            ),
        )
    } else if let Some(token) = bearer_token {
        request.header("Authorization", format!("Bearer {}", token))
    } else {
        request
    }
}

pub async fn register_github_source(
    repo_url: &str,
    registry_url: Option<&str>,
    channel: Option<&str>,
    apply_playground: bool,
    installation_id: Option<u64>,
    auto_sync_on_exists: bool,
    json_output: bool,
) -> Result<SourceRegisterResult> {
    let registry = resolve_registry_url(registry_url).await?;
    let (session_token, bearer_token) = resolve_auth_tokens()?;
    let client = reqwest::Client::new();

    let mut payload = serde_json::json!({
        "repo_url": repo_url,
        "channel": channel.unwrap_or("stable"),
        "apply_playground": apply_playground,
    });
    if let Some(id) = installation_id {
        payload["installation_id"] = serde_json::json!(id);
    }

    let request = client
        .post(format!("{}/v1/sources/github/register", registry))
        .json(&payload);
    let response = with_auth(request, session_token.as_deref(), bearer_token.as_deref())
        .send()
        .await
        .with_context(|| "Failed to register source")?;

    if response.status().is_success() {
        let payload = response
            .json::<RegisterResponse>()
            .await
            .with_context(|| "Invalid source register response")?;

        if !json_output {
            eprintln!("✅ Source registered");
            eprintln!("   Source ID: {}", payload.source_id);
            if let Some(sync_run_id) = payload.sync_run_id.as_ref() {
                eprintln!("   Sync Run:  {}", sync_run_id);
            }
            eprintln!("   Capsule:   {}", payload.capsule_slug);
            eprintln!("   Visibility: {}", payload.visibility);
            eprintln!("   Sync: {}", payload.sync_status);
        }

        return Ok(SourceRegisterResult {
            source_id: payload.source_id,
            sync_run_id: payload.sync_run_id,
            capsule_slug: payload.capsule_slug,
            visibility: payload.visibility,
            sync_status: payload.sync_status,
            auto_submit_playground: payload.auto_submit_playground,
            auto_submit_result: payload.auto_submit_result,
        });
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let parsed = serde_json::from_str::<SourceErrorResponse>(&body).ok();
    let is_source_exists = status == reqwest::StatusCode::CONFLICT
        && parsed
            .as_ref()
            .and_then(|v| v.error.as_deref())
            .map(|v| v == "source_exists")
            .unwrap_or(false);

    if auto_sync_on_exists && is_source_exists {
        let source_id = parsed
            .as_ref()
            .and_then(|v| v.source_id.clone())
            .with_context(|| "source_exists response is missing source_id")?;
        preflight_source_operation(
            &source_id,
            "sync",
            Some(&registry),
            session_token.as_deref(),
            bearer_token.as_deref(),
        )
        .await
        .with_context(|| {
            format!(
                "Preflight failed for existing source {}. Run `ato source sync-status --source-id {} --sync-run-id <id>` after fixing permissions.",
                source_id, source_id
            )
        })?;

        let sync_result = sync_source(
            &source_id,
            Some(&registry),
            session_token.as_deref(),
            bearer_token.as_deref(),
            json_output,
        )
        .await?;

        if sync_result.status != "completed" {
            anyhow::bail!(
                "Publish sync did not complete: source_id={} sync_run_id={} status={} failure_reason={} signature_failure_reason={}. Try `ato source sync-status --source-id {} --sync-run-id {}` or `ato source rebuild --source-id {} --wait`.",
                source_id,
                sync_result.sync_run_id,
                sync_result.status,
                sync_result.failure_reason.clone().unwrap_or_else(|| "-".to_string()),
                sync_result
                    .signature_failure_reason
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                source_id,
                sync_result.sync_run_id,
                source_id
            );
        }

        return Ok(SourceRegisterResult {
            source_id: source_id.clone(),
            sync_run_id: Some(sync_result.sync_run_id),
            capsule_slug: parse_repo_slug(repo_url),
            visibility: sync_result
                .visibility
                .unwrap_or_else(|| "limited".to_string()),
            sync_status: sync_result.status,
            auto_submit_playground: false,
            auto_submit_result: None,
        });
    }

    let parsed_error = parsed
        .as_ref()
        .and_then(|v| v.error.clone())
        .unwrap_or_else(|| "source_register_failed".to_string());
    let parsed_message = parsed
        .as_ref()
        .and_then(|v| v.message.clone())
        .unwrap_or_else(|| body.clone());
    anyhow::bail!(
        "Source registration failed ({}): error={} message={} source_id={} sync_run_id={}",
        status,
        parsed_error,
        parsed_message,
        parsed
            .as_ref()
            .and_then(|v| v.source_id.as_deref())
            .unwrap_or("-"),
        parsed
            .as_ref()
            .and_then(|v| v.sync_run_id.as_deref())
            .unwrap_or("-"),
    );
}

async fn preflight_source_operation(
    source_id: &str,
    operation: &str,
    registry_url: Option<&str>,
    session_token: Option<&str>,
    bearer_token: Option<&str>,
) -> Result<()> {
    let registry = resolve_registry_url(registry_url).await?;
    let client = reqwest::Client::new();
    let request = client
        .post(format!("{}/v1/sources/{}/preflight", registry, source_id))
        .json(&serde_json::json!({ "operation": operation }));
    let response = with_auth(request, session_token, bearer_token)
        .send()
        .await
        .with_context(|| "Failed to preflight source operation")?;
    if response.status().is_success() {
        let payload = response
            .json::<PreflightResponse>()
            .await
            .with_context(|| "Invalid source preflight response")?;
        if !payload.ok {
            anyhow::bail!("Source preflight returned ok=false");
        }
        return Ok(());
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    anyhow::bail!("Source preflight failed ({}): {}", status, body);
}

pub async fn sync_source(
    source_id: &str,
    registry_url: Option<&str>,
    session_token: Option<&str>,
    bearer_token: Option<&str>,
    json_output: bool,
) -> Result<SourceSyncResult> {
    let registry = resolve_registry_url(registry_url).await?;
    let client = reqwest::Client::new();
    let request = client.post(format!("{}/v1/sources/{}/sync", registry, source_id));
    let response = with_auth(request, session_token, bearer_token)
        .send()
        .await
        .with_context(|| "Failed to sync source")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Source sync failed ({}): {}", status, body);
    }
    let payload = response
        .json::<SourceSyncResult>()
        .await
        .with_context(|| "Invalid source sync response")?;
    if !json_output {
        eprintln!(
            "ℹ️  sync: source_id={} sync_run_id={} status={}",
            source_id, payload.sync_run_id, payload.status
        );
    }
    Ok(payload)
}

pub async fn fetch_sync_run_status(
    source_id: &str,
    sync_run_id: &str,
    registry_url: Option<&str>,
    json_output: bool,
) -> Result<SourceSyncRunStatus> {
    let registry = resolve_registry_url(registry_url).await?;
    let (session_token, bearer_token) = resolve_auth_tokens()?;
    let client = reqwest::Client::new();
    let request = client.get(format!(
        "{}/v1/sources/{}/sync-runs/{}",
        registry, source_id, sync_run_id
    ));
    let response = with_auth(request, session_token.as_deref(), bearer_token.as_deref())
        .send()
        .await
        .with_context(|| "Failed to fetch sync run status")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Sync run status request failed ({}): {}", status, body);
    }
    let payload = response
        .json::<SourceSyncRunStatus>()
        .await
        .with_context(|| "Invalid sync run status response")?;
    if !json_output {
        eprintln!(
            "ℹ️  sync-status: source_id={} sync_run_id={} status={} failure_reason={} signature_failure_reason={}",
            source_id,
            sync_run_id,
            payload.status,
            payload
                .failure_reason
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            payload
                .signature_failure_reason
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    Ok(payload)
}

pub async fn rebuild_source(
    source_id: &str,
    reference: Option<&str>,
    wait: bool,
    registry_url: Option<&str>,
    json_output: bool,
) -> Result<SourceRebuildResult> {
    let registry = resolve_registry_url(registry_url).await?;
    let (session_token, bearer_token) = resolve_auth_tokens()?;
    preflight_source_operation(
        source_id,
        "rebuild",
        Some(&registry),
        session_token.as_deref(),
        bearer_token.as_deref(),
    )
    .await?;

    let client = reqwest::Client::new();
    let request = client
        .post(format!("{}/v1/sources/{}/rebuild", registry, source_id))
        .json(&serde_json::json!({
            "ref": reference,
        }));
    let response = with_auth(request, session_token.as_deref(), bearer_token.as_deref())
        .send()
        .await
        .with_context(|| "Failed to trigger source rebuild")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Source rebuild failed ({}): {}", status, body);
    }
    let mut payload = response
        .json::<SourceRebuildResult>()
        .await
        .with_context(|| "Invalid source rebuild response")?;

    if wait {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(35 * 60);
        while tokio::time::Instant::now() < deadline
            && (payload.status == "queued"
                || payload.status == "running"
                || payload.status == "awaiting_signature")
        {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let status =
                fetch_sync_run_status(source_id, &payload.sync_run_id, Some(&registry), true)
                    .await?;
            payload.status = status.status;
            payload.failure_reason = status.failure_reason;
            payload.signature_failure_reason = status.signature_failure_reason;
            payload.target_commit = status.target_commit;
            payload.attempt_count = status.attempt_count;
        }
        if payload.status == "queued"
            || payload.status == "running"
            || payload.status == "awaiting_signature"
        {
            anyhow::bail!(
                "Rebuild wait timeout: source_id={} sync_run_id={} status={}. Check with `ato source sync-status --source-id {} --sync-run-id {}`.",
                source_id,
                payload.sync_run_id,
                payload.status,
                source_id,
                payload.sync_run_id
            );
        }
    }

    if !json_output {
        eprintln!(
            "ℹ️  rebuild: source_id={} sync_run_id={} status={}",
            source_id, payload.sync_run_id, payload.status
        );
    }
    Ok(payload)
}
