//! Source registration commands.
//!
//! `ato source register <repo_url>` を提供する。

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::auth::AuthManager;
use crate::registry::RegistryResolver;

const ENV_SESSION_TOKEN: &str = "ATO_SESSION_TOKEN";
const LEGACY_ENV_SESSION_TOKEN: &str = "CAPSULE_SESSION_TOKEN";

#[derive(Debug, Serialize)]
pub struct SourceRegisterResult {
    pub source_id: String,
    pub capsule_slug: String,
    pub visibility: String,
    pub sync_status: String,
    pub auto_submit_playground: bool,
    pub auto_submit_result: Option<AutoSubmitResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AutoSubmitResult {
    pub deployment_id: String,
    pub review_status: String,
    pub gate_status: String,
}

#[derive(Debug, Deserialize)]
struct RegisterResponse {
    source_id: String,
    capsule_slug: String,
    visibility: String,
    sync_status: String,
    #[serde(default)]
    auto_submit_playground: bool,
    #[serde(default)]
    auto_submit_result: Option<AutoSubmitResult>,
}

fn read_env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub async fn register_github_source(
    repo_url: &str,
    registry_url: Option<&str>,
    channel: Option<&str>,
    apply_playground: bool,
    installation_id: Option<u64>,
    json_output: bool,
) -> Result<SourceRegisterResult> {
    let registry = if let Some(url) = registry_url {
        url.to_string()
    } else {
        let resolver = RegistryResolver::default();
        resolver.resolve("localhost").await?.url
    };

    let session_token = read_env_non_empty(ENV_SESSION_TOKEN)
        .or_else(|| read_env_non_empty(LEGACY_ENV_SESSION_TOKEN));

    let (session_token, bearer_token) = if let Some(token) = session_token {
        (Some(token), None)
    } else {
        let auth = AuthManager::new()?;
        let creds = auth
            .require()
            .context("Source registration requires authentication")?;
        if let Some(token) = creds.session_token {
            (Some(token), None)
        } else if let Some(token) = creds.github_token {
            (None, Some(token))
        } else {
            anyhow::bail!("Source registration requires authentication");
        }
    };

    let client = reqwest::Client::new();
    let mut payload = serde_json::json!({
        "repo_url": repo_url,
        "channel": channel.unwrap_or("stable"),
        "apply_playground": apply_playground,
    });
    if let Some(id) = installation_id {
        payload["installation_id"] = serde_json::json!(id);
    }

    let mut request = client
        .post(format!("{}/v1/sources/github/register", registry))
        .json(&payload);

    if let Some(cookie_token) = session_token {
        request = request.header(
            "Cookie",
            format!(
                "better-auth.session_token={}; __Secure-better-auth.session_token={}",
                cookie_token, cookie_token
            ),
        );
    } else if let Some(token) = bearer_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let response = request
        .send()
        .await
        .with_context(|| "Failed to register source")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Source registration failed ({}): {}", status, body);
    }

    let payload = response
        .json::<RegisterResponse>()
        .await
        .with_context(|| "Invalid source register response")?;

    if !json_output {
        eprintln!("✅ Source registered");
        eprintln!("   Source ID: {}", payload.source_id);
        eprintln!("   Capsule:   {}", payload.capsule_slug);
        eprintln!("   Visibility: {}", payload.visibility);
        eprintln!("   Sync: {}", payload.sync_status);
        eprintln!(
            "   Auto submit playground: {}",
            if payload.auto_submit_playground {
                "enabled"
            } else {
                "disabled"
            }
        );
        if let Some(result) = payload.auto_submit_result.as_ref() {
            eprintln!(
                "   Auto submit result: review_status={}, gate_status={}",
                result.review_status, result.gate_status
            );
        }
    }

    Ok(SourceRegisterResult {
        source_id: payload.source_id,
        capsule_slug: payload.capsule_slug,
        visibility: payload.visibility,
        sync_status: payload.sync_status,
        auto_submit_playground: payload.auto_submit_playground,
        auto_submit_result: payload.auto_submit_result,
    })
}
