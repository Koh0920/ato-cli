//! Authentication module for Ato Store
//!
//! Manages authentication credentials for the ato CLI.
//! Stores credentials in `~/.capsule/credentials.json`.

use anyhow::{Context, Result};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_STORE_API_URL: &str = "https://api.ato.run";
const DEFAULT_STORE_SITE_URL: &str = "https://store.ato.run";
const ENV_STORE_API_URL: &str = "ATO_STORE_API_URL";
const ENV_STORE_SITE_URL: &str = "ATO_STORE_SITE_URL";

/// User credentials stored locally
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Credentials {
    /// GitHub Personal Access Token (legacy fallback)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_token: Option<String>,

    /// Store session token (Device Flow)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,

    /// Publisher DID (set after first successful registration)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_did: Option<String>,

    /// GitHub username (cached from API)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_username: Option<String>,
}

/// Manages authentication credentials
pub struct AuthManager {
    credentials_path: PathBuf,
}

impl AuthManager {
    /// Create a new AuthManager with default credentials path
    pub fn new() -> Result<Self> {
        let home = dirs::home_dir().context("Cannot determine home directory")?;
        let credentials_path = home.join(".capsule").join("credentials.json");
        Ok(Self { credentials_path })
    }

    /// Create AuthManager with custom credentials path (for testing)
    pub fn with_path(credentials_path: PathBuf) -> Self {
        Self { credentials_path }
    }

    /// Load credentials from disk
    pub fn load(&self) -> Result<Option<Credentials>> {
        if !self.credentials_path.exists() {
            return Ok(None);
        }

        let contents = fs::read_to_string(&self.credentials_path).with_context(|| {
            format!(
                "Failed to read credentials from {:?}",
                self.credentials_path
            )
        })?;

        let creds: Credentials = serde_json::from_str(&contents).with_context(|| {
            format!(
                "Failed to parse credentials from {:?}",
                self.credentials_path
            )
        })?;

        Ok(Some(creds))
    }

    /// Save credentials to disk
    pub fn save(&self, creds: &Credentials) -> Result<()> {
        if let Some(parent) = self.credentials_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {:?}", parent))?;
        }

        let json =
            serde_json::to_string_pretty(creds).context("Failed to serialize credentials")?;

        fs::write(&self.credentials_path, json).with_context(|| {
            format!("Failed to write credentials to {:?}", self.credentials_path)
        })?;

        Ok(())
    }

    /// Load credentials or return an error if not authenticated
    pub fn require(&self) -> Result<Credentials> {
        let creds = self.load()?.with_context(|| {
            format!(
                "Not authenticated. Run:\n  ato login\n\nCredentials file not found: {:?}",
                self.credentials_path
            )
        })?;

        if creds.session_token.is_none() && creds.github_token.is_none() {
            anyhow::bail!(
                "Not authenticated. Run:\n  ato login\n\nNo usable token found in {:?}",
                self.credentials_path
            );
        }

        Ok(creds)
    }

    /// Delete stored credentials (logout)
    pub fn delete(&self) -> Result<()> {
        if self.credentials_path.exists() {
            fs::remove_file(&self.credentials_path).with_context(|| {
                format!(
                    "Failed to delete credentials at {:?}",
                    self.credentials_path
                )
            })?;
        }
        Ok(())
    }

    /// Get the path where credentials are stored
    pub fn credentials_path(&self) -> &PathBuf {
        &self.credentials_path
    }
}

/// GitHub user information
#[derive(Debug, Deserialize)]
struct GitHubUser {
    login: String,
}

#[derive(Debug, Deserialize)]
struct DeviceStartResponse {
    device_code: String,
    user_code: String,
    activate_url: String,
    expires_in: u64,
    #[serde(default)]
    interval: Option<u64>,
}

#[derive(Debug, Serialize)]
struct DevicePollRequest<'a> {
    device_code: &'a str,
}

#[derive(Debug, Deserialize)]
struct DevicePollResponse {
    status: String,
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default, alias = "githubUsername")]
    github_username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RetryAfterResponse {
    retry_after: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct StoreSessionUser {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    email: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StoreSessionResponse {
    #[serde(default)]
    user: Option<StoreSessionUser>,
}

fn read_env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn trim_trailing_slash(value: &str) -> String {
    value.trim_end_matches('/').to_string()
}

fn store_api_base_url() -> String {
    trim_trailing_slash(
        &read_env_non_empty(ENV_STORE_API_URL).unwrap_or_else(|| DEFAULT_STORE_API_URL.to_string()),
    )
}

fn store_site_base_url() -> String {
    trim_trailing_slash(
        &read_env_non_empty(ENV_STORE_SITE_URL)
            .unwrap_or_else(|| DEFAULT_STORE_SITE_URL.to_string()),
    )
}

fn try_open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg(url)
            .spawn()
            .context("Failed to launch browser with `open`")?;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open")
            .arg(url)
            .spawn()
            .context("Failed to launch browser with `xdg-open`")?;
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
            .context("Failed to launch browser with `start`")?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Ok(())
}

fn fetch_store_session_user(session_token: &str) -> Result<Option<StoreSessionUser>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .get(format!("{}/api/auth/session", store_api_base_url()))
        .header("Accept", "application/json")
        .header(
            "Cookie",
            format!(
                "better-auth.session_token={}; __Secure-better-auth.session_token={}",
                session_token, session_token
            ),
        )
        .send()
        .context("Failed to fetch Store session")?;

    if response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN {
        return Ok(None);
    }

    if !response.status().is_success() {
        anyhow::bail!("Store session lookup failed (HTTP {})", response.status());
    }

    let body = response
        .json::<StoreSessionResponse>()
        .context("Failed to parse Store session response")?;

    Ok(body.user)
}

/// Verify a GitHub token by calling the GitHub API
async fn verify_github_token(token: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let response = client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {}", token))
        .header("User-Agent", "ato-cli")
        .send()
        .await
        .context("Failed to connect to GitHub API")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();

        anyhow::bail!("Invalid GitHub token (HTTP {}): {}", status, error_text);
    }

    let user: GitHubUser = response
        .json()
        .await
        .context("Failed to parse GitHub user response")?;

    Ok(user.login)
}

/// Login with a GitHub Personal Access Token
pub async fn login_with_token(token: String) -> Result<()> {
    println!("🔐 Verifying GitHub token...");

    let username = verify_github_token(&token).await?;

    let manager = AuthManager::new()?;
    let mut creds = manager.load()?.unwrap_or_default();
    creds.github_token = Some(token);
    creds.github_username = Some(username.clone());
    manager.save(&creds)?;

    println!("✅ Authenticated as @{}", username);
    println!("   Credentials saved to: {:?}", manager.credentials_path());

    Ok(())
}

/// Login with Store Device Flow
pub async fn login_with_store_device_flow() -> Result<()> {
    let api_base = store_api_base_url();
    let site_base = store_site_base_url();
    let client = reqwest::Client::new();

    let start_response = client
        .post(format!("{}/v1/auth/device/start", api_base))
        .json(&serde_json::json!({}))
        .send()
        .await
        .with_context(|| "Failed to start Store device authentication")?;

    if !start_response.status().is_success() {
        let status = start_response.status();
        let body = start_response.text().await.unwrap_or_default();
        anyhow::bail!("Device auth start failed ({}): {}", status, body);
    }

    let start: DeviceStartResponse = start_response
        .json()
        .await
        .context("Invalid device auth start response")?;

    let login_url = format!(
        "{}/auth?next={}",
        site_base,
        urlencoding::encode(&start.activate_url)
    );

    println!("🌐 Opening browser for Ato sign-in...");
    println!("   URL: {}", login_url);
    println!("🔑 Verification code: {}", start.user_code);

    if let Err(error) = try_open_browser(&login_url) {
        eprintln!("⚠️  Could not open browser automatically: {}", error);
        eprintln!("   Open the URL manually to continue sign-in.");
    }

    println!("⏳ Waiting for browser authentication...");

    let poll_timeout_secs = start.expires_in.min(300);
    let poll_interval_secs = start.interval.unwrap_or(3).max(3);
    let started_at = Instant::now();

    loop {
        if started_at.elapsed() >= Duration::from_secs(poll_timeout_secs) {
            anyhow::bail!(
                "Authentication timed out after {} seconds. Run `ato login` again.",
                poll_timeout_secs
            );
        }

        let poll_response = client
            .post(format!("{}/v1/auth/device/poll", api_base))
            .json(&DevicePollRequest {
                device_code: &start.device_code,
            })
            .send()
            .await
            .with_context(|| "Failed to poll device authentication state")?;

        if poll_response.status() == StatusCode::TOO_MANY_REQUESTS {
            let body =
                poll_response
                    .json::<RetryAfterResponse>()
                    .await
                    .unwrap_or(RetryAfterResponse {
                        retry_after: Some(poll_interval_secs),
                    });
            let retry_after = body.retry_after.unwrap_or(poll_interval_secs).max(1);
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            continue;
        }

        if !poll_response.status().is_success() {
            let status = poll_response.status();
            let body = poll_response.text().await.unwrap_or_default();
            anyhow::bail!("Device auth poll failed ({}): {}", status, body);
        }

        let poll: DevicePollResponse = poll_response
            .json()
            .await
            .context("Invalid device auth poll response")?;

        match poll.status.as_str() {
            "pending" => {
                tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;
            }
            "approved" => {
                let session_token = poll
                    .session_token
                    .context("Device auth approved but no session token was returned")?;

                let manager = AuthManager::new()?;
                let mut creds = manager.load()?.unwrap_or_default();
                creds.session_token = Some(session_token);
                if let Some(username) = poll.github_username {
                    creds.github_username = Some(username);
                }
                manager.save(&creds)?;

                println!("✅ Login completed successfully");
                println!("   Credentials saved to: {:?}", manager.credentials_path());
                return Ok(());
            }
            "expired" => {
                anyhow::bail!("Authentication request expired. Run `ato login` again.");
            }
            "consumed" => {
                anyhow::bail!("Authentication token already consumed. Run `ato login` again.");
            }
            "denied" => {
                anyhow::bail!("Authentication denied. Run `ato login` again.");
            }
            other => {
                anyhow::bail!("Unexpected authentication status: {}", other);
            }
        }
    }
}

/// Logout (delete stored credentials)
pub fn logout() -> Result<()> {
    let manager = AuthManager::new()?;

    if !manager.credentials_path().exists() {
        println!("ℹ️  Not currently logged in");
        return Ok(());
    }

    manager.delete()?;
    println!("✅ Logged out successfully");
    println!(
        "   Deleted credentials from: {:?}",
        manager.credentials_path()
    );

    Ok(())
}

/// Show current authentication status
pub fn status() -> Result<()> {
    let manager = AuthManager::new()?;

    match manager.load()? {
        Some(creds) if creds.session_token.is_some() || creds.github_token.is_some() => {
            println!("✅ Authenticated");
            if let Some(session_token) = &creds.session_token {
                println!("   Store session: configured");
                match fetch_store_session_user(session_token) {
                    Ok(Some(user)) => {
                        println!("   User ID: {}", user.id);
                        if let Some(name) = user.name {
                            println!("   Name: {}", name);
                        }
                        if let Some(email) = user.email {
                            println!("   Email: {}", email);
                        }
                    }
                    Ok(None) => {
                        println!("   User: session expired or unavailable");
                    }
                    Err(err) => {
                        println!("   User: failed to fetch ({})", err);
                    }
                }
            }
            if creds.github_token.is_some() {
                println!("   GitHub token: configured");
            }
            if let Some(username) = &creds.github_username {
                println!("   GitHub: @{}", username);
            }
            if let Some(did) = &creds.publisher_did {
                println!("   Publisher DID: {}", did);
            }
            println!("   Credentials: {:?}", manager.credentials_path());
        }
        _ => {
            println!("❌ Not authenticated");
            println!("   Run: ato login");
            println!();
            println!("   Legacy fallback:");
            println!("   ato login --token <github-personal-access-token>");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_credentials_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let creds_path = temp_dir.path().join("credentials.json");

        let manager = AuthManager::with_path(creds_path.clone());

        let original = Credentials {
            github_token: Some("ghp_test123".to_string()),
            session_token: Some("sess_test_123".to_string()),
            publisher_did: Some("did:key:z6Mk...".to_string()),
            github_username: Some("testuser".to_string()),
        };

        manager.save(&original).unwrap();
        let loaded = manager.load().unwrap().unwrap();

        assert_eq!(original.github_token, loaded.github_token);
        assert_eq!(original.session_token, loaded.session_token);
        assert_eq!(original.publisher_did, loaded.publisher_did);
        assert_eq!(original.github_username, loaded.github_username);
    }

    #[test]
    fn test_legacy_credentials_json_compatibility() {
        let temp_dir = TempDir::new().unwrap();
        let creds_path = temp_dir.path().join("credentials.json");

        fs::write(
            &creds_path,
            r#"{
  "github_token": "ghp_legacy123",
  "publisher_did": "did:key:z6MkLegacy",
  "github_username": "legacy-user"
}"#,
        )
        .unwrap();

        let manager = AuthManager::with_path(creds_path);
        let loaded = manager.load().unwrap().unwrap();

        assert_eq!(loaded.github_token.as_deref(), Some("ghp_legacy123"));
        assert_eq!(loaded.session_token, None);
        assert_eq!(loaded.publisher_did.as_deref(), Some("did:key:z6MkLegacy"));
        assert_eq!(loaded.github_username.as_deref(), Some("legacy-user"));
    }

    #[test]
    fn test_require_fails_when_not_authenticated() {
        let temp_dir = TempDir::new().unwrap();
        let creds_path = temp_dir.path().join("nonexistent.json");

        let manager = AuthManager::with_path(creds_path);
        let result = manager.require();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Not authenticated"));
    }

    #[test]
    fn test_require_fails_when_no_tokens() {
        let temp_dir = TempDir::new().unwrap();
        let creds_path = temp_dir.path().join("credentials.json");

        let manager = AuthManager::with_path(creds_path);
        manager
            .save(&Credentials {
                github_token: None,
                session_token: None,
                publisher_did: Some("did:key:z6Mk...".to_string()),
                github_username: Some("testuser".to_string()),
            })
            .unwrap();

        let result = manager.require();
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_credentials() {
        let temp_dir = TempDir::new().unwrap();
        let creds_path = temp_dir.path().join("credentials.json");

        let manager = AuthManager::with_path(creds_path.clone());

        let creds = Credentials {
            github_token: Some("ghp_test123".to_string()),
            session_token: Some("sess_test_123".to_string()),
            publisher_did: None,
            github_username: Some("testuser".to_string()),
        };

        manager.save(&creds).unwrap();
        assert!(creds_path.exists());

        manager.delete().unwrap();
        assert!(!creds_path.exists());
    }
}
