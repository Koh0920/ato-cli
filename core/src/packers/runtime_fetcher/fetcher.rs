use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info};

use crate::error::{CapsuleError, Result};
use super::RuntimeFetcher;

/// A checksum manifest URL and (optional) detached signature URL.
///
/// Phase 1 reads the unsigned checksum text to extract the expected sha256.
/// Phase 2 will verify `signature_url` before trusting the checksum contents.
#[derive(Debug, Clone)]
struct ChecksumManifest {
    unsigned_url: String,
    #[allow(dead_code)]
    signature_url: Option<String>,
}

impl ChecksumManifest {
    fn new(unsigned_url: String, signature_url: Option<String>) -> Self {
        Self {
            unsigned_url,
            signature_url,
        }
    }
}

#[async_trait]
pub(crate) trait ToolchainFetcher: Send + Sync {
    fn language(&self) -> &'static str;

    async fn download_runtime(
        &self,
        provider: &RuntimeFetcher,
        version: &str,
        show_progress: bool,
    ) -> Result<PathBuf>;
}

pub(crate) fn default_fetchers() -> HashMap<&'static str, Box<dyn ToolchainFetcher>> {
    let mut fetchers: HashMap<&'static str, Box<dyn ToolchainFetcher>> = HashMap::new();
    fetchers.insert("python", Box::new(PythonFetcher));
    fetchers.insert("node", Box::new(NodeFetcher));
    fetchers.insert("deno", Box::new(DenoFetcher));
    fetchers.insert("bun", Box::new(BunFetcher));
    fetchers
}

pub(crate) struct PythonFetcher;

#[async_trait]
impl ToolchainFetcher for PythonFetcher {
    fn language(&self) -> &'static str {
        "python"
    }

    async fn download_runtime(
        &self,
        provider: &RuntimeFetcher,
        version: &str,
        show_progress: bool,
    ) -> Result<PathBuf> {
        let runtime_dir = provider.get_runtime_path("python", version);

        if runtime_dir.exists() {
            info!("✓ Python {} already cached", version);
            return Ok(runtime_dir);
        }

        provider
            .reporter
            .notify(format!("⬇️  Downloading Python {} runtime...", version))
            .await?;

        let (os, arch) = RuntimeFetcher::detect_platform()?;
        let download_url = RuntimeFetcher::get_python_download_url(version, &os, &arch)?;

        debug!("Fetching from: {}", download_url);

        let expected_sha256 = provider
            .fetch_expected_sha256(&(download_url.clone() + ".sha256"), None)
            .await?;

        let archive_path = provider
            .cache_dir()
            .join(format!("python-{}.tar.gz", version));
        provider
            .download_with_progress(&download_url, &archive_path, show_progress)
            .await?;

        provider
            .verify_sha256_of_file(&archive_path, &expected_sha256)
            ?;

        let temp_dir = provider.cache_dir().join(format!("tmp-python-{}", version));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir)?;
        }
        std::fs::create_dir_all(&temp_dir)?;

        provider
            .reporter
            .notify(format!("📦 Extracting Python {} runtime...", version))
            .await?;
        RuntimeFetcher::extract_archive_from_file(&archive_path, &temp_dir)?;

        if runtime_dir.exists() {
            std::fs::remove_dir_all(&runtime_dir)?;
        }
        std::fs::rename(&temp_dir, &runtime_dir)
            .map_err(|e| {
                CapsuleError::Pack(format!("Failed to move extracted runtime to cache: {}", e))
            })?;

        let _ = std::fs::remove_file(&archive_path);

        provider
            .reporter
            .notify(format!(
                "✓ Python {} installed at {:?}",
                version, runtime_dir
            ))
            .await?;
        Ok(runtime_dir)
    }
}

pub(crate) struct NodeFetcher;

#[async_trait]
impl ToolchainFetcher for NodeFetcher {
    fn language(&self) -> &'static str {
        "node"
    }

    async fn download_runtime(
        &self,
        provider: &RuntimeFetcher,
        version: &str,
        show_progress: bool,
    ) -> Result<PathBuf> {
        let runtime_dir = provider.get_runtime_path("node", version);
        if runtime_dir.exists() {
            info!("✓ Node {} already cached", version);
            return Ok(runtime_dir);
        }

        provider
            .reporter
            .notify(format!("⬇️  Downloading Node {} runtime...", version))
            .await?;

        let (os, arch) = RuntimeFetcher::detect_platform()?;
        let full_version = RuntimeFetcher::resolve_node_full_version(version).await?;

        let (filename, is_zip) = RuntimeFetcher::node_artifact_filename(&full_version, &os, &arch)?;
        let download_url = format!("https://nodejs.org/dist/v{}/{}", full_version, filename);

        debug!("Fetching from: {}", download_url);

        let archive_path = provider.cache_dir().join(format!(
            "node-{}{}",
            full_version,
            if is_zip { ".zip" } else { ".tar.gz" }
        ));

        provider
            .download_with_progress(&download_url, &archive_path, show_progress)
            .await?;

        let expected_sha256 = provider
            .fetch_expected_sha256(
                &format!("https://nodejs.org/dist/v{}/SHASUMS256.txt", full_version),
                Some(&filename),
            )
            .await?;

        provider
            .verify_sha256_of_file(&archive_path, &expected_sha256)
            ?;

        let temp_dir = provider
            .cache_dir()
            .join(format!("tmp-node-{}", full_version));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir)?;
        }
        std::fs::create_dir_all(&temp_dir)?;

        provider
            .reporter
            .notify(format!("📦 Extracting Node {} runtime...", full_version))
            .await?;
        if is_zip {
            RuntimeFetcher::extract_zip_from_file(&archive_path, &temp_dir)?;
        } else {
            RuntimeFetcher::extract_archive_from_file(&archive_path, &temp_dir)?;
        }

        if runtime_dir.exists() {
            std::fs::remove_dir_all(&runtime_dir)?;
        }
        std::fs::rename(&temp_dir, &runtime_dir)
            .map_err(|e| {
                CapsuleError::Pack(format!("Failed to move extracted runtime to cache: {}", e))
            })?;

        let _ = std::fs::remove_file(&archive_path);

        provider
            .reporter
            .notify(format!(
                "✓ Node {} installed at {:?}",
                full_version, runtime_dir
            ))
            .await?;
        Ok(runtime_dir)
    }
}

pub(crate) struct DenoFetcher;

#[async_trait]
impl ToolchainFetcher for DenoFetcher {
    fn language(&self) -> &'static str {
        "deno"
    }

    async fn download_runtime(
        &self,
        provider: &RuntimeFetcher,
        version: &str,
        show_progress: bool,
    ) -> Result<PathBuf> {
        let runtime_dir = provider.get_runtime_path("deno", version);
        if runtime_dir.exists() {
            info!("✓ Deno {} already cached", version);
            return Ok(runtime_dir);
        }

        provider
            .reporter
            .notify(format!("⬇️  Downloading Deno {} runtime...", version))
            .await?;

        let (os, arch) = RuntimeFetcher::detect_platform()?;
        let download_url = format!(
            "https://github.com/denoland/deno/releases/download/v{}/deno-{}-{}.zip",
            version, os, arch
        );

        debug!("Fetching from: {}", download_url);

        let archive_path = provider.cache_dir().join(format!("deno-{}.zip", version));

        provider
            .download_with_progress(&download_url, &archive_path, show_progress)
            .await?;

        let expected_sha256 = provider
            .fetch_expected_sha256(&(download_url.clone() + ".sha256"), None)
            .await?;

        provider
            .verify_sha256_of_file(&archive_path, &expected_sha256)
            ?;

        let temp_dir = provider.cache_dir().join(format!("tmp-deno-{}", version));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir)?;
        }
        std::fs::create_dir_all(&temp_dir)?;

        provider
            .reporter
            .notify(format!("📦 Extracting Deno {} runtime...", version))
            .await?;
        RuntimeFetcher::extract_zip_from_file(&archive_path, &temp_dir)?;

        if runtime_dir.exists() {
            std::fs::remove_dir_all(&runtime_dir)?;
        }
        std::fs::rename(&temp_dir, &runtime_dir)
            .map_err(|e| {
                CapsuleError::Pack(format!("Failed to move extracted runtime to cache: {}", e))
            })?;

        let _ = std::fs::remove_file(&archive_path);

        provider
            .reporter
            .notify(format!("✓ Deno {} installed at {:?}", version, runtime_dir))
            .await?;
        Ok(runtime_dir)
    }
}

pub(crate) struct BunFetcher;

#[async_trait]
impl ToolchainFetcher for BunFetcher {
    fn language(&self) -> &'static str {
        "bun"
    }

    async fn download_runtime(
        &self,
        provider: &RuntimeFetcher,
        version: &str,
        show_progress: bool,
    ) -> Result<PathBuf> {
        let runtime_dir = provider.get_runtime_path("bun", version);
        if runtime_dir.exists() {
            info!("✓ Bun {} already cached", version);
            return Ok(runtime_dir);
        }

        provider
            .reporter
            .notify(format!("⬇️  Downloading Bun {} runtime...", version))
            .await?;

        let (os, arch) = RuntimeFetcher::detect_platform()?;
        let full_version = RuntimeFetcher::normalize_semverish(version);

        let download_url = format!(
            "https://github.com/oven-sh/bun/releases/download/bun-v{}/bun-{}-{}.zip",
            full_version, os, arch
        );

        debug!("Fetching from: {}", download_url);

        let archive_path = provider
            .cache_dir()
            .join(format!("bun-{}.zip", full_version));

        provider
            .download_with_progress(&download_url, &archive_path, show_progress)
            .await?;

        let expected_sha256 = provider
            .fetch_expected_sha256(&(download_url.clone() + ".sha256"), None)
            .await?;

        provider
            .verify_sha256_of_file(&archive_path, &expected_sha256)
            ?;

        let temp_dir = provider
            .cache_dir()
            .join(format!("tmp-bun-{}", full_version));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir)?;
        }
        std::fs::create_dir_all(&temp_dir)?;

        provider
            .reporter
            .notify(format!("📦 Extracting Bun {} runtime...", full_version))
            .await?;
        RuntimeFetcher::extract_zip_from_file(&archive_path, &temp_dir)?;

        if runtime_dir.exists() {
            std::fs::remove_dir_all(&runtime_dir)?;
        }
        std::fs::rename(&temp_dir, &runtime_dir)
            .map_err(|e| {
                CapsuleError::Pack(format!("Failed to move extracted runtime to cache: {}", e))
            })?;

        let _ = std::fs::remove_file(&archive_path);

        provider
            .reporter
            .notify(format!(
                "✓ Bun {} installed at {:?}",
                full_version, runtime_dir
            ))
            .await?;
        Ok(runtime_dir)
    }
}
