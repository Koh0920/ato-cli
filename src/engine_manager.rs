use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::fs;
use std::path::{Path, PathBuf};

const ENGINES_DIR: &str = ".ato/engines";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineInfo {
    pub name: String,
    pub version: String,
    pub url: String,
    pub sha256: String,
    pub arch: String,
    pub os: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineManifest {
    pub engines: Vec<EngineInfo>,
}

pub struct EngineManager {
    engines_dir: PathBuf,
}

impl EngineManager {
    pub fn new() -> Result<Self> {
        let engines_dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?
            .join(ENGINES_DIR);

        if !engines_dir.exists() {
            fs::create_dir_all(&engines_dir).with_context(|| {
                format!(
                    "Failed to create engines directory: {}",
                    engines_dir.display()
                )
            })?;
        }

        Ok(Self { engines_dir })
    }

    pub fn get_engines_dir(&self) -> &Path {
        &self.engines_dir
    }

    pub fn engine_path(&self, name: &str, version: &str) -> PathBuf {
        self.engines_dir.join(format!("{}-{}", name, version))
    }

    pub fn list_engines(&self) -> Result<Vec<EngineInfo>> {
        if !self.engines_dir.exists() {
            return Ok(Vec::new());
        }

        let mut engines = Vec::new();

        for entry in fs::read_dir(&self.engines_dir).with_context(|| {
            format!(
                "Failed to read engines directory: {}",
                self.engines_dir.display()
            )
        })? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(info) = self.parse_engine_filename(filename) {
                        engines.push(info);
                    }
                }
            }
        }

        Ok(engines)
    }

    fn parse_engine_filename(&self, filename: &str) -> Option<EngineInfo> {
        let parts: Vec<&str> = filename.split('-').collect();
        if parts.len() < 3 {
            return None;
        }

        let name = parts[0];
        let version = parts[1];
        let os_arch = parts[2..].join("-");

        let (os, arch) = if os_arch.contains("-") {
            let os_arch_parts: Vec<&str> = os_arch.splitn(2, '-').collect();
            (os_arch_parts[0], os_arch_parts[1])
        } else {
            ("unknown", os_arch.as_str())
        };

        Some(EngineInfo {
            name: name.to_string(),
            version: version.to_string(),
            url: String::new(),
            sha256: String::new(),
            arch: arch.to_string(),
            os: os.to_string(),
        })
    }

    pub fn download_engine(
        &self,
        name: &str,
        version: &str,
        url: &str,
        sha256: &str,
        reporter: &dyn capsule_core::CapsuleReporter,
    ) -> Result<PathBuf> {
        let output_path = self.engine_path(name, version);

        if output_path.exists() {
            futures::executor::block_on(
                reporter.notify(format!("✅ Engine {} v{} already installed", name, version)),
            )?;
            return Ok(output_path);
        }

        futures::executor::block_on(
            reporter.notify(format!("⬇️  Downloading {} v{}...", name, version)),
        )?;

        let temp_path = output_path.with_extension(".tmp");

        let response = reqwest::blocking::get(url)
            .with_context(|| format!("Failed to download from: {}", url))?;

        if !response.status().is_success() {
            anyhow::bail!("Download failed with status: {}", response.status());
        }

        let content = response
            .bytes()
            .with_context(|| "Failed to read response body")?;

        if !sha256.is_empty() {
            let actual_sha256 = sha2::Sha256::digest(&content)
                .as_slice()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();

            if actual_sha256 != sha256 {
                anyhow::bail!(
                    "SHA256 mismatch: expected {}, got {}",
                    sha256,
                    actual_sha256
                );
            }
        }

        fs::write(&temp_path, &content)
            .with_context(|| format!("Failed to write to: {}", temp_path.display()))?;

        fs::rename(&temp_path, &output_path)
            .with_context(|| format!("Failed to move to: {}", output_path.display()))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output_path, fs::Permissions::from_mode(0o755)).with_context(
                || {
                    format!(
                        "Failed to set executable permission on: {}",
                        output_path.display()
                    )
                },
            )?;
        }

        futures::executor::block_on(reporter.notify(format!(
            "✅ Installed {} v{} to {}",
            name,
            version,
            output_path.display()
        )))?;

        Ok(output_path)
    }

    pub fn remove_engine(&self, name: &str, version: &str) -> Result<bool> {
        let path = self.engine_path(name, version);

        if !path.exists() {
            return Ok(false);
        }

        fs::remove_file(&path).with_context(|| format!("Failed to remove: {}", path.display()))?;

        Ok(true)
    }

    pub fn get_default_engine(&self) -> Option<EngineInfo> {
        let config = capsule_core::config::load_config().ok()?;
        let default = config.default_engine?;

        let engines = self.list_engines().ok()?;
        engines.into_iter().find(|e| e.name == default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_engine_filename() {
        let em = EngineManager::new().unwrap();
        let info = em
            .parse_engine_filename("nacelle-v1.2.3-darwin-x64")
            .unwrap();
        assert_eq!(info.name, "nacelle");
        assert_eq!(info.version, "v1.2.3");
        assert_eq!(info.os, "darwin");
        assert_eq!(info.arch, "x64");
    }

    #[test]
    fn test_parse_engine_filename_linux_arm64() {
        let em = EngineManager::new().unwrap();
        let info = em
            .parse_engine_filename("nacelle-v2.0.0-linux-arm64")
            .unwrap();
        assert_eq!(info.name, "nacelle");
        assert_eq!(info.version, "v2.0.0");
        assert_eq!(info.os, "linux");
        assert_eq!(info.arch, "arm64");
    }

    #[test]
    fn test_parse_engine_filename_invalid() {
        let em = EngineManager::new().unwrap();
        let info = em.parse_engine_filename("invalid");
        assert!(info.is_none());
    }

    #[test]
    fn test_parse_engine_filename_too_short() {
        let em = EngineManager::new().unwrap();
        let info = em.parse_engine_filename("nacelle-v1");
        assert!(info.is_none());
    }

    #[test]
    fn test_engine_path() {
        let em = EngineManager::new().unwrap();
        let path = em.engine_path("nacelle", "v1.2.3");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("nacelle-v1.2.3"));
        assert!(path_str.contains(".ato/engines"));
    }

    #[test]
    fn test_engine_info_serialization() {
        let info = EngineInfo {
            name: "nacelle".to_string(),
            version: "v1.2.3".to_string(),
            url: "https://example.com/nacelle".to_string(),
            sha256: "abc123".to_string(),
            arch: "x64".to_string(),
            os: "darwin".to_string(),
        };

        let serialized = serde_json::to_string(&info).expect("Failed to serialize");
        let deserialized: EngineInfo =
            serde_json::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(info.name, deserialized.name);
        assert_eq!(info.version, deserialized.version);
        assert_eq!(info.url, deserialized.url);
        assert_eq!(info.sha256, deserialized.sha256);
        assert_eq!(info.arch, deserialized.arch);
        assert_eq!(info.os, deserialized.os);
    }

    #[test]
    fn test_engine_manifest_serialization() {
        let manifest = EngineManifest {
            engines: vec![
                EngineInfo {
                    name: "nacelle".to_string(),
                    version: "v1.0.0".to_string(),
                    url: "https://example.com/nacelle".to_string(),
                    sha256: "sha123".to_string(),
                    arch: "x64".to_string(),
                    os: "linux".to_string(),
                },
                EngineInfo {
                    name: "nacelle".to_string(),
                    version: "v1.1.0".to_string(),
                    url: "https://example.com/nacelle-v1.1.0".to_string(),
                    sha256: "sha456".to_string(),
                    arch: "arm64".to_string(),
                    os: "darwin".to_string(),
                },
            ],
        };

        let serialized = serde_json::to_string(&manifest).expect("Failed to serialize");
        let deserialized: EngineManifest =
            serde_json::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(deserialized.engines.len(), 2);
        assert_eq!(deserialized.engines[0].version, "v1.0.0");
        assert_eq!(deserialized.engines[1].version, "v1.1.0");
    }
}
