use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::common::paths::toolchain_cache_dir;
use crate::error::{CapsuleError, Result};
use crate::packers::runtime_fetcher::RuntimeFetcher;
use crate::reporter::CapsuleReporter;

const UV_VERSION: &str = "0.4.19";
const PNPM_VERSION: &str = "9.9.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapsuleLock {
    pub version: String,
    pub meta: LockMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowlist: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtimes: Option<RuntimeSection>,
    #[serde(default)]
    pub targets: HashMap<String, TargetEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockMeta {
    pub created_at: String,
    pub manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uv: Option<ToolTargets>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnpm: Option<ToolTargets>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolTargets {
    pub targets: HashMap<String, ToolArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolArtifact {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub python: Option<RuntimeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deno: Option<RuntimeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<RuntimeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub java: Option<RuntimeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dotnet: Option<RuntimeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEntry {
    pub provider: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub targets: HashMap<String, RuntimeArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeArtifact {
    pub url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub python_lockfile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_lockfile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraints: Option<TargetConstraints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compiled: Option<CompiledEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConstraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glibc: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledEntry {
    pub entrypoint: String,
    pub artifacts: CompiledArtifact,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledArtifact {
    pub url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactEntry {
    pub filename: String,
    pub url: String,
    pub sha256: String,
    #[serde(rename = "type")]
    pub artifact_type: String,
}

pub async fn generate_and_write_lockfile(
    manifest_path: &Path,
    manifest_raw: &toml::Value,
    manifest_text: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    let manifest_dir = manifest_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let lockfile = generate_lockfile(manifest_raw, manifest_text, &manifest_dir, reporter).await?;
    let output_path = manifest_dir.join("capsule.lock");
    let content = toml::to_string_pretty(&lockfile)
        .map_err(|e| CapsuleError::Pack(format!("Failed to serialize capsule.lock: {}", e)))?;
    std::fs::write(&output_path, content)
        .map_err(|e| CapsuleError::Pack(format!("Failed to write capsule.lock: {}", e)))?;
    Ok(output_path)
}

pub fn verify_lockfile_manifest(manifest_path: &Path, lockfile_path: &Path) -> Result<()> {
    let manifest_text = fs::read_to_string(manifest_path)
        .map_err(|e| CapsuleError::Config(format!("Failed to read manifest: {}", e)))?;
    let lockfile = read_lockfile(lockfile_path)?;
    let expected_hash = format!("sha256:{}", sha256_hex(manifest_text.as_bytes()));

    if lockfile.meta.manifest_hash != expected_hash {
        return Err(CapsuleError::Config(format!(
            "capsule.lock manifest hash mismatch (expected {}, got {})",
            expected_hash, lockfile.meta.manifest_hash
        )));
    }

    Ok(())
}

fn read_lockfile(path: &Path) -> Result<CapsuleLock> {
    let raw = fs::read_to_string(path)
        .map_err(|e| CapsuleError::Config(format!("Failed to read capsule.lock: {}", e)))?;
    toml::from_str(&raw)
        .map_err(|e| CapsuleError::Config(format!("Failed to parse capsule.lock: {}", e)))
}

async fn generate_lockfile(
    manifest_raw: &toml::Value,
    manifest_text: &str,
    manifest_dir: &Path,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<CapsuleLock> {
    let allowlist = read_allowlist(manifest_raw);
    let target_key = platform_target_key()?;
    let target_triple = platform_triple()?;
    let required_runtime_version = required_runtime_version(manifest_raw)?;
    let runtime_tools = read_runtime_tools(manifest_raw);

    let mut targets: HashMap<String, TargetEntry> = HashMap::new();
    let mut tools = ToolSection {
        uv: None,
        pnpm: None,
    };
    let mut runtimes = RuntimeSection {
        python: None,
        deno: None,
        node: None,
        java: None,
        dotnet: None,
    };

    let language = detect_language(manifest_raw);
    if let Some(lang) = language.as_deref() {
        if lang == "python" {
            let version = required_runtime_version
                .clone()
                .unwrap_or_else(|| read_language_version(manifest_raw, "python", "3.11"));
            let python_lockfile =
                generate_uv_lock(manifest_dir, manifest_raw, reporter.clone()).await?;
            let runtime =
                resolve_python_runtime(&version, &target_triple, reporter.clone()).await?;
            runtimes.python = Some(runtime);
            if python_lockfile.is_some() {
                let python_artifacts = match prepare_python_artifacts(
                    manifest_raw,
                    manifest_dir,
                    &target_key,
                    &version,
                    reporter.clone(),
                )
                .await
                {
                    Ok(artifacts) if !artifacts.is_empty() => Some(artifacts),
                    Ok(_) => None,
                    Err(err) => {
                        reporter
                            .warn(format!("⚠️  Failed to prefetch Python artifacts: {}", err))
                            .await?;
                        None
                    }
                };
                let target_entry = targets.entry(target_key.clone()).or_default();
                target_entry.python_lockfile = Some("uv.lock".to_string());
                if let Some(artifacts) = python_artifacts {
                    target_entry.artifacts.extend(artifacts);
                }
                let uv_url = format!(
                    "https://github.com/astral-sh/uv/releases/download/{0}/uv-{1}.tar.gz",
                    UV_VERSION, target_triple
                );
                let uv_sha256 =
                    resolve_url_sha256(&(uv_url.clone() + ".sha256"), reporter.clone()).await?;
                tools.uv = Some(tool_targets_for(
                    uv_url,
                    UV_VERSION,
                    &target_triple,
                    Some(uv_sha256),
                ));
            }
        } else if lang == "node" {
            let version = required_runtime_version
                .clone()
                .unwrap_or_else(|| read_language_version(manifest_raw, "node", "20"));
            let node_lockfile =
                generate_pnpm_lock(manifest_dir, manifest_raw, &version, reporter.clone()).await?;
            let runtime = resolve_node_runtime(&version, &target_triple, reporter.clone()).await?;
            runtimes.node = Some(runtime);
            if runtimes.deno.is_none() {
                let deno_version = read_language_version(manifest_raw, "deno", "1.46.3");
                let deno_runtime =
                    resolve_deno_runtime(&deno_version, &target_triple, reporter.clone()).await?;
                runtimes.deno = Some(deno_runtime);
            }
            if node_lockfile.is_some() {
                let node_artifacts = match prepare_node_artifacts(
                    manifest_raw,
                    manifest_dir,
                    &target_key,
                    &version,
                    reporter.clone(),
                )
                .await
                {
                    Ok(artifacts) if !artifacts.is_empty() => Some(artifacts),
                    Ok(_) => None,
                    Err(err) => {
                        reporter
                            .warn(format!("⚠️  Failed to prefetch Node artifacts: {}", err))
                            .await?;
                        None
                    }
                };
                let target_entry = targets.entry(target_key.clone()).or_default();
                target_entry.node_lockfile = Some(format!("locks/{}/pnpm-lock.yaml", target_key));
                if let Some(artifacts) = node_artifacts {
                    target_entry.artifacts.extend(artifacts);
                }
                tools.pnpm = Some(tool_targets_for(
                    format!(
                        "https://registry.npmjs.org/pnpm/-/pnpm-{}.tgz",
                        PNPM_VERSION
                    ),
                    PNPM_VERSION,
                    &target_triple,
                    None,
                ));
            }
        } else if lang == "deno" {
            let version = required_runtime_version
                .clone()
                .unwrap_or_else(|| read_language_version(manifest_raw, "deno", "1.46.3"));
            let runtime = resolve_deno_runtime(&version, &target_triple, reporter.clone()).await?;
            runtimes.deno = Some(runtime);

            if let Some(node_version) = runtime_tools.get("node") {
                let runtime =
                    resolve_node_runtime(node_version, &target_triple, reporter.clone()).await?;
                runtimes.node = Some(runtime);
            }
            if let Some(python_version) = runtime_tools.get("python") {
                let runtime =
                    resolve_python_runtime(python_version, &target_triple, reporter.clone())
                        .await?;
                runtimes.python = Some(runtime);

                let uv_url = format!(
                    "https://github.com/astral-sh/uv/releases/download/{0}/uv-{1}.tar.gz",
                    UV_VERSION, target_triple
                );
                let uv_sha256 =
                    resolve_url_sha256(&(uv_url.clone() + ".sha256"), reporter.clone()).await?;
                tools.uv = Some(tool_targets_for(
                    uv_url,
                    UV_VERSION,
                    &target_triple,
                    Some(uv_sha256),
                ));
            }

            // runtime=web/static は静的配信用途であり、Deno runtime 自体は必要だが
            // プロジェクト依存の deno.lock 生成は不要（かつ monorepo で誤検出しやすい）。
            let is_web_static = selected_target_runtime(manifest_raw).as_deref() == Some("web")
                && selected_target_driver(manifest_raw).as_deref() == Some("static");
            if !is_web_static {
                let _ = generate_deno_lock(manifest_dir, manifest_raw, &version, reporter.clone())
                    .await?;
            }
        }
    }

    let tools = if tools.uv.is_none() && tools.pnpm.is_none() {
        detect_tools(&target_triple)
    } else {
        Some(tools)
    };

    Ok(CapsuleLock {
        version: "1".to_string(),
        meta: LockMeta {
            created_at: Utc::now().to_rfc3339(),
            manifest_hash: format!("sha256:{}", sha256_hex(manifest_text.as_bytes())),
        },
        allowlist,
        tools,
        runtimes: if runtimes.python.is_none() && runtimes.node.is_none() && runtimes.deno.is_none()
        {
            None
        } else {
            Some(runtimes)
        },
        targets,
    })
}

async fn generate_uv_lock(
    manifest_dir: &Path,
    manifest: &toml::Value,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<Option<PathBuf>> {
    let deps_path = read_dependencies_path(manifest, "python", manifest_dir)
        .or_else(|| {
            let candidate = manifest_dir.join("pyproject.toml");
            if candidate.exists() {
                Some(candidate)
            } else {
                None
            }
        })
        .or_else(|| {
            let candidate = manifest_dir.join("requirements.txt");
            if candidate.exists() {
                Some(candidate)
            } else {
                None
            }
        });
    let Some(deps_path) = deps_path else {
        return Ok(None);
    };

    let uv_path = ensure_uv(reporter.clone()).await?;
    reporter
        .notify("⚙️  Generating uv.lock".to_string())
        .await?;

    let status = if deps_path.file_name().is_some_and(|n| n == "pyproject.toml") {
        run_command(
            &uv_path,
            &["lock"],
            deps_path.parent().unwrap_or(manifest_dir),
        )
        .await?
    } else if deps_path.extension().and_then(|e| e.to_str()) == Some("txt") {
        let dep_string = deps_path.to_string_lossy().to_string();
        run_command(
            &uv_path,
            &["pip", "compile", dep_string.as_str(), "-o", "uv.lock"],
            manifest_dir,
        )
        .await?
    } else {
        run_command(&uv_path, &["lock"], manifest_dir).await?
    };

    if !status.success() {
        return Err(CapsuleError::Pack("uv lock failed".to_string()));
    }

    let lock_path = manifest_dir.join("uv.lock");
    if lock_path.exists() {
        Ok(Some(lock_path))
    } else {
        Ok(None)
    }
}

async fn generate_pnpm_lock(
    manifest_dir: &Path,
    manifest: &toml::Value,
    node_version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<Option<PathBuf>> {
    // npm プロジェクト（package-lock.json）では pnpm lock 生成を強制しない。
    // source/node 実行側は package-lock.json を Tier1 要件として扱うため、
    // ここでの pnpm 固定生成は不要かつ実運用で失敗要因になる。
    if manifest_dir.join("package-lock.json").exists() {
        reporter
            .notify(
                "ℹ️  package-lock.json detected; skipping pnpm-lock.yaml generation".to_string(),
            )
            .await?;
        return Ok(None);
    }

    let deps_path = read_dependencies_path(manifest, "node", manifest_dir).or_else(|| {
        let candidate = manifest_dir.join("package.json");
        if candidate.exists() {
            Some(candidate)
        } else {
            None
        }
    });
    let Some(_) = deps_path else {
        return Ok(None);
    };

    let node_path = ensure_node(node_version, reporter.clone()).await?;
    let pnpm_cmd = ensure_pnpm(&node_path, reporter.clone()).await?;

    reporter
        .notify("⚙️  Generating pnpm-lock.yaml".to_string())
        .await?;

    let mut cmd = std::process::Command::new(&pnpm_cmd.program);
    cmd.args(&pnpm_cmd.args_prefix)
        .args(["install", "--lockfile-only", "--ignore-scripts", "--silent"])
        .current_dir(manifest_dir);
    let status = run_command_inner(cmd).await?;
    if !status.success() {
        return Err(CapsuleError::Pack(
            "pnpm lock generation failed".to_string(),
        ));
    }

    let lock_path = manifest_dir.join("pnpm-lock.yaml");
    if !lock_path.exists() {
        return Ok(None);
    }
    let target_dir = manifest_dir.join("locks").join(platform_target_key()?);
    std::fs::create_dir_all(&target_dir)
        .map_err(|e| CapsuleError::Pack(format!("Failed to create locks directory: {}", e)))?;
    let target_lock = target_dir.join("pnpm-lock.yaml");
    std::fs::copy(&lock_path, &target_lock)
        .map_err(|e| CapsuleError::Pack(format!("Failed to copy pnpm-lock.yaml: {}", e)))?;
    Ok(Some(target_lock))
}

async fn generate_deno_lock(
    manifest_dir: &Path,
    manifest: &toml::Value,
    deno_version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<Option<PathBuf>> {
    let entrypoint = read_target_entrypoint(manifest).or_else(|| {
        manifest
            .get("execution")
            .and_then(|e| e.get("entrypoint"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    });
    let Some(entrypoint) = entrypoint else {
        return Ok(None);
    };

    let entrypoint_path = manifest_dir.join(&entrypoint);
    if !entrypoint_path.exists() {
        return Ok(None);
    }
    if entrypoint_path.is_dir() {
        // runtime=web/static など、ディレクトリを payload ルートにするケースでは
        // deno cache 対象が曖昧になり不要な解決失敗を引き起こすため lock 生成を行わない。
        return Ok(None);
    }

    reporter
        .notify("⚙️  Generating deno.lock".to_string())
        .await?;

    let deno_path = ensure_deno(deno_version, reporter.clone()).await?;
    let mut cmd = std::process::Command::new(&deno_path);
    cmd.args([
        "cache",
        entrypoint.as_str(),
        "--lock=deno.lock",
        "--lock-write",
    ])
    .current_dir(manifest_dir);

    let status = run_command_inner(cmd).await?;
    if !status.success() {
        return Err(CapsuleError::Pack(
            "deno lock generation failed".to_string(),
        ));
    }

    let lock_path = manifest_dir.join("deno.lock");
    if lock_path.exists() {
        Ok(Some(lock_path))
    } else {
        Ok(None)
    }
}

async fn prepare_python_artifacts(
    _manifest: &toml::Value,
    manifest_dir: &Path,
    target_key: &str,
    python_version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<Vec<ArtifactEntry>> {
    let lock_path = manifest_dir.join("uv.lock");
    if !lock_path.exists() {
        return Ok(Vec::new());
    }

    let uv_path = ensure_uv(reporter.clone()).await?;
    let fetcher = RuntimeFetcher::new_with_reporter(reporter.clone())?;
    let python_path = fetcher.ensure_python(python_version).await?;
    reporter
        .notify("⬇️  Prefetching Python cache".to_string())
        .await?;

    let cache_dir = artifact_root(manifest_dir, target_key).join("uv-cache");
    reset_dir(&cache_dir)?;
    let install_dir = artifact_root(manifest_dir, target_key).join("uv-install");
    reset_dir(&install_dir)?;

    let mut cmd = std::process::Command::new(&uv_path);
    cmd.args([
        "pip",
        "sync",
        lock_path.to_string_lossy().as_ref(),
        "--python",
        python_path.to_string_lossy().as_ref(),
        "--cache-dir",
        cache_dir.to_string_lossy().as_ref(),
        "--target",
        install_dir.to_string_lossy().as_ref(),
    ])
    .current_dir(manifest_dir);

    let status = run_command_inner(cmd).await?;
    if !status.success() {
        return Err(CapsuleError::Pack("uv pip sync failed".to_string()));
    }

    if install_dir.exists() {
        std::fs::remove_dir_all(&install_dir)?;
    }

    let cache_hash = sha256_dir(&cache_dir)?;
    Ok(vec![ArtifactEntry {
        filename: "uv-cache".to_string(),
        url: "https://files.pythonhosted.org/".to_string(),
        sha256: cache_hash,
        artifact_type: "uv-cache".to_string(),
    }])
}

async fn prepare_node_artifacts(
    manifest: &toml::Value,
    manifest_dir: &Path,
    target_key: &str,
    node_version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<Vec<ArtifactEntry>> {
    let deps_path = read_dependencies_path(manifest, "node", manifest_dir).or_else(|| {
        let candidate = manifest_dir.join("package.json");
        if candidate.exists() {
            Some(candidate)
        } else {
            None
        }
    });
    let Some(_) = deps_path else {
        return Ok(Vec::new());
    };
    let lock_path = manifest_dir.join("pnpm-lock.yaml");
    if !lock_path.exists() {
        return Ok(Vec::new());
    }

    let node_path = ensure_node(node_version, reporter.clone()).await?;
    let pnpm_cmd = ensure_pnpm(&node_path, reporter.clone()).await?;
    reporter
        .notify("⬇️  Fetching pnpm store".to_string())
        .await?;

    let store_dir = artifact_root(manifest_dir, target_key).join("pnpm-store");
    reset_dir(&store_dir)?;

    let temp_dir = TempDir::new()
        .map_err(|e| CapsuleError::Pack(format!("Failed to create pnpm temp dir: {}", e)))?;
    let temp_path = temp_dir.path();
    if let Some(path) = deps_path.as_ref() {
        let dest = temp_path.join(path.file_name().unwrap_or_else(|| path.as_os_str()));
        std::fs::copy(path, &dest)
            .map_err(|e| CapsuleError::Pack(format!("Failed to copy {}: {}", path.display(), e)))?;
    }
    let temp_lock = temp_path.join("pnpm-lock.yaml");
    std::fs::copy(&lock_path, &temp_lock)
        .map_err(|e| CapsuleError::Pack(format!("Failed to copy pnpm-lock.yaml: {}", e)))?;

    let mut cmd = std::process::Command::new(&pnpm_cmd.program);
    cmd.args(&pnpm_cmd.args_prefix)
        .args([
            "fetch",
            "--ignore-scripts",
            "--silent",
            "--store-dir",
            store_dir.to_string_lossy().as_ref(),
        ])
        .current_dir(temp_path);
    let status = run_command_inner(cmd).await?;
    if !status.success() {
        return Err(CapsuleError::Pack("pnpm fetch failed".to_string()));
    }

    let store_hash = sha256_dir(&store_dir)?;
    Ok(vec![ArtifactEntry {
        filename: "pnpm-store".to_string(),
        url: "https://registry.npmjs.org/".to_string(),
        sha256: store_hash,
        artifact_type: "pnpm-store".to_string(),
    }])
}

struct PnpmCommand {
    program: PathBuf,
    args_prefix: Vec<String>,
}

async fn ensure_uv(reporter: Arc<dyn CapsuleReporter + 'static>) -> Result<PathBuf> {
    if let Ok(found) = which::which("uv") {
        return Ok(found);
    }

    let version = UV_VERSION;
    reporter
        .notify(format!("⬇️  Downloading uv {}", version))
        .await?;
    let target_triple = platform_triple()?;
    let tools_dir = toolchain_cache_dir()?
        .join("tools")
        .join("uv")
        .join(version);
    std::fs::create_dir_all(&tools_dir)
        .map_err(|e| CapsuleError::Pack(format!("Failed to create uv tools directory: {}", e)))?;
    let archive_path = tools_dir.join(format!("uv-{}.tar.gz", target_triple));
    let url = format!(
        "https://github.com/astral-sh/uv/releases/download/{}/uv-{}.tar.gz",
        version, target_triple
    );
    download_file(&url, &archive_path).await?;
    extract_tgz(&archive_path, &tools_dir)?;
    let uv_bin = find_binary_recursive(&tools_dir, &["uv", "uv.exe"])
        .ok_or_else(|| CapsuleError::Pack("uv binary not found after extraction".to_string()))?;
    Ok(uv_bin)
}

async fn ensure_node(
    version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    if let Ok(found) = which::which("node") {
        return Ok(found);
    }
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    fetcher.ensure_node(version).await
}

async fn ensure_deno(
    version: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    fetcher.ensure_deno(version).await
}

async fn ensure_pnpm(
    node_path: &Path,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<PnpmCommand> {
    if let Ok(found) = which::which("pnpm") {
        return Ok(PnpmCommand {
            program: found,
            args_prefix: Vec::new(),
        });
    }

    let version = PNPM_VERSION;
    reporter
        .notify(format!("⬇️  Downloading pnpm {}", version))
        .await?;
    let tools_dir = toolchain_cache_dir()?
        .join("tools")
        .join("pnpm")
        .join(version);
    std::fs::create_dir_all(&tools_dir)
        .map_err(|e| CapsuleError::Pack(format!("Failed to create pnpm tools directory: {}", e)))?;
    let archive_path = tools_dir.join(format!("pnpm-{}.tgz", version));
    let url = format!("https://registry.npmjs.org/pnpm/-/pnpm-{}.tgz", version);
    download_file(&url, &archive_path).await?;
    extract_tgz(&archive_path, &tools_dir)?;

    let script = tools_dir.join("package").join("bin").join("pnpm.cjs");
    if !script.exists() {
        return Err(CapsuleError::Pack(
            "pnpm.cjs not found after extraction".to_string(),
        ));
    }

    Ok(PnpmCommand {
        program: node_path.to_path_buf(),
        args_prefix: vec![script.to_string_lossy().to_string()],
    })
}

async fn download_file(url: &str, dest: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(CapsuleError::Network)?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(CapsuleError::Network)?;
    if !response.status().is_success() {
        return Err(CapsuleError::Network(
            response.error_for_status().unwrap_err(),
        ));
    }
    let bytes = response.bytes().await.map_err(CapsuleError::Network)?;
    std::fs::write(dest, &bytes)
        .map_err(|e| CapsuleError::Pack(format!("Failed to write {}: {}", url, e)))?;
    Ok(())
}

fn extract_tgz(archive_path: &Path, dest: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    use tar::Archive;

    let file = std::fs::File::open(archive_path)
        .map_err(|e| CapsuleError::Pack(format!("Failed to open archive: {}", e)))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    archive
        .unpack(dest)
        .map_err(|e| CapsuleError::Pack(format!("Failed to extract archive: {}", e)))?;
    Ok(())
}

fn find_binary_recursive(root: &Path, candidates: &[&str]) -> Option<PathBuf> {
    for entry in walkdir::WalkDir::new(root).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if candidates.iter().any(|c| *c == name) {
            return Some(entry.path().to_path_buf());
        }
    }
    None
}

async fn run_command(
    program: &Path,
    args: &[&str],
    cwd: &Path,
) -> Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args).current_dir(cwd);
    run_command_inner(cmd).await
}

async fn run_command_inner(mut cmd: std::process::Command) -> Result<std::process::ExitStatus> {
    tokio::task::spawn_blocking(move || cmd.status())
        .await
        .map_err(|e| CapsuleError::Pack(format!("Failed to run command: {}", e)))?
        .map_err(|e| CapsuleError::Pack(format!("Failed to run command: {}", e)))
}

async fn resolve_python_runtime(
    version: &str,
    target_triple: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<RuntimeEntry> {
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    let (os, arch) = RuntimeFetcher::detect_platform()?;
    let url = RuntimeFetcher::get_python_download_url(version, &os, &arch)?;
    let sha256 = resolve_python_sha256(&fetcher, &url).await?;
    let mut targets = HashMap::new();
    targets.insert(target_triple.to_string(), RuntimeArtifact { url, sha256 });
    Ok(RuntimeEntry {
        provider: "python-build-standalone".to_string(),
        version: version.to_string(),
        targets,
    })
}

async fn resolve_python_sha256(fetcher: &RuntimeFetcher, artifact_url: &str) -> Result<String> {
    let mut candidates: Vec<(String, Option<String>)> = vec![
        (format!("{}.sha256", artifact_url), None),
        (format!("{}.sha256sum", artifact_url), None),
    ];

    if let Some((release_base, filename)) = split_release_base_and_filename(artifact_url) {
        candidates.push((format!("{release_base}/SHA256SUMS"), Some(filename.clone())));
        candidates.push((format!("{release_base}/SHA256SUMS.txt"), Some(filename)));
    }

    let mut last_not_found = None;
    for (checksum_url, hint) in candidates {
        match fetcher
            .fetch_expected_sha256(&checksum_url, hint.as_deref())
            .await
        {
            Ok(sum) => return Ok(sum),
            Err(CapsuleError::NotFound(_)) => {
                last_not_found = Some(checksum_url);
            }
            Err(err) => return Err(err),
        }
    }

    match download_and_sha256(artifact_url).await {
        Ok(sum) => Ok(sum),
        Err(_) => Err(CapsuleError::NotFound(
            last_not_found.unwrap_or_else(|| artifact_url.to_string()),
        )),
    }
}

async fn resolve_node_runtime(
    version: &str,
    target_triple: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<RuntimeEntry> {
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    let (os, arch) = RuntimeFetcher::detect_platform()?;
    let full_version = RuntimeFetcher::resolve_node_full_version(version).await?;
    let (filename, _is_zip) = RuntimeFetcher::node_artifact_filename(&full_version, &os, &arch)?;
    let url = format!("https://nodejs.org/dist/v{}/{}", full_version, filename);
    let sha256 = fetcher
        .fetch_expected_sha256(
            &format!("https://nodejs.org/dist/v{}/SHASUMS256.txt", full_version),
            Some(&filename),
        )
        .await?;
    let mut targets = HashMap::new();
    targets.insert(target_triple.to_string(), RuntimeArtifact { url, sha256 });
    Ok(RuntimeEntry {
        provider: "official".to_string(),
        version: full_version,
        targets,
    })
}

async fn resolve_deno_runtime(
    version: &str,
    target_triple: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<RuntimeEntry> {
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    let (os, arch) = RuntimeFetcher::detect_platform()?;
    let filename = deno_artifact_filename(&os, &arch)?;
    let url = format!(
        "https://github.com/denoland/deno/releases/download/v{}/{}",
        version, filename
    );
    let sha256 = resolve_deno_sha256(&fetcher, version, &filename).await?;
    let mut targets = HashMap::new();
    targets.insert(target_triple.to_string(), RuntimeArtifact { url, sha256 });
    Ok(RuntimeEntry {
        provider: "official".to_string(),
        version: version.to_string(),
        targets,
    })
}

fn deno_artifact_filename(os: &str, arch: &str) -> Result<String> {
    let target = match (os, arch) {
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        ("windows", "aarch64") => "aarch64-pc-windows-msvc",
        _ => {
            return Err(CapsuleError::Pack(format!(
                "Unsupported Deno platform: {} {}",
                os, arch
            )))
        }
    };
    Ok(format!("deno-{}.zip", target))
}

async fn resolve_deno_sha256(
    fetcher: &RuntimeFetcher,
    version: &str,
    filename: &str,
) -> Result<String> {
    let candidates = [
        (
            format!(
                "https://github.com/denoland/deno/releases/download/v{}/{}.sha256sum",
                version, filename
            ),
            None,
        ),
        (
            format!(
                "https://github.com/denoland/deno/releases/download/v{}/{}.sha256",
                version, filename
            ),
            None,
        ),
        (
            format!(
                "https://github.com/denoland/deno/releases/download/v{}/SHASUMS256.txt",
                version
            ),
            Some(filename),
        ),
    ];

    let mut last_not_found = None;
    for (checksum_url, hint) in candidates {
        match fetcher.fetch_expected_sha256(&checksum_url, hint).await {
            Ok(sum) => return Ok(sum),
            Err(CapsuleError::NotFound(_)) => {
                last_not_found = Some(checksum_url);
            }
            Err(err) => return Err(err),
        }
    }

    let artifact_url = format!(
        "https://github.com/denoland/deno/releases/download/v{}/{}",
        version, filename
    );
    match download_and_sha256(&artifact_url).await {
        Ok(sum) => Ok(sum),
        Err(_) => {
            let detail = last_not_found.unwrap_or_else(|| "Deno checksum".to_string());
            Err(CapsuleError::NotFound(detail))
        }
    }
}

fn split_release_base_and_filename(url: &str) -> Option<(String, String)> {
    let idx = url.rfind('/')?;
    let base = url[..idx].to_string();
    let filename = url[idx + 1..].to_string();
    if filename.is_empty() {
        None
    } else {
        Some((base, filename))
    }
}

async fn download_and_sha256(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(CapsuleError::Network)?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(CapsuleError::Network)?;
    if !response.status().is_success() {
        return Err(CapsuleError::NotFound(url.to_string()));
    }
    let bytes = response.bytes().await.map_err(CapsuleError::Network)?;
    Ok(sha256_hex(&bytes))
}

fn detect_tools(target_triple: &str) -> Option<ToolSection> {
    let uv = detect_tool("uv").map(|version| ToolTargets {
        targets: [(
            target_triple.to_string(),
            ToolArtifact {
                url: format!(
                    "https://github.com/astral-sh/uv/releases/download/{0}/uv-{1}.tar.gz",
                    version, target_triple
                ),
                sha256: None,
                version: Some(version),
            },
        )]
        .into_iter()
        .collect(),
    });

    let pnpm = detect_tool("pnpm").map(|version| ToolTargets {
        targets: [(
            target_triple.to_string(),
            ToolArtifact {
                url: format!("https://registry.npmjs.org/pnpm/-/pnpm-{}.tgz", version),
                sha256: None,
                version: Some(version),
            },
        )]
        .into_iter()
        .collect(),
    });

    if uv.is_none() && pnpm.is_none() {
        None
    } else {
        Some(ToolSection { uv, pnpm })
    }
}

fn tool_targets_for(
    url: String,
    version: &str,
    target_triple: &str,
    sha256: Option<String>,
) -> ToolTargets {
    let mut targets = HashMap::new();
    targets.insert(
        target_triple.to_string(),
        ToolArtifact {
            url,
            sha256,
            version: Some(version.to_string()),
        },
    );
    ToolTargets { targets }
}

async fn resolve_url_sha256(
    checksum_url: &str,
    reporter: Arc<dyn CapsuleReporter + 'static>,
) -> Result<String> {
    let fetcher = RuntimeFetcher::new_with_reporter(reporter)?;
    fetcher.fetch_expected_sha256(checksum_url, None).await
}

fn detect_tool(cmd: &str) -> Option<String> {
    let exe = which::which(cmd).ok()?;
    let output = std::process::Command::new(exe)
        .arg("--version")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.split_whitespace().find_map(|token| {
        if token.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            Some(token.trim().to_string())
        } else {
            None
        }
    })
}

fn read_allowlist(manifest: &toml::Value) -> Option<Vec<String>> {
    manifest
        .get("runtime")
        .and_then(|v| v.get("allowlist"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .filter(|list| !list.is_empty())
}

fn read_dependencies_path(
    manifest: &toml::Value,
    language: &str,
    manifest_dir: &Path,
) -> Option<PathBuf> {
    let from_targets = selected_target_table(manifest)
        .and_then(|t| t.get("dependencies"))
        .and_then(|v| v.as_str())
        .map(|s| manifest_dir.join(s));
    if from_targets.as_ref().is_some_and(|p| p.exists()) {
        return from_targets;
    }

    let from_language = manifest
        .get("language")
        .and_then(|v| v.get(language))
        .and_then(|v| v.get("manifest"))
        .and_then(|v| v.as_str())
        .map(|s| manifest_dir.join(s));
    if from_language.as_ref().is_some_and(|p| p.exists()) {
        return from_language;
    }

    None
}

fn detect_language(manifest: &toml::Value) -> Option<String> {
    if let Some(driver) = selected_target_table(manifest)
        .and_then(|t| t.get("driver"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase())
    {
        if matches!(driver.as_str(), "python" | "node" | "deno") {
            return Some(driver);
        }
    }

    if selected_target_runtime(manifest)
        .map(|r| r == "web")
        .unwrap_or(false)
        && selected_target_driver(manifest)
            .map(|d| d == "static")
            .unwrap_or(false)
    {
        return Some("deno".to_string());
    }

    if manifest
        .get("language")
        .and_then(|v| v.get("python"))
        .is_some()
    {
        return Some("python".to_string());
    }
    if manifest
        .get("language")
        .and_then(|v| v.get("node"))
        .is_some()
    {
        return Some("node".to_string());
    }

    let target_lang = selected_target_table(manifest)
        .and_then(|t| t.get("language"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if target_lang.is_some() {
        return target_lang;
    }

    let entrypoint = manifest
        .get("execution")
        .and_then(|e| e.get("entrypoint"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let ext = Path::new(entrypoint)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext == "py" {
        return Some("python".to_string());
    }
    if matches!(ext.as_str(), "js" | "mjs" | "cjs" | "ts") {
        return Some("node".to_string());
    }
    None
}

fn read_language_version(manifest: &toml::Value, language: &str, fallback: &str) -> String {
    let version = manifest
        .get("language")
        .and_then(|v| v.get(language))
        .and_then(|v| v.get("version"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            selected_target_table(manifest)
                .and_then(|t| t.get("version"))
                .and_then(|v| v.as_str())
        })
        .map(|s| s.to_string());

    version.unwrap_or_else(|| fallback.to_string())
}

fn read_runtime_version(manifest: &toml::Value) -> Option<String> {
    selected_target_table(manifest)
        .and_then(|t| t.get("runtime_version"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn read_runtime_tools(manifest: &toml::Value) -> HashMap<String, String> {
    let mut tools = HashMap::new();
    let Some(table) = selected_target_table(manifest)
        .and_then(|t| t.get("runtime_tools"))
        .and_then(|v| v.as_table())
    else {
        return tools;
    };

    for (key, value) in table {
        let Some(raw) = value.as_str() else {
            continue;
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        tools.insert(key.to_ascii_lowercase(), trimmed.to_string());
    }
    tools
}

fn selected_target_runtime(manifest: &toml::Value) -> Option<String> {
    selected_target_table(manifest)
        .and_then(|t| t.get("runtime"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

fn selected_target_driver(manifest: &toml::Value) -> Option<String> {
    selected_target_table(manifest)
        .and_then(|t| t.get("driver"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

fn required_runtime_version(manifest: &toml::Value) -> Result<Option<String>> {
    let runtime = selected_target_runtime(manifest);
    let driver = selected_target_driver(manifest);
    let requires_source = runtime.as_deref() == Some("source")
        && matches!(
            driver.as_deref(),
            Some("python") | Some("node") | Some("deno")
        );
    let requires_web_deno = runtime.as_deref() == Some("web") && driver.as_deref() == Some("deno");
    let requires = requires_source || requires_web_deno;
    if !requires {
        return Ok(None);
    }

    read_runtime_version(manifest).map(Some).ok_or_else(|| {
        CapsuleError::Config(
            "targets.<default_target>.runtime_version is required for source driver deno/node/python and web driver deno".to_string(),
        )
    })
}

fn selected_target_table<'a>(manifest: &'a toml::Value) -> Option<&'a toml::Value> {
    let targets = manifest.get("targets")?;
    let default_target = manifest
        .get("default_target")
        .and_then(|v| v.as_str())
        .unwrap_or("source");

    targets
        .get(default_target)
        .or_else(|| targets.get("source"))
}

fn read_target_entrypoint(manifest: &toml::Value) -> Option<String> {
    selected_target_table(manifest)
        .and_then(|t| t.get("entrypoint"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn platform_target_key() -> Result<String> {
    let os = if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        return Err(CapsuleError::Pack("Unsupported OS".to_string()));
    };
    let arch = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        return Err(CapsuleError::Pack("Unsupported architecture".to_string()));
    };
    Ok(format!("{}-{}", os, arch))
}

fn platform_triple() -> Result<String> {
    let os = if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        return Err(CapsuleError::Pack("Unsupported OS".to_string()));
    };
    let arch = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else {
        return Err(CapsuleError::Pack("Unsupported architecture".to_string()));
    };

    let triple = match (os, arch) {
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        ("windows", "aarch64") => "aarch64-pc-windows-msvc",
        _ => {
            return Err(CapsuleError::Pack(format!(
                "Unsupported platform: {} {}",
                os, arch
            )))
        }
    };

    Ok(triple.to_string())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    hex::encode(digest)
}

fn sha256_dir(root: &Path) -> Result<String> {
    let mut entries = Vec::new();
    for entry in walkdir::WalkDir::new(root).into_iter().flatten() {
        if entry.file_type().is_file() {
            entries.push(entry.path().to_path_buf());
        }
    }
    entries.sort();

    let mut hasher = Sha256::new();
    for path in entries {
        let rel = path.strip_prefix(root).unwrap_or(&path);
        hasher.update(rel.to_string_lossy().as_bytes());
        hasher.update([0]);
        let bytes = std::fs::read(&path)?;
        hasher.update(bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn artifact_root(manifest_dir: &Path, target_key: &str) -> PathBuf {
    manifest_dir.join("artifacts").join(target_key)
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    std::fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_lockfile_with_allowlist() {
        let lockfile = CapsuleLock {
            version: "1".to_string(),
            meta: LockMeta {
                created_at: "2026-01-20T00:00:00Z".to_string(),
                manifest_hash: "sha256:deadbeef".to_string(),
            },
            allowlist: Some(vec!["nodejs.org".to_string()]),
            tools: None,
            runtimes: None,
            targets: HashMap::new(),
        };

        let toml = toml::to_string(&lockfile).unwrap();
        let parsed: CapsuleLock = toml::from_str(&toml).unwrap();
        assert_eq!(parsed.allowlist.unwrap()[0], "nodejs.org");
    }

    #[test]
    fn verify_lockfile_manifest_hash() {
        let temp = TempDir::new().unwrap();
        let manifest_path = temp.path().join("capsule.toml");
        let lockfile_path = temp.path().join("capsule.lock");
        let manifest_text = "name = \"demo\"";
        fs::write(&manifest_path, manifest_text).unwrap();

        let lockfile = CapsuleLock {
            version: "1".to_string(),
            meta: LockMeta {
                created_at: "2026-01-20T00:00:00Z".to_string(),
                manifest_hash: format!("sha256:{}", sha256_hex(manifest_text.as_bytes())),
            },
            allowlist: None,
            tools: None,
            runtimes: None,
            targets: HashMap::new(),
        };

        let toml = toml::to_string(&lockfile).unwrap();
        fs::write(&lockfile_path, toml).unwrap();

        verify_lockfile_manifest(&manifest_path, &lockfile_path).unwrap();
    }

    #[test]
    fn deno_artifact_filename_uses_release_target_triplets() {
        assert_eq!(
            deno_artifact_filename("macos", "aarch64").unwrap(),
            "deno-aarch64-apple-darwin.zip"
        );
        assert_eq!(
            deno_artifact_filename("linux", "x86_64").unwrap(),
            "deno-x86_64-unknown-linux-gnu.zip"
        );
        assert_eq!(
            deno_artifact_filename("windows", "x86_64").unwrap(),
            "deno-x86_64-pc-windows-msvc.zip"
        );
    }

    #[test]
    fn runtime_tools_are_read_from_selected_target() {
        let manifest: toml::Value = toml::from_str(
            r#"
default_target = "default"
[targets.default]
runtime = "web"
driver = "deno"
runtime_tools = { node = "20.11.0", python = "3.11.7" }
"#,
        )
        .unwrap();

        let tools = read_runtime_tools(&manifest);
        assert_eq!(tools.get("node"), Some(&"20.11.0".to_string()));
        assert_eq!(tools.get("python"), Some(&"3.11.7".to_string()));
    }

    #[test]
    fn required_runtime_version_for_web_deno_target() {
        let manifest: toml::Value = toml::from_str(
            r#"
default_target = "default"
[targets.default]
runtime = "web"
driver = "deno"
runtime_version = "1.46.3"
"#,
        )
        .unwrap();

        let version = required_runtime_version(&manifest).unwrap();
        assert_eq!(version.as_deref(), Some("1.46.3"));
    }
}
