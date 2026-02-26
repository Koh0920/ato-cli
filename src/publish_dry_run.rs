use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct PublishDryRunArgs {
    pub json_output: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishDryRunResult {
    pub capsule_name: String,
    pub version: String,
    pub artifact_path: PathBuf,
    pub artifact_size_bytes: u64,
    pub git: GitCheckResult,
    pub ci_workflow: CiWorkflowCheckResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct GitCheckResult {
    pub inside_work_tree: bool,
    pub origin: Option<String>,
    pub manifest_repository: Option<String>,
    pub repository_match: Option<bool>,
    pub dirty: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CiWorkflowCheckResult {
    pub path: String,
    pub exists: bool,
    pub has_oidc_permission: bool,
    pub has_tag_trigger: bool,
    pub has_checksum_verification: bool,
}

pub async fn execute(args: PublishDryRunArgs) -> Result<PublishDryRunResult> {
    let cwd = std::env::current_dir().context("Failed to resolve current directory")?;
    let manifest_path = cwd.join("capsule.toml");
    let manifest_raw = fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;
    let manifest = capsule_core::types::capsule_v1::CapsuleManifestV1::from_toml(&manifest_raw)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml: {}", err))?;
    let manifest_repo = find_manifest_repository(&manifest_raw);
    let git = run_git_checks(manifest_repo.as_deref())?;
    let ci_workflow = validate_ci_workflow(&cwd)?;

    if !args.json_output {
        eprintln!("🔍 Validating capsule.toml... OK");
        eprintln!();
        eprintln!("🐙 Performing Repository Checks...");
        eprintln!("   ✔ Git repository detected.");
        if let Some(origin) = &git.origin {
            eprintln!("   ✔ Origin: {}", origin);
        }
        if let Some(true) = git.repository_match {
            if let Some(repo) = &git.manifest_repository {
                eprintln!("   ✔ Remote origin matches '{}'.", repo);
            }
        } else if git.manifest_repository.is_none() {
            eprintln!("   ⚠️  No repository set in capsule.toml ([metadata].repository).");
        }
        if git.dirty {
            eprintln!("   ⚠️  Warning: Uncommitted changes detected.");
            eprintln!("      CI builds only committed code. Local result may differ.");
        } else {
            eprintln!("   ✔ Working tree is clean.");
        }

        eprintln!();
        eprintln!("🛡️  Validating CI Workflow (.github/workflows/ato-publish.yml)...");
        eprintln!("   ✔ Workflow file exists.");
        eprintln!("   ✔ OIDC permissions are configured (id-token: write).");
        eprintln!("   ✔ Secure binary verification is enabled (sha256sum).");
        eprintln!("   ✔ Tag-based trigger is configured.");
        eprintln!();
        eprintln!("📦 Simulating deterministic build...");
    }

    let artifact_path = crate::publish_ci::build_capsule_artifact(
        &manifest_path,
        &manifest.name,
        &manifest.version,
    )
    .with_context(|| "Failed to build local dry-run artifact")?;
    let artifact_size_bytes = fs::metadata(&artifact_path)
        .with_context(|| format!("Failed to inspect {}", artifact_path.display()))?
        .len();

    Ok(PublishDryRunResult {
        capsule_name: manifest.name,
        version: manifest.version,
        artifact_path,
        artifact_size_bytes,
        git,
        ci_workflow,
    })
}

fn validate_ci_workflow(cwd: &PathBuf) -> Result<CiWorkflowCheckResult> {
    let rel_path = ".github/workflows/ato-publish.yml";
    let path = cwd.join(rel_path);
    if !path.exists() {
        anyhow::bail!(
            "CI workflow not found: {}. Run `ato gen-ci` first.",
            path.display()
        );
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read CI workflow: {}", path.display()))?;

    let has_oidc_permission = content.contains("id-token: write");
    if !has_oidc_permission {
        anyhow::bail!(
            "CI workflow is missing `id-token: write` permission. Regenerate with `ato gen-ci`."
        );
    }

    let has_tag_trigger =
        content.contains("push:") && content.contains("tags:") && content.contains("v*.*.*");
    if !has_tag_trigger {
        anyhow::bail!(
            "CI workflow is missing tag-based trigger (`on.push.tags`). Regenerate with `ato gen-ci`."
        );
    }

    let has_checksum_verification =
        content.contains("ATO_VERSION") && content.contains("sha256sum -c");
    if !has_checksum_verification {
        anyhow::bail!(
            "CI workflow is missing pinned checksum verification. Regenerate with `ato gen-ci`."
        );
    }

    Ok(CiWorkflowCheckResult {
        path: rel_path.to_string(),
        exists: true,
        has_oidc_permission,
        has_tag_trigger,
        has_checksum_verification,
    })
}

fn run_git_checks(manifest_repo: Option<&str>) -> Result<GitCheckResult> {
    let inside = run_git(&["rev-parse", "--is-inside-work-tree"])
        .ok()
        .map(|v| v == "true")
        .unwrap_or(false);
    if !inside {
        anyhow::bail!(
            "Current directory is not inside a Git repository.\nRun `git init` first, or execute `ato publish --dry-run` from an existing Git repository root."
        );
    }

    let origin_raw = run_git(&["remote", "get-url", "origin"]).ok();
    let origin_norm = origin_raw.as_deref().and_then(normalize_origin_to_repo);

    let manifest_repository = manifest_repo.map(normalize_repository_value).transpose()?;

    if let (Some(expected_repo), Some(actual_repo)) =
        (manifest_repository.as_deref(), origin_norm.as_deref())
    {
        if expected_repo != actual_repo {
            anyhow::bail!(
                "Repository mismatch: capsule.toml repository '{}' != git origin '{}'",
                expected_repo,
                actual_repo
            );
        }
    }

    if manifest_repository.is_some() && origin_norm.is_none() {
        anyhow::bail!(
            "capsule.toml has repository but `git remote origin` is missing or not a GitHub repository"
        );
    }

    let dirty = run_git(&["status", "--porcelain"])
        .map(|v| !v.is_empty())
        .unwrap_or(false);

    Ok(GitCheckResult {
        inside_work_tree: true,
        origin: origin_norm.clone(),
        manifest_repository: manifest_repository.clone(),
        repository_match: manifest_repository
            .as_ref()
            .map(|repo| origin_norm.as_ref().map(|o| o == repo).unwrap_or(false)),
        dirty,
    })
}

fn run_git(args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("Failed to execute git {}", args.join(" ")))?;
    if !output.status.success() {
        anyhow::bail!(
            "git {} failed with status {}",
            args.join(" "),
            output
                .status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string())
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn find_manifest_repository(manifest_raw: &str) -> Option<String> {
    let parsed = toml::from_str::<toml::Value>(manifest_raw).ok()?;
    parsed
        .get("metadata")
        .and_then(|v| v.get("repository"))
        .and_then(|v| v.as_str())
        .or_else(|| parsed.get("repository").and_then(|v| v.as_str()))
        .map(|v| v.to_string())
}

fn normalize_repository_value(value: &str) -> Result<String> {
    let raw = value.trim();
    if raw.is_empty() {
        anyhow::bail!("repository is empty");
    }
    if raw.contains("://") {
        let parsed = reqwest::Url::parse(raw).with_context(|| "Invalid repository URL")?;
        let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
        if host != "github.com" && host != "www.github.com" {
            anyhow::bail!("repository must point to github.com");
        }
        let mut segs = parsed
            .path_segments()
            .context("repository URL has no path")?;
        let owner = segs.next().unwrap_or("").trim();
        let repo = segs.next().unwrap_or("").trim_end_matches(".git").trim();
        if owner.is_empty() || repo.is_empty() {
            anyhow::bail!("repository URL must include owner/repo");
        }
        return Ok(format!("{}/{}", owner, repo));
    }

    let mut it = raw.split('/');
    let owner = it.next().unwrap_or("").trim();
    let repo = it.next().unwrap_or("").trim_end_matches(".git").trim();
    if owner.is_empty() || repo.is_empty() || it.next().is_some() {
        anyhow::bail!("repository must be 'owner/repo' or GitHub URL");
    }
    Ok(format!("{}/{}", owner, repo))
}

fn normalize_origin_to_repo(origin: &str) -> Option<String> {
    let trimmed = origin.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(without_prefix) = trimmed.strip_prefix("git@github.com:") {
        let repo = without_prefix.trim_end_matches(".git").trim();
        return if repo.split('/').count() == 2 {
            Some(repo.to_string())
        } else {
            None
        };
    }

    normalize_repository_value(trimmed).ok()
}
