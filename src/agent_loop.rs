use anyhow::{bail, Context, Result};
use capsule_core::CapsuleReporter;
use serde::Serialize;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::reporters::CliReporter;

const AGENT_REQUEST_TMP_DIR: [&str; 3] = [".tmp", "agent", "requests"];

#[derive(Clone, Debug)]
pub struct AgentRunOptions {
    pub provider: String,
    pub model: Option<String>,
    pub no_code_fix: bool,
    pub auto_approve: bool,
}

#[derive(Debug, Serialize)]
struct AgentRequest {
    repo_path: String,
    ato_binary: String,
    provider: String,
    model: Option<String>,
    api_key: Option<String>,
    max_corrections: u32,
    target_env: TargetEnv,
    approval_policy: ApprovalPolicy,
    checkpoint_db: String,
    patterns_db: String,
}

#[derive(Debug, Serialize)]
struct TargetEnv {
    os: String,
    arch: String,
    runtime: String,
}

#[derive(Debug, Serialize)]
struct ApprovalPolicy {
    capsule: String,
    code: String,
}

pub fn should_launch_for_path(path: &Path) -> bool {
    path.is_dir() && !path.join("capsule.toml").exists()
}

pub fn execute(
    repo_path: &Path,
    options: AgentRunOptions,
    reporter: std::sync::Arc<CliReporter>,
) -> Result<()> {
    let python = resolve_python_binary()?;
    let script = resolve_agent_script()?;
    let ato_home = resolve_ato_home()?;
    execute_with_paths(repo_path, options, reporter, &python, &script, &ato_home)
}

fn execute_with_paths(
    repo_path: &Path,
    options: AgentRunOptions,
    reporter: std::sync::Arc<CliReporter>,
    python: &Path,
    script: &Path,
    ato_home: &Path,
) -> Result<()> {
    if !repo_path.is_dir() {
        bail!(
            "Agent-assisted repository runs require a local project directory: {}",
            repo_path.display()
        );
    }

    let repo_path = repo_path
        .canonicalize()
        .with_context(|| format!("Failed to resolve repository path: {}", repo_path.display()))?;

    let request_dir = AGENT_REQUEST_TMP_DIR
        .iter()
        .fold(repo_path.clone(), |path, component| path.join(component));
    fs::create_dir_all(&request_dir).with_context(|| {
        format!(
            "Failed to create agent request directory: {}",
            request_dir.display()
        )
    })?;

    let request_path = request_dir.join(format!(
        "request-{}-{}.json",
        std::process::id(),
        timestamp_millis()?
    ));

    let request = AgentRequest {
        repo_path: repo_path.to_string_lossy().into_owned(),
        ato_binary: std::env::current_exe()
            .context("Failed to resolve current ato binary")?
            .to_string_lossy()
            .into_owned(),
        provider: options.provider.clone(),
        model: options.model.clone(),
        api_key: resolve_api_key(&options.provider),
        max_corrections: 10,
        target_env: TargetEnv {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            runtime: "ato-cli".to_string(),
        },
        approval_policy: ApprovalPolicy {
            capsule: "auto".to_string(),
            code: resolve_code_approval_policy(&options).to_string(),
        },
        // Used by the Python LangGraph layer when SqliteSaver is available so each
        // agent cycle can persist checkpoints for resumable sessions.
        checkpoint_db: ato_home.join("checkpoints.db").display().to_string(),
        patterns_db: ato_home
            .join("agent")
            .join("patterns.db")
            .display()
            .to_string(),
    };

    fs::write(
        &request_path,
        serde_json::to_vec_pretty(&request).context("Failed to serialize agent request")?,
    )
    .with_context(|| format!("Failed to write agent request: {}", request_path.display()))?;

    futures::executor::block_on(reporter.notify(format!(
        "🤖 Launching repository agent for {}",
        repo_path.display()
    )))?;

    let output = Command::new(python)
        .arg(script)
        .arg(&request_path)
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| {
            format!(
                "Failed to launch Python agent with interpreter {}",
                python.display()
            )
        })?;

    if let Err(error) = fs::remove_file(&request_path) {
        futures::executor::block_on(reporter.warn(format!(
            "⚠️  Failed to remove temporary agent request file {}: {}",
            request_path.display(),
            error
        )))?;
    }

    if !output.stdout.is_empty() {
        io::stdout()
            .write_all(&output.stdout)
            .context("Failed to write agent stdout")?;
    }
    if !output.stderr.is_empty() {
        io::stderr()
            .write_all(&output.stderr)
            .context("Failed to write agent stderr")?;
    }

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let requirements_path = script
            .parent()
            .map(|dir| dir.join("requirements.txt"))
            .unwrap_or_else(|| PathBuf::from("agent/requirements.txt"));
        if stderr.contains("ModuleNotFoundError") || stderr.contains("No module named") {
            bail!(
                "Python agent dependencies are missing. Install them with `pip install -r {}` or set ATO_AGENT_PYTHON to a Python environment that already has the agent dependencies.\n{}",
                requirements_path.display(),
                stderr.trim()
            );
        }
        bail!(
            "Python agent exited with status {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string())
        );
    }

    Ok(())
}

fn resolve_code_approval_policy(options: &AgentRunOptions) -> &'static str {
    if options.no_code_fix {
        "ignore"
    } else if options.auto_approve {
        "auto"
    } else {
        "confirm"
    }
}

fn resolve_api_key(provider: &str) -> Option<String> {
    match provider {
        "openai" => std::env::var("OPENAI_API_KEY")
            .ok()
            .or_else(|| std::env::var("LITELLM_API_KEY").ok()),
        "anthropic" => std::env::var("ANTHROPIC_API_KEY")
            .ok()
            .or_else(|| std::env::var("LITELLM_API_KEY").ok()),
        _ => std::env::var("LITELLM_API_KEY").ok(),
    }
}

fn resolve_python_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("ATO_AGENT_PYTHON") {
        return Ok(PathBuf::from(path));
    }

    which::which("python3")
        .or_else(|_| which::which("python"))
        .context("Python 3 is required for repository agent mode")
}

fn resolve_agent_script() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("ATO_AGENT_SCRIPT") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let current_exe = std::env::current_exe().ok();
    let exe_parent = current_exe
        .as_ref()
        .and_then(|path| path.parent().map(Path::to_path_buf));
    let candidates = [
        exe_parent
            .as_ref()
            .map(|dir| dir.join("agent/agent.py"))
            .unwrap_or_else(|| manifest_dir.join("agent/agent.py")),
        exe_parent
            .as_ref()
            .and_then(|dir| dir.parent().map(|parent| parent.join("agent/agent.py")))
            .unwrap_or_else(|| manifest_dir.join("agent/agent.py")),
        exe_parent
            .as_ref()
            .and_then(|dir| {
                dir.parent()
                    .and_then(|parent| parent.parent())
                    .map(|parent| parent.join("agent/agent.py"))
            })
            .unwrap_or_else(|| manifest_dir.join("agent/agent.py")),
        manifest_dir.join("agent/agent.py"),
        std::env::current_dir()
            .unwrap_or_else(|_| manifest_dir.clone())
            .join("agent/agent.py"),
    ];

    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .context("Failed to locate agent/agent.py for repository agent mode")
}

fn resolve_ato_home() -> Result<PathBuf> {
    dirs::home_dir()
        .map(|home| home.join(".ato"))
        .context("Failed to determine ~/.ato path for repository agent mode")
}

fn timestamp_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before UNIX_EPOCH")?
        .as_millis())
}
