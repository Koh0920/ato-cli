use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self as crossterm_event, Event, KeyCode, KeyEvent};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Terminal;
use semver::{BuildMetadata, Prerelease, Version};
use serde::Deserialize;
use toml_edit::{value, DocumentMut};

use crate::auth;
use crate::publish_preflight::{
    self, find_manifest_repository, CiWorkflowCheckResult, GitCheckResult, CI_WORKFLOW_REL_PATH,
};

const MAIN_BRANCH: &str = "main";
const POLL_INTERVAL: Duration = Duration::from_secs(4);
const POLL_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const UI_TICK: Duration = Duration::from_millis(120);
const MAX_LOG_LINES: usize = 200;

#[derive(Debug, Clone)]
pub struct PublishSuccessSummary {
    pub capsule: String,
    pub version: String,
    pub run_url: String,
}

#[derive(Debug, Clone)]
pub struct PublishFailureSummary {
    pub message: String,
    pub run_url: Option<String>,
    pub conclusion: Option<String>,
    pub tag: Option<String>,
    pub details: Vec<JobFailureSummary>,
}

#[derive(Debug, Clone)]
pub struct JobFailureSummary {
    pub name: String,
    pub status: String,
    pub failed_steps: Vec<String>,
    pub log_excerpt: Option<String>,
}

#[derive(Debug, Clone)]
pub enum PublishTuiOutcome {
    Success(PublishSuccessSummary),
    Failure(PublishFailureSummary),
    Cancelled,
}

pub async fn execute() -> Result<PublishTuiOutcome> {
    let mut context = preflight()?;
    let token = resolve_github_token()?;
    context.registration_note = ensure_capsule_registered(&context, &token.token).await?;
    let remote_workflow = fetch_remote_workflow_state(&context.repository, &token.token).await?;
    let mut app = PublishApp::new(context, token, remote_workflow);

    run_publish_dashboard(&mut app).await
}

#[derive(Debug)]
struct PublishContext {
    manifest_path: PathBuf,
    manifest_raw: String,
    manifest: capsule_core::types::capsule_v1::CapsuleManifestV1,
    git: GitCheckResult,
    ci_workflow: CiWorkflowCheckResult,
    ci_workflow_refreshed: bool,
    registration_note: Option<String>,
    branch: String,
    repository: String,
    current_version: Version,
}

fn preflight() -> Result<PublishContext> {
    let cwd = std::env::current_dir().context("Failed to resolve current directory")?;
    let manifest_path = cwd.join("capsule.toml");
    let manifest_raw = fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;

    let manifest = capsule_core::types::capsule_v1::CapsuleManifestV1::from_toml(&manifest_raw)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml: {}", err))?;

    let current_version = Version::parse(&manifest.version).with_context(|| {
        format!(
            "capsule.toml version is not valid semver: {}",
            manifest.version
        )
    })?;

    let manifest_repo = find_manifest_repository(&manifest_raw);
    let git = publish_preflight::run_git_checks(manifest_repo.as_deref())?;
    let branch = publish_preflight::git_current_branch()?;
    if branch != MAIN_BRANCH {
        anyhow::bail!(
            "TUI publish is allowed only on '{}' branch (current: '{}')",
            MAIN_BRANCH,
            branch
        );
    }

    if git.dirty {
        anyhow::bail!("Working tree is not clean. Commit or stash changes before `ato publish`.");
    }

    let workflow_sync = tokio::task::block_in_place(|| crate::gen_ci::sync_workflow_in_dir(&cwd))
        .with_context(|| "Failed to refresh CI workflow to latest recommended template")?;

    let origin = git
        .origin
        .clone()
        .context("`git remote origin` is required for TUI publish")?;

    let expected_repo = git
        .manifest_repository
        .clone()
        .context("[metadata].repository is required in capsule.toml for TUI publish")?;

    if origin != expected_repo {
        anyhow::bail!(
            "Repository mismatch: capsule.toml repository '{}' != git origin '{}'",
            expected_repo,
            origin
        );
    }

    let ci_workflow = publish_preflight::validate_ci_workflow(&cwd)?;

    Ok(PublishContext {
        manifest_path,
        manifest_raw,
        manifest,
        git,
        ci_workflow,
        ci_workflow_refreshed: workflow_sync.changed,
        registration_note: None,
        branch,
        repository: expected_repo,
        current_version,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionCandidate {
    label: &'static str,
    version: Version,
}

fn version_candidates_from_semver(current: &Version) -> Vec<VersionCandidate> {
    let mut patch = current.clone();
    patch.patch += 1;
    patch.pre = Prerelease::EMPTY;
    patch.build = BuildMetadata::EMPTY;

    let mut minor = current.clone();
    minor.minor += 1;
    minor.patch = 0;
    minor.pre = Prerelease::EMPTY;
    minor.build = BuildMetadata::EMPTY;

    let mut major = current.clone();
    major.major += 1;
    major.minor = 0;
    major.patch = 0;
    major.pre = Prerelease::EMPTY;
    major.build = BuildMetadata::EMPTY;

    vec![
        VersionCandidate {
            label: "Patch",
            version: patch,
        },
        VersionCandidate {
            label: "Minor",
            version: minor,
        },
        VersionCandidate {
            label: "Major",
            version: major,
        },
    ]
}

#[derive(Debug, Clone)]
struct ResolvedGitHubToken {
    token: String,
    source: GitHubTokenSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitHubTokenSource {
    Credentials,
    EnvGhToken,
    EnvGithubToken,
    GhCli,
}

fn resolve_github_token() -> Result<ResolvedGitHubToken> {
    let credentials_token = auth::AuthManager::new()?
        .load()?
        .and_then(|creds| creds.github_token)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    let env_gh = read_env_non_empty("GH_TOKEN");
    let env_github = read_env_non_empty("GITHUB_TOKEN");
    let gh_cli = read_gh_cli_token();

    let resolved = choose_github_token(credentials_token, env_gh, env_github, gh_cli).context(
        "GitHub token is required to validate workflow state and monitor Actions runs. Configure one of: `ato login` credentials, GH_TOKEN/GITHUB_TOKEN, or `gh auth token`.",
    )?;

    Ok(resolved)
}

fn choose_github_token(
    credentials_token: Option<String>,
    env_gh_token: Option<String>,
    env_github_token: Option<String>,
    gh_cli_token: Option<String>,
) -> Option<ResolvedGitHubToken> {
    if let Some(token) = credentials_token {
        return Some(ResolvedGitHubToken {
            token,
            source: GitHubTokenSource::Credentials,
        });
    }
    if let Some(token) = env_gh_token {
        return Some(ResolvedGitHubToken {
            token,
            source: GitHubTokenSource::EnvGhToken,
        });
    }
    if let Some(token) = env_github_token {
        return Some(ResolvedGitHubToken {
            token,
            source: GitHubTokenSource::EnvGithubToken,
        });
    }
    gh_cli_token.map(|token| ResolvedGitHubToken {
        token,
        source: GitHubTokenSource::GhCli,
    })
}

impl GitHubTokenSource {
    fn as_label(self) -> &'static str {
        match self {
            GitHubTokenSource::Credentials => "credentials.json",
            GitHubTokenSource::EnvGhToken => "GH_TOKEN",
            GitHubTokenSource::EnvGithubToken => "GITHUB_TOKEN",
            GitHubTokenSource::GhCli => "gh auth token",
        }
    }
}

fn read_env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn read_gh_cli_token() -> Option<String> {
    let output = Command::new("gh").args(["auth", "token"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

fn resolve_store_api_base_url() -> String {
    std::env::var("ATO_STORE_API_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://api.ato.run".to_string())
}

async fn ensure_capsule_registered(
    context: &PublishContext,
    github_token: &str,
) -> Result<Option<String>> {
    let (owner, _) = context
        .repository
        .split_once('/')
        .context("Invalid repository format (expected owner/repo)")?;
    let slug = context.manifest.name.trim();
    if slug.is_empty() {
        anyhow::bail!("capsule.toml name is empty");
    }

    let base = resolve_store_api_base_url();
    let check_url = format!(
        "{}/v1/capsules/by/{}/{}",
        base,
        urlencoding::encode(owner),
        urlencoding::encode(slug)
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("Failed to create Store API client")?;

    let check_resp = client
        .get(&check_url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .send()
        .await
        .with_context(|| "Failed to check capsule registration status")?;

    if check_resp.status().is_success() {
        return Ok(None);
    }

    if check_resp.status() != reqwest::StatusCode::NOT_FOUND {
        let status = check_resp.status();
        let body = check_resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "Failed to check capsule registration ({}): {}",
            status,
            body
        );
    }

    let register_url = format!("{}/v1/sources/github/register", base);
    let repo_url = format!("https://github.com/{}", context.repository);
    let register_resp = client
        .post(&register_url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", github_token))
        .json(&serde_json::json!({
            "repo_url": repo_url,
            "channel": "stable",
            "apply_playground": false
        }))
        .send()
        .await
        .with_context(|| "Failed to auto-register source repository")?;

    let status = register_resp.status();
    if !(status.is_success() || status == reqwest::StatusCode::CONFLICT) {
        let body = register_resp.text().await.unwrap_or_default();
        anyhow::bail!("Source auto-registration failed ({}): {}", status, body);
    }

    let started = Instant::now();
    let timeout = Duration::from_secs(45);
    let poll = Duration::from_secs(2);
    loop {
        let resp = client
            .get(&check_url)
            .header(reqwest::header::USER_AGENT, "ato-cli")
            .send()
            .await
            .with_context(|| "Failed while waiting for capsule registration")?;
        if resp.status().is_success() {
            return Ok(Some(format!(
                "Capsule registration ensured: {}/{}",
                owner, slug
            )));
        }
        if started.elapsed() >= timeout {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Capsule registration did not complete in time. Last response ({}): {}",
                status,
                body
            );
        }
        tokio::time::sleep(poll).await;
    }
}

#[derive(Debug)]
struct RemoteWorkflowState {
    path: String,
    state: String,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowResponse {
    path: String,
    state: String,
    html_url: Option<String>,
}

async fn fetch_remote_workflow_state(repository: &str, token: &str) -> Result<RemoteWorkflowState> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .build()
        .context("Failed to create GitHub API client")?;

    let url = format!(
        "https://api.github.com/repos/{}/actions/workflows/ato-publish.yml",
        repository
    );
    let response = client
        .get(&url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .with_context(|| "Failed to query GitHub workflow metadata")?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!(
            "Remote workflow '.github/workflows/ato-publish.yml' was not found on GitHub. Commit and push it first."
        );
    }
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        anyhow::bail!(
            "GitHub token cannot read workflow metadata (HTTP {}). Ensure repo/workflow read permission.",
            status
        );
    }

    let workflow = response
        .error_for_status()
        .with_context(|| "Failed to query GitHub workflow metadata")?
        .json::<GitHubWorkflowResponse>()
        .await
        .with_context(|| "Failed to parse GitHub workflow metadata response")?;

    if workflow.state != "active" {
        let html = workflow
            .html_url
            .clone()
            .unwrap_or_else(|| format!("https://github.com/{}/actions", repository));
        anyhow::bail!(
            "Remote workflow state is '{}' (expected 'active'). Enable the workflow first: {}",
            workflow.state,
            html
        );
    }

    Ok(RemoteWorkflowState {
        path: workflow.path,
        state: workflow.state,
        html_url: workflow.html_url,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiMode {
    SelectVersion,
    Confirm,
    RunningGitOps,
    MonitoringCi,
    Success,
    Failure,
}

#[derive(Debug)]
struct MonitorState {
    started_at: Instant,
    next_poll_at: Instant,
    missing_cycles: u32,
    last_status: Option<String>,
}

#[derive(Debug)]
struct PublishApp {
    context: PublishContext,
    token: ResolvedGitHubToken,
    remote_workflow: RemoteWorkflowState,
    mode: UiMode,
    logs: VecDeque<String>,
    candidates: Vec<VersionCandidate>,
    selected_idx: usize,
    selected_version: Option<Version>,
    selected_tag: Option<String>,
    spinner_tick: usize,
    git_release: Option<GitReleaseResult>,
    monitor_state: Option<MonitorState>,
    run_url: Option<String>,
    conclusion: Option<String>,
    failure_details: Vec<JobFailureSummary>,
    should_exit: bool,
    outcome: Option<PublishTuiOutcome>,
}

impl PublishApp {
    fn new(
        context: PublishContext,
        token: ResolvedGitHubToken,
        remote_workflow: RemoteWorkflowState,
    ) -> Self {
        let candidates = version_candidates_from_semver(&context.current_version);
        let mut app = Self {
            context,
            token,
            remote_workflow,
            mode: UiMode::SelectVersion,
            logs: VecDeque::new(),
            candidates,
            selected_idx: 0,
            selected_version: None,
            selected_tag: None,
            spinner_tick: 0,
            git_release: None,
            monitor_state: None,
            run_url: None,
            conclusion: None,
            failure_details: Vec::new(),
            should_exit: false,
            outcome: None,
        };
        app.push_preflight_logs();
        app
    }

    fn push_log(&mut self, message: impl Into<String>) {
        self.logs.push_back(message.into());
        while self.logs.len() > MAX_LOG_LINES {
            self.logs.pop_front();
        }
    }

    fn push_preflight_logs(&mut self) {
        self.push_log("✔ capsule.toml loaded");
        self.push_log("✔ Git repository detected");
        self.push_log(format!("✔ Branch: {}", self.context.branch));
        self.push_log("✔ Working tree is clean");
        self.push_log(format!(
            "✔ Repository match: {}",
            self.context
                .git
                .manifest_repository
                .as_deref()
                .unwrap_or("unknown")
        ));
        self.push_log(format!(
            "✔ Workflow validated: {}",
            self.context.ci_workflow.path
        ));
        if self.context.ci_workflow_refreshed {
            self.push_log("✔ Workflow updated to latest recommended template");
        }
        if let Some(note) = &self.context.registration_note {
            self.push_log(format!("✔ {}", note));
        }
        self.push_log(format!(
            "✔ GitHub auth token source: {}",
            self.token.source.as_label()
        ));
        self.push_log(format!(
            "✔ Remote workflow state: {} ({})",
            self.remote_workflow.state, self.remote_workflow.path
        ));
        if let Some(url) = &self.remote_workflow.html_url {
            self.push_log(format!("✔ Remote workflow URL: {}", url));
        }
        self.push_log("Waiting for user input...");
    }

    fn spinner(&self) -> &'static str {
        const FRAMES: [&str; 8] = ["⠁", "⠂", "⠄", "⡀", "⢀", "⠠", "⠐", "⠈"];
        FRAMES[self.spinner_tick % FRAMES.len()]
    }

    fn selected_candidate(&self) -> &VersionCandidate {
        &self.candidates[self.selected_idx]
    }

    fn build_failure_outcome(&self, message: String) -> PublishTuiOutcome {
        PublishTuiOutcome::Failure(PublishFailureSummary {
            message,
            run_url: self.run_url.clone(),
            conclusion: self.conclusion.clone(),
            tag: self.selected_tag.clone(),
            details: self.failure_details.clone(),
        })
    }
}

async fn run_publish_dashboard(app: &mut PublishApp) -> Result<PublishTuiOutcome> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen,
        crossterm::event::EnableMouseCapture
    )?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let loop_result: Result<PublishTuiOutcome> = async {
        loop {
            app.spinner_tick = app.spinner_tick.wrapping_add(1);

            terminal.draw(|frame| render_ui(frame, app))?;

            while crossterm_event::poll(Duration::from_millis(1))? {
                if let Event::Key(key) = crossterm_event::read()? {
                    handle_key_event(app, key);
                }
            }

            advance_state(app).await;

            if app.should_exit {
                break;
            }

            tokio::time::sleep(UI_TICK).await;
        }

        Ok(app.outcome.clone().unwrap_or(PublishTuiOutcome::Cancelled))
    }
    .await;

    disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::event::DisableMouseCapture,
        crossterm::terminal::LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    loop_result
}

fn render_ui(frame: &mut ratatui::Frame<'_>, app: &PublishApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(8),
            Constraint::Length(12),
        ])
        .split(frame.area());

    render_progress(frame, app, chunks[0]);
    render_main_panel(frame, app, chunks[1]);
    render_logs(frame, app, chunks[2]);
}

fn render_progress(frame: &mut ratatui::Frame<'_>, app: &PublishApp, area: ratatui::layout::Rect) {
    let (version, gitops, ci, done) = match app.mode {
        UiMode::SelectVersion | UiMode::Confirm => ("⏳", "⏸", "⏸", "⏸"),
        UiMode::RunningGitOps => ("✅", "⏳", "⏸", "⏸"),
        UiMode::MonitoringCi => ("✅", "✅", "⏳", "⏸"),
        UiMode::Success => ("✅", "✅", "✅", "✅"),
        UiMode::Failure => ("✅", "✅", "❌", "❌"),
    };

    let line = Line::from(vec![
        Span::styled("✅ Preflight ", Style::default().fg(Color::Green)),
        Span::raw("→ "),
        Span::raw(format!("{} Version ", version)),
        Span::raw("→ "),
        Span::raw(format!("{} GitOps ", gitops)),
        Span::raw("→ "),
        Span::raw(format!("{} CI ", ci)),
        Span::raw("→ "),
        Span::raw(format!("{} Done", done)),
    ]);

    let paragraph = Paragraph::new(vec![
        Line::from(format!("🚀 Ato Publish: {}", app.context.repository)),
        Line::from(""),
        line,
    ])
    .block(Block::default().borders(Borders::ALL).title("Progress"));

    frame.render_widget(paragraph, area);
}

fn render_main_panel(
    frame: &mut ratatui::Frame<'_>,
    app: &PublishApp,
    area: ratatui::layout::Rect,
) {
    let block = Block::default().borders(Borders::ALL).title("Main");
    match app.mode {
        UiMode::SelectVersion => {
            let mut lines = vec![
                Line::from("📦 Select next release version"),
                Line::from(""),
                Line::from(format!("Current: {}", app.context.current_version)),
                Line::from(""),
            ];
            for (idx, candidate) in app.candidates.iter().enumerate() {
                let marker = if idx == app.selected_idx { "❯" } else { " " };
                let suffix = if idx == 0 { " (Recommended)" } else { "" };
                lines.push(Line::from(format!(
                    "{} {:<5} {}{}",
                    marker, candidate.label, candidate.version, suffix
                )));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Keys: ↑/↓ or j/k, Enter to confirm, q to cancel",
            ));
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
        UiMode::Confirm => {
            let version = app
                .selected_version
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown".to_string());
            let tag = app.selected_tag.as_deref().unwrap_or("(tag unavailable)");
            let lines = vec![
                Line::from("📝 Confirm release plan"),
                Line::from(""),
                Line::from(format!(
                    "Capsule: {} {} -> {}",
                    app.context.manifest.name, app.context.manifest.version, version
                )),
                Line::from(format!("Repository: {}", app.context.repository)),
                Line::from(format!("Tag: {}", tag)),
                Line::from(""),
                Line::from("Operations:"),
                if app.context.ci_workflow_refreshed {
                    Line::from(" - update .github/workflows/ato-publish.yml")
                } else {
                    Line::from(" - keep .github/workflows/ato-publish.yml unchanged")
                },
                Line::from(" - update capsule.toml version"),
                Line::from(" - git commit -m \"chore: release vX.Y.Z\""),
                Line::from(" - git tag vX.Y.Z"),
                Line::from(" - git push origin main"),
                Line::from(" - git push origin vX.Y.Z"),
                Line::from(""),
                Line::from("Keys: Enter/Y to start, n to back, q to cancel"),
            ];
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
        UiMode::RunningGitOps => {
            let lines = vec![
                Line::from(format!("{} Running GitOps sequence...", app.spinner())),
                Line::from(""),
                Line::from(" - update manifest version"),
                Line::from(" - commit + tag"),
                Line::from(" - push main + tag"),
                Line::from(""),
                Line::from("Please wait..."),
            ];
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
        UiMode::MonitoringCi => {
            let lines = vec![
                Line::from(format!(
                    "{} Monitoring GitHub Actions run...",
                    app.spinner()
                )),
                Line::from(""),
                Line::from(format!("Repository: {}", app.context.repository)),
                Line::from(format!(
                    "Tag: {}",
                    app.selected_tag.as_deref().unwrap_or("unknown")
                )),
                Line::from(format!(
                    "Run URL: {}",
                    app.run_url
                        .as_deref()
                        .unwrap_or("(waiting for run detection)")
                )),
                Line::from(""),
                Line::from("Polling every 4 seconds..."),
            ];
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
        UiMode::Success => {
            let lines = vec![
                Line::from("✅ Publish completed successfully"),
                Line::from(""),
                Line::from(format!(
                    "Run URL: {}",
                    app.run_url.as_deref().unwrap_or("-")
                )),
                Line::from(format!("Capsule: {}", app.context.manifest.name)),
                Line::from(format!(
                    "Version: {}",
                    app.selected_version
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "-".to_string())
                )),
                Line::from(""),
                Line::from("Press Enter or q to exit"),
            ];
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
        UiMode::Failure => {
            let mut lines = vec![
                Line::from(vec![Span::styled(
                    "❌ Publish failed",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )]),
                Line::from(""),
                Line::from(format!(
                    "Conclusion: {}",
                    app.conclusion.as_deref().unwrap_or("unknown")
                )),
            ];
            if let Some(url) = &app.run_url {
                lines.push(Line::from(format!("Run URL: {}", url)));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Press Enter or q to exit and see recovery steps",
            ));
            frame.render_widget(Paragraph::new(lines).block(block), area);
        }
    }
}

fn render_logs(frame: &mut ratatui::Frame<'_>, app: &PublishApp, area: ratatui::layout::Rect) {
    let items: Vec<ListItem<'_>> = app
        .logs
        .iter()
        .rev()
        .take(9)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|line| ListItem::new(line.clone()))
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title("System Events"),
    );
    frame.render_widget(list, area);
}

fn handle_key_event(app: &mut PublishApp, key: KeyEvent) {
    match app.mode {
        UiMode::SelectVersion => match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                app.outcome = Some(PublishTuiOutcome::Cancelled);
                app.should_exit = true;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.selected_idx + 1 < app.candidates.len() {
                    app.selected_idx += 1;
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.selected_idx = app.selected_idx.saturating_sub(1);
            }
            KeyCode::Enter => {
                let candidate = app.selected_candidate().clone();
                let tag = format!("v{}", candidate.version);
                app.selected_version = Some(candidate.version.clone());
                app.selected_tag = Some(tag.clone());
                app.push_log(format!("Selected version: {}", candidate.version));
                match ensure_tag_available(&tag) {
                    Ok(()) => {
                        app.mode = UiMode::Confirm;
                    }
                    Err(err) => {
                        app.push_log(format!("Tag check failed: {}", err));
                        app.outcome = Some(
                            app.build_failure_outcome(format!("Tag precheck failed: {}", err)),
                        );
                        app.mode = UiMode::Failure;
                    }
                }
            }
            _ => {}
        },
        UiMode::Confirm => match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                app.outcome = Some(PublishTuiOutcome::Cancelled);
                app.should_exit = true;
            }
            KeyCode::Char('n') => {
                app.mode = UiMode::SelectVersion;
            }
            KeyCode::Char('y') | KeyCode::Enter => {
                app.mode = UiMode::RunningGitOps;
                app.push_log("Starting GitOps release sequence...");
            }
            _ => {}
        },
        UiMode::Success | UiMode::Failure => match key.code {
            KeyCode::Enter | KeyCode::Char('q') | KeyCode::Esc => {
                app.should_exit = true;
            }
            _ => {}
        },
        UiMode::RunningGitOps | UiMode::MonitoringCi => {
            if matches!(key.code, KeyCode::Char('q')) {
                app.push_log("q pressed during execution; waiting for current step to finish.");
            }
        }
    }
}

async fn advance_state(app: &mut PublishApp) {
    match app.mode {
        UiMode::RunningGitOps => {
            let Some(selected_version) = app.selected_version.clone() else {
                app.outcome = Some(app.build_failure_outcome(
                    "internal error: selected version is not set".to_string(),
                ));
                app.mode = UiMode::Failure;
                return;
            };
            let Some(tag) = app.selected_tag.clone() else {
                app.outcome =
                    Some(app.build_failure_outcome(
                        "internal error: selected tag is not set".to_string(),
                    ));
                app.mode = UiMode::Failure;
                return;
            };

            match execute_git_release_sequence(&app.context, &selected_version, &tag) {
                Ok(release) => {
                    app.push_log(format!("✔ Committed: {}", release.commit_sha));
                    app.push_log(format!("✔ Tagged: {}", tag));
                    app.push_log("✔ Pushed: origin/main and tag");
                    app.git_release = Some(release);
                    app.mode = UiMode::MonitoringCi;
                    app.monitor_state = Some(MonitorState {
                        started_at: Instant::now(),
                        next_poll_at: Instant::now(),
                        missing_cycles: 0,
                        last_status: None,
                    });
                }
                Err(err) => {
                    app.push_log(format!("GitOps failed: {}", err));
                    app.outcome =
                        Some(app.build_failure_outcome(format!("GitOps failed: {}", err)));
                    app.mode = UiMode::Failure;
                }
            }
        }
        UiMode::MonitoringCi => {
            let Some(release) = &app.git_release else {
                app.outcome = Some(app.build_failure_outcome(
                    "internal error: git release state is missing".to_string(),
                ));
                app.mode = UiMode::Failure;
                return;
            };
            let Some(tag) = app.selected_tag.clone() else {
                app.outcome =
                    Some(app.build_failure_outcome("internal error: tag is missing".to_string()));
                app.mode = UiMode::Failure;
                return;
            };
            let Some(monitor_ro) = app.monitor_state.as_ref() else {
                app.outcome =
                    Some(app.build_failure_outcome(
                        "internal error: monitor state is missing".to_string(),
                    ));
                app.mode = UiMode::Failure;
                return;
            };

            if monitor_ro.started_at.elapsed() > POLL_TIMEOUT {
                app.outcome = Some(app.build_failure_outcome(format!(
                    "Timed out waiting for workflow completion. Check: https://github.com/{}/actions/workflows/ato-publish.yml",
                    app.context.repository
                )));
                app.mode = UiMode::Failure;
                return;
            }

            if Instant::now() < monitor_ro.next_poll_at {
                return;
            }
            if let Some(monitor) = app.monitor_state.as_mut() {
                monitor.next_poll_at = Instant::now() + POLL_INTERVAL;
            }

            match fetch_matching_workflow_run(
                &app.context.repository,
                &release.commit_sha,
                &tag,
                &app.token.token,
            )
            .await
            {
                Ok(Some(run)) => {
                    if let Some(monitor) = app.monitor_state.as_mut() {
                        monitor.missing_cycles = 0;
                    }
                    app.run_url = Some(run.html_url.clone());
                    let status = run.status.clone().unwrap_or_else(|| "unknown".to_string());
                    let last_status = app
                        .monitor_state
                        .as_ref()
                        .and_then(|monitor| monitor.last_status.clone());
                    if last_status.as_deref() != Some(status.as_str()) {
                        app.push_log(format!("run {} status: {}", run.id, status));
                        if let Some(monitor) = app.monitor_state.as_mut() {
                            monitor.last_status = Some(status.clone());
                        }
                    }

                    if status == "completed" {
                        let conclusion = run.conclusion.unwrap_or_else(|| "unknown".to_string());
                        app.conclusion = Some(conclusion.clone());
                        if conclusion == "success" {
                            app.mode = UiMode::Success;
                            app.outcome = Some(PublishTuiOutcome::Success(PublishSuccessSummary {
                                capsule: app.context.manifest.name.clone(),
                                version: tag.trim_start_matches('v').to_string(),
                                run_url: run.html_url,
                            }));
                        } else {
                            match fetch_workflow_failure_details(
                                &app.context.repository,
                                run.id,
                                &app.token.token,
                            )
                            .await
                            {
                                Ok(details) => {
                                    app.failure_details = details
                                        .failed_jobs
                                        .into_iter()
                                        .map(|job| JobFailureSummary {
                                            name: job.name,
                                            status: job.status,
                                            failed_steps: job.failed_steps,
                                            log_excerpt: job.log_excerpt,
                                        })
                                        .collect();
                                    let messages: Vec<String> = app
                                        .failure_details
                                        .iter()
                                        .map(|job| {
                                            format!("failed job: {} ({})", job.name, job.status)
                                        })
                                        .collect();
                                    for message in messages {
                                        app.push_log(format!("{}", message));
                                    }
                                }
                                Err(err) => {
                                    app.push_log(format!("failed to fetch job details: {}", err));
                                }
                            }
                            app.mode = UiMode::Failure;
                            app.outcome = Some(app.build_failure_outcome(format!(
                                "CI workflow failed: {}",
                                conclusion
                            )));
                        }
                    }
                }
                Ok(None) => {
                    if let Some(monitor) = app.monitor_state.as_mut() {
                        monitor.missing_cycles += 1;
                    }
                    let last_status_missing = app
                        .monitor_state
                        .as_ref()
                        .and_then(|monitor| monitor.last_status.as_deref())
                        .is_none();
                    if last_status_missing {
                        app.push_log("waiting for workflow run to be queued...");
                        if let Some(monitor) = app.monitor_state.as_mut() {
                            monitor.last_status = Some("waiting".to_string());
                        }
                    }
                    let maybe_wait = app.monitor_state.as_ref().map(|monitor| {
                        (
                            monitor.missing_cycles,
                            monitor.started_at.elapsed().as_secs(),
                        )
                    });
                    if let Some((missing_cycles, elapsed_secs)) = maybe_wait {
                        if missing_cycles % 5 == 0 {
                            app.push_log(format!("still waiting ({}s elapsed)", elapsed_secs));
                        }
                    }
                }
                Err(err) => {
                    app.mode = UiMode::Failure;
                    app.outcome = Some(app.build_failure_outcome(format!(
                        "Failed to query GitHub Actions run: {}",
                        err
                    )));
                }
            }
        }
        UiMode::SelectVersion | UiMode::Confirm | UiMode::Success | UiMode::Failure => {}
    }
}

#[derive(Debug)]
struct GitReleaseState {
    base_head: String,
    commit_sha: Option<String>,
    tag: String,
    manifest_updated: bool,
    commit_created: bool,
    tag_created: bool,
    main_pushed: bool,
}

#[derive(Debug)]
struct GitReleaseResult {
    commit_sha: String,
}

fn execute_git_release_sequence(
    context: &PublishContext,
    selected_version: &Version,
    tag: &str,
) -> Result<GitReleaseResult> {
    let mut state = GitReleaseState {
        base_head: publish_preflight::run_git(&["rev-parse", "HEAD"])
            .context("Failed to resolve current HEAD")?,
        commit_sha: None,
        tag: tag.to_string(),
        manifest_updated: false,
        commit_created: false,
        tag_created: false,
        main_pushed: false,
    };

    let mut run_steps = || -> Result<()> {
        update_manifest_version(&context.manifest_path, selected_version)?;
        state.manifest_updated = true;

        run_git_checked(&["add", "capsule.toml"])?;
        if context.ci_workflow_refreshed {
            run_git_checked(&["add", CI_WORKFLOW_REL_PATH])?;
        }

        let commit_message = format!("chore: release v{}", selected_version);
        run_git_checked(&["commit", "-m", &commit_message])?;
        state.commit_created = true;
        state.commit_sha = Some(publish_preflight::run_git(&["rev-parse", "HEAD"])?);

        run_git_checked(&["tag", &state.tag])?;
        state.tag_created = true;

        run_git_checked(&["push", "origin", MAIN_BRANCH])?;
        state.main_pushed = true;

        run_git_checked(&["push", "origin", &state.tag])?;

        Ok(())
    };

    if let Err(err) = run_steps() {
        if !state.main_pushed {
            rollback_before_push_if_needed(&context.manifest_path, &context.manifest_raw, &state)?;
            return Err(
                err.context("Release sequence failed before push. Rolled back local tag/commit.")
            );
        }
        return Err(err.context(
            "Release sequence failed after push. Automatic rollback was skipped to avoid remote inconsistency.",
        ));
    }

    let commit_sha = state
        .commit_sha
        .clone()
        .context("Missing release commit SHA after git sequence")?;
    Ok(GitReleaseResult { commit_sha })
}

fn update_manifest_version(manifest_path: &Path, selected_version: &Version) -> Result<()> {
    let raw = fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;
    let mut doc: DocumentMut = raw
        .parse()
        .with_context(|| format!("Failed to parse {}", manifest_path.display()))?;

    if doc.get("version").is_some() {
        doc["version"] = value(selected_version.to_string());
    } else if doc
        .get("package")
        .and_then(|table| table.get("version"))
        .is_some()
    {
        doc["package"]["version"] = value(selected_version.to_string());
    } else {
        anyhow::bail!("No version field found in capsule.toml");
    }

    fs::write(manifest_path, doc.to_string())
        .with_context(|| format!("Failed to write {}", manifest_path.display()))?;

    Ok(())
}

fn ensure_tag_available(tag: &str) -> Result<()> {
    let local = publish_preflight::run_git(&["tag", "--list", tag])?;
    if !local.trim().is_empty() {
        anyhow::bail!("Tag '{}' already exists locally", tag);
    }

    let refspec = format!("refs/tags/{}", tag);
    let output = Command::new("git")
        .args(["ls-remote", "--tags", "origin", &refspec])
        .output()
        .with_context(|| "Failed to query remote tags from origin")?;
    if !output.status.success() {
        anyhow::bail!(
            "git ls-remote failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let remote = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !remote.is_empty() {
        anyhow::bail!("Tag '{}' already exists on origin", tag);
    }

    Ok(())
}

fn rollback_before_push_if_needed(
    manifest_path: &Path,
    original_manifest_raw: &str,
    state: &GitReleaseState,
) -> Result<()> {
    if state.tag_created {
        let _ = run_git_checked(&["tag", "-d", &state.tag]);
    }

    if state.commit_created {
        run_git_checked(&["reset", "--mixed", &state.base_head])
            .context("Failed to reset local release commit during rollback")?;
    } else if state.manifest_updated {
        fs::write(manifest_path, original_manifest_raw).with_context(|| {
            format!(
                "Failed to restore {} during rollback",
                manifest_path.display()
            )
        })?;
    }

    Ok(())
}

fn run_git_checked(args: &[&str]) -> Result<()> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("Failed to execute git {}", args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            anyhow::bail!("git {} failed", args.join(" "));
        }
        anyhow::bail!("git {} failed: {}", args.join(" "), stderr);
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WorkflowRunsResponse {
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct WorkflowRun {
    id: u64,
    html_url: String,
    head_sha: String,
    event: Option<String>,
    head_branch: Option<String>,
    status: Option<String>,
    conclusion: Option<String>,
}

async fn fetch_matching_workflow_run(
    repository: &str,
    head_sha: &str,
    tag: &str,
    token: &str,
) -> Result<Option<WorkflowRun>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("Failed to create GitHub API client")?;

    let url = format!(
        "https://api.github.com/repos/{}/actions/workflows/ato-publish.yml/runs?event=push&per_page=30",
        repository
    );

    let payload = client
        .get(&url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .with_context(|| "Failed to query GitHub Actions runs")?
        .error_for_status()
        .with_context(|| "Failed to query GitHub Actions runs")?
        .json::<WorkflowRunsResponse>()
        .await
        .with_context(|| "Failed to parse GitHub Actions response")?;

    let expected_ref = format!("refs/tags/{}", tag);
    let mut runs: Vec<WorkflowRun> = payload
        .workflow_runs
        .into_iter()
        .filter(|run| run.head_sha == head_sha)
        .filter(|run| {
            run.event
                .as_deref()
                .map(|event| event == "push")
                .unwrap_or(true)
        })
        .collect();

    runs.sort_by_key(|run| {
        let is_tag_match = run
            .head_branch
            .as_deref()
            .map(|branch| branch == tag || branch == expected_ref)
            .unwrap_or(false);
        std::cmp::Reverse(is_tag_match)
    });

    Ok(runs.into_iter().next())
}

#[derive(Debug)]
struct WorkflowFailureDetails {
    failed_jobs: Vec<JobFailureDetail>,
}

#[derive(Debug)]
struct JobFailureDetail {
    name: String,
    status: String,
    failed_steps: Vec<String>,
    log_excerpt: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowJobsResponse {
    jobs: Vec<WorkflowJob>,
}

#[derive(Debug, Deserialize)]
struct WorkflowJob {
    id: u64,
    name: String,
    status: Option<String>,
    conclusion: Option<String>,
    steps: Option<Vec<WorkflowStep>>,
}

#[derive(Debug, Deserialize)]
struct WorkflowStep {
    name: String,
    conclusion: Option<String>,
}

async fn fetch_workflow_failure_details(
    repository: &str,
    run_id: u64,
    token: &str,
) -> Result<WorkflowFailureDetails> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("Failed to create GitHub API client")?;

    let jobs_url = format!(
        "https://api.github.com/repos/{}/actions/runs/{}/jobs?per_page=100",
        repository, run_id
    );

    let jobs_payload = client
        .get(&jobs_url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .with_context(|| "Failed to query workflow jobs")?
        .error_for_status()
        .with_context(|| "Failed to query workflow jobs")?
        .json::<WorkflowJobsResponse>()
        .await
        .with_context(|| "Failed to parse workflow jobs response")?;

    let mut failed_jobs = Vec::new();

    for job in jobs_payload.jobs {
        let conclusion = job
            .conclusion
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        if conclusion == "success" {
            continue;
        }

        let failed_steps = job
            .steps
            .as_ref()
            .map(|steps| {
                steps
                    .iter()
                    .filter(|step| step.conclusion.as_deref() == Some("failure"))
                    .map(|step| step.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let log_excerpt = fetch_job_log_excerpt(&client, repository, job.id, token)
            .await
            .ok();

        failed_jobs.push(JobFailureDetail {
            name: job.name,
            status: format!(
                "{} / {}",
                job.status.unwrap_or_else(|| "unknown".to_string()),
                conclusion
            ),
            failed_steps,
            log_excerpt,
        });

        if failed_jobs.len() >= 2 {
            break;
        }
    }

    Ok(WorkflowFailureDetails { failed_jobs })
}

async fn fetch_job_log_excerpt(
    client: &reqwest::Client,
    repository: &str,
    job_id: u64,
    token: &str,
) -> Result<String> {
    let logs_url = format!(
        "https://api.github.com/repos/{}/actions/jobs/{}/logs",
        repository, job_id
    );

    let bytes = client
        .get(&logs_url)
        .header(reqwest::header::USER_AGENT, "ato-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .with_context(|| "Failed to download workflow job logs")?
        .error_for_status()
        .with_context(|| "Failed to download workflow job logs")?
        .bytes()
        .await
        .with_context(|| "Failed to read workflow job logs")?;

    extract_log_excerpt_from_zip(bytes.as_ref()).context("Failed to extract log excerpt")
}

fn extract_log_excerpt_from_zip(bytes: &[u8]) -> Result<String> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).context("Invalid zip log archive")?;

    let mut best_name = String::new();
    let mut best_content = String::new();
    let mut best_score = 0usize;

    for idx in 0..archive.len() {
        let mut file = archive.by_index(idx)?;
        if !file.is_file() {
            continue;
        }

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(&buf).to_string();
        if text.trim().is_empty() {
            continue;
        }

        let lowered = text.to_ascii_lowercase();
        let score = lowered.matches("error").count() + lowered.matches("failed").count();

        if score >= best_score {
            best_score = score;
            best_name = file.name().to_string();
            best_content = text;
        }
    }

    if best_content.is_empty() {
        anyhow::bail!("No readable text logs found in archive");
    }

    let lines: Vec<&str> = best_content.lines().collect();
    let from = lines.len().saturating_sub(80);
    let tail = lines[from..].join("\n");

    Ok(format!("file: {}\n{}", best_name, tail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_candidates_are_generated_from_current_version() {
        let current = Version::parse("1.2.3").unwrap();
        let c = version_candidates_from_semver(&current);
        assert_eq!(c.len(), 3);
        assert_eq!(c[0].version.to_string(), "1.2.4");
        assert_eq!(c[1].version.to_string(), "1.3.0");
        assert_eq!(c[2].version.to_string(), "2.0.0");
    }

    #[test]
    fn github_token_resolution_priority_is_deterministic() {
        let selected = choose_github_token(
            Some("cred".to_string()),
            Some("gh".to_string()),
            Some("github".to_string()),
            Some("cli".to_string()),
        )
        .unwrap();
        assert_eq!(selected.token, "cred");
        assert_eq!(selected.source, GitHubTokenSource::Credentials);

        let selected = choose_github_token(
            None,
            Some("gh".to_string()),
            Some("github".to_string()),
            Some("cli".to_string()),
        )
        .unwrap();
        assert_eq!(selected.token, "gh");
        assert_eq!(selected.source, GitHubTokenSource::EnvGhToken);

        let selected = choose_github_token(None, None, Some("github".to_string()), None).unwrap();
        assert_eq!(selected.token, "github");
        assert_eq!(selected.source, GitHubTokenSource::EnvGithubToken);

        let selected = choose_github_token(None, None, None, Some("cli".to_string())).unwrap();
        assert_eq!(selected.token, "cli");
        assert_eq!(selected.source, GitHubTokenSource::GhCli);

        assert!(choose_github_token(None, None, None, None).is_none());
    }
}
