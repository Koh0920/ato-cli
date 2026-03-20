use anyhow::{Context, Result};
use capsule_core::execution_plan::error::AtoExecutionError;
use capsule_core::CapsuleReporter;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use colored::Colorize;
use serde_json::json;
use std::cmp::Ordering;
use std::io::{self, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn print_animated_logo() {
    let logo = r#"
    ___    __       
   /   |  / /_____  
  / /| | / __/ __ \ 
 / ___ |/ /_/ /_/ / 
/_/  |_|\__/\____/  
"#;

    for line in logo.lines() {
        println!("{}", line.cyan().bold());
        io::stdout().flush().unwrap();
        thread::sleep(Duration::from_millis(30));
    }
    println!();
}

const DEFAULT_RUN_REGISTRY_URL: &str = "https://api.ato.run";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum EnforcementMode {
    Strict,
    BestEffort,
}

impl EnforcementMode {
    fn as_str(self) -> &'static str {
        match self {
            EnforcementMode::Strict => "strict",
            EnforcementMode::BestEffort => "best_effort",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CompatibilityFallbackBackend {
    Host,
}

impl CompatibilityFallbackBackend {
    fn as_str(self) -> &'static str {
        match self {
            CompatibilityFallbackBackend::Host => "host",
        }
    }
}

struct SidecarCleanup {
    sidecar: Option<common::sidecar::SidecarHandle>,
    reporter: std::sync::Arc<reporters::CliReporter>,
}

impl SidecarCleanup {
    fn new(
        sidecar: Option<common::sidecar::SidecarHandle>,
        reporter: std::sync::Arc<reporters::CliReporter>,
    ) -> Self {
        Self { sidecar, reporter }
    }

    fn stop_now(&mut self) {
        if let Some(sidecar) = self.sidecar.take() {
            if let Err(err) = sidecar.stop() {
                let _ = futures::executor::block_on(
                    self.reporter
                        .warn(format!("⚠️  Failed to stop sidecar: {}", err)),
                );
            }
        }
    }
}

impl Drop for SidecarCleanup {
    fn drop(&mut self) {
        self.stop_now();
    }
}

mod artifact_hash;
mod ato_error_jsonl;
mod auth;
mod binding;
mod build_validate_orchestration;
mod capsule_archive;
mod catalog_registry_orchestration;
mod commands;
mod common;
mod consent_store;
mod data_injection;
mod diagnostics;
mod engine_manager;
mod env;
mod error_codes;
mod executors;
mod external_capsule;
mod gen_ci;
mod guest_protocol;
mod inference_feedback;
mod ingress_proxy;
mod init;
mod install;
mod install_command_orchestration;
mod ipc;
mod keygen;
mod local_input;
mod native_delivery;
mod new;
mod payload_guard;
mod preview;
mod process_manager;
mod profile;
mod progressive_ui;
mod provisioner;
mod publish_artifact;
mod publish_ci;
mod publish_command_orchestration;
mod publish_dry_run;
mod publish_official;
mod publish_preflight;
mod publish_prepare;
mod publish_private;
mod registry;
mod registry_delete;
mod registry_http;
mod registry_serve;
mod registry_store;
mod registry_yank;
mod reporters;
mod run_install_orchestration;
mod runtime_manager;
mod runtime_overrides;
mod runtime_tree;
mod scaffold;
mod search;
mod sign;
mod skill;
mod skill_resolver;
mod source;
mod state;
mod tui;
mod verify;

fn cli_styles() -> clap::builder::Styles {
    use clap::builder::styling::{AnsiColor, Effects};
    clap::builder::Styles::styled()
        .header(AnsiColor::Cyan.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Yellow.on_default())
}

#[derive(Parser)]
#[command(name = "ato")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(styles = cli_styles())]
#[command(help_template = "\
{about-with-newline}
Usage: {usage}

Primary Commands:
  run      Execute a capsule or SKILL.md in a strict Zero-Trust sandbox
  build    Pack a project into an immutable .capsule archive
  publish  Publish capsule artifacts to a registry
  install  Install a verified package from the registry
  search   Search the registry for agent skills and packages
  init     Analyze the current project and print an agent-ready capsule.toml prompt

Management:
  ps       List running capsules
  stop     Stop a running capsule
  logs     Show logs of a running capsule
    state    Inspect or register persistent state bindings
    binding  Inspect or register host-side service bindings

Auth:
  login    Login to Ato registry
  logout   Logout
  whoami   Show current authentication status

Advanced Commands:
  inspect  Inspect capsule metadata and runtime requirements
  fetch    Fetch an artifact into local cache for debugging or manual workflows
  finalize Perform local derivation for a fetched native artifact
  project  Add a finalized app to launcher surfaces
  unproject Remove a launcher projection
  key      Manage signing keys
  config   Manage configuration (registry, engine, source)
  gen-ci   Generate GitHub Actions workflow for OIDC CI publish
  registry Manage registry commands (resolve/list/cache/serve)

Options:
{options}

Use 'ato help <command>' for more information.
")]
struct Cli {
    /// Path to nacelle engine binary (overrides NACELLE_PATH)
    #[arg(long)]
    nacelle: Option<PathBuf>,

    /// Emit machine-readable JSON output
    #[arg(long)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(
        next_help_heading = "Primary Commands",
        about = "Run a capsule app or local project"
    )]
    Run {
        /// Local path (./, ../, ~/, /...), store scoped ID (publisher/slug), or GitHub repo (github.com/owner/repo). Default: current directory
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Resolve SKILL.md by skill name from standard locations and run it safely
        #[arg(long = "skill", conflicts_with = "from_skill")]
        skill: Option<String>,

        /// Run from SKILL.md by translating frontmatter into a fail-closed capsule execution plan
        #[arg(long = "from-skill", conflicts_with = "skill")]
        from_skill: Option<PathBuf>,

        /// Target label to execute (e.g. static, cli, widget)
        #[arg(short = 't', long = "target")]
        target: Option<String>,

        /// Run in development mode (foreground) with hot-reloading on file changes
        #[arg(long)]
        watch: bool,

        /// Run in background mode (detached)
        #[arg(long)]
        background: bool,

        /// Path to nacelle engine binary (overrides NACELLE_PATH)
        #[arg(long)]
        nacelle: Option<PathBuf>,

        /// Registry URL for auto-install when app-id is not installed (default: https://api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Explicitly bind a manifest [state.<name>] entry using STATE=/absolute/path or STATE=state-...
        #[arg(long = "state", value_name = "STATE=/ABS/PATH|STATE=state-...")]
        state: Vec<String>,

        /// Inject external data binding using KEY=VALUE for targets that declare [external_injection]
        #[arg(long = "inject", value_name = "KEY=VALUE")]
        inject: Vec<String>,

        /// Network enforcement mode
        #[arg(long, value_enum, default_value_t = EnforcementMode::Strict)]
        enforcement: EnforcementMode,

        /// Explicitly allow Tier2 (python/native) execution via native OS sandbox
        #[arg(long = "sandbox", default_value_t = false)]
        sandbox_mode: bool,

        /// Legacy alias for `--sandbox`
        #[arg(long = "unsafe", hide = true, default_value_t = false)]
        unsafe_mode_legacy: bool,

        /// Legacy alias for `--sandbox`
        #[arg(long = "unsafe-bypass-sandbox", hide = true, default_value_t = false)]
        unsafe_bypass_sandbox_legacy: bool,

        /// Dangerously bypass all Ato runtime permission/sandbox barriers (host-native execution)
        #[arg(
            short = 'U',
            long = "dangerously-skip-permissions",
            default_value_t = false
        )]
        dangerously_skip_permissions: bool,

        /// Run with an explicit compatibility fallback backend instead of the standard runtime path
        #[arg(long = "compatibility-fallback", value_enum)]
        compatibility_fallback: Option<CompatibilityFallbackBackend>,

        /// Skip prompt and auto-install when app-id is not installed
        #[arg(short = 'y', long = "yes", default_value_t = false)]
        yes: bool,

        /// Keep failed GitHub checkout artifacts and generated manifests for debugging
        #[arg(long, hide = true, default_value_t = false)]
        keep_failed_artifacts: bool,

        /// Allow installing/running unverified signatures in non-production environments
        #[arg(long, default_value_t = false)]
        allow_unverified: bool,
    },

    #[command(
        next_help_heading = "Primary Commands",
        about = "Install a package from the store"
    )]
    Install {
        /// Capsule scoped ID (publisher/slug)
        #[arg(required_unless_present = "from_gh_repo")]
        slug: Option<String>,

        /// Build and install directly from a public GitHub repository
        #[arg(
            long = "from-gh-repo",
            value_name = "REPOSITORY",
            conflicts_with = "slug"
        )]
        from_gh_repo: Option<String>,

        /// Registry URL (default: api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Specific version to install
        #[arg(long)]
        version: Option<String>,

        /// Set as default handler for supported content types
        #[arg(long, default_value_t = false)]
        default: bool,

        /// Skip prompts and approve local finalize / projection
        #[arg(short = 'y', long = "yes", default_value_t = false)]
        yes: bool,

        /// Deprecated legacy flag (always rejected)
        #[arg(long = "skip-verify", hide = true, default_value_t = false)]
        skip_verify_legacy: bool,

        /// Allow installing unverified signatures in non-production environments
        #[arg(long, default_value_t = false)]
        allow_unverified: bool,

        /// Output directory (default: ~/.ato/store/)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Create a launcher projection after install
        #[arg(long, default_value_t = false, conflicts_with = "no_project")]
        project: bool,

        /// Do not prompt for or create a launcher projection
        #[arg(long, default_value_t = false, conflicts_with = "project")]
        no_project: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,

        /// Keep failed GitHub checkout artifacts and generated manifests for debugging
        #[arg(long, hide = true, default_value_t = false)]
        keep_failed_artifacts: bool,
    },

    #[command(
        next_help_heading = "Primary Commands",
        about = "Analyze the current project and print an agent-ready capsule.toml prompt"
    )]
    Init,

    #[command(
        next_help_heading = "Primary Commands",
        about = "Build project into a capsule archive"
    )]
    Build {
        /// Directory containing capsule.toml (default: ".")
        #[arg(default_value = ".")]
        dir: PathBuf,

        /// Initialize capsule.toml interactively if not found
        #[arg(long)]
        init: bool,

        /// Path to signing key (optional)
        #[arg(long)]
        key: Option<PathBuf>,

        /// Network enforcement mode
        #[arg(long, value_enum, default_value_t = EnforcementMode::Strict)]
        enforcement: EnforcementMode,

        /// Create self-extracting executable installer (includes nacelle runtime)
        #[arg(long)]
        standalone: bool,

        /// Allow building payloads larger than 200MB
        #[arg(long, default_value_t = false)]
        force_large_payload: bool,

        /// Keep failed build artifacts when smoke test fails
        #[arg(long, default_value_t = false)]
        keep_failed_artifacts: bool,

        /// Print per-phase build timings
        #[arg(long, default_value_t = false)]
        timings: bool,

        /// Disallow fallback when source_digest/CAS(v3 path) is unavailable
        #[arg(long, default_value_t = false)]
        strict_v3: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Validate capsule build/run inputs without executing"
    )]
    Validate {
        /// Directory containing capsule.toml or the manifest file itself (default: ".")
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Update ato CLI to the latest version"
    )]
    Update,

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Inspect capsule metadata and runtime requirements"
    )]
    Inspect {
        #[command(subcommand)]
        command: InspectCommands,
    },

    #[command(
        next_help_heading = "Primary Commands",
        about = "Search the store for packages"
    )]
    Search {
        /// Search query (e.g., "note", "ai chat")
        query: Option<String>,

        /// Filter by category
        #[arg(long)]
        category: Option<String>,

        /// Filter by tag (repeatable, comma-separated supported)
        #[arg(long = "tag", value_delimiter = ',')]
        tags: Vec<String>,

        /// Maximum number of results (default: 20, max: 50)
        #[arg(long)]
        limit: Option<usize>,

        /// Pagination cursor for next page
        #[arg(long)]
        cursor: Option<String>,

        /// Registry URL (default: https://api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,

        /// Disable interactive TUI even when running in TTY
        #[arg(long, default_value_t = false)]
        no_tui: bool,

        /// Show selected capsule's capsule.toml in the TUI right panel
        #[arg(long, default_value_t = false)]
        show_manifest: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Fetch an artifact into local cache for debugging or manual workflows"
    )]
    Fetch {
        /// Capsule ref (`publisher/slug[@version]` or `localhost:8080/slug:version`)
        capsule_ref: String,

        /// Registry URL override (or embed registry in `capsule_ref`)
        #[arg(long)]
        registry: Option<String>,

        /// Specific version to fetch (or use publisher/slug@version)
        #[arg(long)]
        version: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Perform local derivation for a fetched native artifact. Most users should use `ato install`."
    )]
    Finalize {
        /// Path to fetched artifact directory created by `ato fetch`
        fetched_artifact_dir: PathBuf,

        /// Allow external finalize execution (`codesign`) for this PoC
        #[arg(long, default_value_t = false)]
        allow_external_finalize: bool,

        /// Output directory for derived artifacts
        #[arg(long)]
        output_dir: PathBuf,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Add a finalized app to launcher surfaces (experimental)"
    )]
    Project {
        /// Path to a finalized local derived artifact directory created by `ato finalize`
        derived_app_path: Option<PathBuf>,

        /// Override launcher surface directory (default: host-specific launcher dir)
        #[arg(long)]
        launcher_dir: Option<PathBuf>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,

        #[command(subcommand)]
        command: Option<ProjectCommands>,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Remove an experimental launcher projection without mutating the finalized artifact"
    )]
    Unproject {
        /// Projection ID, projected symlink path, or finalized derived .app path
        projection_ref: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(next_help_heading = "Management", about = "List running capsules")]
    Ps {
        /// Show all capsules including stopped ones
        #[arg(long, default_value_t = false)]
        all: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(next_help_heading = "Management", about = "Stop a running capsule")]
    Stop {
        /// Capsule ID (from ps output)
        #[arg(long)]
        id: Option<String>,

        /// Capsule name (partial match)
        #[arg(long)]
        name: Option<String>,

        /// Stop all capsules matching the name
        #[arg(long, default_value_t = false)]
        all: bool,

        /// Force kill (SIGKILL) instead of graceful shutdown (SIGTERM)
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    #[command(
        next_help_heading = "Management",
        about = "Show logs of a running capsule"
    )]
    Logs {
        /// Capsule ID (from ps output)
        #[arg(long)]
        id: Option<String>,

        /// Capsule name (partial match)
        #[arg(long)]
        name: Option<String>,

        /// Follow log output in real-time
        #[arg(long, default_value_t = false)]
        follow: bool,

        /// Show last N lines
        #[arg(long)]
        tail: Option<usize>,
    },

    #[command(
        next_help_heading = "Management",
        about = "Inspect or register persistent state bindings"
    )]
    State {
        #[command(subcommand)]
        command: StateCommands,
    },

    #[command(
        next_help_heading = "Management",
        about = "Inspect or register host-side service bindings"
    )]
    Binding {
        #[command(subcommand)]
        command: BindingCommands,
    },

    #[command(next_help_heading = "Auth", about = "Login to Ato registry")]
    Login {
        /// GitHub Personal Access Token (legacy fallback, scope: read:user)
        #[arg(long)]
        token: Option<String>,

        /// Do not open browser automatically; print activation URL for another device/session
        #[arg(long, default_value_t = false)]
        headless: bool,
    },

    #[command(next_help_heading = "Auth", about = "Logout")]
    Logout,

    #[command(
        next_help_heading = "Auth",
        about = "Show current authentication status"
    )]
    Whoami,

    #[command(next_help_heading = "Advanced Commands", about = "Manage signing keys")]
    Key {
        #[command(subcommand)]
        command: KeyCommands,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Manage configuration (registry, engine)"
    )]
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Publish capsule (default: My Dock direct upload, official registry: CI-first)"
    )]
    Publish {
        /// Registry URL override (default: My Dock when logged in; official Store remains CI-first)
        #[arg(long)]
        registry: Option<String>,

        /// Use prebuilt .capsule artifact (skip repackaging for private registry publish)
        #[arg(long, value_name = "PATH", conflicts_with_all = ["ci", "dry_run"])]
        artifact: Option<PathBuf>,

        /// Explicit scoped ID for artifact publish (publisher/slug)
        #[arg(
            long,
            value_name = "PUBLISHER/SLUG",
            conflicts_with_all = ["ci", "dry_run"],
            requires = "artifact"
        )]
        scoped_id: Option<String>,

        /// Allow idempotent success when same version already exists with identical sha256
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        allow_existing: bool,

        /// Run prepare phase
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        prepare: bool,

        /// Run build phase
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        build: bool,

        /// Run deploy phase
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        deploy: bool,

        /// Use legacy default phases (prepare/build/deploy) for official registry publish
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        legacy_full_publish: bool,

        /// Allow publishing payloads larger than 200MB
        #[arg(long, default_value_t = false)]
        force_large_payload: bool,

        /// Auto-fix official CI workflow once, then rerun diagnostics exactly once
        #[arg(long, default_value_t = false, conflicts_with_all = ["ci", "dry_run"])]
        fix: bool,

        /// Publish from GitHub Actions with OIDC token (CI-only mode)
        #[arg(long, conflicts_with = "dry_run")]
        ci: bool,

        /// Validate local capsule build inputs without publishing
        #[arg(long, conflicts_with = "ci")]
        dry_run: bool,

        /// Disable interactive TUI and show CI guidance instead
        #[arg(long, conflicts_with_all = ["ci", "dry_run", "json"])]
        no_tui: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Generate fixed GitHub Actions workflow for OIDC CI publish"
    )]
    GenCi,

    #[command(hide = true)]
    Engine {
        #[command(subcommand)]
        command: EngineCommands,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Manage registry commands (resolve/list/cache/serve)"
    )]
    Registry {
        #[command(subcommand)]
        command: RegistryCommands,
    },

    #[command(hide = true)]
    Setup {
        /// Engine name to install (default: nacelle)
        #[arg(long, default_value = "nacelle")]
        engine: String,

        /// Engine version (default: latest)
        #[arg(long)]
        version: Option<String>,

        /// Skip SHA256 verification
        #[arg(long, default_value_t = false)]
        skip_verify: bool,
    },

    #[command(hide = true)]
    Open {
        /// Path to a .capsule archive or directory containing capsule.toml
        #[arg()]
        path: PathBuf,

        /// Target label to execute (e.g. static, cli, widget)
        #[arg(short = 't', long = "target")]
        target: Option<String>,

        /// Run in development mode (foreground) with hot-reloading on file changes
        #[arg(long)]
        watch: bool,

        /// Run in background mode (detached)
        #[arg(long)]
        background: bool,

        /// Path to nacelle engine binary (overrides NACELLE_PATH)
        #[arg(long)]
        nacelle: Option<PathBuf>,

        /// Registry URL for auto-install when app-id is not installed (default: https://api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Explicitly bind a manifest [state.<name>] entry using STATE=/absolute/path or STATE=state-...
        #[arg(long = "state", value_name = "STATE=/ABS/PATH|STATE=state-...")]
        state: Vec<String>,

        /// Inject external data binding using KEY=VALUE for targets that declare [external_injection]
        #[arg(long = "inject", value_name = "KEY=VALUE")]
        inject: Vec<String>,

        /// Network enforcement mode
        #[arg(long, value_enum, default_value_t = EnforcementMode::Strict)]
        enforcement: EnforcementMode,

        /// Explicitly allow Tier2 (python/native) execution via native OS sandbox
        #[arg(long = "sandbox", default_value_t = false)]
        sandbox_mode: bool,

        /// Legacy alias for `--sandbox`
        #[arg(long = "unsafe", hide = true, default_value_t = false)]
        unsafe_mode_legacy: bool,

        /// Legacy alias for `--sandbox`
        #[arg(long = "unsafe-bypass-sandbox", hide = true, default_value_t = false)]
        unsafe_bypass_sandbox_legacy: bool,

        /// Dangerously bypass all Ato runtime permission/sandbox barriers (host-native execution)
        #[arg(
            short = 'U',
            long = "dangerously-skip-permissions",
            default_value_t = false
        )]
        dangerously_skip_permissions: bool,

        /// Run with an explicit compatibility fallback backend instead of the standard runtime path
        #[arg(long = "compatibility-fallback", value_enum)]
        compatibility_fallback: Option<CompatibilityFallbackBackend>,

        /// Skip prompt and auto-install when app-id is not installed
        #[arg(short = 'y', long = "yes", default_value_t = false)]
        yes: bool,
    },

    #[command(hide = true)]
    New {
        /// Project name
        name: String,

        /// Template type: python, node, hono, rust, go, shell
        #[arg(long, default_value = "python")]
        template: String,
    },

    #[command(hide = true)]
    Keygen {
        /// Output base path (default: ./private.key and ./public.key)
        #[arg(long)]
        out: Option<PathBuf>,

        /// Overwrite existing keys
        #[arg(long, default_value_t = false)]
        force: bool,

        /// Output keys in StoredKey JSON format
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    #[command(hide = true)]
    Pack {
        /// Directory containing capsule.toml (default: ".")
        #[arg(default_value = ".")]
        dir: PathBuf,

        /// Initialize capsule.toml interactively if not found
        #[arg(long)]
        init: bool,

        /// Path to signing key (optional)
        #[arg(long)]
        key: Option<PathBuf>,

        /// Network enforcement mode
        #[arg(long, value_enum, default_value_t = EnforcementMode::Strict)]
        enforcement: EnforcementMode,

        /// Create self-extracting executable installer (includes nacelle runtime)
        #[arg(long)]
        standalone: bool,

        /// Allow building payloads larger than 200MB
        #[arg(long, hide = true, default_value_t = false)]
        force_large_payload: bool,

        /// Keep failed build artifacts when smoke test fails
        #[arg(long, hide = true, default_value_t = false)]
        keep_failed_artifacts: bool,

        /// Print per-phase build timings
        #[arg(long, hide = true, default_value_t = false)]
        timings: bool,

        /// Disallow fallback when source_digest/CAS(v3 path) is unavailable
        #[arg(long, hide = true, default_value_t = false)]
        strict_v3: bool,
    },

    #[command(hide = true)]
    Scaffold {
        #[command(subcommand)]
        command: ScaffoldCommands,
    },

    #[command(hide = true)]
    Sign {
        /// File to sign
        target: PathBuf,

        /// Path to the secret key
        #[arg(long)]
        key: PathBuf,

        /// Output signature path (default: <target>.sig)
        #[arg(long)]
        out: Option<PathBuf>,
    },

    #[command(hide = true)]
    Verify {
        /// File to verify (the artifact, not the .sig file)
        target: PathBuf,

        /// Path to the signature file (default: <target>.sig)
        #[arg(long)]
        sig: Option<PathBuf>,

        /// Expected signer DID or developer key (optional, for additional check)
        #[arg(long)]
        signer: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(hide = true)]
    Profile {
        #[command(subcommand)]
        command: ProfileCommands,
    },

    #[command(hide = true)]
    Package {
        #[command(subcommand)]
        command: PackageCommands,
    },

    #[command(hide = true)]
    Source {
        #[command(subcommand)]
        command: SourceCommands,
    },

    #[command(hide = true)]
    Close {
        /// Capsule ID (from ps output)
        #[arg(long)]
        id: Option<String>,

        /// Capsule name (partial match)
        #[arg(long)]
        name: Option<String>,

        /// Stop all capsules matching the name
        #[arg(long, default_value_t = false)]
        all: bool,

        /// Force kill (SIGKILL) instead of graceful shutdown (SIGTERM)
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    #[command(hide = true)]
    Guest {
        /// Path to a .sync archive
        #[arg()]
        sync_path: PathBuf,
    },

    #[command(hide = true)]
    Ipc {
        #[command(subcommand)]
        command: IpcCommands,
    },

    #[command(hide = true)]
    Auth,
}

#[derive(Subcommand)]
enum InspectCommands {
    #[command(about = "Inspect runtime requirements from capsule.toml")]
    Requirements {
        /// Local path or scoped store ID (publisher/slug)
        target: String,

        /// Registry URL override for remote inspection
        #[arg(long)]
        registry: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ProjectCommands {
    #[command(
        about = "List experimental projection state and detect broken projections read-only"
    )]
    Ls {
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum KeyCommands {
    /// Generate a new signing keypair
    Gen {
        /// Output base path (default: ./private.key and ./public.key)
        #[arg(long)]
        out: Option<PathBuf>,

        /// Overwrite existing keys
        #[arg(long, default_value_t = false)]
        force: bool,

        /// Output keys in StoredKey JSON format
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Sign an existing artifact
    Sign {
        /// File to sign
        target: PathBuf,

        /// Path to the secret key
        #[arg(long)]
        key: PathBuf,

        /// Output signature path (default: <target>.sig)
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Verify a signed artifact
    Verify {
        /// File to verify (the artifact, not the .sig file)
        target: PathBuf,

        /// Path to the signature file (default: <target>.sig)
        #[arg(long)]
        sig: Option<PathBuf>,

        /// Expected signer DID or developer key (optional, for additional check)
        #[arg(long)]
        signer: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Engine configuration
    Engine {
        #[command(subcommand)]
        command: ConfigEngineCommands,
    },

    /// Registry configuration
    Registry {
        #[command(subcommand)]
        command: ConfigRegistryCommands,
    },
}

#[derive(Subcommand)]
enum ConfigEngineCommands {
    /// Show engine capabilities (JSON)
    Features,

    /// Register a nacelle engine binary (writes ~/.ato/config.toml)
    Register {
        /// Registration name (e.g. "default" or "my-custom-nacelle")
        #[arg(long)]
        name: String,

        /// Path to nacelle engine binary (if omitted, uses NACELLE_PATH)
        #[arg(long)]
        path: Option<PathBuf>,

        /// Set this registration as the default engine
        #[arg(long, default_value_t = false)]
        default: bool,
    },

    /// Download and install an engine
    Install {
        /// Engine name to install (default: nacelle)
        #[arg(long, default_value = "nacelle")]
        engine: String,

        /// Engine version (default: latest)
        #[arg(long)]
        version: Option<String>,

        /// Skip SHA256 verification
        #[arg(long, default_value_t = false)]
        skip_verify: bool,
    },
}

#[derive(Subcommand)]
enum ConfigRegistryCommands {
    /// Resolve registry for a domain
    Resolve {
        /// Domain to resolve (e.g., example.com)
        domain: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// List configured registries
    List {
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Clear registry cache
    ClearCache,
}

#[derive(Subcommand)]
enum IpcCommands {
    /// Show status of running IPC services
    Status {
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Start an IPC service from a capsule directory
    Start {
        /// Path to capsule directory or capsule.toml
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Stop a running IPC service
    Stop {
        /// Service name to stop
        #[arg(long)]
        name: String,

        /// Force kill (SIGKILL) instead of graceful shutdown (SIGTERM)
        #[arg(long, default_value_t = false)]
        force: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Validate and send a JSON-RPC invoke request
    Invoke {
        /// Path to capsule directory or capsule.toml
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Override exported service name
        #[arg(long)]
        service: Option<String>,

        /// Method name to invoke
        #[arg(long)]
        method: String,

        /// JSON arguments payload
        #[arg(long)]
        args: String,

        /// JSON-RPC request id
        #[arg(long, default_value = "invoke-1")]
        id: String,

        /// Maximum serialized message size in bytes
        #[arg(long)]
        max_message_size: Option<usize>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ScaffoldCommands {
    /// Generate a Dockerfile + .dockerignore for running a self-extracting bundle
    Docker {
        /// Path to capsule.toml
        #[arg(long, default_value = "capsule.toml")]
        manifest: PathBuf,

        /// Output Dockerfile path (default: <manifest dir>/Dockerfile)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Output directory (default: manifest directory). Ignored if --output is set.
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Overwrite existing files
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum ProfileCommands {
    /// Create a new profile.sync
    Create {
        /// Display name
        #[arg(long)]
        name: String,

        /// Short bio
        #[arg(long)]
        bio: Option<String>,

        /// Path to avatar image (png/jpg)
        #[arg(long)]
        avatar: Option<PathBuf>,

        /// Path to signing key (JSON format)
        #[arg(long)]
        key: PathBuf,

        /// Output path (default: ./profile.sync)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Website URL
        #[arg(long)]
        website: Option<String>,

        /// GitHub username
        #[arg(long)]
        github: Option<String>,

        /// Twitter/X handle
        #[arg(long)]
        twitter: Option<String>,
    },

    /// Show profile info from a profile.sync file
    Show {
        /// Path to profile.sync
        #[arg()]
        path: PathBuf,

        /// Emit JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum EngineCommands {
    /// Show engine capabilities (JSON)
    Features,

    /// Register a nacelle engine binary (writes ~/.ato/config.toml)
    Register {
        /// Registration name (e.g. "default" or "my-custom-nacelle")
        #[arg(long)]
        name: String,

        /// Path to nacelle engine binary (if omitted, uses NACELLE_PATH)
        #[arg(long)]
        path: Option<PathBuf>,

        /// Set this registration as the default engine
        #[arg(long, default_value_t = false)]
        default: bool,
    },
}

#[derive(Subcommand)]
enum RegistryCommands {
    /// Resolve registry for a domain
    Resolve {
        /// Domain to resolve (e.g., example.com)
        domain: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// List configured registries
    List {
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Clear registry cache
    ClearCache,

    /// Start local HTTP registry server for offline development
    Serve {
        /// Listen port
        #[arg(long, default_value_t = 8787)]
        port: u16,

        /// Data directory for local registry state
        #[arg(long, default_value = "~/.ato/local-registry")]
        data_dir: String,

        /// Listen host (non-loopback requires --auth-token)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Bearer token required for write API (recommended when exposing non-loopback host)
        #[arg(long)]
        auth_token: Option<String>,
    },
}

#[derive(Subcommand)]
enum StateCommands {
    /// List registered persistent states
    #[command(visible_alias = "ls")]
    List {
        /// Filter by owner scope
        #[arg(long)]
        owner_scope: Option<String>,

        /// Filter by manifest state name
        #[arg(long)]
        state_name: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Inspect one persistent state by state-id or ato-state:// URI
    Inspect {
        /// State reference (`state-...` or `ato-state://state-...`)
        state_ref: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Register a persistent state from a manifest contract
    Register {
        /// Path to capsule directory or capsule.toml
        #[arg(long, default_value = ".")]
        manifest: PathBuf,

        /// State name from [state.<name>]
        #[arg(long = "name")]
        state_name: String,

        /// Absolute host directory to bind to this state contract
        #[arg(long = "path", value_name = "/ABS/PATH")]
        path: PathBuf,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum BindingCommands {
    /// List registered host-side service bindings
    #[command(visible_alias = "ls")]
    List {
        /// Filter by owner scope
        #[arg(long)]
        owner_scope: Option<String>,

        /// Filter by service name
        #[arg(long)]
        service_name: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Inspect one host-side service binding by binding-id
    Inspect {
        /// Binding reference (`binding-...`)
        binding_ref: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Resolve a host-side service binding by owner scope, service, and kind
    Resolve {
        /// Binding owner scope
        #[arg(long)]
        owner_scope: String,

        /// Service name from [services.<name>]
        #[arg(long)]
        service_name: String,

        /// Binding kind to resolve
        #[arg(long, default_value = "ingress")]
        binding_kind: String,

        /// Optional caller service for allow_from-restricted bindings
        #[arg(long)]
        caller_service: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Explicitly bootstrap TLS assets and optional trust installation for an ingress binding
    BootstrapTls {
        /// Binding reference (`binding-...`)
        #[arg(long = "binding")]
        binding_ref: String,

        /// Attempt to install the generated certificate into the local user trust store
        #[arg(long, default_value_t = false)]
        install_system_trust: bool,

        /// Skip the interactive consent prompt after reviewing the trust action
        #[arg(short = 'y', long = "yes", default_value_t = false)]
        yes: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Run a host-side ingress reverse proxy for a registered binding
    ServeIngress {
        /// Binding reference (`binding-...`)
        #[arg(long = "binding")]
        binding_ref: String,

        /// Path to capsule directory or capsule.toml used to derive the upstream port
        #[arg(long, default_value = ".")]
        manifest: PathBuf,

        /// Optional upstream URL override
        #[arg(long)]
        upstream_url: Option<String>,
    },

    /// Register a host-side ingress binding from a manifest service
    RegisterIngress {
        /// Path to capsule directory or capsule.toml
        #[arg(long, default_value = ".")]
        manifest: PathBuf,

        /// Service name from [services.<name>]
        #[arg(long)]
        service_name: String,

        /// Host-side ingress URL (http:// or https://)
        #[arg(long)]
        url: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Register a local cross-capsule service binding for a separately launched service
    RegisterService {
        /// Path to capsule directory or capsule.toml
        #[arg(long, default_value = ".")]
        manifest: PathBuf,

        /// Service name from [services.<name>]
        #[arg(long)]
        service_name: String,

        /// Loopback URL for the running local service (http://localhost:PORT or http://127.0.0.1:PORT)
        #[arg(long)]
        url: Option<String>,

        /// Running local process id to derive manifest and target metadata from
        #[arg(long)]
        process_id: Option<String>,

        /// Override the loopback port when registering from a running process
        #[arg(long)]
        port: Option<u16>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Auto-register all eligible local service bindings from a running process
    SyncProcess {
        /// Running local process id
        #[arg(long)]
        process_id: String,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum SourceCommands {
    /// Show sync run status for a source
    SyncStatus {
        /// Source ID
        #[arg(long = "source-id")]
        source_id: String,

        /// Sync run ID
        #[arg(long = "sync-run-id")]
        sync_run_id: String,

        /// Registry URL
        #[arg(long)]
        registry: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
    /// Trigger rebuild/re-sign flow for a source
    Rebuild {
        /// Source ID
        #[arg(long = "source-id")]
        source_id: String,

        /// Optional ref (branch/tag/SHA)
        #[arg(long = "ref", alias = "reference")]
        reference: Option<String>,

        /// Wait and fetch status after trigger
        #[arg(long, default_value_t = false)]
        wait: bool,

        /// Registry URL
        #[arg(long)]
        registry: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum PackageCommands {
    /// Search published packages in the store
    Search {
        /// Search query (e.g., "note", "ai chat")
        query: Option<String>,

        /// Filter by category
        #[arg(long)]
        category: Option<String>,

        /// Filter by tag (repeatable, comma-separated supported)
        #[arg(long = "tag", value_delimiter = ',')]
        tags: Vec<String>,

        /// Maximum number of results (default: 20, max: 50)
        #[arg(long)]
        limit: Option<usize>,

        /// Pagination cursor for next page
        #[arg(long)]
        cursor: Option<String>,

        /// Registry URL (default: https://api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,

        /// Disable interactive TUI even when running in TTY
        #[arg(long, default_value_t = false)]
        no_tui: bool,

        /// Show selected capsule's capsule.toml in the TUI right panel
        #[arg(long, default_value_t = false)]
        show_manifest: bool,
    },
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_mode = args.iter().any(|arg| arg == "--json");
    let command_context = diagnostics::detect_command_context(&args);

    if let Err(err) = run() {
        if json_mode && commands::inspect::try_emit_json_error(&err) {
            std::process::exit(error_codes::EXIT_USER_ERROR);
        }

        if ato_error_jsonl::try_emit_from_anyhow(&err, json_mode) {
            std::process::exit(error_codes::EXIT_USER_ERROR);
        }

        let diagnostic = diagnostics::from_anyhow(&err, command_context);
        let exit_code = diagnostics::map_exit_code(&diagnostic, &err);

        if json_mode {
            if let Ok(payload) = serde_json::to_string(&diagnostic.to_json_envelope()) {
                println!("{}", payload);
            } else {
                let fallback_payload = r#"{"schema_version":"1","status":"error","error":{"code":"E999","name":"internal_error","phase":"internal","message":"failed to serialize error payload","retryable":true,"interactive_resolution":false,"causes":[]}}"#;
                println!("{fallback_payload}");
            }
        } else {
            eprintln!("{:?}", miette::Report::new(diagnostic));
        }

        std::process::exit(exit_code);
    }
}

fn run() -> Result<()> {
    let is_no_args = std::env::args_os().count() == 1;

    if is_no_args {
        print_animated_logo();
        let mut cmd = Cli::command();
        cmd.print_help().context("failed to print CLI help")?;
        println!();
        return Ok(());
    }

    let cli = Cli::parse();
    let reporter = std::sync::Arc::new(reporters::CliReporter::new(cli.json));

    match cli.command {
        Commands::Run {
            path,
            skill,
            from_skill,
            target,
            watch,
            background,
            nacelle,
            registry,
            state,
            inject,
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            compatibility_fallback,
            yes,
            keep_failed_artifacts,
            allow_unverified,
        } => run_install_orchestration::execute_run_like_command(
            run_install_orchestration::RunLikeCommandArgs {
                path,
                target,
                watch,
                background,
                nacelle,
                registry,
                state,
                inject,
                enforcement,
                sandbox_mode,
                unsafe_mode_legacy,
                unsafe_bypass_sandbox_legacy,
                dangerously_skip_permissions,
                compatibility_fallback,
                yes,
                keep_failed_artifacts,
                allow_unverified,
                skill,
                from_skill,
                deprecation_warning: None,
                reporter: reporter.clone(),
            },
        ),

        Commands::Engine { command } => {
            execute_engine_command(command, cli.nacelle, reporter.clone())
        }

        Commands::Registry { command } => {
            catalog_registry_orchestration::execute_registry_command(command)
        }

        Commands::Setup {
            engine,
            version,
            skip_verify,
        } => execute_setup_command(engine, version, skip_verify, reporter.clone()),

        Commands::Open {
            path,
            target,
            watch,
            background,
            nacelle,
            registry,
            state,
            inject,
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            compatibility_fallback,
            yes,
        } => run_install_orchestration::execute_run_like_command(
            run_install_orchestration::RunLikeCommandArgs {
                path,
                target,
                watch,
                background,
                nacelle,
                registry,
                state,
                inject,
                enforcement,
                sandbox_mode,
                unsafe_mode_legacy,
                unsafe_bypass_sandbox_legacy,
                dangerously_skip_permissions,
                compatibility_fallback,
                yes,
                keep_failed_artifacts: false,
                allow_unverified: false,
                skill: None,
                from_skill: None,
                deprecation_warning: Some("⚠️  'ato open' is deprecated. Use 'ato run' instead."),
                reporter: reporter.clone(),
            },
        ),

        Commands::Init => init::execute_prompt(init::PromptArgs { path: None }, reporter.clone()),

        Commands::New { name, template } => new::execute(
            new::NewArgs {
                name,
                template: Some(template),
            },
            reporter.clone(),
        ),

        Commands::Build {
            dir,
            init,
            key,
            standalone,
            force_large_payload,
            enforcement,
            keep_failed_artifacts,
            timings,
            strict_v3,
        } => build_validate_orchestration::execute_build_like_command(
            build_validate_orchestration::BuildLikeCommandArgs {
                dir,
                init,
                key,
                standalone,
                force_large_payload,
                enforcement: enforcement.as_str().to_string(),
                keep_failed_artifacts,
                timings,
                strict_v3,
                json: cli.json,
                nacelle: cli.nacelle,
                deprecation_warning: None,
                reporter: reporter.clone(),
            },
        ),

        Commands::Validate { path, json } => {
            build_validate_orchestration::execute_validate_command(path, cli.json || json)
        }

        Commands::Update => {
            commands::update::update()?;
            Ok(())
        }

        Commands::Inspect { command } => match command {
            InspectCommands::Requirements {
                target,
                registry,
                json,
            } => {
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    commands::inspect::execute_requirements(target, registry, cli.json || json)
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                })
            }
        },

        Commands::Keygen { out, force, json } => {
            keygen::execute(keygen::KeygenArgs { out, force, json }, reporter.clone())
        }

        Commands::Key { command } => match command {
            KeyCommands::Gen { out, force, json } => {
                keygen::execute(keygen::KeygenArgs { out, force, json }, reporter.clone())
            }
            KeyCommands::Sign { target, key, out } => {
                sign::execute(sign::SignArgs { target, key, out }, reporter.clone())
            }
            KeyCommands::Verify {
                target,
                sig,
                signer,
                json,
            } => verify::execute(
                verify::VerifyArgs {
                    target,
                    sig,
                    signer,
                    json,
                },
                reporter.clone(),
            ),
        },

        Commands::Pack {
            dir,
            init,
            key,
            standalone,
            force_large_payload,
            enforcement,
            keep_failed_artifacts,
            timings,
            strict_v3,
        } => build_validate_orchestration::execute_build_like_command(
            build_validate_orchestration::BuildLikeCommandArgs {
                dir,
                init,
                key,
                standalone,
                force_large_payload,
                enforcement: enforcement.as_str().to_string(),
                keep_failed_artifacts,
                timings,
                strict_v3,
                json: cli.json,
                nacelle: cli.nacelle,
                deprecation_warning: Some("⚠️  'ato pack' is deprecated. Use 'ato build' instead."),
                reporter: reporter.clone(),
            },
        ),

        Commands::Scaffold {
            command:
                ScaffoldCommands::Docker {
                    manifest,
                    output,
                    output_dir,
                    force,
                },
        } => scaffold::execute_docker(
            scaffold::ScaffoldDockerArgs {
                manifest_path: manifest,
                output_dir,
                output,
                force,
            },
            reporter.clone(),
        ),

        Commands::Sign { target, key, out } => {
            sign::execute(sign::SignArgs { target, key, out }, reporter.clone())
        }

        Commands::Verify {
            target,
            sig,
            signer,
            json,
        } => verify::execute(
            verify::VerifyArgs {
                target,
                sig,
                signer,
                json,
            },
            reporter.clone(),
        ),

        Commands::Profile { command } => match command {
            ProfileCommands::Create {
                name,
                bio,
                avatar,
                key,
                output,
                website,
                github,
                twitter,
            } => profile::execute_create(
                profile::CreateArgs {
                    name,
                    bio,
                    avatar,
                    key,
                    output,
                    website,
                    github,
                    twitter,
                },
                reporter.clone(),
            ),
            ProfileCommands::Show { path, json } => {
                profile::execute_show(profile::ShowArgs { path, json }, reporter.clone())
            }
        },

        Commands::Install {
            slug,
            from_gh_repo,
            registry,
            version,
            default,
            yes,
            skip_verify_legacy,
            allow_unverified,
            output,
            project,
            no_project,
            json,
            keep_failed_artifacts,
        } => install_command_orchestration::execute_install_command(
            install_command_orchestration::InstallCommandArgs {
                slug,
                from_gh_repo,
                registry,
                version,
                default,
                yes,
                skip_verify_legacy,
                allow_unverified,
                output,
                project,
                no_project,
                json,
                keep_failed_artifacts,
            },
        ),

        Commands::Search {
            query,
            category,
            tags,
            limit,
            cursor,
            registry,
            json,
            no_tui,
            show_manifest,
        } => catalog_registry_orchestration::execute_search_command(
            catalog_registry_orchestration::SearchCommandArgs {
                query,
                category,
                tags,
                limit,
                cursor,
                registry,
                json,
                no_tui,
                show_manifest,
            },
        ),

        Commands::Fetch {
            capsule_ref,
            registry,
            version,
            json,
        } => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                if install::is_slug_only_ref(&capsule_ref) {
                    let suggestions =
                        install::suggest_scoped_capsules(&capsule_ref, registry.as_deref(), 5)
                            .await?;
                    if suggestions.is_empty() {
                        anyhow::bail!(
                            "scoped_id_required: '{}' is ambiguous. Use publisher/slug (for example: koh0920/{})",
                            capsule_ref,
                            capsule_ref
                        );
                    }
                    let mut message = format!(
                        "scoped_id_required: '{}' requires publisher scope.\n\nDid you mean one of these?",
                        capsule_ref
                    );
                    for suggestion in suggestions {
                        message.push_str(&format!(
                            "\n  - {}  ({} downloads)",
                            suggestion.scoped_id, suggestion.downloads
                        ));
                    }
                    anyhow::bail!(message);
                }

                let result = native_delivery::execute_fetch(
                    &capsule_ref,
                    registry.as_deref(),
                    version.as_deref(),
                )
                .await?;
                if cli.json || json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    println!("✅ Fetched to: {}", result.cache_dir.display());
                    println!("   Scoped ID: {}", result.scoped_id);
                    println!("   Version:   {}", result.version);
                    println!("   Digest:    {}", result.parent_digest);
                }
                Ok(())
            })
        }

        Commands::Finalize {
            fetched_artifact_dir,
            allow_external_finalize,
            output_dir,
            json,
        } => {
            let result = native_delivery::execute_finalize(
                &fetched_artifact_dir,
                &output_dir,
                allow_external_finalize,
            )?;
            if cli.json || json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("✅ Finalized to: {}", result.output_dir.display());
                println!("   App:      {}", result.derived_app_path.display());
                println!("   Parent:   {}", result.parent_digest);
                println!("   Derived:  {}", result.derived_digest);
            }
            Ok(())
        }

        Commands::Project {
            derived_app_path,
            launcher_dir,
            json,
            command,
        } => match command {
            Some(ProjectCommands::Ls {
                json: subcommand_json,
            }) => {
                let result = native_delivery::execute_project_ls()?;
                if cli.json || json || subcommand_json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else if result.projections.is_empty() {
                    println!("No experimental projections found.");
                } else {
                    for projection in result.projections {
                        let marker = if projection.state == "ok" {
                            "✅"
                        } else {
                            "⚠️"
                        };
                        println!(
                            "{} [{}] {} -> {}",
                            marker,
                            projection.state,
                            projection.projected_path.display(),
                            projection.derived_app_path.display()
                        );
                        println!("   ID:       {}", projection.projection_id);
                        if !projection.problems.is_empty() {
                            println!("   Problems: {}", projection.problems.join(", "));
                        }
                    }
                }
                Ok(())
            }
            None => {
                let derived_app_path = derived_app_path.ok_or_else(|| {
                    anyhow::anyhow!(
                        "ato project requires <DERIVED_APP_PATH> or use `ato project ls` for read-only status"
                    )
                })?;
                let result =
                    native_delivery::execute_project(&derived_app_path, launcher_dir.as_deref())?;
                if cli.json || json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    println!("✅ Projected to: {}", result.projected_path.display());
                    println!("   ID:       {}", result.projection_id);
                    println!("   Target:   {}", result.derived_app_path.display());
                    println!("   State:    {}", result.state);
                    println!("   Metadata: {}", result.metadata_path.display());
                }
                Ok(())
            }
        },

        Commands::Unproject {
            projection_ref,
            json,
        } => {
            let result = native_delivery::execute_unproject(&projection_ref)?;
            if cli.json || json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("✅ Unprojected: {}", result.projected_path.display());
                println!("   ID:      {}", result.projection_id);
                println!("   State:   {}", result.state_before);
                println!(
                    "   Removed: metadata={}, symlink={}",
                    result.removed_metadata, result.removed_projected_path
                );
            }
            Ok(())
        }

        Commands::Config { command } => match command {
            ConfigCommands::Engine { command } => match command {
                ConfigEngineCommands::Features => {
                    execute_engine_command(EngineCommands::Features, cli.nacelle, reporter.clone())
                }
                ConfigEngineCommands::Register {
                    name,
                    path,
                    default,
                } => execute_engine_command(
                    EngineCommands::Register {
                        name,
                        path,
                        default,
                    },
                    cli.nacelle,
                    reporter.clone(),
                ),
                ConfigEngineCommands::Install {
                    engine,
                    version,
                    skip_verify,
                } => execute_setup_command(engine, version, skip_verify, reporter.clone()),
            },
            ConfigCommands::Registry { command } => {
                let mapped = match command {
                    ConfigRegistryCommands::Resolve { domain, json } => {
                        RegistryCommands::Resolve { domain, json }
                    }
                    ConfigRegistryCommands::List { json } => RegistryCommands::List { json },
                    ConfigRegistryCommands::ClearCache => RegistryCommands::ClearCache,
                };
                catalog_registry_orchestration::execute_registry_command(mapped)
            }
        },

        Commands::Publish {
            registry,
            artifact,
            scoped_id,
            allow_existing,
            prepare,
            build,
            deploy,
            legacy_full_publish,
            force_large_payload,
            fix,
            ci,
            dry_run,
            no_tui,
            json,
        } => {
            if ci {
                publish_command_orchestration::execute_publish_ci_command(
                    json,
                    force_large_payload,
                    reporter.clone(),
                )
            } else if dry_run {
                publish_command_orchestration::execute_publish_dry_run_command(
                    json,
                    reporter.clone(),
                )
            } else {
                publish_command_orchestration::execute_publish_command(
                    publish_command_orchestration::PublishCommandArgs {
                        registry,
                        artifact,
                        scoped_id,
                        allow_existing,
                        prepare,
                        build,
                        deploy,
                        legacy_full_publish,
                        force_large_payload,
                        fix,
                        no_tui,
                        json,
                    },
                    reporter.clone(),
                )
            }
        }

        Commands::GenCi => gen_ci::execute(reporter.clone()),

        Commands::Package {
            command:
                PackageCommands::Search {
                    query,
                    category,
                    tags,
                    limit,
                    cursor,
                    registry,
                    json,
                    no_tui,
                    show_manifest,
                },
        } => catalog_registry_orchestration::execute_search_command(
            catalog_registry_orchestration::SearchCommandArgs {
                query,
                category,
                tags,
                limit,
                cursor,
                registry,
                json,
                no_tui,
                show_manifest,
            },
        ),

        Commands::Source { command } => match command {
            SourceCommands::SyncStatus {
                source_id,
                sync_run_id,
                registry,
                json,
            } => catalog_registry_orchestration::execute_source_sync_status_command(
                source_id,
                sync_run_id,
                registry,
                json,
            ),
            SourceCommands::Rebuild {
                source_id,
                reference,
                wait,
                registry,
                json,
            } => catalog_registry_orchestration::execute_source_rebuild_command(
                source_id, reference, wait, registry, json,
            ),
        },

        Commands::Ps { all, json } => {
            commands::ps::execute(commands::ps::PsArgs { all, json }, reporter.clone())
        }

        Commands::Stop {
            id,
            name,
            all,
            force,
        } => commands::close::execute(
            commands::close::CloseArgs {
                id,
                name,
                all,
                force,
            },
            reporter.clone(),
        ),

        Commands::Close {
            id,
            name,
            all,
            force,
        } => commands::close::execute(
            commands::close::CloseArgs {
                id,
                name,
                all,
                force,
            },
            reporter.clone(),
        ),

        Commands::Logs {
            id,
            name,
            follow,
            tail,
        } => commands::logs::execute(
            commands::logs::LogsArgs {
                id,
                name,
                follow,
                tail,
            },
            reporter.clone(),
        ),

        Commands::State { command } => execute_state_command(command),

        Commands::Binding { command } => execute_binding_command(command),

        Commands::Guest { sync_path } => {
            commands::guest::execute(commands::guest::GuestArgs { sync_path })
        }

        Commands::Ipc {
            command: IpcCommands::Status { json },
        } => commands::ipc::run_ipc_status(json),

        Commands::Ipc {
            command: IpcCommands::Start { path, json },
        } => commands::ipc::run_ipc_start(path, json),

        Commands::Ipc {
            command: IpcCommands::Stop { name, force, json },
        } => commands::ipc::run_ipc_stop(name, force, json),

        Commands::Ipc {
            command:
                IpcCommands::Invoke {
                    path,
                    service,
                    method,
                    args,
                    id,
                    max_message_size,
                    json,
                },
        } => commands::ipc::run_ipc_invoke(path, service, method, args, id, max_message_size, json),

        Commands::Login { token, headless } => {
            let rt = tokio::runtime::Runtime::new()?;
            match token {
                Some(token) => rt.block_on(auth::login_with_token(token)),
                None => rt.block_on(auth::login_with_store_device_flow(headless)),
            }
        }

        Commands::Logout => auth::logout(),

        Commands::Whoami => auth::status(),

        Commands::Auth => auth::status(),
    }
}

fn execute_engine_command(
    command: EngineCommands,
    nacelle_override: Option<PathBuf>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    match command {
        EngineCommands::Features => {
            let nacelle =
                capsule_core::engine::discover_nacelle(capsule_core::engine::EngineRequest {
                    explicit_path: nacelle_override,
                    manifest_path: None,
                })?;
            let payload = json!({ "spec_version": "0.1.0" });
            let resp = capsule_core::engine::run_internal(&nacelle, "features", &payload)?;
            let body = serde_json::to_string_pretty(&resp)?;
            futures::executor::block_on(reporter.notify(body))?;
            Ok(())
        }
        EngineCommands::Register {
            name,
            path,
            default,
        } => {
            let resolved_path = if let Some(p) = path {
                p
            } else if let Ok(env_path) = std::env::var("NACELLE_PATH") {
                PathBuf::from(env_path)
            } else {
                anyhow::bail!("Missing --path and NACELLE_PATH is not set");
            };

            let validated =
                capsule_core::engine::discover_nacelle(capsule_core::engine::EngineRequest {
                    explicit_path: Some(resolved_path),
                    manifest_path: None,
                })?;

            let mut cfg = capsule_core::config::load_config()?;
            cfg.engines.insert(
                name.clone(),
                capsule_core::config::EngineRegistration {
                    path: validated.display().to_string(),
                },
            );
            if default {
                cfg.default_engine = Some(name.clone());
            }
            capsule_core::config::save_config(&cfg)?;

            futures::executor::block_on(reporter.notify(format!(
                "✅ Registered engine '{}' -> {}",
                name,
                validated.display()
            )))?;
            if default {
                futures::executor::block_on(
                    reporter.notify("✅ Set as default engine".to_string()),
                )?;
            }
            Ok(())
        }
    }
}

fn execute_state_command(command: StateCommands) -> Result<()> {
    match command {
        StateCommands::List {
            owner_scope,
            state_name,
            json,
        } => state::list_states(owner_scope.as_deref(), state_name.as_deref(), json),
        StateCommands::Inspect { state_ref, json } => state::inspect_state(&state_ref, json),
        StateCommands::Register {
            manifest,
            state_name,
            path,
            json,
        } => state::register_state_from_manifest(
            &manifest,
            &state_name,
            path.to_string_lossy().as_ref(),
            json,
        ),
    }
}

fn execute_binding_command(command: BindingCommands) -> Result<()> {
    match command {
        BindingCommands::List {
            owner_scope,
            service_name,
            json,
        } => binding::list_bindings(owner_scope.as_deref(), service_name.as_deref(), json),
        BindingCommands::Inspect { binding_ref, json } => {
            binding::inspect_binding(&binding_ref, json)
        }
        BindingCommands::Resolve {
            owner_scope,
            service_name,
            binding_kind,
            caller_service,
            json,
        } => binding::resolve_binding(
            &owner_scope,
            &service_name,
            &binding_kind,
            caller_service.as_deref(),
            json,
        ),
        BindingCommands::BootstrapTls {
            binding_ref,
            install_system_trust,
            yes,
            json,
        } => binding::bootstrap_ingress_tls(&binding_ref, install_system_trust, yes, json),
        BindingCommands::ServeIngress {
            binding_ref,
            manifest,
            upstream_url,
        } => binding::serve_ingress_binding(&binding_ref, &manifest, upstream_url.as_deref()),
        BindingCommands::RegisterIngress {
            manifest,
            service_name,
            url,
            json,
        } => binding::register_ingress_binding_from_manifest(&manifest, &service_name, &url, json),
        BindingCommands::RegisterService {
            manifest,
            service_name,
            url,
            process_id,
            port,
            json,
        } => match (url.as_deref(), process_id.as_deref()) {
            (Some(url), _) => {
                binding::register_service_binding_from_manifest(&manifest, &service_name, url, json)
            }
            (None, Some(process_id)) => binding::register_service_binding_from_process(
                process_id,
                &service_name,
                port,
                json,
            ),
            (None, None) => anyhow::bail!("register-service requires either --url or --process-id"),
        },
        BindingCommands::SyncProcess { process_id, json } => {
            binding::sync_service_bindings_from_process(&process_id, json)
        }
    }
}

fn execute_setup_command(
    engine: String,
    version: Option<String>,
    skip_verify: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let capsule_reporter: &dyn capsule_core::CapsuleReporter = reporter.as_ref();
    let install = engine_manager::install_engine_release(
        &engine,
        version.as_deref(),
        skip_verify,
        capsule_reporter,
    )?;

    futures::executor::block_on(reporter.notify(format!(
        "✅ Engine {} {} installed at {}",
        engine,
        install.version,
        install.path.display()
    )))?;

    Ok(())
}

async fn resolve_installed_capsule_archive(
    scoped_ref: &install::ScopedCapsuleRef,
    registry: Option<&str>,
    preferred_version: Option<&str>,
) -> Result<Option<PathBuf>> {
    let store_root = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ato")
        .join("store");
    if let Some(path) = resolve_installed_capsule_archive_in_store(
        &store_root.join(&scoped_ref.publisher),
        &scoped_ref.slug,
        preferred_version,
    )? {
        return Ok(Some(path));
    }

    let legacy_slug_dir = store_root.join(&scoped_ref.slug);
    if !legacy_slug_dir.exists() || !legacy_slug_dir.is_dir() {
        return Ok(None);
    }

    let scoped_slug_dir = store_root
        .join(&scoped_ref.publisher)
        .join(&scoped_ref.slug);
    if scoped_slug_dir.exists() {
        return resolve_installed_capsule_archive_in_store(
            &store_root.join(&scoped_ref.publisher),
            &scoped_ref.slug,
            preferred_version,
        );
    }

    let effective_registry = registry.unwrap_or(DEFAULT_RUN_REGISTRY_URL);
    let suggestions =
        install::suggest_scoped_capsules(&scoped_ref.slug, Some(effective_registry), 10).await?;
    let scoped_matches: Vec<_> = suggestions
        .iter()
        .filter(|candidate| {
            candidate
                .scoped_id
                .ends_with(&format!("/{}", scoped_ref.slug))
        })
        .collect();
    let unique_match =
        scoped_matches.len() == 1 && scoped_matches[0].scoped_id == scoped_ref.scoped_id;

    if !unique_match {
        anyhow::bail!(
            "Legacy installation found at {} but publisher could not be determined safely. Please reinstall using: ato install {}",
            legacy_slug_dir.display(),
            scoped_ref.scoped_id
        );
    }

    if let Some(parent) = scoped_slug_dir.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "Failed to create scoped store directory: {}",
                parent.display()
            )
        })?;
    }
    std::fs::rename(&legacy_slug_dir, &scoped_slug_dir).with_context(|| {
        format!(
            "Failed to migrate legacy store path {} -> {}",
            legacy_slug_dir.display(),
            scoped_slug_dir.display()
        )
    })?;

    resolve_installed_capsule_archive_in_store(
        &store_root.join(&scoped_ref.publisher),
        &scoped_ref.slug,
        preferred_version,
    )
}

fn show_github_draft_preview(preview_session: &preview::PreviewSession, json: bool) -> Result<()> {
    if json || preview_session.manifest_source.as_deref() != Some("inferred") {
        return Ok(());
    }

    let Some(preview_toml) = preview_session.preview_toml.as_deref() else {
        return Ok(());
    };

    if progressive_ui::can_use_progressive_ui(false) {
        progressive_ui::render_generated_manifest_preview(
            &preview_session.manifest_path,
            preview_toml,
        )?;
    } else {
        eprintln!(
            "   Generated capsule.toml preview: {}",
            preview_session.manifest_path.display()
        );
        eprintln!("   ----- capsule.toml -----");
        for (index, line) in preview_toml.lines().enumerate() {
            eprintln!("   {:>3} | {}", index + 1, line);
        }
        eprintln!("   -----------------------");
    }

    Ok(())
}

fn maybe_keep_failed_github_checkout(
    checkout: &mut install::GitHubCheckout,
    keep_failed_artifacts: bool,
    json: bool,
) {
    if keep_failed_artifacts && !json {
        let kept_checkout = checkout.preserve_for_debugging();
        eprintln!(
            "⚠️  Kept failed GitHub checkout for debugging: {}",
            kept_checkout.display()
        );
    }
}

async fn run_blocking_github_install_step<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .context("GitHub repository build task failed")?
}

async fn build_github_repository_checkout(
    checkout_dir: PathBuf,
    json: bool,
    injected_manifest: Option<String>,
    keep_failed_artifacts: bool,
    suppress_injected_manifest_warning: bool,
) -> Result<commands::build::BuildResult> {
    run_blocking_github_install_step(move || {
        let reporter = std::sync::Arc::new(reporters::CliReporter::new(json));
        commands::build::execute_pack_command_with_injected_manifest(
            checkout_dir,
            false,
            None,
            false,
            false,
            keep_failed_artifacts,
            false,
            EnforcementMode::Strict.as_str().to_string(),
            reporter,
            false,
            json,
            None,
            injected_manifest.as_deref(),
            suppress_injected_manifest_warning,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn retry_github_build_after_manual_fix(
    preview_session: &mut preview::PreviewSession,
    manual_manifest_path: &std::path::Path,
    checkout_dir: &std::path::Path,
    repository: &str,
    install_draft: &install::GitHubInstallDraftResponse,
    inference_attempt: Option<&inference_feedback::InferenceAttemptHandle>,
    json: bool,
    keep_failed_artifacts: bool,
) -> Result<Option<commands::build::BuildResult>> {
    let should_edit = progressive_ui::confirm_with_fallback(
        "Edit generated capsule manifest and retry? ",
        true,
        progressive_ui::can_use_progressive_ui(false),
    )?;
    if !should_edit {
        return Ok(None);
    }

    let inferred_manifest = install_draft
        .preview_toml
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("store draft previewToml missing for manual fix"))?;
    inference_feedback::write_manual_manifest(manual_manifest_path, inferred_manifest)?;

    eprintln!("Open editor for {}", manual_manifest_path.display());
    if !inference_feedback::can_open_editor_automatically() {
        return Err(build_github_manual_intervention_error(
            manual_manifest_path,
            repository,
            install_draft,
            "No editor launcher is available for manual fix mode",
        )?);
    }
    inference_feedback::open_editor(manual_manifest_path)?;
    let edited_manifest = inference_feedback::read_manual_manifest(manual_manifest_path)?;
    if edited_manifest.trim().is_empty() {
        anyhow::bail!("edited capsule.toml is empty");
    }
    preview_session.record_manual_fix(&edited_manifest);
    let _ = preview::persist_session_with_warning(preview_session);

    let retry_result = build_github_repository_checkout(
        checkout_dir.to_path_buf(),
        json,
        Some(edited_manifest.clone()),
        keep_failed_artifacts,
        false,
    )
    .await?;

    eprintln!(
        "{}",
        inference_feedback::summarize_manifest_diff(inferred_manifest, &edited_manifest)
    );
    if let Some(attempt) = inference_attempt {
        let should_share = progressive_ui::confirm_with_fallback(
            "Share this corrected configuration to improve ato for public GitHub repositories? ",
            true,
            progressive_ui::can_use_progressive_ui(false),
        )?;
        if should_share {
            let _ = inference_feedback::submit_verified_fix(attempt, &edited_manifest).await;
        }
    }

    Ok(Some(retry_result))
}

fn github_build_error_requires_manual_intervention(error: &anyhow::Error) -> bool {
    let combined = error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_lowercase();

    combined.contains("uv.lock is missing")
        || combined.contains("uv.lock is required")
        || combined.contains("requires uv.lock")
        || combined.contains("pnpm-lock.yaml is missing")
        || combined.contains("package-lock.json")
        || combined.contains("requires one of package-lock.json")
        || combined.contains("multiple node lockfiles detected")
        || combined.contains("fail-closed provisioning")
        || combined.contains("bun install --frozen-lockfile")
        || combined.contains("lockfile had changes, but lockfile is frozen")
        || combined.contains("lockfile is frozen")
}

fn github_build_error_manual_review_reason(error: &anyhow::Error) -> String {
    let message = error.to_string();
    if !message.trim().is_empty() {
        return message;
    }

    if github_build_error_requires_manual_intervention(error) {
        "Provisioning failed under inferred fail-closed lockfile checks. Review the generated draft and refresh the repository lockfiles before retrying."
            .to_string()
    } else {
        "GitHub inferred draft build failed and requires manual review.".to_string()
    }
}

fn build_github_manual_intervention_error(
    manual_manifest_path: &std::path::Path,
    repository: &str,
    install_draft: &install::GitHubInstallDraftResponse,
    failure_reason: &str,
) -> Result<anyhow::Error> {
    if let Some(preview_toml) = install_draft.preview_toml.as_deref() {
        inference_feedback::write_manual_manifest(manual_manifest_path, preview_toml)?;
    }

    let next_steps = build_github_manual_intervention_next_steps(
        repository,
        install_draft,
        manual_manifest_path,
    );
    let required_env = install_draft
        .preview_toml
        .as_deref()
        .map(preview::required_env_from_preview_toml)
        .unwrap_or_default();
    let lowered = failure_reason.to_ascii_lowercase();

    if !required_env.is_empty()
        && (lowered.contains("required environment")
            || lowered.contains("environment variable")
            || lowered.contains("must be set")
            || required_env.iter().any(|key| failure_reason.contains(key)))
    {
        return Ok(AtoExecutionError::missing_required_env(
            format!(
                "missing required environment variables for inferred GitHub draft: {}",
                required_env.join(", ")
            ),
            required_env,
            Some("github-inference"),
        )
        .into());
    }

    let lockfile_target = if lowered.contains("uv.lock") {
        Some("uv.lock")
    } else if lowered.contains("pnpm-lock.yaml") {
        Some("pnpm-lock.yaml")
    } else if lowered.contains("package-lock.json") {
        Some("package-lock.json")
    } else if lowered.contains("bun.lockb") {
        Some("bun.lockb")
    } else if lowered.contains("bun.lock") {
        Some("bun.lock")
    } else if lowered.contains("multiple node lockfiles") {
        Some("node-lockfile")
    } else {
        None
    };
    if let Some(lockfile_target) = lockfile_target {
        return Ok(AtoExecutionError::lock_incomplete(
            failure_reason.to_string(),
            Some(lockfile_target),
        )
        .into());
    }

    if lowered.contains("ambiguous entrypoint")
        || lowered.contains("multiple candidate entrypoints")
        || lowered.contains("more than one entrypoint")
    {
        return Ok(
            AtoExecutionError::ambiguous_entrypoint(failure_reason.to_string(), Vec::new()).into(),
        );
    }

    Ok(inference_feedback::build_manual_intervention_error(
        manual_manifest_path,
        failure_reason,
        &next_steps,
    )
    .into())
}

#[cfg(test)]
fn build_github_manual_intervention_message(
    repository: &str,
    install_draft: &install::GitHubInstallDraftResponse,
    manifest_path: &std::path::Path,
    failure_reason: &str,
) -> String {
    inference_feedback::build_manual_intervention_message(
        manifest_path,
        failure_reason,
        &build_github_manual_intervention_next_steps(repository, install_draft, manifest_path),
    )
}

fn build_github_manual_intervention_next_steps(
    repository: &str,
    install_draft: &install::GitHubInstallDraftResponse,
    manifest_path: &std::path::Path,
) -> Vec<String> {
    let mut next_steps = Vec::new();
    let required_env = install_draft
        .preview_toml
        .as_deref()
        .map(preview::required_env_from_preview_toml)
        .unwrap_or_default();
    if !required_env.is_empty() {
        next_steps.push(format!(
            "Set the required environment variables before rerunning: {}.",
            required_env.join(", ")
        ));
    }
    if let Some(hint) = install_draft.capsule_hint.as_ref() {
        for warning in hint.warnings.iter().take(2) {
            next_steps.push(warning.clone());
        }
    }
    next_steps.push(format!(
        "Review {} and adjust the generated command or target settings as needed.",
        manifest_path.display()
    ));
    if !inference_feedback::can_open_editor_automatically() {
        next_steps.push(
            "Install a text editor or set VISUAL/EDITOR if you want ato to open the file automatically.".to_string(),
        );
    }
    next_steps.push(format!(
        "Rerun `ato run {repository}` after the prerequisites are ready."
    ));
    next_steps
}

fn resolve_installed_capsule_archive_in_store(
    store_root: &std::path::Path,
    slug: &str,
    preferred_version: Option<&str>,
) -> Result<Option<PathBuf>> {
    let slug_dir = store_root.join(slug);
    if !slug_dir.exists() || !slug_dir.is_dir() {
        return Ok(None);
    }

    if let Some(version) = preferred_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let version_dir = slug_dir.join(version);
        if !version_dir.exists() || !version_dir.is_dir() {
            return Ok(None);
        }
        return select_capsule_file_in_version(&version_dir);
    }

    let mut version_dirs: Vec<(ParsedSemver, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&slug_dir)
        .with_context(|| format!("Failed to read store directory: {}", slug_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(version_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(parsed) = ParsedSemver::parse(version_name) {
            version_dirs.push((parsed, path));
        }
    }

    version_dirs.sort_by(|(a, _), (b, _)| compare_semver(a, b).reverse());

    for (_, version_dir) in version_dirs {
        if let Some(capsule_path) = select_capsule_file_in_version(&version_dir)? {
            return Ok(Some(capsule_path));
        }
    }

    Ok(None)
}

fn select_capsule_file_in_version(version_dir: &std::path::Path) -> Result<Option<PathBuf>> {
    let mut capsules = Vec::new();
    for entry in std::fs::read_dir(version_dir).with_context(|| {
        format!(
            "Failed to read version directory: {}",
            version_dir.display()
        )
    })? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("capsule"))
        {
            capsules.push(path);
        }
    }

    capsules.sort();
    Ok(capsules.into_iter().next())
}

fn prompt_install_confirmation(
    detail: &install::CapsuleDetailSummary,
    resolved_version: &str,
) -> Result<bool> {
    println!();
    println!("[!] Capsule '{}' is not installed.", detail.scoped_id);
    println!();
    let name = if detail.name.trim().is_empty() {
        detail.slug.as_str()
    } else {
        detail.name.trim()
    };
    println!("📦 {} (v{})", name, resolved_version);
    if !detail.description.trim().is_empty() {
        println!("{}", detail.description.trim());
    }

    print_permission_summary(detail.permissions.as_ref());
    println!();

    loop {
        print!("? Do you want to install and run this capsule? (Y/n): ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .context("Failed to read user input")?;

        match input.trim().to_ascii_lowercase().as_str() {
            "" | "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => {
                println!("Please answer 'y' or 'n'.");
            }
        }
    }
}

fn print_permission_summary(permissions: Option<&install::CapsulePermissions>) {
    println!("This capsule requests the following permissions:");
    let Some(permissions) = permissions else {
        println!("  - No permissions metadata declared");
        return;
    };

    let mut printed_any = false;

    if let Some(network) = permissions.network.as_ref() {
        let endpoints = network.merged_endpoints();
        if !endpoints.is_empty() {
            printed_any = true;
            println!("  🌐 Network:");
            for endpoint in endpoints {
                println!("    - {}", endpoint);
            }
        }
    }

    if let Some(isolation) = permissions.isolation.as_ref() {
        if !isolation.allow_env.is_empty() {
            printed_any = true;
            println!("  🔑 Isolation env allowlist:");
            for env in &isolation.allow_env {
                println!("    - {}", env);
            }
        }
    }

    if let Some(filesystem) = permissions.filesystem.as_ref() {
        if !filesystem.read_only.is_empty() {
            printed_any = true;
            println!("  📁 Filesystem read-only:");
            for path in &filesystem.read_only {
                println!("    - {}", path);
            }
        }
        if !filesystem.read_write.is_empty() {
            printed_any = true;
            println!("  ✍️  Filesystem read-write:");
            for path in &filesystem.read_write {
                println!("    - {}", path);
            }
        }
    }

    if !printed_any {
        println!("  - No permissions metadata declared");
    }
}

fn can_prompt_interactively(stdin_is_tty: bool, stdout_is_tty: bool) -> bool {
    tui::can_launch_tui(stdin_is_tty, stdout_is_tty)
}

fn ensure_run_auto_install_allowed(
    yes: bool,
    json_mode: bool,
    stdin_is_tty: bool,
    stdout_is_tty: bool,
) -> Result<()> {
    if json_mode && !yes {
        anyhow::bail!(
            "Non-interactive JSON mode requires -y/--yes when auto-installing missing capsules"
        );
    }

    if !yes && !can_prompt_interactively(stdin_is_tty, stdout_is_tty) {
        anyhow::bail!(
            "Interactive install confirmation requires a TTY. Re-run with -y/--yes in CI or non-interactive environments."
        );
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ParsedSemver {
    major: u64,
    minor: u64,
    patch: u64,
    pre_release: Option<String>,
}

impl ParsedSemver {
    fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        let without_build = trimmed.split('+').next()?;
        let (core, pre_release) = if let Some((core, pre)) = without_build.split_once('-') {
            (core, Some(pre.to_string()))
        } else {
            (without_build, None)
        };

        let mut parts = core.split('.');
        let major = parts.next()?.parse::<u64>().ok()?;
        let minor = parts.next()?.parse::<u64>().ok()?;
        let patch = parts.next()?.parse::<u64>().ok()?;
        if parts.next().is_some() {
            return None;
        }

        Some(Self {
            major,
            minor,
            patch,
            pre_release,
        })
    }
}

fn compare_semver(a: &ParsedSemver, b: &ParsedSemver) -> Ordering {
    a.major
        .cmp(&b.major)
        .then_with(|| a.minor.cmp(&b.minor))
        .then_with(|| a.patch.cmp(&b.patch))
        .then_with(|| match (&a.pre_release, &b.pre_release) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a_pre), Some(b_pre)) => a_pre.cmp(b_pre),
        })
}

fn enforce_sandbox_mode_flags(
    enforcement: EnforcementMode,
    sandbox_requested: bool,
    dangerously_skip_permissions: bool,
    compatibility_fallback: Option<CompatibilityFallbackBackend>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<EnforcementMode> {
    const ENV_ALLOW_UNSAFE: &str = "CAPSULE_ALLOW_UNSAFE";

    if matches!(enforcement, EnforcementMode::BestEffort) {
        anyhow::bail!("--enforcement best-effort is no longer supported; use --enforcement strict");
    }

    if matches!(enforcement, EnforcementMode::Strict) && sandbox_requested {
        futures::executor::block_on(
            reporter.warn(
                "⚠️  Sandbox mode enabled: Tier2 targets will run under strict native sandboxing"
                    .to_string(),
            ),
        )?;
    }

    if dangerously_skip_permissions && compatibility_fallback.is_some() {
        anyhow::bail!(
            "--dangerously-skip-permissions and --compatibility-fallback are mutually exclusive"
        );
    }

    if dangerously_skip_permissions {
        if std::env::var(ENV_ALLOW_UNSAFE).ok().as_deref() != Some("1") {
            anyhow::bail!(
                "--dangerously-skip-permissions requires {}=1",
                ENV_ALLOW_UNSAFE
            );
        }
        futures::executor::block_on(
            reporter.warn(
                "⚠️  Dangerous mode enabled: bypassing all Ato runtime permission and sandbox barriers"
                    .to_string(),
            ),
        )?;
    }

    if let Some(CompatibilityFallbackBackend::Host) = compatibility_fallback {
        futures::executor::block_on(reporter.warn(
            "⚠ Running in Compatibility Mode (Isolated Host Environment). Nacelle sandbox is disabled."
                .to_string(),
        ))?;
    }

    Ok(enforcement)
}

#[allow(clippy::too_many_arguments)]
fn execute_open_command(
    path: PathBuf,
    target: Option<String>,
    watch: bool,
    background: bool,
    nacelle: Option<PathBuf>,
    enforcement: EnforcementMode,
    sandbox_mode: bool,
    dangerously_skip_permissions: bool,
    compatibility_fallback: Option<String>,
    assume_yes: bool,
    state: Vec<String>,
    inject: Vec<String>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let target_path = if path.is_file() || path.extension().is_some_and(|ext| ext == "capsule") {
        path.clone()
    } else {
        path.join("capsule.toml")
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(commands::open::execute(commands::open::OpenArgs {
        target: target_path,
        target_label: target,
        watch,
        background,
        nacelle,
        enforcement: enforcement.as_str().to_string(),
        sandbox_mode,
        dangerously_skip_permissions,
        compatibility_fallback,
        assume_yes,
        state_bindings: state,
        inject_bindings: inject,
        reporter,
        preview_mode: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn semver_prefers_highest_stable_release() {
        let stable = ParsedSemver::parse("1.2.0").unwrap();
        let prerelease = ParsedSemver::parse("1.2.0-rc1").unwrap();
        let older = ParsedSemver::parse("1.1.9").unwrap();

        assert_eq!(compare_semver(&stable, &prerelease), Ordering::Greater);
        assert_eq!(compare_semver(&stable, &older), Ordering::Greater);
        assert_eq!(compare_semver(&prerelease, &older), Ordering::Greater);
    }

    #[test]
    fn select_capsule_file_is_deterministic() {
        let tmp = tempfile::tempdir().unwrap();
        let version_dir = tmp.path().join("1.0.0");
        std::fs::create_dir_all(&version_dir).unwrap();
        std::fs::write(version_dir.join("zeta.capsule"), b"z").unwrap();
        std::fs::write(version_dir.join("alpha.capsule"), b"a").unwrap();

        let selected = select_capsule_file_in_version(&version_dir)
            .unwrap()
            .unwrap();
        assert_eq!(
            selected.file_name().and_then(|name| name.to_str()),
            Some("alpha.capsule")
        );
    }

    #[test]
    fn resolve_installed_capsule_uses_highest_version() {
        let tmp = tempfile::tempdir().unwrap();
        let slug = "demo-app";
        let slug_dir = tmp.path().join(slug);
        std::fs::create_dir_all(slug_dir.join("0.9.0")).unwrap();
        std::fs::create_dir_all(slug_dir.join("1.2.0-rc1")).unwrap();
        std::fs::create_dir_all(slug_dir.join("1.2.0")).unwrap();

        std::fs::write(slug_dir.join("0.9.0/old.capsule"), b"old").unwrap();
        std::fs::write(slug_dir.join("1.2.0-rc1/preview.capsule"), b"preview").unwrap();
        std::fs::write(slug_dir.join("1.2.0/new.capsule"), b"new").unwrap();

        let resolved = resolve_installed_capsule_archive_in_store(tmp.path(), slug, None)
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.file_name().and_then(|name| name.to_str()),
            Some("new.capsule")
        );
    }

    #[test]
    fn resolve_installed_capsule_can_target_exact_version() {
        let tmp = tempfile::tempdir().unwrap();
        let slug = "demo-app";
        let slug_dir = tmp.path().join(slug);
        std::fs::create_dir_all(slug_dir.join("1.0.0")).unwrap();
        std::fs::create_dir_all(slug_dir.join("2.0.0")).unwrap();

        std::fs::write(slug_dir.join("1.0.0/rolled-back.capsule"), b"old").unwrap();
        std::fs::write(slug_dir.join("2.0.0/current.capsule"), b"new").unwrap();

        let resolved = resolve_installed_capsule_archive_in_store(tmp.path(), slug, Some("1.0.0"))
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.file_name().and_then(|name| name.to_str()),
            Some("rolled-back.capsule")
        );
    }

    #[test]
    fn tty_prompt_gate_requires_both_streams() {
        assert!(can_prompt_interactively(true, true));
        assert!(!can_prompt_interactively(true, false));
        assert!(!can_prompt_interactively(false, true));
        assert!(!can_prompt_interactively(false, false));
    }

    #[test]
    fn run_auto_install_gate_requires_yes_or_tty() {
        assert!(ensure_run_auto_install_allowed(false, false, true, true).is_ok());
        assert!(ensure_run_auto_install_allowed(true, false, false, false).is_ok());

        let err = ensure_run_auto_install_allowed(false, false, false, false)
            .expect_err("non-interactive auto-install must fail without --yes");
        assert!(err
            .to_string()
            .contains("Interactive install confirmation requires a TTY"));

        let err = ensure_run_auto_install_allowed(false, true, true, true)
            .expect_err("json mode must require --yes");
        assert!(err
            .to_string()
            .contains("Non-interactive JSON mode requires -y/--yes"));
    }

    #[test]
    fn resolve_run_target_rejects_noncanonical_github_url_input() {
        let reporter = std::sync::Arc::new(reporters::CliReporter::new(false));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(run_install_orchestration::resolve_run_target_or_install(
                PathBuf::from("https://github.com/Koh0920/demo-repo"),
                true,
                false,
                false,
                None,
                reporter,
            ))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("ato run github.com/Koh0920/demo-repo"),
            "error={error:#}"
        );
    }

    #[test]
    fn resolve_run_target_requires_yes_or_tty_for_github_repo_install() {
        let error = ensure_run_auto_install_allowed(false, false, false, false)
            .expect_err("non-interactive auto-install must fail without --yes");
        assert!(
            error
                .to_string()
                .contains("Interactive install confirmation requires a TTY"),
            "error={error:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn github_install_build_step_runs_outside_async_runtime_worker() {
        let value = run_blocking_github_install_step(|| {
            let runtime = tokio::runtime::Runtime::new()?;
            Ok::<u8, anyhow::Error>(runtime.block_on(async { 7 }))
        })
        .await
        .unwrap();

        assert_eq!(value, 7);
    }

    #[test]
    fn search_tui_gate_requires_tty_and_flags_allowing_tui() {
        assert!(catalog_registry_orchestration::should_use_search_tui(
            true, true, false, false,
        ));
        assert!(!catalog_registry_orchestration::should_use_search_tui(
            false, true, false, false,
        ));
        assert!(!catalog_registry_orchestration::should_use_search_tui(
            true, false, false, false,
        ));
        assert!(!catalog_registry_orchestration::should_use_search_tui(
            true, true, true, false,
        ));
        assert!(!catalog_registry_orchestration::should_use_search_tui(
            true, true, false, true,
        ));
    }

    #[test]
    fn run_command_parses_explicit_state_bindings() {
        let cli = Cli::try_parse_from([
            "ato",
            "run",
            ".",
            "--state",
            "data=/var/lib/ato/persistent/demo",
            "--state",
            "cache=/var/lib/ato/persistent/cache",
        ])
        .expect("parse");

        match cli.command {
            Commands::Run { state, .. } => assert_eq!(
                state,
                vec![
                    "data=/var/lib/ato/persistent/demo".to_string(),
                    "cache=/var/lib/ato/persistent/cache".to_string()
                ]
            ),
            other => panic!("unexpected command: {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn state_command_parses_register_and_inspect_forms() {
        let register = Cli::try_parse_from([
            "ato",
            "state",
            "register",
            "--manifest",
            ".",
            "--name",
            "data",
            "--path",
            "/var/lib/ato/persistent/demo",
        ])
        .expect("parse register");

        match register.command {
            Commands::State {
                command:
                    StateCommands::Register {
                        manifest,
                        state_name,
                        path,
                        json,
                    },
            } => {
                assert_eq!(manifest, PathBuf::from("."));
                assert_eq!(state_name, "data");
                assert_eq!(path, PathBuf::from("/var/lib/ato/persistent/demo"));
                assert!(!json);
            }
            other => panic!("unexpected command: {:?}", std::mem::discriminant(&other)),
        }

        let inspect =
            Cli::try_parse_from(["ato", "state", "inspect", "state-demo"]).expect("parse inspect");
        match inspect.command {
            Commands::State {
                command: StateCommands::Inspect { state_ref, json },
            } => {
                assert_eq!(state_ref, "state-demo");
                assert!(!json);
            }
            other => panic!("unexpected command: {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn parse_sha256_for_artifact_supports_sha256sums_format() {
        let body = "\
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  nacelle-v1.2.3-darwin-arm64
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  nacelle-v1.2.3-linux-x64
";
        let parsed =
            crate::engine_manager::parse_sha256_for_artifact(body, "nacelle-v1.2.3-linux-x64");
        assert_eq!(
            parsed.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
    }

    #[test]
    fn parse_sha256_for_artifact_supports_bsd_style_format() {
        let body = "SHA256 (nacelle-v1.2.3-darwin-arm64) = CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
        let parsed =
            crate::engine_manager::parse_sha256_for_artifact(body, "nacelle-v1.2.3-darwin-arm64");
        assert_eq!(
            parsed.as_deref(),
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
        );
    }

    #[test]
    fn extract_first_sha256_hex_reads_single_file_checksum() {
        let body = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd  nacelle-v1.2.3-darwin-arm64";
        let parsed = crate::engine_manager::extract_first_sha256_hex(body);
        assert_eq!(
            parsed.as_deref(),
            Some("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
        );
    }

    #[test]
    fn dangerous_skip_permissions_requires_explicit_opt_in_env() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::remove_var("CAPSULE_ALLOW_UNSAFE");

        let reporter = std::sync::Arc::new(reporters::CliReporter::new(true));
        let err = enforce_sandbox_mode_flags(EnforcementMode::Strict, false, true, None, reporter)
            .expect_err("must fail closed without env opt-in");
        assert!(err
            .to_string()
            .contains("--dangerously-skip-permissions requires CAPSULE_ALLOW_UNSAFE=1"));
    }

    #[test]
    fn dangerous_skip_permissions_allows_with_explicit_opt_in_env() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::set_var("CAPSULE_ALLOW_UNSAFE", "1");

        let reporter = std::sync::Arc::new(reporters::CliReporter::new(true));
        let result =
            enforce_sandbox_mode_flags(EnforcementMode::Strict, false, true, None, reporter);
        assert!(result.is_ok());

        std::env::remove_var("CAPSULE_ALLOW_UNSAFE");
    }

    #[test]
    fn compatibility_fallback_is_mutually_exclusive_with_dangerous_mode() {
        let reporter = std::sync::Arc::new(reporters::CliReporter::new(true));
        let err = enforce_sandbox_mode_flags(
            EnforcementMode::Strict,
            false,
            true,
            Some(CompatibilityFallbackBackend::Host),
            reporter,
        )
        .expect_err("must reject overlapping fallback and dangerous mode");

        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn publish_private_status_message_for_build_path() {
        assert_eq!(
            publish_command_orchestration::publish_private_status_message(
                publish_command_orchestration::PublishTargetMode::CustomDirect,
                false,
            ),
            "📦 Building capsule artifact for private registry publish..."
        );
    }

    #[test]
    fn publish_private_status_message_for_upload_path() {
        assert_eq!(
            publish_command_orchestration::publish_private_status_message(
                publish_command_orchestration::PublishTargetMode::CustomDirect,
                true,
            ),
            "📤 Publishing provided artifact to private registry..."
        );
    }

    #[test]
    fn publish_private_status_message_for_personal_dock_build_path() {
        assert_eq!(
            publish_command_orchestration::publish_private_status_message(
                publish_command_orchestration::PublishTargetMode::PersonalDockDirect,
                false,
            ),
            "📦 Building capsule artifact for Personal Dock publish..."
        );
    }

    #[test]
    fn publish_private_status_message_for_personal_dock_upload_path() {
        assert_eq!(
            publish_command_orchestration::publish_private_status_message(
                publish_command_orchestration::PublishTargetMode::PersonalDockDirect,
                true,
            ),
            "📤 Publishing provided artifact to Personal Dock..."
        );
    }

    #[test]
    fn publish_private_start_summary_line_build_path() {
        let line = publish_command_orchestration::publish_private_start_summary_line(
            publish_command_orchestration::PublishTargetMode::CustomDirect,
            "http://127.0.0.1:8787",
            "build",
            "local/demo-app",
            "1.2.3",
            false,
        );
        assert!(line.contains("registry=http://127.0.0.1:8787"));
        assert!(line.contains("source=build"));
        assert!(line.contains("scoped_id=local/demo-app"));
        assert!(line.contains("version=1.2.3"));
        assert!(line.contains("allow_existing=false"));
    }

    #[test]
    fn publish_private_start_summary_line_artifact_path() {
        let line = publish_command_orchestration::publish_private_start_summary_line(
            publish_command_orchestration::PublishTargetMode::CustomDirect,
            "http://127.0.0.1:8787",
            "artifact",
            "team-x/demo-app",
            "1.2.3",
            true,
        );
        assert!(line.contains("source=artifact"));
        assert!(line.contains("allow_existing=true"));
    }

    #[test]
    fn publish_private_start_summary_line_marks_personal_dock_target() {
        let line = publish_command_orchestration::publish_private_start_summary_line(
            publish_command_orchestration::PublishTargetMode::PersonalDockDirect,
            "https://api.ato.run",
            "artifact",
            "koh0920/demo-app",
            "1.2.3",
            false,
        );
        assert!(line.contains("🔎 dock publish target"));
    }

    fn test_publish_args() -> publish_command_orchestration::PublishCommandArgs {
        publish_command_orchestration::PublishCommandArgs {
            registry: Some("http://127.0.0.1:8787".to_string()),
            artifact: None,
            scoped_id: None,
            allow_existing: false,
            prepare: false,
            build: false,
            deploy: false,
            legacy_full_publish: false,
            force_large_payload: false,
            fix: false,
            no_tui: false,
            json: true,
        }
    }

    #[test]
    fn publish_phase_selection_defaults_to_all_for_private() {
        let selected =
            publish_command_orchestration::select_publish_phases(false, false, false, false, false);
        assert!(selected.prepare);
        assert!(selected.build);
        assert!(selected.deploy);
        assert!(!selected.explicit_filter);
    }

    #[test]
    fn publish_phase_selection_respects_filter_flags() {
        let selected =
            publish_command_orchestration::select_publish_phases(true, false, true, true, false);
        assert!(selected.prepare);
        assert!(!selected.build);
        assert!(selected.deploy);
        assert!(selected.explicit_filter);
    }

    #[test]
    fn publish_phase_selection_defaults_to_deploy_for_official() {
        let selected =
            publish_command_orchestration::select_publish_phases(false, false, false, true, false);
        assert!(!selected.prepare);
        assert!(!selected.build);
        assert!(selected.deploy);
        assert!(!selected.explicit_filter);
    }

    #[test]
    fn publish_phase_selection_legacy_full_publish_keeps_all_for_official() {
        let selected =
            publish_command_orchestration::select_publish_phases(false, false, false, true, true);
        assert!(selected.prepare);
        assert!(selected.build);
        assert!(selected.deploy);
        assert!(!selected.explicit_filter);
    }

    #[test]
    fn resolve_publish_target_prefers_cli_registry_over_other_sources() {
        let resolved = publish_command_orchestration::resolve_publish_target_from_sources(
            Some("https://api.ato.run"),
            Some("http://127.0.0.1:8787"),
            Some("koh0920"),
        )
        .expect("resolve");

        assert_eq!(resolved.registry_url, "https://api.ato.run");
        assert_eq!(
            resolved.mode,
            publish_command_orchestration::PublishTargetMode::OfficialCi
        );
    }

    #[test]
    fn resolve_publish_target_uses_manifest_before_logged_in_default() {
        let resolved = publish_command_orchestration::resolve_publish_target_from_sources(
            None,
            Some("http://127.0.0.1:8787"),
            Some("koh0920"),
        )
        .expect("resolve");

        assert_eq!(resolved.registry_url, "http://127.0.0.1:8787");
        assert_eq!(
            resolved.mode,
            publish_command_orchestration::PublishTargetMode::CustomDirect
        );
    }

    #[test]
    fn resolve_publish_target_uses_logged_in_default_when_no_explicit_target_exists() {
        let resolved = publish_command_orchestration::resolve_publish_target_from_sources(
            None,
            None,
            Some("koh0920"),
        )
        .expect("resolve");

        assert_eq!(resolved.registry_url, "https://api.ato.run");
        assert_eq!(
            resolved.mode,
            publish_command_orchestration::PublishTargetMode::PersonalDockDirect
        );
        assert_eq!(resolved.publisher_handle.as_deref(), Some("koh0920"));
    }

    #[test]
    fn resolve_publish_target_errors_without_login_or_registry_override() {
        let err =
            publish_command_orchestration::resolve_publish_target_from_sources(None, None, None)
                .expect_err("must fail without any publish target");

        assert!(err.to_string().contains("Run `ato login`"));
        assert!(err.to_string().contains("--registry https://api.ato.run"));
    }

    #[test]
    fn resolve_publish_target_rejects_legacy_dock_registry_urls() {
        let err = publish_command_orchestration::resolve_publish_target_from_sources(
            Some("https://ato.run/d/koh0920"),
            None,
            Some("koh0920"),
        )
        .expect_err("must reject legacy dock url");
        assert!(err.to_string().contains("https://api.ato.run"));
        assert!(err.to_string().contains("/d/<handle>"));
    }

    #[test]
    fn is_legacy_dock_publish_registry_detects_dock_path_prefix() {
        assert!(
            publish_command_orchestration::is_legacy_dock_publish_registry(
                "https://ato.run/d/koh0920"
            )
        );
        assert!(
            publish_command_orchestration::is_legacy_dock_publish_registry(
                "https://ato.run/publish/d/koh0920"
            )
        );
        assert!(
            !publish_command_orchestration::is_legacy_dock_publish_registry("https://api.ato.run")
        );
    }

    #[test]
    fn publish_validate_rejects_allow_existing_without_deploy() {
        let mut args = test_publish_args();
        args.allow_existing = true;
        let selected =
            publish_command_orchestration::select_publish_phases(false, true, false, false, false);
        let err =
            publish_command_orchestration::validate_publish_phase_options(&args, selected, false)
                .expect_err("must fail closed");
        assert!(err.to_string().contains("--allow-existing"));
    }

    #[test]
    fn publish_validate_rejects_fix_for_private_registry() {
        let mut args = test_publish_args();
        args.fix = true;
        let selected =
            publish_command_orchestration::select_publish_phases(false, false, true, false, false);
        let err =
            publish_command_orchestration::validate_publish_phase_options(&args, selected, false)
                .expect_err("must fail closed");
        assert!(err.to_string().contains("--fix"));
    }

    #[test]
    fn publish_validate_requires_artifact_or_build_for_private_deploy_only() {
        let args = test_publish_args();
        let selected =
            publish_command_orchestration::select_publish_phases(false, false, true, false, false);
        let err =
            publish_command_orchestration::validate_publish_phase_options(&args, selected, false)
                .expect_err("must fail closed");
        assert!(err.to_string().contains("--deploy requires --artifact"));
    }

    #[test]
    fn github_manual_intervention_extracts_required_env() {
        let required = preview::required_env_from_preview_toml(
            r#"
[env]
required = ["DATABASE_URL", "REDIS_URL"]
"#,
        );

        assert_eq!(required, vec!["DATABASE_URL", "REDIS_URL"]);
    }

    #[test]
    fn github_manual_intervention_prefers_root_required_env() {
        let required = preview::required_env_from_preview_toml(
            r#"
required_env = ["DATABASE_URL", "REDIS_URL"]

[env]
required = ["LEGACY_ONLY"]
"#,
        );

        assert_eq!(required, vec!["DATABASE_URL", "REDIS_URL"]);
    }

    #[test]
    fn github_manual_intervention_message_mentions_manifest_and_repo() {
        let draft = install::GitHubInstallDraftResponse {
            repo: install::GitHubInstallDraftRepo {
                owner: "octocat".to_string(),
                repo: "hello-world".to_string(),
                full_name: "octocat/hello-world".to_string(),
                default_branch: "main".to_string(),
            },
            capsule_toml: install::GitHubInstallDraftCapsuleToml { exists: false },
            repo_ref: "octocat/hello-world".to_string(),
            proposed_run_command: None,
            proposed_install_command: "ato run github.com/octocat/hello-world".to_string(),
            resolved_ref: install::GitHubInstallDraftResolvedRef {
                ref_name: "main".to_string(),
                sha: "deadbeef".to_string(),
            },
            manifest_source: "inferred".to_string(),
            preview_toml: Some(
                r#"
[env]
required = ["DATABASE_URL"]
"#
                .to_string(),
            ),
            capsule_hint: Some(install::GitHubInstallDraftHint {
                confidence: "medium".to_string(),
                warnings: vec!["外部DBの準備が必要です。".to_string()],
                launchability: Some("manual_review".to_string()),
            }),
            inference_mode: Some("rules".to_string()),
            retryable: false,
        };

        let message = build_github_manual_intervention_message(
            "github.com/octocat/hello-world",
            &draft,
            std::path::Path::new("/repo/.tmp/ato-inference/attempt/capsule.toml"),
            "Smoke failed",
        );

        assert!(message.contains("manual intervention required"));
        assert!(message.contains("DATABASE_URL"));
        assert!(message.contains("github.com/octocat/hello-world"));
        assert!(message.contains("/repo/.tmp/ato-inference/attempt/capsule.toml"));
    }

    #[test]
    fn github_build_error_requires_manual_intervention_for_missing_uv_lock() {
        let error = anyhow::anyhow!(
            "uv.lock is missing for '/tmp/demo/pyproject.toml'. Generate it with `uv lock`."
        );

        assert!(github_build_error_requires_manual_intervention(&error));
        assert!(github_build_error_manual_review_reason(&error).contains("uv.lock"));
    }

    #[test]
    fn github_build_error_requires_manual_intervention_for_stale_bun_lock() {
        let error = anyhow::anyhow!(
            "provision command failed with exit code 1: bun install --frozen-lockfile\nerror: lockfile had changes, but lockfile is frozen"
        );

        assert!(github_build_error_requires_manual_intervention(&error));
        assert!(github_build_error_manual_review_reason(&error)
            .contains("bun install --frozen-lockfile"));
    }

    #[test]
    fn github_manual_intervention_returns_e103_for_required_env_failure() {
        let draft = install::GitHubInstallDraftResponse {
            repo: install::GitHubInstallDraftRepo {
                owner: "octocat".to_string(),
                repo: "hello-world".to_string(),
                full_name: "octocat/hello-world".to_string(),
                default_branch: "main".to_string(),
            },
            capsule_toml: install::GitHubInstallDraftCapsuleToml { exists: false },
            repo_ref: "octocat/hello-world".to_string(),
            proposed_run_command: None,
            proposed_install_command: "ato run github.com/octocat/hello-world".to_string(),
            resolved_ref: install::GitHubInstallDraftResolvedRef {
                ref_name: "main".to_string(),
                sha: "deadbeef".to_string(),
            },
            manifest_source: "inferred".to_string(),
            preview_toml: Some("required_env = [\"DATABASE_URL\"]\n".to_string()),
            capsule_hint: None,
            inference_mode: Some("rules".to_string()),
            retryable: false,
        };
        let tempdir = tempfile::tempdir().expect("tempdir");
        let manifest_path = tempdir.path().join("capsule.toml");

        let err = build_github_manual_intervention_error(
            &manifest_path,
            "github.com/octocat/hello-world",
            &draft,
            "DATABASE_URL is required",
        )
        .expect("manual intervention error");
        let execution_err = err
            .downcast_ref::<AtoExecutionError>()
            .expect("ato execution error");
        assert_eq!(execution_err.name, "missing_required_env");
    }

    #[test]
    fn github_manual_intervention_returns_e104_for_lockfile_failure() {
        let draft = install::GitHubInstallDraftResponse {
            repo: install::GitHubInstallDraftRepo {
                owner: "octocat".to_string(),
                repo: "hello-world".to_string(),
                full_name: "octocat/hello-world".to_string(),
                default_branch: "main".to_string(),
            },
            capsule_toml: install::GitHubInstallDraftCapsuleToml { exists: false },
            repo_ref: "octocat/hello-world".to_string(),
            proposed_run_command: None,
            proposed_install_command: "ato run github.com/octocat/hello-world".to_string(),
            resolved_ref: install::GitHubInstallDraftResolvedRef {
                ref_name: "main".to_string(),
                sha: "deadbeef".to_string(),
            },
            manifest_source: "inferred".to_string(),
            preview_toml: Some("required_env = []\n".to_string()),
            capsule_hint: None,
            inference_mode: Some("rules".to_string()),
            retryable: false,
        };
        let tempdir = tempfile::tempdir().expect("tempdir");
        let manifest_path = tempdir.path().join("capsule.toml");

        let err = build_github_manual_intervention_error(
            &manifest_path,
            "github.com/octocat/hello-world",
            &draft,
            "uv.lock is missing for '/tmp/demo/pyproject.toml'. Generate it with `uv lock`.",
        )
        .expect("manual intervention error");
        let execution_err = err
            .downcast_ref::<AtoExecutionError>()
            .expect("ato execution error");
        assert_eq!(execution_err.name, "dependency_lock_missing");
    }
}
