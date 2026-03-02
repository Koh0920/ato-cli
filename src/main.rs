use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use colored::Colorize;
use serde_json::json;
use std::cmp::Ordering;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use tracing::debug;

use capsule_core::CapsuleReporter;

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

mod ato_error_jsonl;
mod auth;
#[cfg(feature = "manifest-signing")]
mod capsule_capnp;
mod commands;
mod common;
mod consent_store;
mod diagnostics;
mod engine_manager;
mod env;
mod error_codes;
mod executors;
mod gen_ci;
mod guest_protocol;
mod init;
mod install;
mod ipc;
mod keygen;
mod new;
mod observability;
mod payload_guard;
mod process_manager;
mod profile;
mod publish_artifact;
mod publish_ci;
mod publish_dry_run;
mod publish_preflight;
mod publish_private;
mod publish_tui;
mod registry;
mod registry_serve;
mod reporters;
mod runtime_manager;
mod scaffold;
mod search;
mod sign;
mod skill;
mod skill_resolver;
mod source;
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
  install  Install a verified package from the registry
  init     Initialize a new project
  build    Pack a project into an immutable .capsule archive
  search   Search the registry for agent skills and packages

Management:
  ps       List running capsules
  stop     Stop a running capsule
  logs     Show logs of a running capsule

Auth:
  login    Login to Ato registry
  logout   Logout
  whoami   Show current authentication status

Advanced Commands:
  key      Manage signing keys
  config   Manage configuration (registry, engine, source)
  publish  Publish capsule (official: CI-first, private: direct upload)
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
        /// Local path (./, ../, ~/, /...) or store scoped ID (publisher/slug). Default: current directory
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

        /// Skip prompt and auto-install when app-id is not installed
        #[arg(short = 'y', long = "yes", default_value_t = false)]
        yes: bool,

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
        slug: String,

        /// Registry URL (default: registry.capsule.app)
        #[arg(long)]
        registry: Option<String>,

        /// Specific version to install
        #[arg(long)]
        version: Option<String>,

        /// Set as default handler for supported content types
        #[arg(long, default_value_t = false)]
        default: bool,

        /// Deprecated legacy flag (always rejected)
        #[arg(long = "skip-verify", hide = true, default_value_t = false)]
        skip_verify_legacy: bool,

        /// Allow installing unverified signatures in non-production environments
        #[arg(long, default_value_t = false)]
        allow_unverified: bool,

        /// Output directory (default: ~/.capsule/store/)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(
        next_help_heading = "Primary Commands",
        about = "Initialize a new project"
    )]
    Init {
        /// Project name
        name: String,

        /// Template type: python, node, hono, rust, go, shell
        #[arg(long, default_value = "python")]
        template: String,
    },

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

    #[command(next_help_heading = "Auth", about = "Login to Ato registry")]
    Login {
        /// GitHub Personal Access Token (legacy fallback, scope: read:user)
        #[arg(long)]
        token: Option<String>,
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
        about = "Publish capsule (official registry: CI-first, private registry: direct upload)"
    )]
    Publish {
        /// Registry URL override (default: https://api.ato.run)
        #[arg(long)]
        registry: Option<String>,

        /// Use prebuilt .capsule artifact (skip repackaging for private registry publish)
        #[arg(long, value_name = "PATH", conflicts_with_all = ["ci", "dry_run"])]
        artifact: Option<PathBuf>,

        /// Allow publishing payloads larger than 200MB
        #[arg(long, default_value_t = false)]
        force_large_payload: bool,

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

    /// Register a nacelle engine binary (writes ~/.capsule/config.toml)
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

    /// Register a nacelle engine binary (writes ~/.capsule/config.toml)
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
        #[arg(long, default_value = "~/.capsule/local-registry")]
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
        if ato_error_jsonl::try_emit_from_anyhow(&err, json_mode) {
            std::process::exit(error_codes::EXIT_USER_ERROR);
        }

        let diagnostic = diagnostics::from_anyhow(&err, command_context);
        let exit_code = diagnostics::map_exit_code(&diagnostic, &err);

        if json_mode {
            if let Ok(payload) = serde_json::to_string(&diagnostic.to_json_envelope()) {
                println!("{}", payload);
            } else {
                println!(
                    r#"{{"schema_version":"1","type":"error","code":"E999","message":"failed to serialize error payload","causes":[]}}"#
                );
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
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            yes,
            allow_unverified,
        } => execute_run_like_command(
            path,
            target,
            watch,
            background,
            nacelle,
            registry,
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            yes,
            allow_unverified,
            skill,
            from_skill,
            None,
            reporter.clone(),
        ),

        Commands::Engine { command } => {
            execute_engine_command(command, cli.nacelle, reporter.clone())
        }

        Commands::Registry { command } => execute_registry_command(command),

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
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            yes,
        } => execute_run_like_command(
            path,
            target,
            watch,
            background,
            nacelle,
            registry,
            enforcement,
            sandbox_mode,
            unsafe_mode_legacy,
            unsafe_bypass_sandbox_legacy,
            dangerously_skip_permissions,
            yes,
            false,
            None,
            None,
            Some("⚠️  'ato open' is deprecated. Use 'ato run' instead."),
            reporter.clone(),
        ),

        Commands::Init { name, template } => new::execute(
            new::NewArgs {
                name,
                template: Some(template),
            },
            reporter.clone(),
        ),

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
        } => commands::build::execute_pack_command(
            dir,
            init,
            key,
            standalone,
            force_large_payload,
            keep_failed_artifacts,
            enforcement.as_str().to_string(),
            reporter.clone(),
            cli.json,
            cli.nacelle,
        ),

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
        } => {
            eprintln!("⚠️  'ato pack' is deprecated. Use 'ato build' instead.");
            commands::build::execute_pack_command(
                dir,
                init,
                key,
                standalone,
                force_large_payload,
                keep_failed_artifacts,
                enforcement.as_str().to_string(),
                reporter.clone(),
                cli.json,
                cli.nacelle,
            )
        }

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
            registry,
            version,
            default,
            skip_verify_legacy,
            allow_unverified,
            output,
            json,
        } => {
            if skip_verify_legacy {
                anyhow::bail!(
                    "--skip-verify is no longer supported. Signature/hash verification is always required."
                );
            }
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                if install::is_slug_only_ref(&slug) {
                    let suggestions = install::suggest_scoped_capsules(
                        &slug,
                        registry.as_deref(),
                        5,
                    )
                    .await?;
                    if suggestions.is_empty() {
                        anyhow::bail!(
                            "scoped_id_required: '{}' is ambiguous. Use publisher/slug (for example: koh0920/{})",
                            slug,
                            slug
                        );
                    }
                    let mut message = format!(
                        "scoped_id_required: '{}' requires publisher scope.\n\nDid you mean one of these?",
                        slug
                    );
                    for suggestion in suggestions {
                        message.push_str(&format!(
                            "\n  - {}  ({} downloads)",
                            suggestion.scoped_id, suggestion.downloads
                        ));
                    }
                    message.push_str("\n\nRun `ato search ");
                    message.push_str(&slug);
                    message.push_str("` to see more options.");
                    anyhow::bail!(message);
                }

                let result = install::install_app(
                    &slug,
                    registry.as_deref(),
                    version.as_deref(),
                    output,
                    default,
                    allow_unverified,
                    json,
                )
                .await?;

                if json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    println!("\n✅ Installation complete!");
                    println!("   Capsule: {}", result.slug);
                    println!("   Version: {}", result.version);
                    println!("   Path:    {}", result.path.display());
                    println!("   Hash:    {}", result.content_hash);
                }
                Ok(())
            })
        }

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
        } => execute_search_command(
            query,
            category,
            tags,
            limit,
            cursor,
            registry,
            json,
            no_tui,
            show_manifest,
        ),

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
                execute_registry_command(mapped)
            }
        },

        Commands::Publish {
            registry,
            artifact,
            force_large_payload,
            ci,
            dry_run,
            no_tui,
            json,
        } => {
            if ci {
                execute_publish_ci_command(json, force_large_payload, reporter.clone())
            } else if dry_run {
                execute_publish_dry_run_command(json, reporter.clone())
            } else {
                let resolved_registry = resolve_publish_registry_url(registry)?;
                if is_official_publish_registry(&resolved_registry) {
                    if std::io::stdout().is_terminal() && !json && !no_tui {
                        execute_publish_tui_command(reporter.clone())
                    } else {
                        execute_publish_guidance_command(json, &resolved_registry)
                    }
                } else {
                    execute_publish_private_command(
                        resolved_registry,
                        artifact,
                        force_large_payload,
                        json,
                        reporter.clone(),
                    )
                }
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
        } => execute_search_command(
            query,
            category,
            tags,
            limit,
            cursor,
            registry,
            json,
            no_tui,
            show_manifest,
        ),

        Commands::Source { command } => match command {
            SourceCommands::SyncStatus {
                source_id,
                sync_run_id,
                registry,
                json,
            } => execute_source_sync_status_command(source_id, sync_run_id, registry, json),
            SourceCommands::Rebuild {
                source_id,
                reference,
                wait,
                registry,
                json,
            } => execute_source_rebuild_command(source_id, reference, wait, registry, json),
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

        Commands::Login { token } => {
            let rt = tokio::runtime::Runtime::new()?;
            match token {
                Some(token) => rt.block_on(auth::login_with_token(token)),
                None => rt.block_on(auth::login_with_store_device_flow()),
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

fn execute_registry_command(command: RegistryCommands) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match command {
            RegistryCommands::Resolve { domain, json } => {
                let resolver = registry::RegistryResolver::default();
                match resolver.resolve(&domain).await {
                    Ok(info) => {
                        if json {
                            println!("{}", serde_json::to_string_pretty(&info)?);
                        } else {
                            println!("📡 Registry for {}:", domain);
                            println!("   URL:    {}", info.url);
                            if let Some(name) = &info.name {
                                println!("   Name:   {}", name);
                            }
                            if let Some(key) = &info.public_key {
                                println!("   Key:    {}", key);
                            }
                            println!("   Source: {:?}", info.source);
                        }
                    }
                    Err(e) => {
                        if json {
                            println!(r#"{{"error": "{}"}}"#, e);
                        } else {
                            eprintln!("❌ Failed to resolve registry: {}", e);
                        }
                    }
                }
                Ok(())
            }
            RegistryCommands::List { json } => {
                let resolver = registry::RegistryResolver::default();
                let info = resolver.resolve_for_app("default").await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&[&info])?);
                } else {
                    println!("📋 Configured registries:");
                    println!(
                        "   • {} ({})",
                        info.url,
                        format!("{:?}", info.source).to_lowercase()
                    );
                }
                Ok(())
            }
            RegistryCommands::ClearCache => {
                let cache = registry::RegistryCache::new();
                cache.clear()?;
                println!("✅ Registry cache cleared");
                Ok(())
            }
            RegistryCommands::Serve {
                port,
                data_dir,
                host,
                auth_token,
            } => {
                if host != "127.0.0.1"
                    && auth_token
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or("")
                        .is_empty()
                {
                    anyhow::bail!("--auth-token is required when --host is not 127.0.0.1");
                }
                registry_serve::serve(registry_serve::RegistryServerConfig {
                    host,
                    port,
                    data_dir,
                    auth_token,
                })
                .await
            }
        }
    })
}

fn execute_publish_ci_command(
    json_output: bool,
    force_large_payload: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result = publish_ci::execute(publish_ci::PublishCiArgs {
            json_output,
            force_large_payload,
        })
        .await?;

        if json_output {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("✅ Successfully published to Ato Store!");
            println!();
            println!(
                "📦 Capsule:   {} v{}",
                result.capsule_scoped_id, result.version
            );
            if let Some(sha256) = &result.artifact_sha256 {
                println!("🛡️  Integrity: sha256:{}", sha256);
            } else if let Some(blake3) = &result.artifact_blake3 {
                println!("🛡️  Integrity: {}", blake3);
            }
            println!();
            println!("🌐 Store URL:      {}", result.urls.store);
            if let Some(playground) = &result.urls.playground {
                println!("🎮 Playground URL: {}", playground);
            }
            println!();
            println!("👉 Next step: ato run {}", result.capsule_scoped_id);
            println!();
            println!("   Event ID:   {}", result.publish_event_id);
            println!("   Release ID: {}", result.release_id);
            println!("   Artifact:   {}", result.artifact_id);
            println!("   Status:     {}", result.verification_status);
        }
        futures::executor::block_on(
            reporter
                .notify("CI publish completed using GitHub OIDC workflow identity.".to_string()),
        )?;
        Ok(())
    })
}

fn execute_publish_dry_run_command(
    json_output: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result =
            publish_dry_run::execute(publish_dry_run::PublishDryRunArgs { json_output }).await?;

        if json_output {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("✅ Dry-run successful! Your capsule is ready to be published via CI.");
            println!("   Capsule: {}", result.capsule_name);
            println!("   Version: {}", result.version);
            println!("   Artifact: {}", result.artifact_path.display());
            println!("   Size: {} bytes", result.artifact_size_bytes);
        }
        futures::executor::block_on(
            reporter.notify("Local publish dry-run completed (no upload performed).".to_string()),
        )?;
        Ok(())
    })
}

fn execute_publish_guidance_command(json_output: bool, registry_url: &str) -> Result<()> {
    if json_output {
        let payload = serde_json::json!({
            "ok": false,
            "code": "CI_ONLY_PUBLISH",
            "message": "Official registry publishing is CI-first. Use `ato publish --ci` in GitHub Actions, or `ato publish --dry-run` locally."
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "❌ Direct local publishing is disabled for official registry ({}).",
            registry_url
        );
        println!();
        println!("Ato uses a strict CI-first publishing model via GitHub Actions (OIDC).");
        println!("This guarantees published capsules match committed source.");
        println!();
        println!("👉 Next steps:");
        println!("  1. Run `ato gen-ci` to generate `.github/workflows/ato-publish.yml`.");
        println!("  2. Commit and tag your release (e.g. `git tag v0.1.0`).");
        println!("  3. Push the tag to GitHub (`git push origin v0.1.0`).");
        println!("  4. GitHub Actions runs `ato publish --ci` automatically.");
        println!();
        println!("💡 Tip: Run `ato publish --dry-run` to validate locally before pushing.");
        println!("💡 Private registry directly publish: `ato publish --registry <url>`");
    }
    Ok(())
}

fn execute_publish_private_command(
    registry_url: String,
    artifact_path: Option<PathBuf>,
    force_large_payload: bool,
    json_output: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let result = publish_private::execute(publish_private::PublishPrivateArgs {
        registry_url,
        artifact_path,
        force_large_payload,
    })?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("✅ Successfully published to private registry!");
        println!();
        println!("📦 Capsule:   {} v{}", result.scoped_id, result.version);
        println!("🛡️  Integrity: {}, {}", result.sha256, result.blake3);
        println!();
        println!("🌐 Artifact URL: {}", result.artifact_url);
        println!();
        println!(
            "👉 Next step: ato install {} --registry {}",
            result.scoped_id, result.registry_url
        );
    }
    futures::executor::block_on(
        reporter.notify("Private registry publish completed.".to_string()),
    )?;
    Ok(())
}

fn resolve_publish_registry_url(cli_registry: Option<String>) -> Result<String> {
    if let Some(url) = cli_registry {
        return normalize_registry_url(&url);
    }

    let cwd = std::env::current_dir().context("Failed to resolve current directory")?;
    let manifest_path = cwd.join("capsule.toml");
    if manifest_path.exists() {
        let raw = std::fs::read_to_string(&manifest_path)
            .with_context(|| format!("Failed to read {}", manifest_path.display()))?;
        let parsed: toml::Value = toml::from_str(&raw)
            .with_context(|| format!("Failed to parse {}", manifest_path.display()))?;
        if let Some(url) = parsed
            .get("store")
            .and_then(|v| v.get("registry"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return normalize_registry_url(url);
        }
    }

    Ok(DEFAULT_RUN_REGISTRY_URL.to_string())
}

fn normalize_registry_url(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("Registry URL cannot be empty");
    }
    let parsed =
        reqwest::Url::parse(trimmed).with_context(|| format!("Invalid registry URL: {}", raw))?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        anyhow::bail!(
            "Registry URL must use http or https scheme (got '{}')",
            parsed.scheme()
        );
    }
    Ok(trimmed.trim_end_matches('/').to_string())
}

fn is_official_publish_registry(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("api.ato.run") || host.eq_ignore_ascii_case("staging.api.ato.run")
}

fn execute_publish_tui_command(reporter: std::sync::Arc<reporters::CliReporter>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let outcome = publish_tui::execute().await?;
        match outcome {
            publish_tui::PublishTuiOutcome::Success(summary) => {
                println!("✅ Ato publish completed successfully.");
                println!("   Capsule: {} v{}", summary.capsule, summary.version);
                println!("   Run URL: {}", summary.run_url);
                futures::executor::block_on(
                    reporter.notify("Interactive publish orchestration completed.".to_string()),
                )?;
                Ok(())
            }
            publish_tui::PublishTuiOutcome::Failure(summary) => {
                println!("❌ Ato publish failed.");
                println!("   {}", summary.message);
                if let Some(conclusion) = summary.conclusion {
                    println!("   Conclusion: {}", conclusion);
                }
                if let Some(run_url) = summary.run_url {
                    println!("   Run URL: {}", run_url);
                }
                if !summary.details.is_empty() {
                    println!();
                    println!("Failure details:");
                    for job in summary.details {
                        println!(" - Job: {} ({})", job.name, job.status);
                        if job.failed_steps.is_empty() {
                            println!("   Failed steps: (not available)");
                        } else {
                            println!("   Failed steps: {}", job.failed_steps.join(", "));
                        }
                        if let Some(excerpt) = job.log_excerpt {
                            for line in excerpt.lines().take(12) {
                                println!("   {}", line);
                            }
                            if excerpt.lines().count() > 12 {
                                println!("   ... (truncated)");
                            }
                        }
                    }
                }
                if let Some(tag) = summary.tag {
                    println!();
                    println!("💡 Recovery:");
                    println!("   git tag -d {}", tag);
                    println!("   git push --delete origin {}", tag);
                    println!("   fix and run `ato publish` again");
                }
                anyhow::bail!(summary.message)
            }
            publish_tui::PublishTuiOutcome::Cancelled => {
                println!("Publish was cancelled.");
                Ok(())
            }
        }
    })
}

fn execute_setup_command(
    engine: String,
    version: Option<String>,
    skip_verify: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let em = engine_manager::EngineManager::new()?;
    let requested_version = version.unwrap_or_else(|| "latest".to_string());

    let (resolved_version, url, sha256) = match engine.as_str() {
        "nacelle" => {
            let os = if cfg!(target_os = "macos") {
                "darwin"
            } else if cfg!(target_os = "linux") {
                "linux"
            } else {
                anyhow::bail!("Unsupported OS");
            };
            let arch = if cfg!(target_arch = "x86_64") {
                "x64"
            } else if cfg!(target_arch = "aarch64") {
                "arm64"
            } else {
                anyhow::bail!("Unsupported architecture");
            };

            let resolved = if requested_version == "latest" {
                fetch_latest_nacelle_version()?
            } else {
                requested_version.clone()
            };

            let binary_name = format!("nacelle-{}-{}-{}", resolved, os, arch);
            let base_url = format!("https://releases.capsule.dev/nacelle/{}", resolved);
            let url = format!("{}/{}", base_url, binary_name);
            let sha256 = if skip_verify {
                String::new()
            } else {
                fetch_release_sha256(&base_url, &binary_name)?
            };
            (resolved, url, sha256)
        }
        _ => {
            anyhow::bail!(
                "Unknown engine: {}. Currently only 'nacelle' is supported.",
                engine
            );
        }
    };

    let path = em.download_engine(&engine, &resolved_version, &url, &sha256, &*reporter)?;

    futures::executor::block_on(reporter.notify(format!(
        "✅ Engine {} v{} installed at {}",
        engine,
        resolved_version,
        path.display()
    )))?;

    let mut cfg = capsule_core::config::load_config()?;
    cfg.engines.insert(
        engine.clone(),
        capsule_core::config::EngineRegistration {
            path: path.display().to_string(),
        },
    );
    if cfg.default_engine.is_none() {
        cfg.default_engine = Some(engine.clone());
    }
    capsule_core::config::save_config(&cfg)?;

    futures::executor::block_on(reporter.notify("✅ Registered as default engine".to_string()))?;

    Ok(())
}

fn fetch_latest_nacelle_version() -> Result<String> {
    let resp = reqwest::blocking::get("https://releases.capsule.dev/nacelle/latest.txt")
        .context("Failed to fetch latest nacelle version")?
        .text()?;
    let version = resp.trim();
    if version.is_empty() {
        anyhow::bail!("Latest nacelle version response was empty");
    }
    Ok(version.to_string())
}

fn fetch_release_sha256(base_url: &str, binary_name: &str) -> Result<String> {
    let checksum_urls = [
        format!("{}/SHA256SUMS", base_url),
        format!("{}/SHA256SUMS.txt", base_url),
        format!("{}/SHASUMS256.txt", base_url),
        format!("{}/{}.sha256", base_url, binary_name),
    ];

    for checksum_url in checksum_urls {
        let response = match reqwest::blocking::get(&checksum_url) {
            Ok(resp) => resp,
            Err(_) => continue,
        };

        if !response.status().is_success() {
            continue;
        }

        let body = response
            .text()
            .with_context(|| format!("Failed to read checksum file {}", checksum_url))?;

        if let Some(hash) = parse_sha256_for_artifact(&body, binary_name) {
            return Ok(hash);
        }

        if checksum_url.ends_with(".sha256") {
            if let Some(hash) = extract_first_sha256_hex(&body) {
                return Ok(hash);
            }
        }
    }

    anyhow::bail!(
        "Failed to resolve SHA256 for {} from release metadata at {}",
        binary_name,
        base_url
    )
}

fn parse_sha256_for_artifact(checksum_body: &str, binary_name: &str) -> Option<String> {
    for raw_line in checksum_body.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(stripped) = line
            .strip_prefix("SHA256 (")
            .or_else(|| line.strip_prefix("sha256 ("))
        {
            if let Some((name, hash_part)) = stripped.split_once(')') {
                let hash = hash_part.trim().trim_start_matches('=').trim();
                if name.trim() == binary_name && is_sha256_hex(hash) {
                    return Some(hash.to_ascii_lowercase());
                }
            }
        }

        let mut parts = line.split_whitespace();
        let Some(hash) = parts.next() else {
            continue;
        };
        let Some(name) = parts.last() else {
            continue;
        };
        let normalized_name = name.trim_start_matches('*').trim_start_matches("./");
        if normalized_name == binary_name && is_sha256_hex(hash) {
            return Some(hash.to_ascii_lowercase());
        }
    }

    None
}

fn extract_first_sha256_hex(raw: &str) -> Option<String> {
    raw.split_whitespace()
        .find(|token| is_sha256_hex(token))
        .map(|token| token.to_ascii_lowercase())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn execute_run_like_command(
    path: PathBuf,
    target: Option<String>,
    watch: bool,
    background: bool,
    nacelle: Option<PathBuf>,
    registry: Option<String>,
    enforcement: EnforcementMode,
    sandbox_mode: bool,
    unsafe_mode_legacy: bool,
    unsafe_bypass_sandbox_legacy: bool,
    dangerously_skip_permissions: bool,
    yes: bool,
    allow_unverified: bool,
    skill: Option<String>,
    from_skill: Option<PathBuf>,
    deprecation_warning: Option<&str>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    if let Some(warning) = deprecation_warning {
        eprintln!("{warning}");
    }

    let rt = tokio::runtime::Runtime::new()?;

    let resolved_skill_path = match (skill, from_skill) {
        (Some(skill_name), None) => Some(skill_resolver::resolve_skill_path(&skill_name)?),
        (None, Some(path)) => Some(path),
        (None, None) => None,
        (Some(_), Some(_)) => {
            anyhow::bail!("--skill and --from-skill are mutually exclusive");
        }
    };

    if let Some(skill_path) = resolved_skill_path {
        if watch {
            anyhow::bail!("--skill/--from-skill does not support --watch in MVP mode");
        }
        if background {
            anyhow::bail!("--skill/--from-skill does not support --background in MVP mode");
        }

        let generated = skill::materialize_skill_capsule(&skill_path)?;
        debug!(
            manifest_path = %generated.manifest_path().display(),
            "Translated SKILL.md to capsule"
        );

        let sandbox_requested = sandbox_mode || unsafe_mode_legacy || unsafe_bypass_sandbox_legacy;
        let effective_enforcement = enforce_sandbox_mode_flags(
            enforcement,
            sandbox_requested,
            dangerously_skip_permissions,
            reporter.clone(),
        )?;
        return execute_open_command(
            generated.manifest_path().to_path_buf(),
            target,
            watch,
            background,
            nacelle,
            effective_enforcement,
            sandbox_requested,
            dangerously_skip_permissions,
            yes,
            reporter,
        );
    }

    let path = rt.block_on(resolve_run_target_or_install(
        path,
        yes,
        allow_unverified,
        registry.as_deref(),
        reporter.clone(),
    ))?;

    let sandbox_requested = sandbox_mode || unsafe_mode_legacy || unsafe_bypass_sandbox_legacy;
    let effective_enforcement = enforce_sandbox_mode_flags(
        enforcement,
        sandbox_requested,
        dangerously_skip_permissions,
        reporter.clone(),
    )?;
    execute_open_command(
        path,
        target,
        watch,
        background,
        nacelle,
        effective_enforcement,
        sandbox_requested,
        dangerously_skip_permissions,
        yes,
        reporter,
    )
}

async fn resolve_run_target_or_install(
    path: PathBuf,
    yes: bool,
    allow_unverified: bool,
    registry: Option<&str>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<PathBuf> {
    let raw = path.to_string_lossy().to_string();
    if is_explicit_local_path_input(&raw) {
        return Ok(expand_explicit_local_path(&raw));
    }

    let scoped_ref = match install::parse_capsule_ref(&raw) {
        Ok(value) => value,
        Err(error) => {
            if install::is_slug_only_ref(&raw) {
                let effective_registry = registry.unwrap_or(DEFAULT_RUN_REGISTRY_URL);
                let suggestions =
                    install::suggest_scoped_capsules(&raw, Some(effective_registry), 5).await?;
                if suggestions.is_empty() {
                    anyhow::bail!(
                        "scoped_id_required: '{}' is not valid for `ato run`. Use publisher/slug (for example: koh0920/{}).",
                        raw,
                        raw.trim()
                    );
                }

                let mut message = format!(
                    "scoped_id_required: '{}' is ambiguous. Specify publisher/slug.\n\nDid you mean one of these?",
                    raw
                );
                for suggestion in suggestions {
                    message.push_str(&format!(
                        "\n  - {}  ({} downloads)",
                        suggestion.scoped_id, suggestion.downloads
                    ));
                }
                message.push_str("\n\nRun `ato search ");
                message.push_str(raw.trim());
                message.push_str("` to see more options.");
                anyhow::bail!(message);
            }
            return Err(error).context(
                "Invalid run target. Use ./, ../, ~/, / for local paths, or publisher/slug for store capsules.",
            );
        }
    };

    if let Some(installed_capsule) =
        resolve_installed_capsule_archive(&scoped_ref, registry).await?
    {
        debug!(
            capsule = %installed_capsule.display(),
            "Using installed capsule"
        );
        return Ok(installed_capsule);
    }

    let json_mode = matches!(reporter.as_ref(), reporters::CliReporter::Json(_));
    if json_mode && !yes {
        anyhow::bail!(
            "Non-interactive JSON mode requires -y/--yes when auto-installing missing capsules"
        );
    }

    if !yes
        && !can_prompt_interactively(
            std::io::stdin().is_terminal(),
            std::io::stdout().is_terminal(),
        )
    {
        anyhow::bail!(
            "Interactive install confirmation requires a TTY. Re-run with -y/--yes in CI or non-interactive environments."
        );
    }

    let effective_registry = registry.unwrap_or(DEFAULT_RUN_REGISTRY_URL);
    let detail =
        install::fetch_capsule_detail(&scoped_ref.scoped_id, Some(effective_registry)).await?;
    let installable_version = detail
        .latest_version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot auto-install '{}': no installable version is published.",
                detail.scoped_id
            )
        })?
        .to_string();

    if !yes {
        let approved = prompt_install_confirmation(&detail, &installable_version)?;
        if !approved {
            anyhow::bail!("Installation cancelled by user");
        }
    } else {
        debug!(
            scoped_id = %detail.scoped_id,
            "Capsule not installed; continuing with -y auto-install"
        );
    }

    let install_result = install::install_app(
        &scoped_ref.scoped_id,
        Some(effective_registry),
        Some(installable_version.as_str()),
        None,
        false,
        allow_unverified,
        json_mode,
    )
    .await?;
    Ok(install_result.path)
}

fn is_explicit_local_path_input(raw: &str) -> bool {
    if raw.is_empty() {
        return false;
    }
    if raw == "." || raw == ".." {
        return true;
    }
    if raw.starts_with("./")
        || raw.starts_with("../")
        || raw.starts_with(".\\")
        || raw.starts_with("..\\")
        || raw.starts_with("~/")
        || raw.starts_with("~\\")
        || raw.starts_with('/')
        || raw.starts_with('\\')
    {
        return true;
    }
    raw.len() >= 3
        && raw.as_bytes()[1] == b':'
        && (raw.as_bytes()[2] == b'/' || raw.as_bytes()[2] == b'\\')
        && raw.as_bytes()[0].is_ascii_alphabetic()
}

fn expand_explicit_local_path(raw: &str) -> PathBuf {
    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("~\\")) {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

async fn resolve_installed_capsule_archive(
    scoped_ref: &install::ScopedCapsuleRef,
    registry: Option<&str>,
) -> Result<Option<PathBuf>> {
    let store_root = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".capsule")
        .join("store");
    if let Some(path) = resolve_installed_capsule_archive_in_store(
        &store_root.join(&scoped_ref.publisher),
        &scoped_ref.slug,
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
    )
}

fn resolve_installed_capsule_archive_in_store(
    store_root: &std::path::Path,
    slug: &str,
) -> Result<Option<PathBuf>> {
    let slug_dir = store_root.join(slug);
    if !slug_dir.exists() || !slug_dir.is_dir() {
        return Ok(None);
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
    stdin_is_tty && stdout_is_tty
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

    Ok(enforcement)
}

fn execute_open_command(
    path: PathBuf,
    target: Option<String>,
    watch: bool,
    background: bool,
    nacelle: Option<PathBuf>,
    enforcement: EnforcementMode,
    sandbox_mode: bool,
    dangerously_skip_permissions: bool,
    assume_yes: bool,
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
        assume_yes,
        reporter,
    }))
}

fn execute_source_sync_status_command(
    source_id: String,
    sync_run_id: String,
    registry: Option<String>,
    json: bool,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result =
            source::fetch_sync_run_status(&source_id, &sync_run_id, registry.as_deref(), json)
                .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Ok(())
    })
}

fn execute_source_rebuild_command(
    source_id: String,
    reference: Option<String>,
    wait: bool,
    registry: Option<String>,
    json: bool,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result = source::rebuild_source(
            &source_id,
            reference.as_deref(),
            wait,
            registry.as_deref(),
            json,
        )
        .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Ok(())
    })
}

fn execute_search_command(
    query: Option<String>,
    category: Option<String>,
    tags: Vec<String>,
    limit: Option<usize>,
    cursor: Option<String>,
    registry: Option<String>,
    json: bool,
    no_tui: bool,
    show_manifest: bool,
) -> Result<()> {
    if std::io::stdout().is_terminal() && !json && !no_tui {
        let selected = tui::run_search_tui(tui::SearchTuiArgs {
            query: query.clone(),
            category: category.clone(),
            tags: tags.clone(),
            limit,
            cursor: cursor.clone(),
            registry: registry.clone(),
            show_manifest,
        })?;
        if let Some(scoped_id) = selected {
            println!("{}", scoped_id);
        }
        return Ok(());
    }

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result = search::search_capsules(
            query.as_deref(),
            category.as_deref(),
            Some(tags.as_slice()),
            limit,
            cursor.as_deref(),
            registry.as_deref(),
            json,
        )
        .await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Ok(())
    })
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
    fn explicit_local_path_rules() {
        assert!(is_explicit_local_path_input("./foo"));
        assert!(is_explicit_local_path_input("../foo"));
        assert!(is_explicit_local_path_input("~/foo"));
        assert!(is_explicit_local_path_input("/tmp/foo"));
        assert!(is_explicit_local_path_input("."));
        assert!(is_explicit_local_path_input(".."));
        assert!(!is_explicit_local_path_input("foo"));
        assert!(!is_explicit_local_path_input("foo/bar"));
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
    fn bare_relative_scoped_like_input_is_not_local() {
        let tmp = tempfile::tempdir().unwrap();
        let scoped_like = tmp.path().join("my-org").join("my-tool");
        std::fs::create_dir_all(&scoped_like).unwrap();
        assert!(!is_explicit_local_path_input("my-org/my-tool"));
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

        let resolved = resolve_installed_capsule_archive_in_store(tmp.path(), slug)
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.file_name().and_then(|name| name.to_str()),
            Some("new.capsule")
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
    fn parse_sha256_for_artifact_supports_sha256sums_format() {
        let body = "\
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  nacelle-v1.2.3-darwin-arm64
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  nacelle-v1.2.3-linux-x64
";
        let parsed = parse_sha256_for_artifact(body, "nacelle-v1.2.3-linux-x64");
        assert_eq!(
            parsed.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
    }

    #[test]
    fn parse_sha256_for_artifact_supports_bsd_style_format() {
        let body = "SHA256 (nacelle-v1.2.3-darwin-arm64) = CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
        let parsed = parse_sha256_for_artifact(body, "nacelle-v1.2.3-darwin-arm64");
        assert_eq!(
            parsed.as_deref(),
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
        );
    }

    #[test]
    fn extract_first_sha256_hex_reads_single_file_checksum() {
        let body = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd  nacelle-v1.2.3-darwin-arm64";
        let parsed = extract_first_sha256_hex(body);
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
        let err = enforce_sandbox_mode_flags(EnforcementMode::Strict, false, true, reporter)
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
        let result = enforce_sandbox_mode_flags(EnforcementMode::Strict, false, true, reporter);
        assert!(result.is_ok());

        std::env::remove_var("CAPSULE_ALLOW_UNSAFE");
    }
}
