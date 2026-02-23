use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde_json::json;
use std::cmp::Ordering;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;

use capsule_core::CapsuleReporter;

const ATO_ASCII_ART: &str = r#"
     _   _        
    / \ | |_ ___  
   / _ \| __/ _ \ 
  / ___ \ || (_) |
 /_/   \_\__\___/ 

        Ato
"#;
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

mod auth;
#[cfg(feature = "manifest-signing")]
mod capsule_capnp;
mod commands;
mod common;
mod diagnostics;
mod engine_manager;
mod env;
mod error_codes;
mod executors;
mod init;
mod install;
mod ipc;
mod keygen;
mod new;
mod observability;
mod process_manager;
mod profile;
mod registry;
mod reporters;
mod scaffold;
mod search;
mod sign;
mod source;
mod verify;

#[derive(Parser)]
#[command(name = "ato")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "ato CLI (meta-runtime) - dispatches to engines like nacelle")]
#[command(help_template = "\
{about-with-newline}
Usage: {usage}

Primary Commands:
  run      Run a capsule app or local project (strict sandbox by default)
  install  Install a package from the store
  init     Initialize a new project
  build    Build project into a capsule archive (includes smoke test)
  search   Search the store for packages

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
  publish  Register a GitHub repository to the registry

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
        /// Path to a .capsule archive or directory containing capsule.toml (default: current directory)
        #[arg(default_value = ".")]
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

        /// Explicitly allow non-strict sandbox execution (unsafe)
        #[arg(long, default_value_t = false)]
        unsafe_bypass_sandbox: bool,

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
        /// Capsule slug or ID
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
        about = "Manage configuration (registry, engine, source)"
    )]
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    #[command(
        next_help_heading = "Advanced Commands",
        about = "Register a GitHub repository to the registry"
    )]
    Publish {
        /// GitHub repository URL (public)
        repo_url: String,

        /// Registry URL (default: localhost:8787 for beta)
        #[arg(long)]
        registry: Option<String>,

        /// Distribution channel (stable|beta)
        #[arg(long)]
        channel: Option<String>,

        /// Automatically submit to playground review queue after sync
        #[arg(short = 'p', long = "apply-playground", default_value_t = false)]
        apply_playground: bool,

        /// GitHub App installation ID (required for session-token based flow)
        #[arg(long)]
        installation_id: Option<u64>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    #[command(hide = true)]
    Engine {
        #[command(subcommand)]
        command: EngineCommands,
    },

    #[command(hide = true)]
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

        /// Explicitly allow non-strict sandbox execution (unsafe)
        #[arg(long, default_value_t = false)]
        unsafe_bypass_sandbox: bool,

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

    /// Source configuration
    Source {
        #[command(subcommand)]
        command: ConfigSourceCommands,
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
enum ConfigSourceCommands {
    /// Register a public GitHub repository URL as a distribution source
    Register {
        /// GitHub repository URL (public)
        repo_url: String,

        /// Registry URL (default: localhost:8787 for beta)
        #[arg(long)]
        registry: Option<String>,

        /// Distribution channel (stable|beta)
        #[arg(long)]
        channel: Option<String>,

        /// Automatically submit to playground review queue after sync
        #[arg(short = 'p', long = "apply-playground", default_value_t = false)]
        apply_playground: bool,

        /// GitHub App installation ID (required for session-token based flow)
        #[arg(long)]
        installation_id: Option<u64>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
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
}

#[derive(Subcommand)]
enum SourceCommands {
    /// Register a public GitHub repository URL as a distribution source
    Register {
        /// GitHub repository URL (public)
        repo_url: String,

        /// Registry URL (default: localhost:8787 for beta)
        #[arg(long)]
        registry: Option<String>,

        /// Distribution channel (stable|beta)
        #[arg(long)]
        channel: Option<String>,

        /// Automatically submit to playground review queue after sync
        #[arg(short = 'p', long = "apply-playground", default_value_t = false)]
        apply_playground: bool,

        /// GitHub App installation ID (required for session-token based flow)
        #[arg(long)]
        installation_id: Option<u64>,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
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
    },
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_mode = args.iter().any(|arg| arg == "--json");
    let command_context = diagnostics::detect_command_context(&args);

    if let Err(err) = run() {
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
    if std::env::args_os().count() == 1 {
        println!("{}", ATO_ASCII_ART);
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
            target,
            watch,
            background,
            nacelle,
            registry,
            enforcement,
            unsafe_bypass_sandbox,
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
            unsafe_bypass_sandbox,
            yes,
            allow_unverified,
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
            skip_verify: _,
        } => execute_setup_command(engine, version, reporter.clone()),

        Commands::Open {
            path,
            target,
            watch,
            background,
            nacelle,
            registry,
            enforcement,
            unsafe_bypass_sandbox,
            yes,
        } => execute_run_like_command(
            path,
            target,
            watch,
            background,
            nacelle,
            registry,
            enforcement,
            unsafe_bypass_sandbox,
            yes,
            false,
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
            enforcement,
            keep_failed_artifacts,
        } => commands::build::execute_pack_command(
            dir,
            init,
            key,
            standalone,
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
            enforcement,
            keep_failed_artifacts,
        } => {
            eprintln!("⚠️  'ato pack' is deprecated. Use 'ato build' instead.");
            commands::build::execute_pack_command(
                dir,
                init,
                key,
                standalone,
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
        } => execute_search_command(query, category, tags, limit, cursor, registry, json),

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
                    skip_verify: _,
                } => execute_setup_command(engine, version, reporter.clone()),
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
            ConfigCommands::Source { command } => match command {
                ConfigSourceCommands::Register {
                    repo_url,
                    registry,
                    channel,
                    apply_playground,
                    installation_id,
                    json,
                } => execute_source_register_command(
                    repo_url,
                    registry,
                    channel,
                    apply_playground,
                    installation_id,
                    false,
                    json,
                ),
            },
        },

        Commands::Publish {
            repo_url,
            registry,
            channel,
            apply_playground,
            installation_id,
            json,
        } => {
            if !json {
                println!("Registering GitHub repository as the source of truth...");
                println!(
                    "(Not uploading local artifacts. Ensure your changes are pushed to main.)"
                );
            }
            execute_source_register_command(
                repo_url,
                registry,
                channel,
                apply_playground,
                installation_id,
                true,
                json,
            )
        }

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
                },
        } => execute_search_command(query, category, tags, limit, cursor, registry, json),

        Commands::Source { command } => match command {
            SourceCommands::Register {
                repo_url,
                registry,
                channel,
                apply_playground,
                installation_id,
                json,
            } => execute_source_register_command(
                repo_url,
                registry,
                channel,
                apply_playground,
                installation_id,
                false,
                json,
            ),
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
        }
    })
}

fn execute_setup_command(
    engine: String,
    version: Option<String>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    let em = engine_manager::EngineManager::new()?;
    let version = version.unwrap_or_else(|| "latest".to_string());

    let (url, sha256) = match engine.as_str() {
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

            let ver = if version == "latest" {
                let resp =
                    reqwest::blocking::get("https://releases.capsule.dev/nacelle/latest.txt")
                        .context("Failed to fetch latest version")?
                        .text()?;
                resp.trim().to_string()
            } else {
                version.clone()
            };

            let url = format!(
                "https://releases.capsule.dev/nacelle/{}/nacelle-{}-{}-{}",
                ver, ver, os, arch
            );
            (url, "".to_string())
        }
        _ => {
            anyhow::bail!(
                "Unknown engine: {}. Currently only 'nacelle' is supported.",
                engine
            );
        }
    };

    let path = em.download_engine(&engine, &version, &url, &sha256, &*reporter)?;

    futures::executor::block_on(reporter.notify(format!(
        "✅ Engine {} v{} installed at {}",
        engine,
        version,
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

fn execute_run_like_command(
    path: PathBuf,
    target: Option<String>,
    watch: bool,
    background: bool,
    nacelle: Option<PathBuf>,
    registry: Option<String>,
    enforcement: EnforcementMode,
    unsafe_bypass_sandbox: bool,
    yes: bool,
    allow_unverified: bool,
    deprecation_warning: Option<&str>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    if let Some(warning) = deprecation_warning {
        eprintln!("{warning}");
    }

    let rt = tokio::runtime::Runtime::new()?;
    let path = rt.block_on(resolve_run_target_or_install(
        path,
        yes,
        allow_unverified,
        registry.as_deref(),
        reporter.clone(),
    ))?;

    enforce_sandbox_mode_flags(enforcement, unsafe_bypass_sandbox, reporter.clone())?;
    execute_open_command(
        path,
        target,
        watch,
        background,
        nacelle,
        enforcement,
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
    if is_local_run_target(&path) {
        return Ok(path);
    }

    if !is_store_slug_candidate(&path) {
        return Ok(path);
    }

    let slug = path.to_string_lossy().to_string();
    if let Some(installed_capsule) = resolve_installed_capsule_archive(&slug)? {
        reporter
            .notify(format!(
                "📦 Using installed capsule: {}",
                installed_capsule.display()
            ))
            .await?;
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
    let detail = install::fetch_capsule_detail(&slug, Some(effective_registry)).await?;
    if !yes {
        let approved = prompt_install_confirmation(&detail)?;
        if !approved {
            anyhow::bail!("Installation cancelled by user");
        }
    } else {
        reporter
            .notify(format!(
                "📦 '{}' is not installed; continuing with -y auto-install",
                detail.slug
            ))
            .await?;
    }

    let install_result = install::install_app(
        &slug,
        Some(effective_registry),
        None,
        None,
        false,
        allow_unverified,
        json_mode,
    )
    .await?;
    Ok(install_result.path)
}

fn is_local_run_target(path: &std::path::Path) -> bool {
    if path.extension().is_some_and(|ext| ext == "capsule") {
        return true;
    }
    path.exists()
}

fn is_store_slug_candidate(path: &std::path::Path) -> bool {
    if path.is_absolute() {
        return false;
    }

    let Some(raw) = path.to_str() else {
        return false;
    };
    if raw.is_empty()
        || raw == "."
        || raw == ".."
        || raw.contains('/')
        || raw.contains('\\')
        || raw.contains('.')
    {
        return false;
    }

    raw.chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

fn resolve_installed_capsule_archive(slug: &str) -> Result<Option<PathBuf>> {
    let store_root = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".capsule")
        .join("store");
    resolve_installed_capsule_archive_in_store(&store_root, slug)
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

fn prompt_install_confirmation(detail: &install::CapsuleDetailSummary) -> Result<bool> {
    println!();
    println!("[!] Capsule '{}' is not installed.", detail.slug);
    println!();
    let version = if detail.latest_version.trim().is_empty() {
        "unknown"
    } else {
        detail.latest_version.trim()
    };
    let name = if detail.name.trim().is_empty() {
        detail.slug.as_str()
    } else {
        detail.name.trim()
    };
    println!("📦 {} (v{})", name, version);
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
    unsafe_bypass_sandbox: bool,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<()> {
    if matches!(enforcement, EnforcementMode::BestEffort) && !unsafe_bypass_sandbox {
        anyhow::bail!("--enforcement best-effort requires --unsafe-bypass-sandbox");
    }
    if matches!(enforcement, EnforcementMode::BestEffort) && unsafe_bypass_sandbox {
        futures::executor::block_on(
            reporter.warn("⚠️  Unsafe mode enabled: running with best_effort sandbox".to_string()),
        )?;
    }

    Ok(())
}

fn execute_open_command(
    path: PathBuf,
    target: Option<String>,
    watch: bool,
    background: bool,
    nacelle: Option<PathBuf>,
    enforcement: EnforcementMode,
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
        reporter,
    }))
}

fn execute_source_register_command(
    repo_url: String,
    registry: Option<String>,
    channel: Option<String>,
    apply_playground: bool,
    installation_id: Option<u64>,
    auto_sync_on_exists: bool,
    json: bool,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let result = source::register_github_source(
            &repo_url,
            registry.as_deref(),
            channel.as_deref(),
            apply_playground,
            installation_id,
            auto_sync_on_exists,
            json,
        )
        .await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Ok(())
    })
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
) -> Result<()> {
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

    #[test]
    fn store_slug_candidate_rules() {
        assert!(is_store_slug_candidate(std::path::Path::new(
            "ato-explorer"
        )));
        assert!(is_store_slug_candidate(std::path::Path::new("abc-123")));
        assert!(!is_store_slug_candidate(std::path::Path::new("foo/bar")));
        assert!(!is_store_slug_candidate(std::path::Path::new("./foo")));
        assert!(!is_store_slug_candidate(std::path::Path::new(
            "capsule.toml"
        )));
        assert!(!is_store_slug_candidate(std::path::Path::new("Foo")));
        assert!(!is_store_slug_candidate(std::path::Path::new(".")));
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
    fn existing_directory_is_treated_as_local_target() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(is_local_run_target(tmp.path()));
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
}
