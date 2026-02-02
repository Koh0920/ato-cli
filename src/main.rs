use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::json;
use std::io::IsTerminal;
use std::path::PathBuf;

use capsule_core::CapsuleReporter;

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

#[cfg(feature = "manifest-signing")]
mod capsule_capnp;
mod commands;
mod common;
mod engine_manager;
mod env;
mod error_codes;
mod executors;
mod init;
mod keygen;
mod new;
mod observability;
mod process_manager;
mod reporters;
mod scaffold;
mod sign;

#[derive(Parser)]
#[command(name = "capsule")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "capsule CLI (meta-runtime) - dispatches to engines like nacelle")]
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
    /// Engine-related commands
    Engine {
        #[command(subcommand)]
        command: EngineCommands,
    },

    /// Setup and download engines
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

    /// Run a capsule (local or archive)
    Open {
        /// Path to a .capsule archive or directory containing capsule.toml
        #[arg()]
        path: PathBuf,

        /// Run in development mode (foreground) with hot-reloading on file changes
        #[arg(long)]
        watch: bool,

        /// Run in background mode (detached)
        #[arg(long)]
        background: bool,

        /// Path to nacelle engine binary (overrides NACELLE_PATH)
        #[arg(long)]
        nacelle: Option<PathBuf>,

        /// Network enforcement mode
        #[arg(long, value_enum, default_value_t = EnforcementMode::BestEffort)]
        enforcement: EnforcementMode,
    },

    /// Create a new capsule project from a template
    New {
        /// Project name
        name: String,

        /// Template type: python, node, hono, rust, go, shell
        #[arg(long, default_value = "python")]
        template: String,
    },

    /// Generate a new Ed25519 signing keypair
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

    /// Create a capsule artifact from a directory
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
        #[arg(long, value_enum, default_value_t = EnforcementMode::BestEffort)]
        enforcement: EnforcementMode,

        /// Create self-extracting executable installer (includes nacelle runtime)
        #[arg(long)]
        standalone: bool,
    },

    /// Scaffold supporting files (e.g. Dockerfile)
    Scaffold {
        #[command(subcommand)]
        command: ScaffoldCommands,
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

    /// List running capsules
    Ps {
        /// Show all capsules including stopped ones
        #[arg(long, default_value_t = false)]
        all: bool,

        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },

    /// Stop a running capsule
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

    /// Show logs of a running capsule
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

    /// Handle guest mode requests (internal)
    Guest {
        /// Path to a .sync archive
        #[arg()]
        sync_path: PathBuf,
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

fn main() {
    if let Err(err) = run() {
        if let Some(capsule_error) = err.downcast_ref::<capsule_core::CapsuleError>() {
            match capsule_error {
                capsule_core::CapsuleError::AuthRequired(target) => {
                    eprintln!("🛑 Authentication Required");
                    eprintln!("Please login or provide credentials: {}", target);
                }
                capsule_core::CapsuleError::ContainerEngine(msg) => {
                    eprintln!("🛑 Container engine unavailable");
                    eprintln!("{}", msg);
                }
                capsule_core::CapsuleError::Pack(msg) => {
                    eprintln!("❌ Build Failed: {}", msg);
                }
                _ => {
                    eprintln!("Error: {}", err);
                }
            }
        } else {
            eprintln!("Unexpected Error: {:?}", err);
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let reporter = std::sync::Arc::new(reporters::CliReporter::new(cli.json));

    match cli.command {
        Commands::Engine {
            command: EngineCommands::Features,
        } => {
            let nacelle =
                capsule_core::engine::discover_nacelle(capsule_core::engine::EngineRequest {
                    explicit_path: cli.nacelle,
                    manifest_path: None,
                })?;
            let payload = json!({ "spec_version": "0.1.0" });
            let resp = capsule_core::engine::run_internal(&nacelle, "features", &payload)?;
            let body = serde_json::to_string_pretty(&resp)?;
            futures::executor::block_on(reporter.notify(body))?;
            Ok(())
        }

        Commands::Engine {
            command:
                EngineCommands::Register {
                    name,
                    path,
                    default,
                },
        } => {
            let resolved_path = if let Some(p) = path {
                p
            } else if let Ok(env_path) = std::env::var("NACELLE_PATH") {
                PathBuf::from(env_path)
            } else {
                anyhow::bail!("Missing --path and NACELLE_PATH is not set");
            };

            // Validate path by running the resolver with explicit_path.
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

        Commands::Setup {
            engine,
            version,
            skip_verify: _,
        } => {
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
                        let resp = reqwest::blocking::get(
                            "https://releases.capsule.dev/nacelle/latest.txt",
                        )
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

            futures::executor::block_on(
                reporter.notify("✅ Registered as default engine".to_string()),
            )?;

            Ok(())
        }

        Commands::Open {
            path,
            watch,
            background,
            nacelle,
            enforcement,
        } => {
            let target = if path.is_file() || path.extension().map_or(false, |ext| ext == "capsule")
            {
                path.clone()
            } else {
                path.join("capsule.toml")
            };

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(commands::open::execute(commands::open::OpenArgs {
                target,
                watch,
                background,
                nacelle,
                enforcement: enforcement.as_str().to_string(),
                reporter: reporter.clone(),
            }))
        }

        Commands::New { name, template } => new::execute(
            new::NewArgs {
                name,
                template: Some(template),
            },
            reporter.clone(),
        ),

        Commands::Keygen { out, force, json } => {
            keygen::execute(keygen::KeygenArgs { out, force, json }, reporter.clone())
        }

        Commands::Pack {
            dir,
            init,
            key,
            standalone,
            enforcement,
        } => {
            let dir = dir
                .canonicalize()
                .with_context(|| format!("Failed to resolve directory: {}", dir.display()))?;
            if !dir.is_dir() {
                anyhow::bail!("Target is not a directory: {}", dir.display());
            }

            let manifest = dir.join("capsule.toml");
            if !manifest.exists() {
                let stdin_is_tty = std::io::stdin().is_terminal();
                if init {
                    if !stdin_is_tty {
                        anyhow::bail!("--init requires an interactive TTY");
                    }
                    if cli.json {
                        anyhow::bail!("--init cannot be used with --json output");
                    }
                    init::execute(
                        init::InitArgs {
                            path: Some(dir.clone()),
                            yes: false,
                        },
                        reporter.clone(),
                    )?;
                } else {
                    anyhow::bail!(
                        "capsule.toml not found. Use --init to create one, or specify a directory with capsule.toml."
                    );
                }
            }

            if !manifest.exists() {
                anyhow::bail!("capsule.toml not found after initialization");
            }

            let manifest = dir.join("capsule.toml");
            if !manifest.exists() {
                let stdin_is_tty = std::io::stdin().is_terminal();
                if init {
                    if !stdin_is_tty {
                        anyhow::bail!("--init requires an interactive TTY");
                    }
                    if cli.json {
                        anyhow::bail!("--init cannot be used with --json output");
                    }
                    init::execute(
                        init::InitArgs {
                            path: Some(dir.clone()),
                            yes: false,
                        },
                        reporter.clone(),
                    )?;
                } else {
                    anyhow::bail!(
                        "capsule.toml not found. Use --init to create one, or specify a directory with capsule.toml."
                    );
                }
            }

            if !manifest.exists() {
                anyhow::bail!("capsule.toml not found after initialization");
            }

            futures::executor::block_on(
                reporter.notify("📦 Capsule Pack - Pure Runtime Architecture v3.0".to_string()),
            )?;
            futures::executor::block_on(
                reporter.notify("   Performing build-time validations...\n".to_string()),
            )?;

            let decision = capsule_core::router::route_manifest(
                &manifest,
                capsule_core::router::ExecutionProfile::Release,
            )?;

            futures::executor::block_on(reporter.notify(format!(
                "🧭 RuntimeRouter: {:?} ({})",
                decision.kind, decision.reason
            )))?;

            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));

            match decision.kind {
                capsule_core::router::RuntimeKind::Source => {
                    let artifact_path = capsule_core::packers::source::pack(
                        &decision.plan,
                        capsule_core::packers::source::SourcePackOptions {
                            manifest_path: manifest.clone(),
                            manifest_dir: manifest_dir.clone(),
                            output: None,
                            runtime: None,
                            skip_l1: false,
                            skip_validation: false,
                            enforcement: enforcement.as_str().to_string(),
                            nacelle_override: cli.nacelle,
                            standalone,
                        },
                        reporter.clone(),
                    )?;

                    let _ = sign_if_requested(&artifact_path, key.as_ref(), reporter.clone())?;

                    futures::executor::block_on(
                        reporter.notify(format!("✅ Pack complete: {}", artifact_path.display())),
                    )?;
                }
                capsule_core::router::RuntimeKind::Oci => {
                    let result =
                        capsule_core::packers::oci::pack(&decision.plan, None, reporter.as_ref())?;
                    if let Some(path) = result.archive {
                        let _ = sign_if_requested(&path, key.as_ref(), reporter.clone())?;
                        futures::executor::block_on(
                            reporter.notify(format!("✅ Pack complete: {}", path.display())),
                        )?;
                    } else if key.is_some() {
                        futures::executor::block_on(reporter.warn(
                            "ℹ️  Signature skipped: OCI pack produced no archive file".to_string(),
                        ))?;
                    } else {
                        futures::executor::block_on(
                            reporter.notify(format!("✅ Pack complete: {}", result.image)),
                        )?;
                    }
                }
                capsule_core::router::RuntimeKind::Wasm => {
                    let result = capsule_core::packers::wasm::pack(
                        &decision.plan,
                        None,
                        None,
                        reporter.as_ref(),
                    )?;
                    futures::executor::block_on(
                        reporter.notify(format!("✅ Pack complete: {}", result.artifact.display())),
                    )?;
                    let _ = sign_if_requested(&result.artifact, key.as_ref(), reporter.clone())?;
                }
            }

            Ok(())
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

        Commands::Ps { all, json } => {
            commands::ps::execute(commands::ps::PsArgs { all, json }, reporter.clone())
        }

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
    }
}

fn sign_if_requested(
    target: &std::path::Path,
    key: Option<&PathBuf>,
    reporter: std::sync::Arc<reporters::CliReporter>,
) -> Result<Option<PathBuf>> {
    if let Some(key_path) = key {
        futures::executor::block_on(
            reporter.notify("🔐 Generating detached signature...".to_string()),
        )?;
        let sig_path = capsule_core::signing::sign_artifact(target, key_path, "capsule-cli", None)?;
        futures::executor::block_on(
            reporter.notify(format!("✅ Signature: {}", sig_path.display())),
        )?;
        return Ok(Some(sig_path));
    }
    Ok(None)
}
