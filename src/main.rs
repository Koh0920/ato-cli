use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;

mod config;
mod engine;
mod init;
mod keygen;
mod new;

#[derive(Parser)]
#[command(name = "capsule")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "capsule CLI (meta-runtime) - dispatches to engines like nacelle")]
struct Cli {
    /// Path to nacelle engine binary (overrides NACELLE_PATH)
    #[arg(long)]
    nacelle: Option<PathBuf>,

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

    /// Run a capsule in dev mode (dispatches to engine)
    Dev {
        /// Path to capsule.toml
        #[arg(long, default_value = "capsule.toml")]
        manifest: PathBuf,
    },

    /// Create a new capsule project from a template
    New {
        /// Project name
        name: String,

        /// Template type: python, node, hono, rust, shell
        #[arg(long, default_value = "python")]
        template: String,
    },

    /// Initialize an existing project as a capsule (creates capsule.toml)
    Init {
        /// Target directory (default: current directory)
        #[arg(long)]
        path: Option<PathBuf>,

        /// Skip prompts and use detected defaults
        #[arg(long)]
        yes: bool,
    },

    /// Generate a new Ed25519 signing keypair
    Keygen {
        /// Name for the key (default: timestamp-based)
        #[arg(long)]
        name: Option<String>,
    },

    /// Build artifacts (dispatches to engine)
    Pack {
        /// Path to capsule.toml
        #[arg(long, default_value = "capsule.toml")]
        manifest: PathBuf,

        /// Create a self-extracting bundle (default)
        #[arg(long, default_value_t = true)]
        bundle: bool,

        /// Output path (bundle)
        #[arg(long)]
        output: Option<PathBuf>,

        /// Use a specific runtime directory (optional)
        #[arg(long)]
        runtime: Option<PathBuf>,
    },

    /// Run a built self-extracting bundle
    Open {
        /// Path to bundle executable
        path: PathBuf,
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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Engine {
            command: EngineCommands::Features,
        } => {
            let nacelle = engine::discover_nacelle(engine::EngineRequest {
                explicit_path: cli.nacelle,
                manifest_path: None,
            })?;
            let payload = json!({ "spec_version": "0.1.0" });
            let resp = engine::run_internal(&nacelle, "features", &payload)?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
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
            let validated = engine::discover_nacelle(engine::EngineRequest {
                explicit_path: Some(resolved_path),
                manifest_path: None,
            })?;

            let mut cfg = config::load_config()?;
            cfg.engines.insert(
                name.clone(),
                config::EngineRegistration {
                    path: validated.display().to_string(),
                },
            );
            if default {
                cfg.default_engine = Some(name.clone());
            }
            config::save_config(&cfg)?;

            println!("✅ Registered engine '{}' -> {}", name, validated.display());
            if default {
                println!("✅ Set as default engine");
            }
            Ok(())
        }

        Commands::Dev { manifest } => {
            let nacelle = engine::discover_nacelle(engine::EngineRequest {
                explicit_path: cli.nacelle,
                manifest_path: Some(manifest.clone()),
            })?;
            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));

            let payload = json!({
                "spec_version": "0.1.0",
                "interactive": true,
                "workload": {
                    "type": "source",
                    "path": manifest_dir,
                    "manifest": manifest
                }
            });

            let exit_code = engine::run_internal_streaming(&nacelle, "exec", &payload)?;
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
            Ok(())
        }

        Commands::New { name, template } => new::execute(new::NewArgs {
            name,
            template: Some(template),
        }),

        Commands::Init { path, yes } => init::execute(init::InitArgs { path, yes }),

        Commands::Keygen { name } => keygen::execute(keygen::KeygenArgs { name }),

        Commands::Pack {
            manifest,
            bundle,
            output,
            runtime,
        } => {
            if !bundle {
                anyhow::bail!("Only bundle output is supported (use --bundle)");
            }

            let nacelle = engine::discover_nacelle(engine::EngineRequest {
                explicit_path: cli.nacelle,
                manifest_path: Some(manifest.clone()),
            })?;
            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));

            let payload = json!({
                "spec_version": "0.1.0",
                "workload": {
                    "type": "source",
                    "path": manifest_dir,
                    "manifest": manifest
                },
                "output": {
                    "format": "bundle",
                    "path": output
                },
                "runtime_path": runtime,
                "options": {
                    "sign": false
                }
            });

            let resp = engine::run_internal(&nacelle, "pack", &payload)?;

            let artifact_path = resp
                .get("artifact")
                .and_then(|a| a.get("path"))
                .and_then(|p| p.as_str())
                .unwrap_or("<unknown>");

            println!("✅ Bundle created: {}", artifact_path);
            Ok(())
        }

        Commands::Open { path } => {
            let status = Command::new(&path)
                .status()
                .with_context(|| format!("Failed to execute bundle: {}", path.display()))?;

            if !status.success() {
                std::process::exit(status.code().unwrap_or(1));
            }
            Ok(())
        }
    }
}
