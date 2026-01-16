use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;

#[cfg(feature = "manifest-signing")]
mod capsule_capnp;
mod capsule_types; // UARC manifest types
mod common;
mod config;
mod executors;
mod engine;
mod hardware;
mod init;
mod keygen;
mod manifest;
mod new;
mod observability;
mod packers;
mod policy; // v3.0: L4 Egress Policy Resolution
mod r3_config;
mod resource; // v3.0: provisioning/CAS integration
mod runtime_router;
mod scaffold;
#[cfg(feature = "manifest-signing")]
mod schema; // canonical manifest encoding
mod security;
mod signing; // v3.0: L2 Signature Creation/Verification
mod validation; // v3.0: L1 Source Policy Scanning // v3.0: R3 Configuration Generator

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

        /// Template type: python, node, hono, rust, go, shell
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

        /// Path to signing key for L2 verification (optional)
        #[arg(long)]
        key: Option<PathBuf>,

        /// Skip L1 source policy scan (dangerous!)
        #[arg(long)]
        skip_l1: bool,

        /// Skip all validations (use only for testing)
        #[arg(long)]
        skip_validation: bool,

        /// Network enforcement mode for R3 config.json (best_effort or strict)
        #[arg(long, default_value = "best_effort", value_parser = ["best_effort", "strict"])]
        enforcement: String,
    },

    /// Run a built self-extracting bundle
    Open {
        /// Path to bundle executable
        path: PathBuf,
    },

    /// Scaffold supporting files (e.g. Dockerfile)
    Scaffold {
        #[command(subcommand)]
        command: ScaffoldCommands,
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
            let decision = runtime_router::route_manifest(
                &manifest,
                runtime_router::ExecutionProfile::Dev,
            )?;

            println!(
                "🧭 RuntimeRouter: {:?} ({})",
                decision.kind, decision.reason
            );

            let metrics = observability::RunMetrics::start(decision.kind);

            let exit_code = match decision.kind {
                runtime_router::RuntimeKind::Source => {
                    executors::source::execute(&decision.plan, cli.nacelle)?
                }
                runtime_router::RuntimeKind::Oci => executors::oci::execute(&decision.plan)?,
                runtime_router::RuntimeKind::Wasm => executors::wasm::execute(&decision.plan)?,
            };

            metrics.finish(exit_code).print();

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
            key,
            skip_l1,
            skip_validation,
            enforcement,
        } => {
            if !bundle {
                anyhow::bail!("Only bundle output is supported (use --bundle)");
            }

            println!("📦 Capsule Pack - Pure Runtime Architecture v3.0");
            println!("   Performing build-time validations...\n");

            let decision = runtime_router::route_manifest(
                &manifest,
                runtime_router::ExecutionProfile::Release,
            )?;

            println!(
                "🧭 RuntimeRouter: {:?} ({})",
                decision.kind, decision.reason
            );

            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));

            match decision.kind {
                runtime_router::RuntimeKind::Source => {
                    let artifact_path = packers::source::pack(
                        &decision.plan,
                        packers::source::SourcePackOptions {
                            manifest_path: manifest.clone(),
                            manifest_dir: manifest_dir.clone(),
                            output,
                            runtime,
                            skip_l1,
                            skip_validation,
                            enforcement,
                            nacelle_override: cli.nacelle,
                        },
                    )?;

                    // Phase 4: L2 Sign bundle (if key provided)
                    if let Some(key_path) = key {
                        if !skip_validation {
                            println!("🔐 Phase 4: L2 Signature Generation");
                            match signing::sign_bundle(&artifact_path, &key_path, "capsule-cli") {
                                Ok(_) => println!("   ✅ Bundle signed successfully\n"),
                                Err(e) => {
                                    eprintln!("   ❌ Signing failed: {}", e);
                                    anyhow::bail!("L2 Signature generation failed");
                                }
                            }
                        }
                    } else {
                        println!("ℹ️  Phase 4: L2 Signature skipped (no --key provided)\n");
                    }

                    println!("✅ Pack complete: {}", artifact_path.display());
                }
                runtime_router::RuntimeKind::Oci => {
                    if key.is_some() {
                        println!("ℹ️  L2 Signature skipped (OCI pack does not sign bundles)");
                    }
                    let result = packers::oci::pack(&decision.plan, output)?;
                    if let Some(path) = result.archive {
                        println!("✅ Pack complete: {}", path.display());
                    } else {
                        println!("✅ Pack complete: {}", result.image);
                    }
                }
                runtime_router::RuntimeKind::Wasm => {
                    let result = packers::wasm::pack(&decision.plan, output, key)?;
                    println!("✅ Pack complete: {}", result.artifact.display());
                    if let Some(sig) = result.signature {
                        println!("✅ Signature: {}", sig.display());
                    }
                }
            }

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

        Commands::Scaffold {
            command:
                ScaffoldCommands::Docker {
                    manifest,
                    output,
                    output_dir,
                    force,
                },
        } => scaffold::execute_docker(scaffold::ScaffoldDockerArgs {
            manifest_path: manifest,
            output_dir,
            output,
            force,
        }),
    }
}
