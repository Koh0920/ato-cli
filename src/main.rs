use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;

use capsule_core::CapsuleReporter;

#[cfg(feature = "manifest-signing")]
mod capsule_capnp;
mod common;
mod executors;
mod init;
mod keygen;
mod new;
mod observability;
mod reporters;
mod scaffold;

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

        Commands::Dev { manifest } => {
            let decision = capsule_core::router::route_manifest(
                &manifest,
                capsule_core::router::ExecutionProfile::Dev,
            )?;

            futures::executor::block_on(reporter.notify(format!(
                "🧭 RuntimeRouter: {:?} ({})",
                decision.kind, decision.reason
            )))?;

            let metrics = if matches!(
                decision.kind,
                capsule_core::router::RuntimeKind::Source | capsule_core::router::RuntimeKind::Oci
            ) {
                None
            } else {
                Some(observability::RunMetrics::start(
                    decision.kind,
                    reporter.clone(),
                ))
            };

            let exit_code = match decision.kind {
                capsule_core::router::RuntimeKind::Source => {
                    executors::source::execute(&decision.plan, cli.nacelle, reporter.clone())?
                }
                capsule_core::router::RuntimeKind::Oci => {
                    executors::oci::execute(&decision.plan, reporter.clone())?
                }
                capsule_core::router::RuntimeKind::Wasm => {
                    executors::wasm::execute(&decision.plan, reporter.clone())?
                }
            };

            if let Some(metrics) = metrics {
                metrics.finish(exit_code).print()?;
            }

            if exit_code != 0 {
                std::process::exit(exit_code);
            }
            Ok(())
        }

        Commands::New { name, template } => new::execute(
            new::NewArgs {
                name,
                template: Some(template),
            },
            reporter.clone(),
        ),

        Commands::Init { path, yes } => {
            init::execute(init::InitArgs { path, yes }, reporter.clone())
        }

        Commands::Keygen { name } => keygen::execute(keygen::KeygenArgs { name }, reporter.clone()),

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
                            output,
                            runtime,
                            skip_l1,
                            skip_validation,
                            enforcement,
                            nacelle_override: cli.nacelle,
                        },
                        reporter.clone(),
                    )?;

                    if let Some(key_path) = key {
                        if !skip_validation {
                            futures::executor::block_on(
                                reporter.notify("🔐 Phase 4: L2 Signature Generation".to_string()),
                            )?;
                            match capsule_core::signing::sign::sign_bundle(
                                &artifact_path,
                                &key_path,
                                "capsule-cli",
                            ) {
                                Ok(_) => {
                                    futures::executor::block_on(
                                        reporter.notify(
                                            "   ✅ Bundle signed successfully\n".to_string(),
                                        ),
                                    )?;
                                }
                                Err(e) => {
                                    futures::executor::block_on(
                                        reporter.warn(format!("   ❌ Signing failed: {}", e)),
                                    )?;
                                    anyhow::bail!("L2 Signature generation failed");
                                }
                            }
                        }
                    } else {
                        futures::executor::block_on(reporter.notify(
                            "ℹ️  Phase 4: L2 Signature skipped (no --key provided)\n".to_string(),
                        ))?;
                    }

                    futures::executor::block_on(
                        reporter.notify(format!("✅ Pack complete: {}", artifact_path.display())),
                    )?;
                }
                capsule_core::router::RuntimeKind::Oci => {
                    if key.is_some() {
                        futures::executor::block_on(reporter.notify(
                            "ℹ️  L2 Signature skipped (OCI pack does not sign bundles)".to_string(),
                        ))?;
                    }
                    let result = capsule_core::packers::oci::pack(
                        &decision.plan,
                        output,
                        reporter.as_ref(),
                    )?;
                    if let Some(path) = result.archive {
                        futures::executor::block_on(
                            reporter.notify(format!("✅ Pack complete: {}", path.display())),
                        )?;
                    } else {
                        futures::executor::block_on(
                            reporter.notify(format!("✅ Pack complete: {}", result.image)),
                        )?;
                    }
                }
                capsule_core::router::RuntimeKind::Wasm => {
                    let result = capsule_core::packers::wasm::pack(
                        &decision.plan,
                        output,
                        key,
                        reporter.as_ref(),
                    )?;
                    futures::executor::block_on(
                        reporter.notify(format!("✅ Pack complete: {}", result.artifact.display())),
                    )?;
                    if let Some(sig) = result.signature {
                        futures::executor::block_on(
                            reporter.notify(format!("✅ Signature: {}", sig.display())),
                        )?;
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
        } => scaffold::execute_docker(
            scaffold::ScaffoldDockerArgs {
                manifest_path: manifest,
                output_dir,
                output,
                force,
            },
            reporter.clone(),
        ),
    }
}
