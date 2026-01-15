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
mod policy; // v3.0: L4 Egress Policy Resolution
mod r3_config;
mod scaffold;
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

            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));

            // Phase 1: L1 Source Policy Scan
            if !skip_validation && !skip_l1 {
                println!("🔍 Phase 1: L1 Source Policy Scan");
                let source_dir = manifest_dir.join("source");
                if source_dir.exists() {
                    let scan_extensions = &["py", "sh", "js", "ts", "go", "rs"];
                    match validation::source_policy::scan_source_directory(
                        &source_dir,
                        scan_extensions,
                    ) {
                        Ok(()) => {
                            println!("   ✅ No dangerous patterns detected\n");
                        }
                        Err(e) => {
                            eprintln!("   ❌ L1 Policy violation: {}", e);
                            eprintln!("\n💡 Tip: Fix the security issue or use --skip-l1 (not recommended)");
                            anyhow::bail!("L1 Source Policy check failed");
                        }
                    }
                } else {
                    println!("   ⚠️  No source/ directory found, skipping scan\n");
                }
            } else if skip_l1 {
                println!("⚠️  Phase 1: L1 Source Policy Scan SKIPPED (--skip-l1)\n");
            }

            // Phase 2: Generate R3 config.json (services-first)
            println!("🧭 Phase 2: Generating R3 config.json");
            let config_path = r3_config::generate_and_write_config(&manifest, Some(enforcement))?;
            println!("   ✅ config.json generated: {}\n", config_path.display());

            // Phase 3: Call nacelle to create bundle
            println!("📦 Phase 3: Building bundle with nacelle");
            let nacelle = engine::discover_nacelle(engine::EngineRequest {
                explicit_path: cli.nacelle,
                manifest_path: Some(manifest.clone()),
            })?;

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
                .map(PathBuf::from)
                .ok_or_else(|| anyhow::anyhow!("No artifact path in response"))?;

            println!("   ✅ Bundle created: {}\n", artifact_path.display());

            let bundle_source_dir = artifact_path;

            // Phase 4: L2 Sign bundle (if key provided)
            if let Some(key_path) = key {
                if !skip_validation {
                    println!("🔐 Phase 4: L2 Signature Generation");
                    match signing::sign_bundle(&bundle_source_dir, &key_path, "capsule-cli") {
                        Ok(_) => {
                            println!("   ✅ Bundle signed successfully\n");
                        }
                        Err(e) => {
                            eprintln!("   ❌ Signing failed: {}", e);
                            anyhow::bail!("L2 Signature generation failed");
                        }
                    }
                }
            } else {
                println!("ℹ️  Phase 4: L2 Signature skipped (no --key provided)\n");
            }

            println!("✅ Pack complete: {}", bundle_source_dir.display());
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
