//! `capsule init` - initialize an existing project as a capsule.

use anyhow::{Context, Result};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct InitArgs {
    pub path: Option<PathBuf>,
    pub yes: bool,
}

#[derive(Debug)]
struct ProjectInfo {
    name: String,
    project_type: ProjectType,
    entrypoint: Vec<String>,
    node_dev_entrypoint: Option<Vec<String>>,
    node_release_entrypoint: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
enum ProjectType {
    Python,
    NodeJs,
    Rust,
    Go,
    Ruby,
    Unknown,
}

impl ProjectType {
    fn as_str(&self) -> &'static str {
        match self {
            ProjectType::Python => "Python",
            ProjectType::NodeJs => "Node.js",
            ProjectType::Rust => "Rust",
            ProjectType::Go => "Go",
            ProjectType::Ruby => "Ruby",
            ProjectType::Unknown => "Unknown",
        }
    }
}

pub fn execute(args: InitArgs) -> Result<()> {
    let project_dir = args
        .path
        .unwrap_or_else(|| PathBuf::from("."))
        .canonicalize()
        .context("Failed to resolve project directory")?;

    println!("🔍 Initializing capsule in: {}\n", project_dir.display());

    let manifest_path = project_dir.join("capsule.toml");
    if manifest_path.exists() {
        anyhow::bail!(
            "capsule.toml already exists!\n\
            Use 'capsule dev --manifest capsule.toml' to run, or delete the file to re-initialize."
        );
    }

    let mut info = detect_project(&project_dir)?;
    println!("   Detected: {} project", info.project_type.as_str());
    if !info.entrypoint.is_empty() {
        println!("   Entrypoint: {}", info.entrypoint.join(" "));
    }

    if !args.yes {
        info = prompt_for_details(info)?;
    }

    let manifest_content = generate_manifest(&info);
    fs::write(&manifest_path, &manifest_content).context("Failed to write capsule.toml")?;

    println!("\n✨ Created capsule.toml!");
    println!("\nNext steps:");
    println!("   capsule dev           # Run locally (no bundling)");
    println!("   capsule pack --bundle # Create self-extracting bundle");

    if project_dir.join(".git").exists() {
        add_to_gitignore(&project_dir)?;
    }

    Ok(())
}

fn detect_project(dir: &Path) -> Result<ProjectInfo> {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("my-capsule")
        .to_string();

    if dir.join("requirements.txt").exists()
        || dir.join("pyproject.toml").exists()
        || dir.join("setup.py").exists()
    {
        let entrypoint = detect_python_entrypoint(dir);
        return Ok(ProjectInfo {
            name,
            project_type: ProjectType::Python,
            entrypoint,
            node_dev_entrypoint: None,
            node_release_entrypoint: None,
        });
    }

    if dir.join("package.json").exists() {
        let (release_entrypoint, dev_entrypoint) = detect_nodejs_entrypoints(dir)?;
        return Ok(ProjectInfo {
            name,
            project_type: ProjectType::NodeJs,
            entrypoint: release_entrypoint.clone(),
            node_dev_entrypoint: dev_entrypoint,
            node_release_entrypoint: Some(release_entrypoint),
        });
    }

    if dir.join("Cargo.toml").exists() {
        return Ok(ProjectInfo {
            name,
            project_type: ProjectType::Rust,
            entrypoint: vec!["cargo".to_string(), "run".to_string()],
            node_dev_entrypoint: None,
            node_release_entrypoint: None,
        });
    }

    if dir.join("go.mod").exists() {
        return Ok(ProjectInfo {
            name,
            project_type: ProjectType::Go,
            entrypoint: vec!["go".to_string(), "run".to_string(), ".".to_string()],
            node_dev_entrypoint: None,
            node_release_entrypoint: None,
        });
    }

    if dir.join("Gemfile").exists() {
        let entrypoint = detect_ruby_entrypoint(dir);
        return Ok(ProjectInfo {
            name,
            project_type: ProjectType::Ruby,
            entrypoint,
            node_dev_entrypoint: None,
            node_release_entrypoint: None,
        });
    }

    let entrypoint = detect_generic_entrypoint(dir);
    Ok(ProjectInfo {
        name,
        project_type: ProjectType::Unknown,
        entrypoint,
        node_dev_entrypoint: None,
        node_release_entrypoint: None,
    })
}

fn detect_python_entrypoint(dir: &Path) -> Vec<String> {
    for candidate in ["main.py", "app.py", "run.py", "server.py"] {
        if dir.join(candidate).exists() {
            return vec!["python".to_string(), candidate.to_string()];
        }
    }

    if dir.join("__main__.py").exists() {
        return vec!["python".to_string(), ".".to_string()];
    }

    if dir.join("pyproject.toml").exists() {
        return vec!["python".to_string(), "-m".to_string(), "app".to_string()];
    }

    vec!["python".to_string(), "main.py".to_string()]
}

fn detect_nodejs_entrypoints(dir: &Path) -> Result<(Vec<String>, Option<Vec<String>>)> {
    let package_json_path = dir.join("package.json");
    let content = fs::read_to_string(&package_json_path).context("Failed to read package.json")?;

    let bun_project = dir.join("bun.lockb").exists() || dir.join("bunfig.toml").exists();

    if let Ok(pkg) = serde_json::from_str::<serde_json::Value>(&content) {
        let package_manager = pkg
            .get("packageManager")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_default();

        let is_bun = bun_project || package_manager.starts_with("bun@");

        if let Some(scripts) = pkg.get("scripts") {
            let has_start = scripts.get("start").is_some();
            let has_dev = scripts.get("dev").is_some();

            let dev = if has_dev {
                if is_bun {
                    Some(vec!["bun".to_string(), "run".to_string(), "dev".to_string()])
                } else {
                    Some(vec!["npm".to_string(), "run".to_string(), "dev".to_string()])
                }
            } else {
                None
            };

            if has_start {
                let release = if is_bun {
                    vec!["bun".to_string(), "run".to_string(), "start".to_string()]
                } else {
                    vec!["npm".to_string(), "start".to_string()]
                };
                return Ok((release, dev));
            }
        }

        if let Some(main) = pkg.get("main").and_then(|m| m.as_str()) {
            let release = vec!["node".to_string(), main.to_string()];
            return Ok((release, None));
        }
    }

    for candidate in ["index.js", "main.js", "app.js", "server.js"] {
        if dir.join(candidate).exists() {
            return Ok((vec!["node".to_string(), candidate.to_string()], None));
        }
    }

    Ok((vec!["npm".to_string(), "start".to_string()], None))
}

fn detect_ruby_entrypoint(dir: &Path) -> Vec<String> {
    if dir.join("config.ru").exists() {
        return vec!["bundle".to_string(), "exec".to_string(), "rackup".to_string()];
    }

    for candidate in ["app.rb", "main.rb", "server.rb"] {
        if dir.join(candidate).exists() {
            return vec!["ruby".to_string(), candidate.to_string()];
        }
    }

    vec!["ruby".to_string(), "app.rb".to_string()]
}

fn detect_generic_entrypoint(dir: &Path) -> Vec<String> {
    for (file, cmd) in [
        ("main.py", vec!["python", "main.py"]),
        ("index.js", vec!["node", "index.js"]),
        ("main.sh", vec!["bash", "main.sh"]),
        ("run.sh", vec!["bash", "run.sh"]),
    ] {
        if dir.join(file).exists() {
            return cmd.iter().map(|s| s.to_string()).collect();
        }
    }

    if dir.join("Dockerfile").exists() {
        return vec![
            "echo".to_string(),
            "Container project - specify entrypoint".to_string(),
        ];
    }

    vec![]
}

fn prompt_for_details(mut info: ProjectInfo) -> Result<ProjectInfo> {
    print!("\n? Package name: ({}) ", info.name);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if !input.is_empty() {
        info.name = input.to_string();
    }

    if !info.entrypoint.is_empty() {
        let default_cmd = info.entrypoint.join(" ");
        print!("? Entry command: ({}) ", default_cmd);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if !input.is_empty() {
            info.entrypoint = input.split_whitespace().map(|s| s.to_string()).collect();
            if matches!(info.project_type, ProjectType::NodeJs) {
                info.node_dev_entrypoint = Some(info.entrypoint.clone());
                info.node_release_entrypoint = Some(info.entrypoint.clone());
            }
        }
    } else {
        print!("? Entry command: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if !input.is_empty() {
            info.entrypoint = input.split_whitespace().map(|s| s.to_string()).collect();
            if matches!(info.project_type, ProjectType::NodeJs) {
                info.node_dev_entrypoint = Some(info.entrypoint.clone());
                info.node_release_entrypoint = Some(info.entrypoint.clone());
            }
        }
    }

    Ok(info)
}

fn generate_manifest(info: &ProjectInfo) -> String {
    let entry_command = if info.entrypoint.is_empty() {
        "echo 'Hello, capsule!'".to_string()
    } else {
        info.entrypoint.join(" ")
    };

    // Prefer sample-compatible source target shape for Python when we can safely infer it.
    let python_entrypoint_file = if matches!(info.project_type, ProjectType::Python)
        && info.entrypoint.len() >= 2
        && info.entrypoint[0].to_ascii_lowercase().starts_with("python")
    {
        Some(info.entrypoint[1].clone())
    } else {
        None
    };

    let (execution_entrypoint, targets_block) = if let Some(file) = python_entrypoint_file {
        (
            file.clone(),
            format!(
                "\n[targets]\npreference = [\"source\"]\n\n[targets.source]\nlanguage = \"python\"\nversion = \"^3.11\"\nentrypoint = \"{}\"\ndependencies = \"requirements.txt\"\ndev_mode = true\n",
                file
            ),
        )
    } else {
        (entry_command, String::new())
    };

    // For Node.js projects, provide explicit dev/release profiles by default.
    // Use detected scripts when available (e.g. bun/npm), while keeping a single
    // fallback execution.entrypoint for compatibility.
    let node_profiles_block = if matches!(info.project_type, ProjectType::NodeJs) {
        let dev_ep = info
            .node_dev_entrypoint
            .as_ref()
            .map(|v| v.join(" "))
            .unwrap_or_else(|| execution_entrypoint.clone());

        let release_ep = info
            .node_release_entrypoint
            .as_ref()
            .map(|v| v.join(" "))
            .unwrap_or_else(|| execution_entrypoint.clone());

        format!(
            "\n[execution.dev]\nentrypoint = \"{}\"\n\n[execution.release]\nentrypoint = \"{}\"\n",
            dev_ep, release_ep
        )
    } else {
        String::new()
    };

    format!(
        r#"# Capsule Manifest - UARC V1.1.0
# Generated by: capsule init

schema_version = "1.0"
name = "{name}"
version = "0.1.0"
type = "app"

[metadata]
description = "Capsule generated from existing {project_type} project"

[requirements]

[execution]
runtime = "source"
entrypoint = "{entrypoint}"
{node_profiles_block}
{targets_block}

[storage]

[routing]
"#,
        name = info.name,
        project_type = info.project_type.as_str(),
    entrypoint = execution_entrypoint,
    node_profiles_block = node_profiles_block,
    targets_block = targets_block
    )
}

fn add_to_gitignore(dir: &Path) -> Result<()> {
    let gitignore_path = dir.join(".gitignore");

    let existing = if gitignore_path.exists() {
        fs::read_to_string(&gitignore_path).unwrap_or_default()
    } else {
        String::new()
    };

    if existing.contains(".capsule/") || existing.contains("*.capsule") {
        return Ok(());
    }

    let addition = "\n# Capsule\n.capsule/\n*.capsule\n*.sig\n";
    let new_content = format!("{}{}", existing.trim_end(), addition);

    fs::write(&gitignore_path, new_content).context("Failed to update .gitignore")?;
    println!("   ✓ Updated .gitignore");
    Ok(())
}
