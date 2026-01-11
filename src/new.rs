//! `capsule new` - create a new capsule project from scratch.

use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct NewArgs {
    pub name: String,
    pub template: Option<String>,
}

pub fn execute(args: NewArgs) -> Result<()> {
    let project_dir = PathBuf::from(&args.name);

    if project_dir.exists() {
        anyhow::bail!("Directory '{}' already exists!", args.name);
    }

    let template = args.template.as_deref().unwrap_or("python");

    println!("🎉 Creating new capsule project: {}", args.name);
    println!("   Template: {}\n", template);

    fs::create_dir_all(&project_dir)
        .with_context(|| format!("Failed to create directory: {}", project_dir.display()))?;

    match template {
        "python" | "py" => create_python_project(&project_dir, &args.name)?,
        "node" | "nodejs" | "js" => create_nodejs_project(&project_dir, &args.name)?,
        "rust" | "rs" => create_rust_project(&project_dir, &args.name)?,
        "shell" | "sh" | "bash" => create_shell_project(&project_dir, &args.name)?,
        _ => {
            anyhow::bail!(
                "Unknown template: '{}'\n\
                Available templates: python, node, rust, shell",
                template
            );
        }
    }

    create_gitignore(&project_dir)?;
    create_readme(&project_dir, &args.name)?;

    println!("\n✨ Project created successfully!");
    println!("\nNext steps:");
    println!("   cd {}", args.name);
    println!("   capsule dev");

    Ok(())
}

fn create_python_project(dir: &Path, name: &str) -> Result<()> {
    let manifest = format!(
        r#"# Capsule Manifest - UARC V1.1.0
schema_version = "1.0"
name = "{name}"
version = "0.1.0"
type = "app"

[metadata]
description = "A new capsule application"

[requirements]

[execution]
runtime = "source"
entrypoint = "main.py"

[targets]
preference = ["source"]

[targets.source]
language = "python"
version = "^3.11"
entrypoint = "main.py"
dependencies = "requirements.txt"
dev_mode = true

[storage]

[routing]
"#
    );
    fs::write(dir.join("capsule.toml"), manifest)?;

    let main_py = r#"#!/usr/bin/env python3
"""
Main entry point for the capsule application.
"""

def main():
    print("Hello from capsule! 🎉")
    print("Edit main.py to get started.")

if __name__ == "__main__":
    main()
"#;
    fs::write(dir.join("main.py"), main_py)?;

    fs::write(dir.join("requirements.txt"), "# Add your dependencies here\n")?;

    println!("   ✓ Created capsule.toml");
    println!("   ✓ Created main.py");
    println!("   ✓ Created requirements.txt");
    Ok(())
}

fn create_nodejs_project(dir: &Path, name: &str) -> Result<()> {
    let manifest = format!(
        r#"# Capsule Manifest - UARC V1.1.0
schema_version = "1.0"
name = "{name}"
version = "0.1.0"
type = "app"

[metadata]
description = "A new capsule application"

[requirements]

[execution]
runtime = "source"
entrypoint = "node index.js"

[storage]

[routing]
"#
    );
    fs::write(dir.join("capsule.toml"), manifest)?;

    let package_json = format!(
        r#"{{
  "name": "{name}",
  "version": "0.1.0",
  "main": "index.js",
  "scripts": {{
    "start": "node index.js"
  }}
}}
"#
    );
    fs::write(dir.join("package.json"), package_json)?;

    let index_js = r#"/**
 * Main entry point for the capsule application.
 */

console.log("Hello from capsule! 🎉");
console.log("Edit index.js to get started.");
"#;
    fs::write(dir.join("index.js"), index_js)?;

    println!("   ✓ Created capsule.toml");
    println!("   ✓ Created package.json");
    println!("   ✓ Created index.js");
    Ok(())
}

fn create_rust_project(dir: &Path, name: &str) -> Result<()> {
    let manifest = format!(
        r#"# Capsule Manifest - UARC V1.1.0
schema_version = "1.0"
name = "{name}"
version = "0.1.0"
type = "app"

[metadata]
description = "A new capsule application"

[requirements]

[execution]
runtime = "source"
entrypoint = "cargo run --release"

[storage]

[routing]

# Alternative: Build to Wasm for sandboxed execution
# [targets.wasm]
# digest = "sha256:..."
# world = "wasi:cli/command"
"#
    );
    fs::write(dir.join("capsule.toml"), manifest)?;

    let cargo_toml = format!(
        r#"[package]
name = "{}"
version = "0.1.0"
edition = "2021"

[dependencies]
"#,
        name.replace('-', "_")
    );
    fs::write(dir.join("Cargo.toml"), cargo_toml)?;

    fs::create_dir_all(dir.join("src"))?;
    let main_rs = r#"fn main() {
    println!("Hello from capsule! 🎉");
    println!("Edit src/main.rs to get started.");
}
"#;
    fs::write(dir.join("src/main.rs"), main_rs)?;

    println!("   ✓ Created capsule.toml");
    println!("   ✓ Created Cargo.toml");
    println!("   ✓ Created src/main.rs");
    Ok(())
}

fn create_shell_project(dir: &Path, name: &str) -> Result<()> {
    let manifest = format!(
        r#"# Capsule Manifest - UARC V1.1.0
schema_version = "1.0"
name = "{name}"
version = "0.1.0"
type = "app"

[metadata]
description = "A new capsule application"

[requirements]

[execution]
runtime = "source"
entrypoint = "bash main.sh"

[storage]

[routing]
"#
    );
    fs::write(dir.join("capsule.toml"), manifest)?;

    let main_sh = r#"#!/bin/bash
#
# Main entry point for the capsule application.
#

echo "Hello from capsule! 🎉"
echo "Edit main.sh to get started."
"#;
    fs::write(dir.join("main.sh"), main_sh)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(dir.join("main.sh"))?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(dir.join("main.sh"), perms)?;
    }

    println!("   ✓ Created capsule.toml");
    println!("   ✓ Created main.sh");
    Ok(())
}

fn create_gitignore(dir: &Path) -> Result<()> {
    let content = r#"# Capsule
.capsule/
*.capsule
*.sig

# Common
.DS_Store
*.log

# Python
__pycache__/
*.py[cod]
.venv/
venv/

# Node
node_modules/

# Rust
target/
"#;
    fs::write(dir.join(".gitignore"), content)?;
    println!("   ✓ Created .gitignore");
    Ok(())
}

fn create_readme(dir: &Path, name: &str) -> Result<()> {
    let content = format!(
        r#"# {name}

A capsule application built with UARC V1.1.0.

## Quick Start

```bash
# Run locally (no bundling)
capsule dev

# Create a self-extracting bundle
capsule pack --bundle

# Run bundle
./nacelle-bundle
```

## Project Structure

- `capsule.toml` - Capsule manifest (package config, permissions, runtime)
- Entry file depends on template (main.py, index.js, etc.)

## Learn More

- UARC Specification: https://uarc.dev
"#
    );
    fs::write(dir.join("README.md"), content)?;
    println!("   ✓ Created README.md");
    Ok(())
}
