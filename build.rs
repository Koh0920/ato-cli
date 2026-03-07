use std::env;
use std::path::Path;
use std::process::{Command, ExitStatus};

fn npm_status(ui_dir: &Path, args: &[&str]) -> std::io::Result<ExitStatus> {
    Command::new("npm")
        .arg("--prefix")
        .arg(ui_dir)
        .args(args)
        .status()
}

fn main() {
    let ui_dir = Path::new("apps/ato-store-local");
    let ui_src = ui_dir.join("src");
    let ui_public = ui_dir.join("public");
    let ui_package = ui_dir.join("package.json");
    let ui_lockfile = ui_dir.join("package-lock.json");
    let ui_vite_bin = ui_dir
        .join("node_modules")
        .join(".bin")
        .join(if cfg!(windows) { "vite.cmd" } else { "vite" });

    println!("cargo:rerun-if-env-changed=ATO_SKIP_UI_BUILD");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", ui_package.display());
    if ui_lockfile.exists() {
        println!("cargo:rerun-if-changed={}", ui_lockfile.display());
    }
    if ui_src.exists() {
        println!("cargo:rerun-if-changed={}", ui_src.display());
    }
    if ui_public.exists() {
        println!("cargo:rerun-if-changed={}", ui_public.display());
    }

    if env::var("ATO_SKIP_UI_BUILD")
        .ok()
        .as_deref()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    {
        println!("cargo:warning=Skipping UI build because ATO_SKIP_UI_BUILD is set");
        return;
    }

    if !ui_package.exists() {
        println!(
            "cargo:warning=Skipping UI build because {} was not found",
            ui_package.display()
        );
        return;
    }

    if !ui_vite_bin.exists() {
        let install_args: &[&str] = if ui_lockfile.exists() {
            &["ci", "--include=dev"]
        } else {
            &["install", "--include=dev"]
        };
        println!(
            "cargo:warning=Installing UI dependencies (including Vite) because {} is missing",
            ui_vite_bin.display()
        );
        match npm_status(ui_dir, install_args) {
            Ok(status) if status.success() => {}
            Ok(status) => panic!(
                "UI dependency install failed (status: {}). Run `npm install --prefix apps/ato-store-local` and retry.",
                status
            ),
            Err(err) => panic!(
                "Failed to execute npm for UI dependency install: {}. Install Node.js/npm or set ATO_SKIP_UI_BUILD=1.",
                err
            ),
        }

        if !ui_vite_bin.exists() {
            panic!(
                "UI dependency install completed but {} is still missing. Ensure npm devDependencies are enabled and retry.",
                ui_vite_bin.display()
            );
        }
    }

    let status = npm_status(ui_dir, &["run", "build"]);

    match status {
        Ok(status) if status.success() => {}
        Ok(status) => panic!(
            "UI build failed (status: {}). Run `npm install --prefix apps/ato-store-local` and retry.",
            status
        ),
        Err(err) => panic!(
            "Failed to execute npm for UI build: {}. Install Node.js/npm or set ATO_SKIP_UI_BUILD=1.",
            err
        ),
    }
}
