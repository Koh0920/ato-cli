use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    let ui_dir = Path::new("apps/ato-store-local");
    let ui_src = ui_dir.join("src");
    let ui_public = ui_dir.join("public");
    let ui_package = ui_dir.join("package.json");

    println!("cargo:rerun-if-env-changed=ATO_SKIP_UI_BUILD");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", ui_package.display());
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

    let status = Command::new("npm")
        .args(["run", "build", "--prefix"])
        .arg(ui_dir)
        .status();

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
