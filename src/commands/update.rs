use anyhow::{Context, Result};
use axoupdater::AxoUpdater;

/// Update the ato CLI to the latest version
pub fn update() -> Result<()> {
    println!("🔍 更新を確認中...");

    let updater = AxoUpdater::new_for("Koh0920/ato-cli")
        .context("Failed to create updater")?;

    match updater.update() {
        Ok(Some(version)) => {
            println!("✅ 最新版 (v{}) に更新しました", version);
        }
        Ok(None) => {
            println!("✨ すでに最新版です");
        }
        Err(e) => {
            return Err(e).context("Failed to update ato CLI");
        }
    }

    Ok(())
}