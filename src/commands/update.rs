use anyhow::{Context, Result};
use axoupdater::AxoUpdater;

/// Update the ato CLI to the latest version
pub fn update() -> Result<()> {
    println!("🔍 更新を確認中...");

    let mut updater = AxoUpdater::new_for("ato");
    updater
        .load_receipt()
        .context("ato のインストール情報を読み込めませんでした")?;
    updater.disable_installer_output();

    let update_result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("更新確認用のランタイムを初期化できませんでした")?
        .block_on(updater.run())
        .context("ato CLI の更新に失敗しました")?;

    match update_result {
        Some(result) => println!("✅ 最新版 (v{}) に更新しました", result.new_version),
        None => println!("✨ すでに最新版です"),
    }

    Ok(())
}
