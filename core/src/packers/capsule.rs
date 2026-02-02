use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::executor;
use tar::Builder;
use zstd::stream::encode_all;

use crate::error::{CapsuleError, Result as CapsuleResult};
use crate::r3_config;

/// Capsule Format v2 PAX TAR Archive Structure:
/// ```text
/// my-app.capsule (PAX TAR)
/// ├── capsule.toml
/// ├── signature.json
/// └── payload.tar.zst
///     ├── source/ (code)
///     ├── config.json (generated)
///     └── capsule.lock (generated)
/// ```

#[derive(Debug, Clone)]
pub struct CapsulePackOptions {
    pub manifest_path: PathBuf,
    pub manifest_dir: PathBuf,
    pub output: Option<PathBuf>,
    pub enforcement: String,
    pub standalone: bool,
}

const ZSTD_COMPRESSION_LEVEL: i32 = 19;

pub async fn pack(
    _plan: &crate::router::ManifestData,
    opts: CapsulePackOptions,
    reporter: Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> CapsuleResult<PathBuf> {
    futures::executor::block_on(reporter.notify(
        "📦 Creating Capsule Archive (.capsule format)".to_string(),
    ))?;

    let loaded = crate::manifest::load_manifest(&opts.manifest_path)?;
    let manifest_content = loaded.raw_text;

    // Step 1: Generate config.json
    futures::executor::block_on(
        reporter.notify("🧭 Phase 1: Generating config.json".to_string()),
    )?;

    let config_path = r3_config::generate_and_write_config(
        &opts.manifest_path,
        Some(opts.enforcement.clone()),
        false,
    )?;

    // Step 2: Generate capsule.lock (skip for now, placeholder)
    futures::executor::block_on(
        reporter.notify("🧾 Phase 2: Generating capsule.lock (placeholder)".to_string()),
    )?;

    let lockfile_path = opts.manifest_dir.join("capsule.lock");
    if !lockfile_path.exists() {
        fs::write(&lockfile_path, "# Placeholder - generated during pack")?;
    }

    // Step 3: Prepare source/ directory
    let source_dir = opts.manifest_dir.join("source");
    if !source_dir.exists() {
        futures::executor::block_on(
            reporter.warn(
                "⚠️  No source/ directory found. Creating empty source/ directory.".to_string(),
            ),
        )?;
        fs::create_dir_all(&source_dir).with_context(|| {
            format!(
                "Failed to create source/ directory: {}",
                source_dir.display()
            )
        })?;
    }

    // Step 4: Create payload.tar.zst
    futures::executor::block_on(
        reporter.notify("📦 Phase 3: Creating payload.tar.zst".to_string()),
    )?;

    let temp_dir = std::env::temp_dir().join(format!("capsule-payload-{}", std::process::id()));
    fs::create_dir_all(&temp_dir)?;

    let payload_tar_path = temp_dir.join("payload.tar");
    let payload_zst_path = temp_dir.join("payload.tar.zst");

    // Create payload.tar
    let mut payload_file = fs::File::create(&payload_tar_path)?;
    let mut ar = Builder::new(&mut payload_file);

    // Add source/ directory contents
    copy_dir_to_tar(&mut ar, &source_dir, "source")?;

    // Add config.json using append_path_with_name
    ar.append_path_with_name(&config_path, "config.json")?;

    // Add capsule.lock using append_path_with_name
    ar.append_path_with_name(&lockfile_path, "capsule.lock")?;

    ar.finish()?;
    drop(ar);

    // Compress with zstd
    futures::executor::block_on(reporter.notify(format!(
        "✓ Compressing with Zstd Level {}...",
        ZSTD_COMPRESSION_LEVEL
    )))?;

    let payload_tar_size = fs::metadata(&payload_tar_path)?.len();
    let payload_tar_content = fs::read(&payload_tar_path)?;
    let mut cursor = Cursor::new(payload_tar_content);
    let compressed = encode_all(&mut cursor, ZSTD_COMPRESSION_LEVEL)?;

    let mut zst_file = fs::File::create(&payload_zst_path)?;
    zst_file.write_all(&compressed)?;

    let compression_ratio = (compressed.len() as f64 / payload_tar_size as f64) * 100.0;
    futures::executor::block_on(reporter.notify(format!(
        "✓ Compressed size: {} (ratio: {:.1}%)",
        format_bytes(compressed.len()),
        compression_ratio
    )))?;

    // Step 5: Create final .capsule archive
    futures::executor::block_on(
        reporter.notify("📦 Phase 4: Creating .capsule archive".to_string()),
    )?;

    let output_path = opts.output.clone().unwrap_or_else(|| {
        let name_str = loaded.model.name.replace('\"', "-");
        opts.manifest_dir.join(format!("{}.capsule", name_str))
    });

    let mut capsule_file = fs::File::create(&output_path)?;
    let mut outer_ar = Builder::new(&mut capsule_file);

    // Write actual manifest content
    let manifest_temp_path = temp_dir.join("capsule.toml");
    fs::write(&manifest_temp_path, &manifest_content)?;
    outer_ar.append_path_with_name(&manifest_temp_path, "capsule.toml")?;

    // Add signature.json (placeholder for now)
    let sig_temp_path = temp_dir.join("signature.json");
    fs::write(
        &sig_temp_path,
        r#"{"signed": false, "note": "To be signed"}"#,
    )?;
    outer_ar.append_path_with_name(&sig_temp_path, "signature.json")?;

    // Add payload.tar.zst
    outer_ar.append_path_with_name(&payload_zst_path, "payload.tar.zst")?;

    outer_ar.finish()?;
    drop(outer_ar);

    // Clean up temp directory
    fs::remove_dir_all(&temp_dir)?;

    let final_size = fs::metadata(&output_path)?.len();
    futures::executor::block_on(reporter.notify(format!(
        "✅ Capsule created: {} ({})",
        output_path.display(),
        format_bytes(final_size as usize)
    )))?;

    Ok(output_path)
}

fn copy_dir_to_tar(ar: &mut Builder<&mut fs::File>, src: &Path, prefix: &str) -> Result<()> {
    let target = prefix.to_string();
    if !src.exists() {
        ar.append_dir(&target, &src)?;
        return Ok(());
    }

    let mut entries = fs::read_dir(src)?;
    if entries.next().is_none() {
        ar.append_dir(&target, &src)?;
        return Ok(());
    }

    drop(entries);

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let target = format!("{}/{}", prefix, name.to_string_lossy());

        if entry.file_type()?.is_dir() {
            ar.append_dir(&target, &path)?;
        } else {
            ar.append_path_with_name(&path, &target)?;
        }
    }

    Ok(())
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
