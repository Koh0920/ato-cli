use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tar::{Builder, EntryType, Header};
use tracing::debug;
use walkdir::WalkDir;
use zstd::stream::encode_all;

use crate::error::Result as CapsuleResult;
use crate::packers::pack_filter::PackFilter;
use crate::packers::sbom::{generate_embedded_sbom_async, SBOM_PATH};
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
    _reporter: Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> CapsuleResult<PathBuf> {
    debug!("Creating capsule archive (.capsule format)");

    let loaded = crate::manifest::load_manifest(&opts.manifest_path)?;
    let manifest_content = loaded.raw_text;
    let pack_filter = PackFilter::from_manifest(&loaded.model)?;

    // Step 1: Generate config.json
    debug!("Phase 1: generating config.json");

    let config_path = r3_config::generate_and_write_config(
        &opts.manifest_path,
        Some(opts.enforcement.clone()),
        false,
    )?;

    // Step 2: Generate capsule.lock (skip for now, placeholder)
    debug!("Phase 2: generating capsule.lock (placeholder)");

    let lockfile_path = opts.manifest_dir.join("capsule.lock");
    if !lockfile_path.exists() {
        fs::write(&lockfile_path, "# Placeholder - generated during pack")?;
    }

    // Step 3: Resolve source payload input
    let source_dir = opts.manifest_dir.join("source");

    // Step 4: Create payload.tar.zst
    debug!("Phase 3: creating payload.tar.zst");

    let temp_dir = std::env::temp_dir().join(format!("capsule-payload-{}", std::process::id()));
    fs::create_dir_all(&temp_dir)?;

    let payload_tar_path = temp_dir.join("payload.tar");
    let payload_zst_path = temp_dir.join("payload.tar.zst");

    // Create payload.tar
    let mut payload_file = fs::File::create(&payload_tar_path)?;
    let mut ar = Builder::new(&mut payload_file);

    // Add source/ directory contents.
    // If source/ does not exist, fallback to project root contents (excluding generated/build files).
    let mut sbom_files = Vec::new();
    if source_dir.exists() {
        copy_dir_to_tar(
            &mut ar,
            &source_dir,
            "source",
            &pack_filter,
            &mut sbom_files,
        )?;
    } else {
        debug!("No source/ directory found; packaging project root as source/");
        copy_dir_to_tar(
            &mut ar,
            &opts.manifest_dir,
            "source",
            &pack_filter,
            &mut sbom_files,
        )?;
    }

    // Add config.json using append_path_with_name
    ar.append_path_with_name(&config_path, "config.json")?;
    sbom_files.push(("config.json".to_string(), config_path.clone()));

    // Add capsule.lock using append_path_with_name
    ar.append_path_with_name(&lockfile_path, "capsule.lock")?;
    sbom_files.push(("capsule.lock".to_string(), lockfile_path.clone()));

    ar.finish()?;
    drop(ar);

    // Compress with zstd
    debug!(
        "Compressing payload with zstd level {}",
        ZSTD_COMPRESSION_LEVEL
    );

    let payload_tar_size = fs::metadata(&payload_tar_path)?.len();
    let payload_tar_content = fs::read(&payload_tar_path)?;
    let mut cursor = Cursor::new(payload_tar_content);
    let compressed = encode_all(&mut cursor, ZSTD_COMPRESSION_LEVEL)?;

    let mut zst_file = fs::File::create(&payload_zst_path)?;
    zst_file.write_all(&compressed)?;

    let compression_ratio = (compressed.len() as f64 / payload_tar_size as f64) * 100.0;
    debug!(
        "Compressed payload size: {} (ratio: {:.1}%)",
        format_bytes(compressed.len()),
        compression_ratio
    );

    // Step 5: Create final .capsule archive
    debug!("Phase 4: creating final .capsule archive");

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

    let sbom = generate_embedded_sbom_async(loaded.model.name.clone(), sbom_files).await?;
    let sbom_temp_path = temp_dir.join(SBOM_PATH);
    fs::write(&sbom_temp_path, &sbom.document)?;
    outer_ar.append_path_with_name(&sbom_temp_path, SBOM_PATH)?;

    // Add signature.json metadata
    let sig_temp_path = temp_dir.join("signature.json");
    let signature = serde_json::json!({
        "signed": false,
        "note": "To be signed",
        "sbom": {
            "path": SBOM_PATH,
            "sha256": sbom.sha256,
            "format": "spdx-json",
        }
    });
    let signature_bytes = serde_jcs::to_vec(&signature).map_err(|e| {
        crate::error::CapsuleError::Pack(format!(
            "Failed to serialize signature metadata (JCS): {e}"
        ))
    })?;
    fs::write(&sig_temp_path, signature_bytes)?;
    outer_ar.append_path_with_name(&sig_temp_path, "signature.json")?;

    // Add payload.tar.zst
    outer_ar.append_path_with_name(&payload_zst_path, "payload.tar.zst")?;

    outer_ar.finish()?;
    drop(outer_ar);

    // Clean up temp directory
    fs::remove_dir_all(&temp_dir)?;

    let final_size = fs::metadata(&output_path)?.len();
    debug!(
        "Capsule created: {} ({})",
        output_path.display(),
        format_bytes(final_size as usize)
    );

    Ok(output_path)
}

fn copy_dir_to_tar(
    ar: &mut Builder<&mut fs::File>,
    src_root: &Path,
    prefix: &str,
    filter: &PackFilter,
    sbom_files: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    for entry in WalkDir::new(src_root)
        .follow_links(false)
        .into_iter()
        .flatten()
    {
        let path = entry.path();
        if entry.file_type().is_dir() {
            continue;
        }
        let rel = match path.strip_prefix(src_root) {
            Ok(rel) => rel,
            Err(_) => continue,
        };
        if rel.as_os_str().is_empty() {
            continue;
        }
        if !filter.should_include_file(rel) || should_skip_reserved_file(rel) {
            continue;
        }
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        let target = format!("{}/{}", prefix, rel_str);
        // Preserve symlink entries instead of dereferencing them.
        // Next.js standalone output (and Deno/npm node_modules layouts) rely on symlink
        // topology for correct Node.js module resolution.
        if entry.file_type().is_symlink() {
            let link_target = std::fs::read_link(path)?;
            let mut header = Header::new_gnu();
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            ar.append_link(&mut header, &target, link_target)?;
        } else if entry.file_type().is_file() {
            ar.append_path_with_name(path, &target)?;
            sbom_files.push((target, path.to_path_buf()));
        }
    }
    Ok(())
}

fn should_skip_reserved_file(rel: &Path) -> bool {
    let file_name = rel
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    if matches!(
        file_name.as_str(),
        "capsule.toml"
            | "config.json"
            | "capsule.lock"
            | "signature.json"
            | "sbom.spdx.json"
            | "payload.tar"
            | "payload.tar.zst"
    ) {
        return true;
    }

    file_name.ends_with(".capsule") || file_name.ends_with(".sig")
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
