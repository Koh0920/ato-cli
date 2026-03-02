use std::fs;
use std::io::{Cursor, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use tar::Builder;
use tracing::debug;
use zstd::stream::encode_all;

use crate::error::{CapsuleError, Result};
use crate::lockfile;
use crate::manifest;
use crate::packers::sbom::{generate_embedded_sbom_async, SBOM_PATH};
use crate::router::ManifestData;

#[derive(Debug, Clone)]
pub struct WebPackOptions {
    pub manifest_path: PathBuf,
    pub manifest_dir: PathBuf,
    pub output: Option<PathBuf>,
}

const ZSTD_COMPRESSION_LEVEL: i32 = 19;

pub fn pack(
    plan: &ManifestData,
    opts: WebPackOptions,
    reporter: Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    let runtime = plan
        .execution_runtime()
        .map(|v| v.to_ascii_lowercase())
        .unwrap_or_default();
    if runtime != "web" {
        return Err(CapsuleError::Pack(
            "web packer requires runtime=web target".to_string(),
        ));
    }

    let driver = plan
        .execution_driver()
        .map(|v| v.trim().to_ascii_lowercase())
        .ok_or_else(|| CapsuleError::Pack("runtime=web target requires driver".to_string()))?;
    if driver != "static" {
        return Err(CapsuleError::Pack(format!(
            "web packer only supports driver=static (got '{}')",
            driver
        )));
    }

    let loaded = manifest::load_manifest(&opts.manifest_path)?;
    let entrypoint = plan
        .execution_entrypoint()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| CapsuleError::Pack("runtime=web target requires entrypoint".to_string()))?;
    let (entrypoint_dir, entrypoint_prefix) =
        resolve_static_entrypoint(&opts.manifest_dir, &entrypoint)?;

    let temp_dir = tempfile::tempdir().map_err(CapsuleError::Io)?;
    let payload_tar_path = temp_dir.path().join("payload.tar");
    let payload_zst_path = temp_dir.path().join("payload.tar.zst");

    debug!("Packing runtime=web static payload");
    let mut payload_file = fs::File::create(&payload_tar_path).map_err(CapsuleError::Io)?;
    let mut payload_builder = Builder::new(&mut payload_file);
    let mut sbom_files = Vec::new();
    append_directory_tree(
        &mut payload_builder,
        &entrypoint_dir,
        &entrypoint_prefix,
        &mut sbom_files,
    )?;
    payload_builder.finish().map_err(CapsuleError::Io)?;
    drop(payload_builder);

    let payload_bytes = fs::read(&payload_tar_path).map_err(CapsuleError::Io)?;
    let mut cursor = Cursor::new(payload_bytes);
    let compressed = encode_all(&mut cursor, ZSTD_COMPRESSION_LEVEL).map_err(CapsuleError::Io)?;
    let mut zst_file = fs::File::create(&payload_zst_path).map_err(CapsuleError::Io)?;
    zst_file.write_all(&compressed).map_err(CapsuleError::Io)?;

    let output_path = opts.output.unwrap_or_else(|| {
        let name = loaded.model.name.replace('\"', "-");
        opts.manifest_dir.join(format!("{}.capsule", name))
    });

    let mut capsule_file = fs::File::create(&output_path).map_err(CapsuleError::Io)?;
    let mut outer = Builder::new(&mut capsule_file);
    let manifest_tmp = temp_dir.path().join("capsule.toml");
    fs::write(&manifest_tmp, &loaded.raw_text).map_err(CapsuleError::Io)?;
    let lockfile_path = ensure_lockfile(
        &opts.manifest_path,
        &opts.manifest_dir,
        &loaded.raw,
        &loaded.raw_text,
        reporter.clone(),
    )?;
    outer
        .append_path_with_name(&manifest_tmp, "capsule.toml")
        .map_err(CapsuleError::Io)?;
    outer
        .append_path_with_name(&lockfile_path, "capsule.lock")
        .map_err(CapsuleError::Io)?;

    let sbom = generate_sbom_for_sync_context(loaded.model.name.clone(), sbom_files)?;
    let sbom_tmp = temp_dir.path().join(SBOM_PATH);
    fs::write(&sbom_tmp, sbom.document).map_err(CapsuleError::Io)?;
    outer
        .append_path_with_name(&sbom_tmp, SBOM_PATH)
        .map_err(CapsuleError::Io)?;

    let signature_tmp = temp_dir.path().join("signature.json");
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
        CapsuleError::Pack(format!("Failed to serialize signature metadata (JCS): {e}"))
    })?;
    fs::write(&signature_tmp, signature_bytes).map_err(CapsuleError::Io)?;
    outer
        .append_path_with_name(&signature_tmp, "signature.json")
        .map_err(CapsuleError::Io)?;
    outer
        .append_path_with_name(&payload_zst_path, "payload.tar.zst")
        .map_err(CapsuleError::Io)?;
    outer.finish().map_err(CapsuleError::Io)?;

    Ok(output_path)
}

fn ensure_lockfile(
    manifest_path: &Path,
    manifest_dir: &Path,
    manifest_raw: &toml::Value,
    manifest_text: &str,
    reporter: Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> Result<PathBuf> {
    let lock_path = manifest_dir.join("capsule.lock");
    if lock_path.exists() {
        if lockfile::verify_lockfile_manifest(manifest_path, &lock_path).is_ok() {
            return Ok(lock_path);
        }
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return tokio::task::block_in_place(|| {
            handle.block_on(lockfile::generate_and_write_lockfile(
                manifest_path,
                manifest_raw,
                manifest_text,
                reporter,
            ))
        });
    }

    let rt = tokio::runtime::Runtime::new().map_err(CapsuleError::Io)?;
    rt.block_on(lockfile::generate_and_write_lockfile(
        manifest_path,
        manifest_raw,
        manifest_text,
        reporter,
    ))
}

fn generate_sbom_for_sync_context(
    capsule_name: String,
    sbom_files: Vec<(String, PathBuf)>,
) -> Result<crate::packers::sbom::EmbeddedSbom> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return tokio::task::block_in_place(|| {
            handle.block_on(generate_embedded_sbom_async(capsule_name, sbom_files))
        });
    }
    let rt = tokio::runtime::Runtime::new().map_err(CapsuleError::Io)?;
    rt.block_on(generate_embedded_sbom_async(capsule_name, sbom_files))
}

fn resolve_static_entrypoint(manifest_dir: &Path, entrypoint: &str) -> Result<(PathBuf, PathBuf)> {
    let trimmed = entrypoint.trim();
    let raw = PathBuf::from(trimmed);
    if raw.is_absolute() {
        return Err(CapsuleError::Pack(format!(
            "runtime=web static entrypoint '{}' must be a relative directory path",
            entrypoint
        )));
    }

    let mut cleaned = PathBuf::new();
    for component in raw.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => cleaned.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CapsuleError::Pack(format!(
                    "runtime=web static entrypoint '{}' is unsafe",
                    entrypoint
                )));
            }
        }
    }

    let entrypoint_dir = manifest_dir.join(&cleaned);
    if !entrypoint_dir.exists() || !entrypoint_dir.is_dir() {
        return Err(CapsuleError::Pack(format!(
            "runtime=web static entrypoint '{}' must be an existing directory",
            entrypoint
        )));
    }

    let root = manifest_dir
        .canonicalize()
        .unwrap_or_else(|_| manifest_dir.to_path_buf());
    let canonical_entrypoint = entrypoint_dir.canonicalize().map_err(CapsuleError::Io)?;
    if !canonical_entrypoint.starts_with(&root) {
        return Err(CapsuleError::Pack(format!(
            "runtime=web static entrypoint '{}' resolves outside manifest directory",
            entrypoint
        )));
    }

    Ok((canonical_entrypoint, cleaned))
}

fn append_directory_tree(
    builder: &mut Builder<&mut fs::File>,
    source_root: &Path,
    tar_prefix: &Path,
    sbom_files: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    if !tar_prefix.as_os_str().is_empty() {
        builder
            .append_dir(tar_prefix, source_root)
            .map_err(CapsuleError::Io)?;
    }
    append_directory_tree_recursive(builder, source_root, source_root, tar_prefix, sbom_files)
}

fn append_directory_tree_recursive(
    builder: &mut Builder<&mut fs::File>,
    source_root: &Path,
    current_dir: &Path,
    tar_prefix: &Path,
    sbom_files: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    for entry in fs::read_dir(current_dir).map_err(CapsuleError::Io)? {
        let entry = entry.map_err(CapsuleError::Io)?;
        let path = entry.path();
        let rel = path
            .strip_prefix(source_root)
            .map_err(|e| CapsuleError::Pack(format!("Failed to compute archive path: {}", e)))?;
        if should_skip_entry(rel, entry.file_type().map_err(CapsuleError::Io)?.is_dir()) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path).map_err(CapsuleError::Io)?;
        if metadata.file_type().is_symlink() {
            return Err(CapsuleError::Pack(format!(
                "symlink is not allowed in static web payload: {}",
                path.display()
            )));
        }

        let archive_path = if tar_prefix.as_os_str().is_empty() {
            rel.to_path_buf()
        } else {
            tar_prefix.join(rel)
        };

        if metadata.is_dir() {
            builder
                .append_dir(&archive_path, &path)
                .map_err(CapsuleError::Io)?;
            append_directory_tree_recursive(builder, source_root, &path, tar_prefix, sbom_files)?;
            continue;
        }

        if metadata.is_file() {
            builder
                .append_path_with_name(&path, &archive_path)
                .map_err(CapsuleError::Io)?;
            // SPDX identifiers/filenames are string based; we intentionally normalize
            // path bytes lossily for non-UTF8 files for compatibility.
            sbom_files.push((archive_path.to_string_lossy().to_string(), path.clone()));
        }
    }

    Ok(())
}

fn should_skip_entry(rel: &Path, is_dir: bool) -> bool {
    for component in rel.components() {
        let part = component.as_os_str().to_string_lossy();
        if matches!(
            part.as_ref(),
            ".git" | ".capsule" | "target" | "node_modules" | ".venv" | "__pycache__"
        ) {
            return true;
        }
    }

    if is_dir {
        return false;
    }

    let file_name = rel
        .file_name()
        .map(|v| v.to_string_lossy().to_string())
        .unwrap_or_default();
    if matches!(
        file_name.as_str(),
        "capsule.toml"
            | "capsule.lock"
            | "config.json"
            | "signature.json"
            | "sbom.spdx.json"
            | "payload.tar"
            | "payload.tar.zst"
    ) {
        return true;
    }
    file_name.ends_with(".capsule") || file_name.ends_with(".sig")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::NoOpReporter;
    use crate::router::ExecutionProfile;
    use std::io::Read;

    #[test]
    fn pack_static_emits_capsule_with_lock_and_without_config() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let manifest_path = tmp.path().join("capsule.toml");
        std::fs::create_dir_all(tmp.path().join("dist/assets")).expect("mkdir");
        std::fs::write(tmp.path().join("dist/index.html"), "<h1>hello</h1>").expect("write html");
        std::fs::write(tmp.path().join("dist/assets/app.js"), "console.log('ok')")
            .expect("write js");
        std::fs::write(
            tmp.path().join("capsule.lock"),
            r#"version = "1"

[meta]
created_at = "2026-01-01T00:00:00Z"
manifest_hash = "sha256:dummy"
"#,
        )
        .expect("write lock");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "web-static-pack"
version = "0.1.0"
type = "app"
default_target = "static"

[targets.static]
runtime = "web"
driver = "static"
entrypoint = "dist"
port = 8080
"#,
        )
        .expect("write manifest");

        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let output_path = tmp.path().join("web-static-pack.capsule");
        let out = pack(
            &decision.plan,
            WebPackOptions {
                manifest_path: manifest_path.clone(),
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(output_path.clone()),
            },
            Arc::new(NoOpReporter),
        )
        .expect("pack");
        assert_eq!(out, output_path);

        let mut outer = tar::Archive::new(fs::File::open(&out).expect("open capsule"));
        let mut has_capsule_toml = false;
        let mut has_lock = false;
        let mut has_payload = false;
        let mut has_signature = false;
        let mut has_sbom = false;
        let mut payload_bytes = Vec::new();
        for entry in outer.entries().expect("entries") {
            let mut entry = entry.expect("entry");
            let path = entry.path().expect("path").to_string_lossy().to_string();
            if path == "capsule.toml" {
                has_capsule_toml = true;
            } else if path == "capsule.lock" {
                has_lock = true;
            } else if path == "signature.json" {
                has_signature = true;
            } else if path == SBOM_PATH {
                has_sbom = true;
            } else if path == "payload.tar.zst" {
                has_payload = true;
                entry.read_to_end(&mut payload_bytes).expect("read payload");
            }
        }

        assert!(has_capsule_toml);
        assert!(has_lock);
        assert!(has_signature);
        assert!(has_sbom);
        assert!(has_payload);

        let embedded = crate::packers::sbom::extract_and_verify_embedded_sbom(&out)
            .expect("verify embedded sbom");
        assert!(embedded.contains("\"fileName\": \"dist/index.html\""));

        let decoder =
            zstd::stream::Decoder::new(std::io::Cursor::new(payload_bytes)).expect("decoder");
        let mut payload = tar::Archive::new(decoder);
        let mut files = Vec::new();
        for entry in payload.entries().expect("payload entries") {
            let entry = entry.expect("payload entry");
            files.push(
                entry
                    .path()
                    .expect("payload path")
                    .to_string_lossy()
                    .to_string(),
            );
        }
        files.sort();

        assert!(files.iter().any(|p| p == "dist/index.html"));
        assert!(files.iter().any(|p| p == "dist/assets/app.js"));
        assert!(!files.iter().any(|p| p == "capsule.lock"));
        assert!(!files.iter().any(|p| p == "config.json"));
    }

    #[cfg(unix)]
    #[test]
    fn pack_static_rejects_symlink_escape() {
        use std::os::unix::fs as unix_fs;

        let tmp = tempfile::tempdir().expect("tempdir");
        let manifest_path = tmp.path().join("capsule.toml");
        std::fs::create_dir_all(tmp.path().join("dist")).expect("mkdir");
        std::fs::write(tmp.path().join("outside.txt"), "secret").expect("write outside");
        unix_fs::symlink("../outside.txt", tmp.path().join("dist/link.txt")).expect("symlink");

        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "web-static-pack"
version = "0.1.0"
type = "app"
default_target = "static"

[targets.static]
runtime = "web"
driver = "static"
entrypoint = "dist"
port = 8080
"#,
        )
        .expect("write manifest");

        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let err = pack(
            &decision.plan,
            WebPackOptions {
                manifest_path,
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(tmp.path().join("web-static-pack.capsule")),
            },
            Arc::new(NoOpReporter),
        )
        .expect_err("must fail");

        assert!(err.to_string().contains("symlink is not allowed"));
    }
}
