use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};

use sha2::{Digest, Sha256};
use tar::{Builder, EntryType, Header};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::debug;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::error::{CapsuleError, Result as CapsuleResult};
use crate::packers::pack_filter::PackFilter;
use crate::packers::sbom::{generate_embedded_sbom_from_inputs_async, SbomFileInput, SBOM_PATH};
use crate::r3_config;

/// Capsule Format v2 PAX TAR Archive Structure:
/// ```text
/// my-app.capsule (PAX TAR)
/// ├── capsule.toml
/// ├── signature.json
/// └── payload.tar.zst
///     ├── source/ (code)
///     ├── config.json (prepared by controller)
///     └── capsule.lock (prepared by controller)
/// ```

#[derive(Debug, Clone)]
pub struct CapsulePackOptions {
    pub manifest_path: PathBuf,
    pub manifest_dir: PathBuf,
    pub output: Option<PathBuf>,
    pub config_json: Arc<r3_config::ConfigJson>,
    pub config_path: PathBuf,
    pub lockfile_path: PathBuf,
}

#[derive(Debug, Clone)]
struct PayloadFileEntry {
    archive_path: String,
    disk_path: PathBuf,
    size: u64,
    mode: u32,
}

#[derive(Debug, Clone)]
enum PayloadEntry {
    File(PayloadFileEntry),
    Symlink {
        archive_path: String,
        link_target: PathBuf,
    },
}

impl PayloadEntry {
    fn archive_path(&self) -> &str {
        match self {
            Self::File(file) => &file.archive_path,
            Self::Symlink { archive_path, .. } => archive_path,
        }
    }
}

enum TarStreamCommand {
    File {
        archive_path: String,
        size: u64,
        mode: u32,
        chunks: tokio_mpsc::Receiver<PayloadChunk>,
    },
    Symlink {
        archive_path: String,
        link_target: PathBuf,
    },
}

enum PayloadChunk {
    Data(Vec<u8>),
    Error(String),
}

struct PayloadChunkReader {
    chunks: tokio_mpsc::Receiver<PayloadChunk>,
    current: Cursor<Vec<u8>>,
    finished: bool,
}

impl PayloadChunkReader {
    fn new(chunks: tokio_mpsc::Receiver<PayloadChunk>) -> Self {
        Self {
            chunks,
            current: Cursor::new(Vec::new()),
            finished: false,
        }
    }
}

impl Read for PayloadChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = std::io::Read::read(&mut self.current, buf)?;
            if read > 0 {
                return Ok(read);
            }
            if self.finished {
                return Ok(0);
            }

            match self.chunks.blocking_recv() {
                Some(PayloadChunk::Data(chunk)) => {
                    self.current = Cursor::new(chunk);
                }
                Some(PayloadChunk::Error(message)) => {
                    return Err(std::io::Error::other(message));
                }
                None => {
                    self.finished = true;
                    return Ok(0);
                }
            }
        }
    }
}

const ZSTD_COMPRESSION_LEVEL: i32 = 19;
const PAYLOAD_CHUNK_BYTES: usize = 64 * 1024;
const PAYLOAD_CHANNEL_DEPTH: usize = 8;
const DEFAULT_REPRO_MTIME: u64 = 0;

pub async fn pack(
    _plan: &crate::router::ManifestData,
    opts: CapsulePackOptions,
    _reporter: Arc<dyn crate::reporter::CapsuleReporter + 'static>,
) -> CapsuleResult<PathBuf> {
    debug!("Creating capsule archive (.capsule format)");

    let loaded = crate::manifest::load_manifest(&opts.manifest_path)?;
    let manifest_content = loaded.raw_text;
    let pack_filter = PackFilter::from_manifest(&loaded.model)?;

    // config/lockfile are prepared by the caller once and injected into this packer.
    debug!(
        "Phase 1: using prepared config.json (version={})",
        opts.config_json.version
    );
    let config_path = opts.config_path.clone();
    let lockfile_path = opts.lockfile_path.clone();
    if !config_path.exists() {
        return Err(CapsuleError::Pack(format!(
            "config.json is missing: {}",
            config_path.display()
        )));
    }
    if !lockfile_path.exists() {
        return Err(CapsuleError::Pack(format!(
            "capsule.lock is missing: {}",
            lockfile_path.display()
        )));
    }

    // Step 2: Resolve source payload input
    let source_dir = opts.manifest_dir.join("source");
    let source_root = if source_dir.exists() {
        source_dir.as_path()
    } else {
        debug!("No source/ directory found; packaging project root as source/");
        opts.manifest_dir.as_path()
    };

    // Step 3: Create payload.tar.zst (single-pass stream)
    debug!("Phase 2: creating payload.tar.zst (streaming)");

    let mut payload_entries = collect_payload_entries(source_root, "source", &pack_filter).await?;
    payload_entries.push(PayloadEntry::File(
        payload_file_entry(&config_path, "config.json".to_string()).await?,
    ));
    payload_entries.push(PayloadEntry::File(
        payload_file_entry(&lockfile_path, "capsule.lock".to_string()).await?,
    ));
    payload_entries.sort_by(|a, b| a.archive_path().cmp(b.archive_path()));

    let temp_dir = tempfile::tempdir()?;
    let payload_zst_path = temp_dir.path().join("payload.tar.zst");

    let (tar_tx, tar_rx) = mpsc::channel::<TarStreamCommand>();
    let writer_payload_path = payload_zst_path.clone();
    let writer_handle =
        std::thread::spawn(move || write_payload_tar_zstd_stream(&writer_payload_path, tar_rx));

    let stream_result = stream_payload_entries_to_writer(&payload_entries, &tar_tx).await;
    drop(tar_tx);
    let writer_result = writer_handle
        .join()
        .map_err(|_| CapsuleError::Pack("Payload writer thread panicked".to_string()))?;

    let sbom_inputs = match (stream_result, writer_result) {
        (Ok(files), Ok(())) => files,
        (Err(producer_err), Ok(())) => return Err(producer_err),
        (Ok(_), Err(writer_err)) => return Err(writer_err),
        (Err(producer_err), Err(writer_err)) => {
            return Err(CapsuleError::Pack(format!(
                "Payload stream failed: {producer_err}; writer failed: {writer_err}"
            )))
        }
    };

    let compressed_size = fs::metadata(&payload_zst_path)?.len() as usize;
    debug!("Compressed payload size: {}", format_bytes(compressed_size));

    // Step 4: Create final .capsule archive
    debug!("Phase 3: creating final .capsule archive");

    let output_path = opts.output.clone().unwrap_or_else(|| {
        let name_str = loaded.model.name.replace('\"', "-");
        opts.manifest_dir.join(format!("{}.capsule", name_str))
    });

    let mut capsule_file = fs::File::create(&output_path)?;
    let mut outer_ar = Builder::new(&mut capsule_file);

    // Write actual manifest content
    let manifest_temp_path = temp_dir.path().join("capsule.toml");
    fs::write(&manifest_temp_path, &manifest_content)?;
    append_regular_file_normalized(
        &mut outer_ar,
        &manifest_temp_path,
        "capsule.toml",
        reproducible_mtime_epoch(),
    )?;

    let sbom =
        generate_embedded_sbom_from_inputs_async(loaded.model.name.clone(), sbom_inputs).await?;
    let sbom_temp_path = temp_dir.path().join(SBOM_PATH);
    fs::write(&sbom_temp_path, &sbom.document)?;
    append_regular_file_normalized(
        &mut outer_ar,
        &sbom_temp_path,
        SBOM_PATH,
        reproducible_mtime_epoch(),
    )?;

    // Add signature.json metadata
    let sig_temp_path = temp_dir.path().join("signature.json");
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
    fs::write(&sig_temp_path, signature_bytes)?;
    append_regular_file_normalized(
        &mut outer_ar,
        &sig_temp_path,
        "signature.json",
        reproducible_mtime_epoch(),
    )?;

    // Add payload.tar.zst
    append_regular_file_normalized(
        &mut outer_ar,
        &payload_zst_path,
        "payload.tar.zst",
        reproducible_mtime_epoch(),
    )?;

    outer_ar.finish()?;
    drop(outer_ar);

    let final_size = fs::metadata(&output_path)?.len();
    debug!(
        "Capsule created: {} ({})",
        output_path.display(),
        format_bytes(final_size as usize)
    );

    Ok(output_path)
}

async fn collect_payload_entries(
    src_root: &Path,
    prefix: &str,
    filter: &PackFilter,
) -> CapsuleResult<Vec<PayloadEntry>> {
    let mut entries = Vec::new();
    let mut stack = vec![src_root.to_path_buf()];

    while let Some(dir_path) = stack.pop() {
        let mut read_dir = tokio::fs::read_dir(&dir_path).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;

            if file_type.is_dir() {
                stack.push(path);
                continue;
            }

            let rel = match path.strip_prefix(src_root) {
                Ok(rel) if !rel.as_os_str().is_empty() => rel,
                _ => continue,
            };
            if !filter.should_include_file(rel) || should_skip_reserved_file(rel) {
                continue;
            }

            let rel_str = rel.to_string_lossy().replace('\\', "/");
            let archive_path = format!("{}/{}", prefix, rel_str);
            if file_type.is_symlink() {
                let link_target = tokio::fs::read_link(&path).await?;
                entries.push(PayloadEntry::Symlink {
                    archive_path,
                    link_target,
                });
                continue;
            }
            if file_type.is_file() {
                let metadata = entry.metadata().await?;
                entries.push(PayloadEntry::File(PayloadFileEntry {
                    archive_path,
                    disk_path: path,
                    size: metadata.len(),
                    mode: metadata_mode(&metadata),
                }));
            }
        }
    }

    Ok(entries)
}

async fn payload_file_entry(path: &Path, archive_path: String) -> CapsuleResult<PayloadFileEntry> {
    let metadata = tokio::fs::metadata(path).await?;
    Ok(PayloadFileEntry {
        archive_path,
        disk_path: path.to_path_buf(),
        size: metadata.len(),
        mode: metadata_mode(&metadata),
    })
}

async fn stream_payload_entries_to_writer(
    entries: &[PayloadEntry],
    tar_tx: &mpsc::Sender<TarStreamCommand>,
) -> CapsuleResult<Vec<SbomFileInput>> {
    let mut sbom_inputs = Vec::new();
    for entry in entries {
        match entry {
            PayloadEntry::Symlink {
                archive_path,
                link_target,
            } => {
                tar_tx
                    .send(TarStreamCommand::Symlink {
                        archive_path: archive_path.clone(),
                        link_target: link_target.clone(),
                    })
                    .map_err(|e| {
                        CapsuleError::Pack(format!(
                            "Failed to enqueue symlink tar entry {archive_path}: {e}"
                        ))
                    })?;
            }
            PayloadEntry::File(file_entry) => {
                let sha256 = stream_file_to_tar_writer(tar_tx, file_entry).await?;
                sbom_inputs.push(SbomFileInput {
                    archive_path: file_entry.archive_path.clone(),
                    sha256,
                    disk_path: Some(file_entry.disk_path.clone()),
                });
            }
        }
    }
    Ok(sbom_inputs)
}

async fn stream_file_to_tar_writer(
    tar_tx: &mpsc::Sender<TarStreamCommand>,
    file_entry: &PayloadFileEntry,
) -> CapsuleResult<String> {
    let (chunk_tx, chunk_rx) = tokio_mpsc::channel::<PayloadChunk>(PAYLOAD_CHANNEL_DEPTH);
    tar_tx
        .send(TarStreamCommand::File {
            archive_path: file_entry.archive_path.clone(),
            size: file_entry.size,
            mode: file_entry.mode,
            chunks: chunk_rx,
        })
        .map_err(|e| {
            CapsuleError::Pack(format!(
                "Failed to enqueue tar file entry {}: {}",
                file_entry.archive_path, e
            ))
        })?;

    let mut file = tokio::fs::File::open(&file_entry.disk_path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; PAYLOAD_CHUNK_BYTES];
    let mut remaining = file_entry.size;

    while remaining > 0 {
        let to_read = std::cmp::min(buffer.len() as u64, remaining) as usize;
        let bytes_read = match file.read(&mut buffer[..to_read]).await {
            Ok(n) => n,
            Err(err) => {
                let _ = chunk_tx
                    .send(PayloadChunk::Error(format!(
                        "Failed to read {}: {}",
                        file_entry.disk_path.display(),
                        err
                    )))
                    .await;
                return Err(CapsuleError::Io(err));
            }
        };
        if bytes_read == 0 {
            let message = format!(
                "File changed while packaging (unexpected EOF): {}",
                file_entry.disk_path.display()
            );
            let _ = chunk_tx.send(PayloadChunk::Error(message.clone())).await;
            return Err(CapsuleError::Pack(message));
        }
        let chunk = &buffer[..bytes_read];
        hasher.update(chunk);
        if chunk_tx
            .send(PayloadChunk::Data(chunk.to_vec()))
            .await
            .is_err()
        {
            return Err(CapsuleError::Pack(
                "Payload writer thread disconnected while streaming file chunks".to_string(),
            ));
        }
        remaining -= bytes_read as u64;
    }
    if remaining != 0 {
        let message = format!(
            "File changed while packaging (size drift): {}",
            file_entry.disk_path.display()
        );
        let _ = chunk_tx.send(PayloadChunk::Error(message.clone())).await;
        return Err(CapsuleError::Pack(message));
    }
    if file_entry.size == 0 {
        // zero-length file: send no chunks, only EOF by dropping sender.
    } else {
        let mut probe = [0u8; 1];
        let probe_read = file.read(&mut probe).await?;
        if probe_read > 0 {
            let message = format!(
                "File changed while packaging (grew after metadata read): {}",
                file_entry.disk_path.display()
            );
            let _ = chunk_tx.send(PayloadChunk::Error(message.clone())).await;
            return Err(CapsuleError::Pack(message));
        }
    }
    drop(chunk_tx);

    Ok(hex::encode(hasher.finalize()))
}

fn write_payload_tar_zstd_stream(
    payload_zst_path: &Path,
    tar_rx: mpsc::Receiver<TarStreamCommand>,
) -> CapsuleResult<()> {
    let payload_file = fs::File::create(payload_zst_path)?;
    let mut encoder = ZstdEncoder::new(payload_file, ZSTD_COMPRESSION_LEVEL)?;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    if threads > 1 {
        encoder.multithread(threads)?;
    }
    let mut tar = Builder::new(encoder);
    let mtime = reproducible_mtime_epoch();

    while let Ok(command) = tar_rx.recv() {
        match command {
            TarStreamCommand::Symlink {
                archive_path,
                link_target,
            } => {
                let mut header = Header::new_gnu();
                header.set_entry_type(EntryType::Symlink);
                header.set_size(0);
                header.set_mode(0o777);
                header.set_mtime(mtime);
                header.set_uid(0);
                header.set_gid(0);
                tar.append_link(&mut header, &archive_path, link_target)?;
            }
            TarStreamCommand::File {
                archive_path,
                size,
                mode,
                chunks,
            } => {
                let mut header = Header::new_gnu();
                header.set_size(size);
                header.set_mode(normalize_file_mode(mode));
                header.set_mtime(mtime);
                header.set_uid(0);
                header.set_gid(0);
                header.set_cksum();
                let mut reader = PayloadChunkReader::new(chunks);
                tar.append_data(&mut header, &archive_path, &mut reader)?;
            }
        }
    }

    tar.finish()?;
    let encoder = tar.into_inner()?;
    let _ = encoder.finish()?;
    Ok(())
}

#[cfg(unix)]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn metadata_mode(_: &fs::Metadata) -> u32 {
    0o644
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

fn append_regular_file_normalized<W: std::io::Write>(
    builder: &mut Builder<W>,
    source_path: &Path,
    archive_path: &str,
    mtime: u64,
) -> CapsuleResult<()> {
    let mut file = fs::File::open(source_path)?;
    let metadata = file.metadata()?;
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_size(metadata.len());
    header.set_mode(normalize_file_mode(metadata_mode(&metadata)));
    header.set_mtime(mtime);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();
    builder.append_data(&mut header, archive_path, &mut file)?;
    Ok(())
}

fn normalize_file_mode(mode: u32) -> u32 {
    if mode & 0o111 != 0 {
        0o755
    } else {
        0o644
    }
}

fn reproducible_mtime_epoch() -> u64 {
    std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REPRO_MTIME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::NoOpReporter;
    use crate::router::ExecutionProfile;

    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    #[tokio::test]
    async fn pack_source_is_reproducible_for_identical_inputs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let manifest_path = tmp.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "repro-source-pack"
version = "0.1.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "native"
entrypoint = "source/main.sh"
"#,
        )
        .expect("write manifest");

        std::fs::create_dir_all(tmp.path().join("source")).expect("mkdir source");
        std::fs::write(tmp.path().join("source/main.sh"), "echo repro").expect("write source");

        let lock_path = tmp.path().join("capsule.lock");
        std::fs::write(
            &lock_path,
            r#"version = "1"

[meta]
created_at = "2026-01-01T00:00:00Z"
manifest_hash = "sha256:dummy"
"#,
        )
        .expect("write lockfile");

        let config = Arc::new(
            crate::r3_config::generate_config(&manifest_path, Some("strict".to_string()), false)
                .expect("generate config"),
        );
        let config_path =
            crate::r3_config::write_config(&manifest_path, config.as_ref()).expect("write config");

        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out1 = tmp.path().join("repro-1.capsule");
        let out2 = tmp.path().join("repro-2.capsule");

        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path: manifest_path.clone(),
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out1.clone()),
                config_json: config.clone(),
                config_path: config_path.clone(),
                lockfile_path: lock_path.clone(),
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("first pack");

        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path: manifest_path.clone(),
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out2.clone()),
                config_json: config,
                config_path,
                lockfile_path: lock_path,
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("second pack");

        let first = std::fs::read(out1).expect("read first artifact");
        let second = std::fs::read(out2).expect("read second artifact");
        assert_eq!(sha256_hex(&first), sha256_hex(&second));
    }
}
