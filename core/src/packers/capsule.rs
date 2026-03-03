use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};

use sha2::{Digest, Sha256};
use tar::{Builder, EntryType, Header};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::capsule_v3::{
    CasProvider, CasStore, FastCdcWriteReport, FastCdcWriter, FastCdcWriterConfig,
    V3_PAYLOAD_MANIFEST_PATH,
};
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
const EXPERIMENTAL_V3_ENV: &str = "ATO_EXPERIMENTAL_V3_PACK";

struct PayloadWriteResult {
    v3_report: Option<FastCdcWriteReport>,
}

struct DualSinkWriter {
    v2: Option<ZstdEncoder<'static, fs::File>>,
    v3: Option<FastCdcWriter>,
    sticky_error: Option<String>,
}

impl DualSinkWriter {
    fn new(v2: ZstdEncoder<'static, fs::File>, v3: FastCdcWriter) -> Self {
        Self {
            v2: Some(v2),
            v3: Some(v3),
            sticky_error: None,
        }
    }

    fn finalize(mut self) -> CapsuleResult<FastCdcWriteReport> {
        self.ensure_healthy()?;
        let v2 = self.v2.take().ok_or_else(|| {
            CapsuleError::Pack("DualSinkWriter v2 encoder already finalized".to_string())
        })?;
        v2.finish()
            .map_err(|e| CapsuleError::Pack(format!("Failed to finish v2 zstd payload: {}", e)))?;
        let v3 = self.v3.take().ok_or_else(|| {
            CapsuleError::Pack("DualSinkWriter v3 writer already finalized".to_string())
        })?;
        v3.finalize()
    }

    fn ensure_healthy(&self) -> std::io::Result<()> {
        if let Some(message) = &self.sticky_error {
            return Err(std::io::Error::other(message.clone()));
        }
        Ok(())
    }

    fn mark_error(&mut self, message: impl Into<String>) -> std::io::Error {
        let message = message.into();
        self.sticky_error = Some(message.clone());
        std::io::Error::other(message)
    }
}

impl Write for DualSinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.ensure_healthy()?;
        if buf.is_empty() {
            return Ok(0);
        }

        let v2 = match self.v2.as_mut() {
            Some(v2) => v2,
            None => {
                return Err(self.mark_error("DualSinkWriter v2 encoder not available during write"));
            }
        };
        write_all_exact(v2, buf).map_err(|e| {
            self.mark_error(format!(
                "Failed to write full chunk to v2 payload encoder: {}",
                e
            ))
        })?;

        let v3 = match self.v3.as_mut() {
            Some(v3) => v3,
            None => {
                return Err(self.mark_error("DualSinkWriter v3 writer not available during write"));
            }
        };
        v3.write_bytes(buf).map_err(|e| {
            self.mark_error(format!(
                "Failed to write chunk into v3 FastCDC pipeline: {}",
                e
            ))
        })?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.ensure_healthy()?;
        let v2 = match self.v2.as_mut() {
            Some(v2) => v2,
            None => {
                return Err(self.mark_error("DualSinkWriter v2 encoder not available during flush"));
            }
        };
        v2.flush().map_err(|e| {
            self.mark_error(format!(
                "Failed to flush v2 payload encoder before finalize: {}",
                e
            ))
        })
    }
}

enum PayloadSink {
    V2Only(Option<ZstdEncoder<'static, fs::File>>),
    Dual(Option<DualSinkWriter>),
}

impl PayloadSink {
    fn finalize(self) -> CapsuleResult<Option<FastCdcWriteReport>> {
        match self {
            Self::V2Only(Some(encoder)) => {
                encoder.finish().map_err(|e| {
                    CapsuleError::Pack(format!("Failed to finish v2 payload encoder: {}", e))
                })?;
                Ok(None)
            }
            Self::Dual(Some(dual)) => dual.finalize().map(Some),
            Self::V2Only(None) | Self::Dual(None) => Err(CapsuleError::Pack(
                "Payload sink was finalized more than once".to_string(),
            )),
        }
    }
}

impl Write for PayloadSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::V2Only(Some(encoder)) => write_all_exact(encoder, buf).map(|()| buf.len()),
            Self::Dual(Some(dual)) => dual.write(buf),
            Self::V2Only(None) | Self::Dual(None) => {
                Err(std::io::Error::other("payload sink is not available"))
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::V2Only(Some(encoder)) => encoder.flush(),
            Self::Dual(Some(dual)) => dual.flush(),
            Self::V2Only(None) | Self::Dual(None) => {
                Err(std::io::Error::other("payload sink is not available"))
            }
        }
    }
}

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
    let v3_cas = if experimental_v3_pack_enabled() {
        debug!(
            "{}=1 detected: enabling payload v3 CAS dual-write path",
            EXPERIMENTAL_V3_ENV
        );
        match CasProvider::from_env() {
            CasProvider::Enabled(cas) => Some(cas),
            CasProvider::Disabled(reason) => {
                CasProvider::log_disabled_once("capsule_pack_v3", &reason);
                None
            }
        }
    } else {
        None
    };

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
    let writer_handle = std::thread::spawn(move || {
        write_payload_tar_zstd_stream(&writer_payload_path, tar_rx, v3_cas)
    });

    let stream_result = stream_payload_entries_to_writer(&payload_entries, &tar_tx).await;
    drop(tar_tx);
    let writer_result = writer_handle
        .join()
        .map_err(|_| CapsuleError::Pack("Payload writer thread panicked".to_string()))?;

    let (sbom_inputs, v3_report) = match (stream_result, writer_result) {
        (Ok(files), Ok(result)) => (files, result.v3_report),
        (Err(producer_err), Ok(_)) => return Err(producer_err),
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
    if let Some(report) = &v3_report {
        let v3_manifest_temp_path = temp_dir.path().join(V3_PAYLOAD_MANIFEST_PATH);
        let canonical_manifest = serde_jcs::to_vec(&report.manifest).map_err(|e| {
            CapsuleError::Pack(format!(
                "Failed to serialize payload v3 manifest (JCS): {e}"
            ))
        })?;
        fs::write(&v3_manifest_temp_path, canonical_manifest)?;
        append_regular_file_normalized(
            &mut outer_ar,
            &v3_manifest_temp_path,
            V3_PAYLOAD_MANIFEST_PATH,
            reproducible_mtime_epoch(),
        )?;
    }

    // Add signature.json metadata
    let sig_temp_path = temp_dir.path().join("signature.json");
    let mut signature = serde_json::json!({
        "signed": false,
        "note": "To be signed",
        "sbom": {
            "path": SBOM_PATH,
            "sha256": sbom.sha256,
            "format": "spdx-json",
        }
    });
    if let Some(report) = &v3_report {
        if let Some(obj) = signature.as_object_mut() {
            obj.insert(
                "payload_v3".to_string(),
                serde_json::json!({
                    "path": V3_PAYLOAD_MANIFEST_PATH,
                    "artifact_hash": report.manifest.artifact_hash,
                    "schema_version": report.manifest.schema_version,
                    "chunk_count": report.manifest.chunks.len(),
                    "total_raw_size": report.total_raw_size,
                }),
            );
        }
    }
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
    v3_cas: Option<CasStore>,
) -> CapsuleResult<PayloadWriteResult> {
    let payload_file = fs::File::create(payload_zst_path)?;
    let mut encoder = ZstdEncoder::new(payload_file, ZSTD_COMPRESSION_LEVEL)?;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    if threads > 1 {
        encoder.multithread(threads)?;
    }

    let sink = if let Some(cas) = v3_cas {
        match FastCdcWriter::new(FastCdcWriterConfig::default(), cas) {
            Ok(fastcdc_writer) => {
                PayloadSink::Dual(Some(DualSinkWriter::new(encoder, fastcdc_writer)))
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to initialize payload v3 FastCDC writer; continuing with v2-only payload"
                );
                PayloadSink::V2Only(Some(encoder))
            }
        }
    } else {
        PayloadSink::V2Only(Some(encoder))
    };

    let mut tar = Builder::new(sink);
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
    let sink = tar.into_inner()?;
    let v3_report = sink.finalize()?;
    Ok(PayloadWriteResult { v3_report })
}

fn write_all_exact<W: Write>(writer: &mut W, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        let written = writer.write(buf)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "writer returned zero bytes",
            ));
        }
        buf = &buf[written..];
    }
    Ok(())
}

fn experimental_v3_pack_enabled() -> bool {
    std::env::var(EXPERIMENTAL_V3_ENV)
        .ok()
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
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
            | "payload.v3.manifest.json"
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
    use std::collections::HashMap;
    use std::ffi::{OsStr, OsString};
    use std::sync::{Mutex, OnceLock};

    use crate::capsule_v3::hash::verify_artifact_hash;
    use crate::capsule_v3::CapsuleManifestV3;
    use crate::reporter::NoOpReporter;
    use crate::router::ExecutionProfile;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: Option<&OsStr>) -> Self {
            let previous = std::env::var_os(key);
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    fn create_source_fixture(
        tmp: &tempfile::TempDir,
        name: &str,
    ) -> (PathBuf, Arc<crate::r3_config::ConfigJson>, PathBuf, PathBuf) {
        let manifest_path = tmp.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            format!(
                r#"
schema_version = "0.2"
name = "{name}"
version = "0.1.0"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "native"
entrypoint = "source/main.sh"
"#,
            ),
        )
        .expect("write manifest");

        std::fs::create_dir_all(tmp.path().join("source")).expect("mkdir source");
        std::fs::write(tmp.path().join("source/main.sh"), "echo repro").expect("write source");
        std::fs::write(
            tmp.path().join("source/data.bin"),
            vec![b'x'; 512 * 1024 + 333],
        )
        .expect("write payload");

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

        (manifest_path, config, config_path, lock_path)
    }

    fn read_outer_entries(path: &Path) -> HashMap<String, Vec<u8>> {
        let mut out = HashMap::new();
        let file = std::fs::File::open(path).expect("open capsule");
        let mut archive = tar::Archive::new(file);
        for entry in archive.entries().expect("entries") {
            let mut entry = entry.expect("entry");
            let key = entry.path().expect("path").to_string_lossy().to_string();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).expect("read");
            out.insert(key, bytes);
        }
        out
    }

    fn extract_payload_tar_bytes_from_entries(entries: &HashMap<String, Vec<u8>>) -> Vec<u8> {
        let payload = entries
            .get("payload.tar.zst")
            .expect("payload.tar.zst missing");
        let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(payload))
            .expect("create payload decoder");
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("decode payload");
        out
    }

    fn reconstruct_payload_tar_from_cas(manifest: &CapsuleManifestV3, cas: &CasStore) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in &manifest.chunks {
            let path = cas.chunk_path(&chunk.raw_hash).expect("chunk path");
            let mut decoder =
                zstd::stream::Decoder::new(std::fs::File::open(path).expect("open chunk from cas"))
                    .expect("decoder");
            decoder.read_to_end(&mut out).expect("decode chunk");
        }
        out
    }

    struct PartialWriter {
        inner: Vec<u8>,
        max_write: usize,
    }

    impl PartialWriter {
        fn new(max_write: usize) -> Self {
            Self {
                inner: Vec::new(),
                max_write,
            }
        }
    }

    impl Write for PartialWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let n = std::cmp::min(self.max_write.max(1), buf.len());
            self.inner.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_dual_sink_handles_partial_write_correctly() {
        let payload = vec![0xAB; 16 * 1024];
        let mut writer = PartialWriter::new(17);
        write_all_exact(&mut writer, &payload).expect("write_all_exact");
        assert_eq!(writer.inner, payload);
    }

    #[test]
    fn test_dual_sink_propagates_secondary_sink_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let v2_file = std::fs::File::create(tmp.path().join("v2.zst")).expect("v2 file");
        let v2_encoder = ZstdEncoder::new(v2_file, 3).expect("v2 encoder");
        let cas = CasStore::new(tmp.path().join("cas")).expect("cas");
        let mut v3_writer =
            FastCdcWriter::new(FastCdcWriterConfig::default(), cas).expect("v3 writer");
        v3_writer.inject_sticky_error_for_test("injected secondary sink failure");
        let mut dual = DualSinkWriter::new(v2_encoder, v3_writer);

        let err = dual.write(b"abc").expect_err("must fail");
        assert!(err.to_string().contains("secondary sink failure"));
    }

    #[tokio::test]
    async fn pack_source_is_reproducible_for_identical_inputs() {
        let _env_guard = env_lock().lock().expect("env lock");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, None);
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", None);
        let _epoch = EnvVarGuard::set("SOURCE_DATE_EPOCH", Some(OsStr::new("0")));

        let tmp = tempfile::tempdir().expect("tempdir");
        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "repro-source-pack");

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

    #[tokio::test]
    async fn test_experimental_v3_off_keeps_v2_layout() {
        let _env_guard = env_lock().lock().expect("env lock");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, None);

        let tmp = tempfile::tempdir().expect("tempdir");
        let cas_root = tmp.path().join("cas-off-store");
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", Some(cas_root.as_os_str()));
        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "v3-off-layout");
        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out = tmp.path().join("off.capsule");
        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path,
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out.clone()),
                config_json: config,
                config_path,
                lockfile_path: lock_path,
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("pack");

        let entries = read_outer_entries(&out);
        assert!(entries.contains_key("payload.tar.zst"));
        assert!(!entries.contains_key(V3_PAYLOAD_MANIFEST_PATH));
        let signature: serde_json::Value =
            serde_json::from_slice(entries.get("signature.json").expect("signature"))
                .expect("json");
        assert!(signature.get("payload_v3").is_none());
        assert!(
            !cas_root.exists(),
            "v3 disabled must not create CAS root: {}",
            cas_root.display()
        );
    }

    #[tokio::test]
    async fn test_experimental_v3_on_embeds_manifest_and_writes_cas() {
        let _env_guard = env_lock().lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas_root = tmp.path().join("cas-store");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, Some(OsStr::new("1")));
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", Some(cas_root.as_os_str()));
        let _epoch = EnvVarGuard::set("SOURCE_DATE_EPOCH", Some(OsStr::new("0")));

        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "v3-on-layout");
        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out = tmp.path().join("on.capsule");
        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path,
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out.clone()),
                config_json: config,
                config_path,
                lockfile_path: lock_path,
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("pack");

        let entries = read_outer_entries(&out);
        let manifest_bytes = entries
            .get(V3_PAYLOAD_MANIFEST_PATH)
            .expect("v3 manifest missing");
        let manifest: CapsuleManifestV3 =
            serde_json::from_slice(manifest_bytes).expect("parse v3 manifest");
        verify_artifact_hash(&manifest).expect("verify hash");

        let cas = CasStore::new(&cas_root).expect("cas");
        for chunk in &manifest.chunks {
            assert!(cas.has_chunk(&chunk.raw_hash).expect("has_chunk"));
        }

        let signature: serde_json::Value =
            serde_json::from_slice(entries.get("signature.json").expect("signature"))
                .expect("json");
        assert_eq!(
            signature
                .get("payload_v3")
                .and_then(|v| v.get("path"))
                .and_then(|v| v.as_str()),
            Some(V3_PAYLOAD_MANIFEST_PATH)
        );
    }

    #[tokio::test]
    async fn test_v3_manifest_reconstructs_payload_tar_bytes() {
        let _env_guard = env_lock().lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas_root = tmp.path().join("cas-store");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, Some(OsStr::new("1")));
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", Some(cas_root.as_os_str()));
        let _epoch = EnvVarGuard::set("SOURCE_DATE_EPOCH", Some(OsStr::new("0")));

        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "v3-reconstruct");
        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out = tmp.path().join("reconstruct.capsule");
        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path,
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out.clone()),
                config_json: config,
                config_path,
                lockfile_path: lock_path,
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("pack");

        let entries = read_outer_entries(&out);
        let payload_tar_bytes = extract_payload_tar_bytes_from_entries(&entries);
        let manifest: CapsuleManifestV3 =
            serde_json::from_slice(entries.get(V3_PAYLOAD_MANIFEST_PATH).expect("manifest"))
                .expect("parse manifest");
        let cas = CasStore::new(&cas_root).expect("cas");
        let reconstructed = reconstruct_payload_tar_from_cas(&manifest, &cas);
        assert_eq!(payload_tar_bytes, reconstructed);
        assert_eq!(
            crate::capsule_v3::manifest::blake3_digest(&payload_tar_bytes),
            crate::capsule_v3::manifest::blake3_digest(&reconstructed)
        );
    }

    #[tokio::test]
    async fn test_experimental_v3_cas_init_error_falls_back_to_v2() {
        let _env_guard = env_lock().lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas_root_file = tmp.path().join("cas-root-as-file");
        std::fs::write(&cas_root_file, "not-a-directory").expect("write cas file");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, Some(OsStr::new("1")));
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", Some(cas_root_file.as_os_str()));

        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "v3-fail-closed");
        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out = tmp.path().join("fail.capsule");
        pack(
            &decision.plan,
            CapsulePackOptions {
                manifest_path,
                manifest_dir: tmp.path().to_path_buf(),
                output: Some(out.clone()),
                config_json: config,
                config_path,
                lockfile_path: lock_path,
            },
            Arc::new(NoOpReporter),
        )
        .await
        .expect("pack should fall back to v2");

        let entries = read_outer_entries(&out);
        assert!(entries.contains_key("payload.tar.zst"));
        assert!(!entries.contains_key(V3_PAYLOAD_MANIFEST_PATH));
        let signature: serde_json::Value =
            serde_json::from_slice(entries.get("signature.json").expect("signature"))
                .expect("json");
        assert!(signature.get("payload_v3").is_none());
    }

    #[tokio::test]
    async fn test_experimental_v3_reproducible_manifest_for_identical_inputs() {
        let _env_guard = env_lock().lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let cas_root = tmp.path().join("cas-store");
        let _exp = EnvVarGuard::set(EXPERIMENTAL_V3_ENV, Some(OsStr::new("1")));
        let _cas = EnvVarGuard::set("ATO_CAS_ROOT", Some(cas_root.as_os_str()));
        let _epoch = EnvVarGuard::set("SOURCE_DATE_EPOCH", Some(OsStr::new("0")));

        let (manifest_path, config, config_path, lock_path) =
            create_source_fixture(&tmp, "v3-repro-manifest");
        let decision =
            crate::router::route_manifest(&manifest_path, ExecutionProfile::Release, None)
                .expect("route");
        let out1 = tmp.path().join("v3-1.capsule");
        let out2 = tmp.path().join("v3-2.capsule");

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
                manifest_path,
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

        let entries1 = read_outer_entries(&out1);
        let entries2 = read_outer_entries(&out2);
        assert_eq!(
            entries1.get(V3_PAYLOAD_MANIFEST_PATH),
            entries2.get(V3_PAYLOAD_MANIFEST_PATH)
        );
    }
}
