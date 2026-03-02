use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct PublishArtifactArgs {
    pub artifact_path: PathBuf,
    pub scoped_id: String,
    pub registry_url: String,
    pub force_large_payload: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishArtifactResult {
    pub scoped_id: String,
    pub version: String,
    pub artifact_url: String,
    pub file_name: String,
    pub sha256: String,
    pub blake3: String,
    pub size_bytes: u64,
}

#[derive(Debug)]
struct ArtifactPayload {
    publisher: String,
    slug: String,
    version: String,
    file_name: String,
    bytes: Vec<u8>,
    sha256: String,
    blake3: String,
}

pub fn publish_artifact(args: PublishArtifactArgs) -> Result<PublishArtifactResult> {
    let base_url = normalize_registry_url(&args.registry_url)?;
    crate::payload_guard::ensure_payload_size(
        &args.artifact_path,
        args.force_large_payload,
        "--force-large-payload",
    )?;
    let payload = load_artifact_payload(&args.artifact_path, &args.scoped_id)?;
    let endpoint = format!(
        "{}/v1/local/capsules/{}/{}/{}?file_name={}",
        base_url,
        urlencoding::encode(&payload.publisher),
        urlencoding::encode(&payload.slug),
        urlencoding::encode(&payload.version),
        urlencoding::encode(&payload.file_name),
    );

    let request = reqwest::blocking::Client::new()
        .put(&endpoint)
        .header("content-type", "application/octet-stream")
        .header("x-ato-sha256", &payload.sha256)
        .header("x-ato-blake3", &payload.blake3);

    let request = if let Some(token) = read_ato_token() {
        request.header("authorization", format!("Bearer {}", token))
    } else {
        request
    };

    let response = request
        .body(payload.bytes)
        .send()
        .with_context(|| format!("Failed to upload artifact to {}", endpoint))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        bail!("Artifact upload failed ({}): {}", status, body);
    }

    let result = response
        .json::<PublishArtifactResult>()
        .context("Invalid local registry upload response")?;
    Ok(result)
}

fn load_artifact_payload(path: &Path, scoped_id: &str) -> Result<ArtifactPayload> {
    if !path.exists() {
        bail!("Artifact not found: {}", path.display());
    }
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| !ext.eq_ignore_ascii_case("capsule"))
        .unwrap_or(true)
    {
        bail!("--artifact must point to a .capsule file");
    }

    let scoped = crate::install::parse_capsule_ref(scoped_id)?;
    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read artifact: {}", path.display()))?;
    let manifest = extract_manifest_from_capsule(&bytes)?;
    let parsed = capsule_core::types::capsule_v1::CapsuleManifestV1::from_toml(&manifest)
        .map_err(|err| anyhow::anyhow!("Failed to parse capsule.toml from artifact: {}", err))?;

    if parsed.name != scoped.slug {
        bail!(
            "--scoped-id slug '{}' must match artifact manifest.name '{}'",
            scoped.slug,
            parsed.name
        );
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Failed to derive artifact file name"))?;

    Ok(ArtifactPayload {
        publisher: scoped.publisher,
        slug: scoped.slug,
        version: parsed.version,
        file_name,
        sha256: compute_sha256(&bytes),
        blake3: compute_blake3(&bytes),
        bytes,
    })
}

fn extract_manifest_from_capsule(bytes: &[u8]) -> Result<String> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut entries = archive
        .entries()
        .context("Failed to read .capsule archive entries")?;

    while let Some(entry) = entries.next() {
        let mut entry = entry.context("Invalid .capsule entry")?;
        let entry_path = entry
            .path()
            .context("Failed to read archive entry path")?
            .to_string_lossy()
            .to_string();
        if entry_path == "capsule.toml" {
            let mut manifest = String::new();
            entry
                .read_to_string(&mut manifest)
                .context("Failed to read capsule.toml from artifact")?;
            return Ok(manifest);
        }
    }

    bail!("Invalid artifact: capsule.toml not found in .capsule archive")
}

fn normalize_registry_url(raw: &str) -> Result<String> {
    let url = reqwest::Url::parse(raw)
        .with_context(|| format!("Invalid --registry URL for artifact publish: {}", raw))?;
    let scheme = url.scheme().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        bail!(
            "Registry URL must use http or https scheme (got '{}')",
            url.scheme()
        );
    }
    Ok(raw.trim().trim_end_matches('/').to_string())
}

fn read_ato_token() -> Option<String> {
    std::env::var("ATO_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn compute_blake3(data: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    let hash = hasher.finalize();
    format!("blake3:{}", hex::encode(hash.as_bytes()))
}

fn compute_sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tar::Builder;

    fn test_capsule_bytes(name: &str, version: &str) -> Vec<u8> {
        let manifest = format!(
            r#"schema_version = "0.2"
name = "{name}"
version = "{version}"
type = "app"
default_target = "cli"

[targets.cli]
runtime = "source"
driver = "deno"
entrypoint = "main.ts"
"#
        );
        let mut buf = Vec::<u8>::new();
        {
            let mut builder = Builder::new(&mut buf);
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(manifest.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, "capsule.toml", manifest.as_bytes())
                .expect("append manifest");

            let sig = r#"{"signed":false}"#;
            let mut sig_header = tar::Header::new_gnu();
            sig_header.set_mode(0o644);
            sig_header.set_size(sig.len() as u64);
            sig_header.set_cksum();
            builder
                .append_data(&mut sig_header, "signature.json", sig.as_bytes())
                .expect("append signature");
            builder.finish().expect("finish tar");
        }
        buf
    }

    #[test]
    fn extract_manifest_from_capsule_succeeds() {
        let bytes = test_capsule_bytes("sample-capsule", "1.0.0");
        let manifest = extract_manifest_from_capsule(&bytes).expect("extract manifest");
        assert!(manifest.contains("name = \"sample-capsule\""));
        assert!(manifest.contains("version = \"1.0.0\""));
    }

    #[test]
    fn slug_mismatch_is_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("sample-capsule.capsule");
        std::fs::write(&path, test_capsule_bytes("sample-capsule", "1.0.0")).expect("write");

        let err = load_artifact_payload(&path, "koh0920/another-slug")
            .err()
            .expect("must fail");
        assert!(err
            .to_string()
            .contains("must match artifact manifest.name"));
    }

    #[test]
    fn hash_generation_is_stable() {
        let data = b"capsule-bytes";
        let s1 = compute_sha256(data);
        let s2 = compute_sha256(data);
        let b1 = compute_blake3(data);
        let b2 = compute_blake3(data);
        assert_eq!(s1, s2);
        assert_eq!(b1, b2);
        assert!(s1.starts_with("sha256:"));
        assert!(b1.starts_with("blake3:"));
    }
}
