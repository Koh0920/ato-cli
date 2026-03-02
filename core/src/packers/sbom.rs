use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{CapsuleError, Result};

pub const SBOM_PATH: &str = "sbom.spdx.json";

#[derive(Debug, Clone)]
pub struct EmbeddedSbom {
    pub document: String,
    pub sha256: String,
}

#[derive(Serialize)]
struct SpdxCreationInfo {
    created: String,
    creators: Vec<String>,
}

#[derive(Serialize)]
struct SpdxFile {
    #[serde(rename = "SPDXID")]
    spdx_id: String,
    #[serde(rename = "fileName")]
    file_name: String,
    #[serde(rename = "checksums")]
    checksums: Vec<SpdxChecksum>,
}

#[derive(Serialize)]
struct SpdxChecksum {
    #[serde(rename = "algorithm")]
    algorithm: String,
    #[serde(rename = "checksumValue")]
    checksum_value: String,
}

#[derive(Serialize)]
struct SpdxDocument {
    #[serde(rename = "spdxVersion")]
    spdx_version: String,
    #[serde(rename = "dataLicense")]
    data_license: String,
    #[serde(rename = "SPDXID")]
    spdx_id: String,
    name: String,
    #[serde(rename = "documentNamespace")]
    document_namespace: String,
    #[serde(rename = "creationInfo")]
    creation_info: SpdxCreationInfo,
    files: Vec<SpdxFile>,
}

pub fn generate_embedded_sbom(
    capsule_name: &str,
    files: &[(String, PathBuf)],
) -> Result<EmbeddedSbom> {
    let mut sbom_files = Vec::new();
    for (archive_path, disk_path) in files {
        let data = fs::read(disk_path).map_err(CapsuleError::Io)?;
        sbom_files.push(SpdxFile {
            spdx_id: format!("SPDXRef-File-{}", sanitize_spdx_id(archive_path)),
            file_name: archive_path.clone(),
            checksums: vec![SpdxChecksum {
                algorithm: "SHA256".to_string(),
                checksum_value: sha256_hex(&data),
            }],
        });
    }
    sbom_files.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    let document = SpdxDocument {
        spdx_version: "SPDX-2.3".to_string(),
        data_license: "CC0-1.0".to_string(),
        spdx_id: "SPDXRef-DOCUMENT".to_string(),
        name: format!("{}-sbom", capsule_name),
        document_namespace: format!(
            "https://ato.run/sbom/{}/{}",
            capsule_name,
            Utc::now().format("%Y%m%d%H%M%S")
        ),
        creation_info: SpdxCreationInfo {
            created: Utc::now().to_rfc3339(),
            creators: vec!["Tool: ato-cli".to_string()],
        },
        files: sbom_files,
    };
    let document = serde_json::to_string_pretty(&document)
        .map_err(|e| CapsuleError::Pack(format!("Failed to serialize SBOM: {e}")))?;
    let sha256 = sha256_hex(document.as_bytes());

    Ok(EmbeddedSbom { document, sha256 })
}

pub fn extract_and_verify_embedded_sbom(capsule_path: &Path) -> Result<String> {
    let mut archive = tar::Archive::new(fs::File::open(capsule_path).map_err(CapsuleError::Io)?);
    let mut sbom = None;
    let mut expected_sha = None;

    for entry in archive.entries().map_err(CapsuleError::Io)? {
        let mut entry = entry.map_err(CapsuleError::Io)?;
        let path = entry
            .path()
            .map_err(CapsuleError::Io)?
            .to_string_lossy()
            .to_string();
        if path == SBOM_PATH {
            let mut bytes = Vec::new();
            use std::io::Read;
            entry.read_to_end(&mut bytes).map_err(CapsuleError::Io)?;
            sbom = Some(bytes);
        } else if path == "signature.json" {
            let mut text = String::new();
            use std::io::Read;
            entry.read_to_string(&mut text).map_err(CapsuleError::Io)?;
            let parsed: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| CapsuleError::Pack(format!("Invalid signature.json: {e}")))?;
            expected_sha = parsed
                .get("sbom")
                .and_then(|v| v.get("sha256"))
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
        }
    }

    let sbom = sbom
        .ok_or_else(|| CapsuleError::Pack("Embedded SBOM file not found in capsule".to_string()))?;
    let expected_sha = expected_sha
        .ok_or_else(|| CapsuleError::Pack("SBOM metadata missing in signature.json".to_string()))?;

    let actual_sha = sha256_hex(&sbom);
    if actual_sha != expected_sha {
        return Err(CapsuleError::Pack(format!(
            "Embedded SBOM hash mismatch: expected {expected_sha}, got {actual_sha}"
        )));
    }

    let text = String::from_utf8(sbom)
        .map_err(|e| CapsuleError::Pack(format!("Embedded SBOM is not UTF-8: {e}")))?;
    serde_json::from_str::<serde_json::Value>(&text)
        .map_err(|e| CapsuleError::Pack(format!("Embedded SBOM is not valid JSON: {e}")))?;
    Ok(text)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn sanitize_spdx_id(path: &str) -> String {
    path.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tar::Builder;

    #[test]
    fn sbom_generation_fails_closed_for_missing_files() {
        let result = generate_embedded_sbom(
            "demo",
            &[(
                "source/missing.txt".to_string(),
                PathBuf::from("/definitely/missing/file.txt"),
            )],
        );
        assert!(result.is_err());
    }

    #[test]
    fn embedded_sbom_can_be_extracted_and_verified() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let capsule_path = tmp.path().join("demo.capsule");
        let sbom_text = r#"{"spdxVersion":"SPDX-2.3","files":[]}"#;
        let sbom_sha = sha256_hex(sbom_text.as_bytes());
        let signature = serde_json::json!({
            "signed": false,
            "sbom": {
                "path": SBOM_PATH,
                "sha256": sbom_sha,
                "format": "spdx-json"
            }
        });

        let mut file = fs::File::create(&capsule_path).expect("create capsule");
        let mut ar = Builder::new(&mut file);
        let mut sig_header = tar::Header::new_gnu();
        sig_header.set_size(signature.to_string().len() as u64);
        sig_header.set_mode(0o644);
        sig_header.set_cksum();
        ar.append_data(
            &mut sig_header,
            "signature.json",
            signature.to_string().as_bytes(),
        )
        .expect("append signature");

        let mut sbom_header = tar::Header::new_gnu();
        sbom_header.set_size(sbom_text.len() as u64);
        sbom_header.set_mode(0o644);
        sbom_header.set_cksum();
        ar.append_data(&mut sbom_header, SBOM_PATH, sbom_text.as_bytes())
            .expect("append sbom");
        ar.finish().expect("finish");
        drop(ar);

        let extracted = extract_and_verify_embedded_sbom(&capsule_path).expect("extract");
        let parsed: serde_json::Value = serde_json::from_str(&extracted).expect("json");
        assert_eq!(parsed["spdxVersion"], "SPDX-2.3");
    }
}
