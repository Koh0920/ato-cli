#![cfg(feature = "manifest-signing")]
#![allow(dead_code)]

use ed25519_dalek::Signature;
use serde_json::Value;

use crate::error::{CapsuleError, Result};
use crate::schema::manifest_to_capnp_bytes;
use crate::types::capsule_v1::CapsuleManifestV1;
use crate::types::signing::{parse_developer_key, verify_signature_file, SignatureFile};

const SIGNATURE_VERSION: u8 = 0x01;
const KEY_TYPE_ED25519: u8 = 0x01;

/// Manifest signature verifier (legacy compatibility for tests).
#[derive(Debug, Clone)]
pub struct ManifestVerifier {
    trusted_key: Option<String>,
    strict: bool,
}

impl ManifestVerifier {
    pub fn new(trusted_key: Option<String>, strict: bool) -> Self {
        Self {
            trusted_key,
            strict,
        }
    }

    /// Verify a manifest signature using JSON input.
    pub fn verify(&self, manifest_bytes: &[u8], signature: &[u8], capsule_id: &str) -> Result<()> {
        let manifest: CapsuleManifestV1 = serde_json::from_slice(manifest_bytes)
            .map_err(|e| CapsuleError::Crypto(format!("failed to parse manifest JSON: {}", e)))?;
        self.verify_manifest(&manifest, signature, capsule_id)
    }

    /// Verify a manifest signature using the manifest struct (canonical Cap'n Proto bytes).
    pub fn verify_manifest(
        &self,
        manifest: &CapsuleManifestV1,
        signature: &[u8],
        _capsule_id: &str,
    ) -> Result<()> {
        let canonical_bytes = manifest_to_capnp_bytes(manifest)
            .map_err(|e| CapsuleError::Crypto(format!("failed to build canonical bytes: {}", e)))?;
        self.verify_bytes(&canonical_bytes, signature)
    }

    fn verify_bytes(&self, message: &[u8], signature: &[u8]) -> Result<()> {
        if self.trusted_key.is_none() {
            if self.strict {
                return Err(CapsuleError::AuthRequired(
                    "signature verification failed: no trusted key configured".to_string(),
                ));
            }
            return Ok(());
        }

        let sig = parse_signature_bytes(signature)?;

        if let Some(trusted_key) = &self.trusted_key {
            let trusted = parse_developer_key(trusted_key)?;
            if sig.public_key != trusted {
                return Err(CapsuleError::AuthRequired(
                    "signature public key is not the trusted signer".to_string(),
                ));
            }
        }

        if let Err(_err) = verify_signature_file(&sig, message) {
            return Err(CapsuleError::Crypto(
                "Cryptographic verification failed".to_string(),
            ));
        }

        Ok(())
    }
}

fn parse_signature_bytes(data: &[u8]) -> Result<SignatureFile> {
    if data.len() < 1 + 1 + 32 + 64 + 2 {
        return Err(CapsuleError::Crypto("Invalid signature format".to_string()));
    }

    let version = data[0];
    let key_type = data[1];
    if version != SIGNATURE_VERSION {
        return Err(CapsuleError::Crypto(format!(
            "unsupported signature version {}",
            version
        )));
    }
    if key_type != KEY_TYPE_ED25519 {
        return Err(CapsuleError::Crypto(format!(
            "unsupported key_type {}",
            key_type
        )));
    }

    let mut offset = 2;
    let mut public_key = [0u8; 32];
    public_key.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&data[offset..offset + 64]);
    offset += 64;
    let signature = Signature::from_bytes(&sig_bytes);

    let metadata_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    if data.len() < offset + metadata_len {
        return Err(CapsuleError::Crypto("Invalid signature format".to_string()));
    }
    let metadata_bytes = &data[offset..offset + metadata_len];
    let metadata: Value = serde_json::from_slice(metadata_bytes).map_err(|e| {
        CapsuleError::Crypto(format!("failed to parse signature metadata JSON: {}", e))
    })?;

    Ok(SignatureFile {
        version,
        key_type,
        public_key,
        signature,
        metadata,
    })
}
