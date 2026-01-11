//! `capsule keygen` - generate signing keys for capsules.

use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub struct KeygenArgs {
    pub name: Option<String>,
}

pub fn execute(args: KeygenArgs) -> Result<()> {
    let key_name = args.name.unwrap_or_else(|| {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("capsule-key-{}", timestamp)
    });

    let keys_dir = get_keys_directory()?;
    fs::create_dir_all(&keys_dir)
        .with_context(|| format!("Failed to create keys directory: {:?}", keys_dir))?;

    let mut csprng = OsRng;
    let signing_key = SigningKey::generate(&mut csprng);
    let verifying_key: VerifyingKey = (&signing_key).into();

    let secret_key_path = keys_dir.join(format!("{}.secret", key_name));
    let secret_bytes = signing_key.to_bytes();
    fs::write(&secret_key_path, secret_bytes)
        .with_context(|| format!("Failed to write secret key: {:?}", secret_key_path))?;

    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&secret_key_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&secret_key_path, perms)
            .with_context(|| format!("Failed to set permissions on: {:?}", secret_key_path))?;
    }

    let public_key_path = keys_dir.join(format!("{}.public", key_name));
    let public_bytes = verifying_key.to_bytes();
    fs::write(&public_key_path, public_bytes)
        .with_context(|| format!("Failed to write public key: {:?}", public_key_path))?;

    let mut hasher = Sha256::new();
    hasher.update(public_bytes);
    let fingerprint = hasher.finalize();
    let fingerprint_hex: String = fingerprint.iter().map(|b| format!("{:02x}", b)).collect();

    println!("✅ Key generated successfully!");
    println!();
    println!("Key name:      {}", key_name);
    println!("Private key:   {}", secret_key_path.display());
    println!("Public key:    {}", public_key_path.display());
    println!();
    println!("Public key (hex):");
    println!("{}", hex::encode(public_bytes));
    println!();
    println!("Fingerprint (SHA256):");
    println!("{}", fingerprint_hex);
    println!();
    println!("⚠️  Keep your private key secure!");

    Ok(())
}

fn get_keys_directory() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().context("Failed to determine home directory")?;
    Ok(home_dir.join(".capsule").join("keys"))
}
