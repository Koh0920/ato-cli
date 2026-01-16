#![allow(dead_code)]

use anyhow::{Context, Result};
use std::path::PathBuf;

/// Returns the root directory used by nacelle/capsule for per-user state.
///
/// We intentionally standardize on `~/.nacelle` for runtime caches.
pub fn nacelle_home_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().context("Failed to determine home directory")?;
    Ok(home.join(".nacelle"))
}

/// Returns the toolchain cache directory.
///
/// Layout: `~/.nacelle/toolchain`
pub fn toolchain_cache_dir() -> Result<PathBuf> {
    Ok(nacelle_home_dir()?.join("toolchain"))
}
