use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CapsuleError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Manifest error in {0}: {1}")]
    Manifest(PathBuf, String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Process execution error: {0}")]
    Execution(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Authentication required: {0}")]
    AuthRequired(String),

    #[error("Build/Pack error: {0}")]
    Pack(String),

    #[error("Unknown error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, CapsuleError>;
