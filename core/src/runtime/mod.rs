use std::fmt;
use std::io;

use crate::metrics::UnifiedMetrics;
use async_trait::async_trait;

pub mod native;
pub mod oci;
pub mod wasm;

#[derive(Debug, Clone)]
pub struct MetricsError {
    message: String,
}

impl MetricsError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn unsupported(feature: &str) -> Self {
        Self::new(format!("{feature} is not implemented"))
    }
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for MetricsError {}

impl From<io::Error> for MetricsError {
    fn from(err: io::Error) -> Self {
        Self::new(err.to_string())
    }
}

pub type MetricsResult<T> = Result<T, MetricsError>;

#[async_trait]
pub trait Measurable {
    async fn capture_metrics(&self) -> MetricsResult<UnifiedMetrics>;
    async fn wait_and_finalize(&self) -> MetricsResult<UnifiedMetrics>;
}

pub trait RuntimeHandle: Measurable + Send + Sync {
    fn id(&self) -> &str;
    fn kill(&mut self) -> MetricsResult<()>;
}
