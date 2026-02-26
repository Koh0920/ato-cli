use std::time::{Duration, Instant};

use capsule_core::router::RuntimeKind;

use crate::reporters::CliReporter;
use capsule_core::CapsuleReporter;

pub struct RunMetrics {
    runtime: RuntimeKind,
    started_at: Instant,
    reporter: std::sync::Arc<CliReporter>,
}

pub struct RunReport {
    pub runtime: RuntimeKind,
    pub duration: Duration,
    pub exit_code: i32,
    reporter: std::sync::Arc<CliReporter>,
}

impl RunMetrics {
    pub fn start(runtime: RuntimeKind, reporter: std::sync::Arc<CliReporter>) -> Self {
        Self {
            runtime,
            started_at: Instant::now(),
            reporter,
        }
    }

    pub fn finish(self, exit_code: i32) -> RunReport {
        RunReport {
            runtime: self.runtime,
            duration: self.started_at.elapsed(),
            exit_code,
            reporter: self.reporter,
        }
    }
}

impl RunReport {
    pub fn print(&self) -> anyhow::Result<()> {
        futures::executor::block_on(self.reporter.notify(format!(
            "📈 Metrics: runtime={:?}, exit_code={}, duration_ms={}",
            self.runtime,
            self.exit_code,
            self.duration.as_millis()
        )))?;
        Ok(())
    }
}
