use std::time::{Duration, Instant};

use crate::runtime_router::RuntimeKind;

pub struct RunMetrics {
    runtime: RuntimeKind,
    started_at: Instant,
}

pub struct RunReport {
    pub runtime: RuntimeKind,
    pub duration: Duration,
    pub exit_code: i32,
}

impl RunMetrics {
    pub fn start(runtime: RuntimeKind) -> Self {
        Self {
            runtime,
            started_at: Instant::now(),
        }
    }

    pub fn finish(self, exit_code: i32) -> RunReport {
        RunReport {
            runtime: self.runtime,
            duration: self.started_at.elapsed(),
            exit_code,
        }
    }
}

impl RunReport {
    pub fn print(&self) {
        println!(
            "📈 Metrics: runtime={:?}, exit_code={}, duration_ms={}",
            self.runtime,
            self.exit_code,
            self.duration.as_millis()
        );
    }
}
