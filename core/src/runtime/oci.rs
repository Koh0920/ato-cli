use async_trait::async_trait;
use bollard::container::{StatsOptions, StopContainerOptions, WaitContainerOptions};
use bollard::errors::Error as BollardError;
use bollard::Docker;
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{CapsuleError, Result};
use crate::metrics::{MetricsSession, ResourceStats, RuntimeMetadata, UnifiedMetrics};
use crate::runtime::{Measurable, RuntimeHandle};

/// OCI(Docker/Podman) 実行のメトリクスハンドル。
pub struct OciHandle {
    session: MetricsSession,
    container_id: String,
    image_hash: String,
    docker: Docker,
    last_resources: Arc<Mutex<ResourceStats>>,
}

impl OciHandle {
    pub fn new(
        session_id: impl Into<String>,
        container_id: impl Into<String>,
        image_hash: impl Into<String>,
        docker: Docker,
    ) -> Self {
        let session = MetricsSession::new(session_id);
        let container_id = container_id.into();
        let image_hash = image_hash.into();
        let last_resources = Arc::new(Mutex::new(ResourceStats::default()));

        Self::spawn_stats_worker(
            docker.clone(),
            session.clone(),
            container_id.clone(),
            Arc::clone(&last_resources),
        );

        Self {
            session,
            container_id,
            image_hash,
            docker,
            last_resources,
        }
    }

    fn metadata(&self, exit_code: Option<i32>) -> RuntimeMetadata {
        RuntimeMetadata::Oci {
            container_id: self.container_id.clone(),
            image_hash: self.image_hash.clone(),
            exit_code,
        }
    }

    pub async fn finalize_from_cache(&self, exit_code: Option<i32>) -> UnifiedMetrics {
        let mut resources = self.last_resources.lock().await.clone();
        resources.duration_ms = self.session.elapsed_ms();
        self.session.finalize(resources, self.metadata(exit_code))
    }

    fn spawn_stats_worker(
        docker: Docker,
        session: MetricsSession,
        container_id: String,
        last_resources: Arc<Mutex<ResourceStats>>,
    ) {
        let _ = tokio::spawn(async move {
            let mut attempts = 0usize;
            loop {
                let mut got_sample = false;
                let mut stats_stream = docker.stats(
                    &container_id,
                    Some(StatsOptions {
                        stream: true,
                        one_shot: false,
                    }),
                );

                while let Some(next) = stats_stream.next().await {
                    let stats = match next {
                        Ok(value) => value,
                        Err(_) => break,
                    };

                    got_sample = true;

                    let mut resources = last_resources.lock().await;
                    resources.duration_ms = session.elapsed_ms();

                    if let Some(cpu_seconds) = extract_cpu_seconds(&stats) {
                        resources.cpu_seconds = cpu_seconds;
                    }

                    if let Some(mem_bytes) = extract_memory_bytes(&stats) {
                        resources.peak_memory_bytes = mem_bytes;
                    }
                }

                if got_sample {
                    break;
                }

                attempts += 1;
                if attempts >= 20 {
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }
}

impl RuntimeHandle for OciHandle {
    fn id(&self) -> &str {
        &self.container_id
    }

    fn kill(&mut self) -> Result<()> {
        let docker = self.docker.clone();
        let container_id = self.container_id.clone();
        let runtime =
            tokio::runtime::Runtime::new().map_err(|err| CapsuleError::Runtime(err.to_string()))?;

        runtime.block_on(async move {
            docker
                .stop_container(&container_id, Some(StopContainerOptions { t: 0 }))
                .await
                .map_err(map_bollard_error)
        })
    }
}

#[async_trait]
impl Measurable for OciHandle {
    async fn capture_metrics(&self) -> Result<UnifiedMetrics> {
        let mut resources = self.last_resources.lock().await.clone();
        if resources.duration_ms == 0 {
            resources.duration_ms = self.session.elapsed_ms();
        }
        Ok(self.session.snapshot(resources, self.metadata(None)))
    }

    async fn wait_and_finalize(&self) -> Result<UnifiedMetrics> {
        let mut wait_stream = self
            .docker
            .wait_container(&self.container_id, None::<WaitContainerOptions<String>>);
        let exit_code = match wait_stream.next().await {
            Some(Ok(response)) => Some(response.status_code as i32),
            Some(Err(BollardError::DockerContainerWaitError { code, .. })) => Some(code as i32),
            Some(Err(err)) => return Err(map_bollard_error(err)),
            None => None,
        };

        let mut resources = self.last_resources.lock().await.clone();
        resources.duration_ms = self.session.elapsed_ms();
        Ok(self.session.finalize(resources, self.metadata(exit_code)))
    }
}

fn map_bollard_error(err: BollardError) -> CapsuleError {
    let message = err.to_string();
    if is_engine_unavailable(&message) {
        return CapsuleError::ContainerEngine(message);
    }
    CapsuleError::Runtime(message)
}

fn is_engine_unavailable(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("cannot connect")
        || msg.contains("connection refused")
        || msg.contains("is the docker daemon running")
        || msg.contains("no such file or directory")
        || msg.contains("connection error")
        || msg.contains("timed out")
}

fn extract_cpu_seconds(stats: &bollard::container::Stats) -> Option<f64> {
    let total_usage = stats.cpu_stats.cpu_usage.total_usage;
    Some(total_usage as f64 / 1_000_000_000.0)
}

fn extract_memory_bytes(stats: &bollard::container::Stats) -> Option<u64> {
    let mem = &stats.memory_stats;
    if let Some(max_usage) = mem.max_usage {
        return Some(max_usage);
    }
    if let Some(usage) = mem.usage {
        return Some(usage);
    }
    None
}
