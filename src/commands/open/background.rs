use super::*;

pub(super) fn process_runtime_label(
    plan: &capsule_core::router::ManifestData,
    dangerous_skip_permissions: bool,
    compatibility_host_mode: CompatibilityHostMode,
) -> String {
    if matches!(compatibility_host_mode, CompatibilityHostMode::Enabled) {
        let runtime = plan
            .execution_runtime()
            .unwrap_or_else(|| "source".to_string());
        let driver = plan.execution_driver();
        return match driver {
            Some(driver) if !driver.trim().is_empty() => {
                format!("{}/{} [host-fallback]", runtime, driver)
            }
            _ => format!("{} [host-fallback]", runtime),
        };
    }
    if dangerous_skip_permissions {
        return "host".to_string();
    }
    "nacelle".to_string()
}

pub(super) fn background_ready_message(
    id: &str,
    compatibility_host_mode: CompatibilityHostMode,
) -> String {
    if matches!(compatibility_host_mode, CompatibilityHostMode::Enabled) {
        return format!("✔ Capsule is ready (Host Fallback, ID: {id})");
    }
    format!("🚀 Capsule started in background and is ready (ID: {id})")
}

pub(super) fn background_timeout_message(
    id: &str,
    compatibility_host_mode: CompatibilityHostMode,
) -> String {
    if matches!(compatibility_host_mode, CompatibilityHostMode::Enabled) {
        return format!(
            "⏳ Capsule is still starting in compatibility mode (Host Fallback, ID: {}). Use `ato ps --all` to inspect readiness.",
            id
        );
    }
    format!(
        "⏳ Capsule is still starting in background (ID: {}). Use `ato ps --all` to inspect readiness.",
        id
    )
}

pub(super) fn background_failure_prefix(
    id: &str,
    compatibility_host_mode: CompatibilityHostMode,
) -> String {
    if matches!(compatibility_host_mode, CompatibilityHostMode::Enabled) {
        return format!(
            "Background capsule failed before readiness in compatibility mode (Host Fallback, ID: {id})"
        );
    }
    format!("Background capsule failed before readiness (ID: {id})")
}

pub(super) fn spawn_foreground_native_event_reporter(
    reporter: Arc<CliReporter>,
    event_rx: Option<Receiver<LifecycleEvent>>,
    sandbox_initialized: bool,
    ipc_socket_mapped: bool,
    progress: Option<ProgressBar>,
) -> Result<Option<JoinHandle<()>>> {
    let Some(event_rx) = event_rx else {
        return Ok(None);
    };

    for message in initial_foreground_native_messages(sandbox_initialized, ipc_socket_mapped) {
        if let Some(progress) = progress.as_ref() {
            progress.set_message(message);
        } else {
            futures::executor::block_on(CapsuleReporter::notify(&*reporter, message))?;
        }
    }

    Ok(Some(std::thread::spawn(move || {
        let mut ready_reported = false;
        for event in event_rx {
            for message in foreground_native_event_messages(&event, ready_reported) {
                match message {
                    ForegroundEventMessage::Notify(message) => {
                        if let Some(progress) = progress.as_ref() {
                            progress.set_message(message);
                        } else {
                            let _ = futures::executor::block_on(CapsuleReporter::notify(
                                &*reporter, message,
                            ));
                        }
                    }
                    ForegroundEventMessage::Warn(message) => {
                        if let Some(progress) = progress.as_ref() {
                            progress.set_message(message);
                        } else {
                            let _ = futures::executor::block_on(CapsuleReporter::warn(
                                &*reporter, message,
                            ));
                        }
                    }
                }
            }

            if matches!(event, LifecycleEvent::Ready { .. }) {
                ready_reported = true;
            }
        }
    })))
}

pub(super) fn wait_for_background_native_startup(
    process: &mut crate::executors::source::CapsuleProcess,
    process_manager: &crate::process_manager::ProcessManager,
    process_id: &str,
) -> Result<(BackgroundStartupOutcome, Option<Receiver<LifecycleEvent>>)> {
    let Some(event_rx) = process.event_rx.take() else {
        return Ok((BackgroundStartupOutcome::TimedOut, None));
    };
    let event_rx = Some(event_rx);

    let deadline = Instant::now() + background_ready_wait_timeout();

    loop {
        if let Some(status) = process.child.try_wait()? {
            let exit_code = status.code();
            let _ = process_manager.update_pid(process_id, |info| {
                info.exit_code = exit_code;
                info.last_event = Some("process_exited".to_string());
                if matches!(info.status, crate::process_manager::ProcessStatus::Starting) {
                    info.status = crate::process_manager::ProcessStatus::Failed;
                    if info.last_error.is_none() {
                        info.last_error = Some("process exited before readiness".to_string());
                    }
                } else if info.status.is_active() {
                    info.status = crate::process_manager::ProcessStatus::Exited;
                }
            });
            return Ok((BackgroundStartupOutcome::FailedBeforeReady, event_rx));
        }

        let now = Instant::now();
        if now >= deadline {
            let _ = process_manager.update_pid(process_id, |info| {
                info.last_event = Some("startup_timeout".to_string());
            });
            return Ok((BackgroundStartupOutcome::TimedOut, event_rx));
        }

        let wait_for = std::cmp::min(Duration::from_millis(100), deadline - now);
        match event_rx
            .as_ref()
            .expect("event receiver should still be present during startup wait")
            .recv_timeout(wait_for)
        {
            Ok(event) => {
                match persist_background_native_event(process_manager, process_id, &event)? {
                    BackgroundStartupOutcome::Ready => {
                        return Ok((BackgroundStartupOutcome::Ready, event_rx));
                    }
                    BackgroundStartupOutcome::FailedBeforeReady => {
                        return Ok((BackgroundStartupOutcome::FailedBeforeReady, event_rx));
                    }
                    BackgroundStartupOutcome::TimedOut => {}
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                let _ = process_manager.update_pid(process_id, |info| {
                    if matches!(info.status, crate::process_manager::ProcessStatus::Starting) {
                        info.status = crate::process_manager::ProcessStatus::Unknown;
                        info.last_error =
                            Some("event stream disconnected before readiness".to_string());
                    }
                });
                return Ok((BackgroundStartupOutcome::TimedOut, None));
            }
        }
    }
}

fn background_ready_wait_timeout() -> Duration {
    std::env::var(BACKGROUND_READY_WAIT_TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .filter(|duration| !duration.is_zero())
        .unwrap_or(BACKGROUND_READY_WAIT_TIMEOUT)
}

fn persist_background_native_event(
    process_manager: &crate::process_manager::ProcessManager,
    process_id: &str,
    event: &LifecycleEvent,
) -> Result<BackgroundStartupOutcome> {
    let now = SystemTime::now();
    let updated = process_manager.update_pid(process_id, |info| match event {
        LifecycleEvent::Ready { .. } => {
            info.status = crate::process_manager::ProcessStatus::Ready;
            info.ready_at = Some(now);
            info.last_event = Some("ready".to_string());
            info.last_error = None;
        }
        LifecycleEvent::Exited { service, exit_code } => {
            info.exit_code = *exit_code;
            info.last_event = Some("exited".to_string());
            if matches!(info.status, crate::process_manager::ProcessStatus::Starting) {
                info.status = crate::process_manager::ProcessStatus::Failed;
                info.last_error = Some(format!("service '{}' exited before readiness", service));
            } else if info.status.is_active() {
                info.status = crate::process_manager::ProcessStatus::Exited;
            }
        }
    })?;

    Ok(match updated.status {
        crate::process_manager::ProcessStatus::Ready => BackgroundStartupOutcome::Ready,
        crate::process_manager::ProcessStatus::Failed => {
            BackgroundStartupOutcome::FailedBeforeReady
        }
        _ => BackgroundStartupOutcome::TimedOut,
    })
}

pub(super) fn cleanup_process_artifacts(paths: &[PathBuf]) {
    for path in paths {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(super) async fn cleanup_existing_scoped_processes_before_run(
    scoped_id: &str,
    reporter: &Arc<CliReporter>,
) -> Result<()> {
    let process_manager = crate::process_manager::ProcessManager::new()?;
    let cleaned = process_manager.cleanup_scoped_processes(scoped_id, true)?;
    if cleaned > 0 {
        reporter
            .warn(format!(
                "🧹 Cleaned up {} existing process record(s) for {} before run",
                cleaned, scoped_id
            ))
            .await?;
    }
    Ok(())
}

pub(super) fn initial_foreground_native_messages(
    sandbox_initialized: bool,
    ipc_socket_mapped: bool,
) -> Vec<String> {
    let mut messages = Vec::new();
    if sandbox_initialized {
        messages.push("[✓] Sandbox initialized".to_string());
    }
    if ipc_socket_mapped {
        messages.push("[✓] IPC socket mapped".to_string());
    }
    messages
}

pub(super) fn foreground_native_event_messages(
    event: &LifecycleEvent,
    ready_reported: bool,
) -> Vec<ForegroundEventMessage> {
    match event {
        LifecycleEvent::Ready { service, .. } if !ready_reported => {
            let ready_message = if service == "main" {
                "[✓] Service is ready (ready event received)".to_string()
            } else {
                format!("[✓] Service '{service}' is ready (ready event received)")
            };
            vec![
                ForegroundEventMessage::Notify(ready_message),
                ForegroundEventMessage::Notify("    Streaming logs...".to_string()),
            ]
        }
        LifecycleEvent::Exited { service, exit_code } if !ready_reported => {
            let exit_code = exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            vec![ForegroundEventMessage::Warn(format!(
                "❌ Service '{service}' exited before readiness (exit code: {exit_code})"
            ))]
        }
        _ => Vec::new(),
    }
}

pub(super) async fn notify_web_endpoint(
    plan: &capsule_core::router::ManifestData,
    reporter: &Arc<CliReporter>,
) -> Result<()> {
    let port = runtime_overrides::override_port(plan.execution_port()).ok_or_else(|| {
        anyhow::anyhow!(
            "runtime=web target '{}' requires targets.<label>.port",
            plan.selected_target_label()
        )
    })?;

    reporter
        .notify(format!(
            "🌐 Web target '{}' is available at http://127.0.0.1:{}/",
            plan.selected_target_label(),
            port
        ))
        .await?;
    Ok(())
}
