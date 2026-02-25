//! `ato ipc` subcommand — IPC service management.
//!
//! ## Subcommands
//!
//! - `ato ipc status` — List running IPC services and their status.
//! - `ato ipc start`  — Start an IPC service from a capsule directory.
//! - `ato ipc stop`   — Stop a running IPC service by name.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};

use crate::ipc::broker::IpcBroker;
use crate::ipc::types::{ActivationMode, IpcRuntimeKind, IpcServiceInfo, IpcTransport};

/// Run `ato ipc status`.
///
/// Displays a table of all running IPC services with their:
/// - Name, sharing mode, reference count
/// - Transport, endpoint, runtime
/// - Uptime, PID
pub fn run_ipc_status(json_output: bool) -> Result<()> {
    // Create a broker pointing to the default socket directory
    let socket_dir = default_socket_dir();
    let broker = IpcBroker::new(socket_dir);

    let snapshot = broker.registry.status_snapshot();

    if json_output {
        let json = serde_json::to_string_pretty(&snapshot)?;
        println!("{}", json);
        return Ok(());
    }

    if snapshot.is_empty() {
        println!("No IPC services running.");
        println!();
        println!("Hint: Run a capsule with [ipc.exports] in its capsule.toml to start a service.");
        return Ok(());
    }

    // Table header
    println!(
        "{:<20} {:<12} {:<10} {:<10} {:<30} {:<8} {:<10}",
        "SERVICE", "MODE", "REFCOUNT", "TRANSPORT", "ENDPOINT", "RUNTIME", "UPTIME"
    );
    println!("{}", "-".repeat(100));

    for svc in &snapshot {
        let uptime = format_uptime(svc.uptime_secs);
        let mode = format!("{:?}", svc.mode).to_lowercase();

        println!(
            "{:<20} {:<12} {:<10} {:<10} {:<30} {:<8} {:<10}",
            svc.name,
            mode,
            svc.ref_count,
            svc.transport,
            truncate(&svc.endpoint, 28),
            format!("{}", svc.runtime),
            uptime,
        );
    }

    println!();
    println!("{} service(s) running.", snapshot.len());

    Ok(())
}

/// Run `ato ipc start`.
///
/// Starts an IPC service from a capsule directory. The capsule must have
/// `[ipc.exports]` configured in its `capsule.toml`.
pub fn run_ipc_start(path: PathBuf, json_output: bool) -> Result<()> {
    let capsule_root = if path.is_file() {
        path.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."))
    } else {
        path.clone()
    };

    let manifest_path = capsule_root.join("capsule.toml");
    if !manifest_path.exists() {
        anyhow::bail!(
            "capsule.toml not found at {}. Provide a capsule directory with [ipc.exports].",
            manifest_path.display()
        );
    }

    // Parse [ipc] section from capsule.toml
    let raw_text = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;
    let raw: toml::Value = toml::from_str(&raw_text)
        .with_context(|| format!("Failed to parse {}", manifest_path.display()))?;

    let ipc_config = parse_ipc_section(&raw)?;

    let service_name = ipc_config
        .as_ref()
        .and_then(|c| c.exports.as_ref())
        .and_then(|e| e.name.clone())
        .unwrap_or_else(|| {
            capsule_root
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unnamed")
                .to_string()
        });

    let socket_dir = default_socket_dir();
    let broker = IpcBroker::new(socket_dir);

    // Check if already running
    if broker.registry.lookup(&service_name).is_some() {
        if json_output {
            println!(
                "{}",
                serde_json::json!({"error": "already_running", "service": service_name})
            );
        } else {
            println!(
                "⚠️  Service '{}' is already running. Use `ato ipc stop --name {}` first.",
                service_name, service_name
            );
        }
        return Ok(());
    }

    // Register the service (actual process launch delegated to broker/executors)
    let socket_path = broker.socket_path(&service_name);
    let info = IpcServiceInfo {
        name: service_name.clone(),
        pid: None, // Will be set after process spawn
        endpoint: IpcTransport::UnixSocket(socket_path),
        capabilities: ipc_config
            .as_ref()
            .and_then(|c| c.exports.as_ref())
            .map(|e| e.methods.iter().map(|m| m.name.clone()).collect())
            .unwrap_or_default(),
        ref_count: 0,
        started_at: Some(Instant::now()),
        runtime_kind: IpcRuntimeKind::Source,
        sharing_mode: ipc_config
            .as_ref()
            .and_then(|c| c.exports.as_ref())
            .map(|e| e.sharing.mode)
            .unwrap_or_default(),
        activation: ActivationMode::Eager,
        capsule_root: capsule_root.clone(),
        port: None,
    };
    broker.registry.register(info);

    if json_output {
        println!(
            "{}",
            serde_json::json!({
                "status": "registered",
                "service": service_name,
                "capsule_root": capsule_root.display().to_string(),
            })
        );
    } else {
        println!("🚀 IPC service '{}' registered.", service_name);
        println!("   Capsule: {}", capsule_root.display());
        println!("   Note: Full process launch requires `ato open` with IPC integration.");
    }

    Ok(())
}

/// Run `ato ipc stop`.
///
/// Stops a running IPC service by name.
pub fn run_ipc_stop(name: String, force: bool, json_output: bool) -> Result<()> {
    let socket_dir = default_socket_dir();
    let broker = IpcBroker::new(socket_dir);

    let info = broker.registry.lookup(&name);
    if info.is_none() {
        if json_output {
            println!(
                "{}",
                serde_json::json!({"error": "not_found", "service": name})
            );
        } else {
            eprintln!("❌ Service '{}' is not running.", name);
            eprintln!("   Use `ato ipc status` to list running services.");
        }
        return Ok(());
    }

    let info = info.unwrap();

    // Send signal to process if it has a PID
    if let Some(pid) = info.pid {
        #[cfg(unix)]
        {
            let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
            let signal_name = if force { "SIGKILL" } else { "SIGTERM" };

            let ret = unsafe { libc::kill(pid as i32, signal) };
            if ret != 0 {
                let errno = std::io::Error::last_os_error();
                if json_output {
                    println!(
                        "{}",
                        serde_json::json!({
                            "warning": "signal_failed",
                            "service": name,
                            "pid": pid,
                            "error": errno.to_string(),
                        })
                    );
                } else {
                    eprintln!(
                        "⚠️  Failed to send {} to PID {}: {}",
                        signal_name, pid, errno
                    );
                }
            }
        }
    }

    // Remove from registry
    broker.registry.unregister(&name);

    // Revoke associated tokens
    broker.token_manager.revoke_all();

    // Clean up socket file
    let socket_path = broker.socket_path(&name);
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    if json_output {
        println!(
            "{}",
            serde_json::json!({"status": "stopped", "service": name})
        );
    } else {
        println!("⏹  Service '{}' stopped.", name);
    }

    Ok(())
}

/// Parse the `[ipc]` section from a raw TOML value.
fn parse_ipc_section(raw: &toml::Value) -> Result<Option<crate::ipc::types::IpcConfig>> {
    if let Some(ipc_table) = raw.get("ipc") {
        let ipc_str = toml::to_string(ipc_table).context("Failed to serialize [ipc] section")?;
        let config: crate::ipc::types::IpcConfig =
            toml::from_str(&ipc_str).context("Failed to parse [ipc] section")?;
        Ok(Some(config))
    } else {
        Ok(None)
    }
}

/// Format uptime seconds into a human-readable string.
fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Truncate a string to max length with ellipsis.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max.saturating_sub(3)])
    }
}

/// Default IPC socket directory.
fn default_socket_dir() -> PathBuf {
    std::env::temp_dir().join("capsule-ipc")
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uptime_seconds() {
        assert_eq!(format_uptime(0), "0s");
        assert_eq!(format_uptime(30), "30s");
    }

    #[test]
    fn test_format_uptime_minutes() {
        assert_eq!(format_uptime(90), "1m30s");
        assert_eq!(format_uptime(300), "5m0s");
    }

    #[test]
    fn test_format_uptime_hours() {
        assert_eq!(format_uptime(3661), "1h1m");
        assert_eq!(format_uptime(7200), "2h0m");
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long() {
        let long = "unix:///tmp/capsule-ipc/greeter-service.sock";
        let truncated = truncate(long, 20);
        assert!(truncated.len() <= 20);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn test_default_socket_dir() {
        let dir = default_socket_dir();
        assert!(dir.to_str().unwrap().contains("capsule-ipc"));
    }
}
