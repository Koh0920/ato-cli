use anyhow::Result;
use std::sync::Arc;

use crate::process_manager::{format_duration, get_process_uptime, ProcessManager, ProcessStatus};
use crate::reporters::CliReporter;
use capsule_core::CapsuleReporter;

pub struct PsArgs {
    pub json: bool,
    pub all: bool,
}

pub fn execute(args: PsArgs, reporter: Arc<CliReporter>) -> Result<()> {
    futures::executor::block_on(reporter.notify("📋 Listing running capsules...".to_string()))?;

    let pm = ProcessManager::new()?;
    pm.cleanup_dead_processes()?;
    let mut processes = pm.list_processes()?;

    if !args.all {
        processes.retain(|p| p.status == ProcessStatus::Running);
    }

    if processes.is_empty() {
        futures::executor::block_on(reporter.notify("No capsules found.".to_string()))?;
        return Ok(());
    }

    if args.json {
        let json_output: Vec<serde_json::Value> = processes
            .iter()
            .map(|p| {
                let uptime = get_process_uptime(p.start_time)
                    .map(format_duration)
                    .unwrap_or_else(|_| "unknown".to_string());

                serde_json::json!({
                    "id": p.id,
                    "name": p.name,
                    "pid": p.pid,
                    "status": p.status.to_string(),
                    "runtime": p.runtime,
                    "uptime": uptime,
                    "manifest": p.manifest_path.as_ref().map(|m| m.display().to_string())
                })
            })
            .collect();

        let output = serde_json::to_string_pretty(&json_output)?;
        futures::executor::block_on(reporter.notify(output))?;
    } else {
        futures::executor::block_on(reporter.notify("-".repeat(100)))?;
        futures::executor::block_on(reporter.notify(format!(
            "{:>8} {:>8} {:>12} {:>15} {:>20} {}",
            "PID", "ID", "NAME", "STATUS", "RUNTIME", "UPTIME"
        )))?;
        futures::executor::block_on(reporter.notify("-".repeat(100)))?;
        futures::executor::block_on(reporter.notify(format!(
            "{:>8} {:>8} {:>12} {:>15} {:>20} {}",
            "PID", "ID", "NAME", "STATUS", "RUNTIME", "UPTIME"
        )))?;
        futures::executor::block_on(reporter.notify("-".repeat(100)))?;

        for p in &processes {
            let uptime = get_process_uptime(p.start_time)
                .map(format_duration)
                .unwrap_or_else(|_| "unknown".to_string());

            let status_str = match p.status {
                ProcessStatus::Running => "🟢 running",
                ProcessStatus::Stopped => "🔴 stopped",
                ProcessStatus::Unknown => "🟡 unknown",
            };

            let name = if p.name.len() > 12 {
                &p.name[..12]
            } else {
                &p.name
            };

            let id = if p.id.len() > 8 { &p.id[..8] } else { &p.id };

            futures::executor::block_on(reporter.notify(format!(
                "{:>8} {:>8} {:>12} {:>15} {:>20} {}",
                p.pid, id, name, status_str, p.runtime, uptime
            )))?;
        }

        futures::executor::block_on(reporter.notify("-".repeat(100)))?;
        futures::executor::block_on(
            reporter.notify(format!("Total: {} capsule(s)", processes.len())),
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ps_args_default() {
        let args = PsArgs {
            json: false,
            all: false,
        };
        assert!(!args.json);
        assert!(!args.all);
    }

    #[test]
    fn test_ps_args_json() {
        let args = PsArgs {
            json: true,
            all: false,
        };
        assert!(args.json);
        assert!(!args.all);
    }

    #[test]
    fn test_ps_args_all() {
        let args = PsArgs {
            json: false,
            all: true,
        };
        assert!(!args.json);
        assert!(args.all);
    }
}
