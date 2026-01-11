use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

pub fn discover_nacelle(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }

    if let Ok(env_path) = std::env::var("NACELLE_PATH") {
        if !env_path.trim().is_empty() {
            return Ok(PathBuf::from(env_path));
        }
    }

    which::which("nacelle").context("Failed to find nacelle engine (set NACELLE_PATH or add nacelle to PATH)")
}

pub fn run_internal(engine: &Path, subcommand: &str, payload: &Value) -> Result<Value> {
    let mut child = Command::new(engine)
        .arg("internal")
        .arg("--input")
        .arg("-")
        .arg(subcommand)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("Failed to spawn engine: {}", engine.display()))?;

    {
        let mut stdin = child.stdin.take().context("Failed to open stdin")?;
        let bytes = serde_json::to_vec(payload).context("Failed to serialize payload")?;
        stdin.write_all(&bytes).context("Failed to write payload")?;
    }

    let output = child
        .wait_with_output()
        .with_context(|| format!("Engine invocation failed: internal {subcommand}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if stdout.is_empty() {
        return Err(anyhow!(
            "Engine returned empty stdout (exit={})",
            output.status.code().unwrap_or(1)
        ));
    }

    let json: Value = serde_json::from_str(&stdout).with_context(|| {
        format!(
            "Failed to parse engine JSON output (exit={}): {}",
            output.status.code().unwrap_or(1),
            stdout
        )
    })?;

    // Engine may exit non-zero for workload exit codes; surface JSON either way.
    Ok(json)
}

/// Run an internal subcommand in streaming mode.
///
/// - stdin: JSON payload
/// - stdout/stderr: inherited (logs stream directly)
/// - returns: exit code of the engine process
pub fn run_internal_streaming(engine: &Path, subcommand: &str, payload: &Value) -> Result<i32> {
    let mut child = Command::new(engine)
        .arg("internal")
        .arg("--input")
        .arg("-")
        .arg(subcommand)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("Failed to spawn engine: {}", engine.display()))?;

    {
        let mut stdin = child.stdin.take().context("Failed to open stdin")?;
        let bytes = serde_json::to_vec(payload).context("Failed to serialize payload")?;
        stdin.write_all(&bytes).context("Failed to write payload")?;
    }

    let child_slot: Arc<Mutex<Option<std::process::Child>>> = Arc::new(Mutex::new(Some(child)));
    let child_slot_for_handler = Arc::clone(&child_slot);

    // Forward Ctrl-C to the engine process.
    // If Ctrl-C fires before we install the handler, the default behavior
    // (SIGINT to the process group) still applies.
    ctrlc::set_handler(move || {
        #[cfg(unix)]
        {
            if let Ok(mut guard) = child_slot_for_handler.lock() {
                if let Some(ref mut c) = *guard {
                    let _ = unsafe { libc::kill(c.id() as i32, libc::SIGINT) };
                }
            }
        }
    })
    .context("Failed to set Ctrl-C handler")?;

    let status = {
        let mut guard = child_slot.lock().expect("lock poisoned");
        let mut child = guard.take().expect("child missing");
        child
            .wait()
            .with_context(|| format!("Engine invocation failed: internal {subcommand}"))?
    };

    Ok(status.code().unwrap_or(1))
}
