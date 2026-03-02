use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(any(unix, windows))]
use std::process::Command;
use std::time::SystemTime;

const RUN_DIR: &str = ".capsule/run";
const PID_FILE_EXT: &str = ".pid";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub id: String,
    pub name: String,
    pub pid: i32,
    pub status: ProcessStatus,
    pub runtime: String,
    pub start_time: SystemTime,
    pub manifest_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessStatus {
    Running,
    Stopped,
    Unknown,
}

impl std::fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessStatus::Running => write!(f, "running"),
            ProcessStatus::Stopped => write!(f, "stopped"),
            ProcessStatus::Unknown => write!(f, "unknown"),
        }
    }
}

pub struct ProcessManager {
    run_dir: PathBuf,
}

impl ProcessManager {
    pub fn new() -> Result<Self> {
        let run_dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?
            .join(RUN_DIR);

        if !run_dir.exists() {
            fs::create_dir_all(&run_dir).with_context(|| {
                format!("Failed to create run directory: {}", run_dir.display())
            })?;
        }

        Ok(Self { run_dir })
    }

    pub fn get_run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn pid_file_path(&self, id: &str) -> PathBuf {
        self.run_dir.join(format!("{}{}", id, PID_FILE_EXT))
    }

    pub fn write_pid(&self, info: &ProcessInfo) -> Result<PathBuf> {
        let pid_path = self.pid_file_path(&info.id);
        let content = toml::to_string(info).with_context(|| "Failed to serialize process info")?;
        fs::write(&pid_path, content)
            .with_context(|| format!("Failed to write PID file: {}", pid_path.display()))?;
        Ok(pid_path)
    }

    pub fn read_pid(&self, id: &str) -> Result<ProcessInfo> {
        let pid_path = self.pid_file_path(id);
        let content = fs::read_to_string(&pid_path)
            .with_context(|| format!("Failed to read PID file: {}", pid_path.display()))?;
        toml::from_str(&content)
            .with_context(|| format!("Failed to parse PID file: {}", pid_path.display()))
    }

    pub fn delete_pid(&self, id: &str) -> Result<()> {
        let pid_path = self.pid_file_path(id);
        if pid_path.exists() {
            fs::remove_file(&pid_path)
                .with_context(|| format!("Failed to remove PID file: {}", pid_path.display()))?;
        }
        Ok(())
    }

    pub fn list_processes(&self) -> Result<Vec<ProcessInfo>> {
        let mut processes = Vec::new();

        if !self.run_dir.exists() {
            return Ok(processes);
        }

        for entry in fs::read_dir(&self.run_dir)
            .with_context(|| format!("Failed to read run directory: {}", self.run_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();

            if path
                .extension()
                .map_or(false, |ext| ext == PID_FILE_EXT.trim_start_matches('.'))
            {
                if let Some(filename) = path.file_stem() {
                    if let Some(id) = filename.to_str() {
                        if let Ok(info) = self.read_pid(id) {
                            let updated_info = self.update_process_status(&info);
                            if updated_info.status == ProcessStatus::Stopped
                                && info.status != ProcessStatus::Stopped
                            {
                                let _ = self.write_pid(&updated_info);
                            }
                            processes.push(updated_info);
                        }
                    }
                }
            }
        }

        Ok(processes)
    }

    fn update_process_status(&self, info: &ProcessInfo) -> ProcessInfo {
        if info.status == ProcessStatus::Stopped {
            return info.clone();
        }

        let is_alive = is_process_alive(info.pid) && process_identity_matches(info);
        ProcessInfo {
            status: if is_alive {
                ProcessStatus::Running
            } else {
                ProcessStatus::Stopped
            },
            ..info.clone()
        }
    }

    pub fn find_by_name(&self, name: &str) -> Result<Vec<ProcessInfo>> {
        let all = self.list_processes()?;
        Ok(all
            .into_iter()
            .filter(|p| p.name.to_lowercase() == name.to_lowercase())
            .collect())
    }

    pub fn stop_process(&self, id: &str, force: bool) -> Result<bool> {
        let info = match self.read_pid(id) {
            Ok(i) => i,
            Err(_) => return Ok(false),
        };

        if info.status == ProcessStatus::Stopped {
            return Ok(false);
        }

        if !is_process_alive(info.pid) {
            self.delete_pid(id)?;
            return Ok(false);
        }

        if !process_identity_matches(&info) {
            self.delete_pid(id)?;
            return Ok(false);
        }

        if terminate_process(info.pid, force)? {
            wait_for_process_exit(info.pid, 10)?;
            self.delete_pid(id)?;
            Ok(true)
        } else {
            self.delete_pid(id)?;
            Ok(false)
        }
    }

    pub fn cleanup_dead_processes(&self) -> Result<usize> {
        let mut cleaned = 0;
        for entry in fs::read_dir(&self.run_dir)
            .with_context(|| format!("Failed to read run directory: {}", self.run_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();

            if path
                .extension()
                .map_or(false, |ext| ext == PID_FILE_EXT.trim_start_matches('.'))
            {
                if let Some(filename) = path.file_stem() {
                    if let Some(id) = filename.to_str() {
                        if let Ok(info) = self.read_pid(id) {
                            if info.status == ProcessStatus::Stopped
                                || (info.status == ProcessStatus::Running
                                    && (!is_process_alive(info.pid)
                                        || !process_identity_matches(&info)))
                            {
                                let _ = fs::remove_file(&path);
                                cleaned += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(cleaned)
    }
}

fn is_process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }

    #[cfg(unix)]
    unsafe {
        let result = libc::kill(pid as i32, 0);
        return result == 0 || errno() != libc::ESRCH;
    }

    #[cfg(windows)]
    {
        let output = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output();

        let Ok(output) = output else {
            return false;
        };
        if !output.status.success() {
            return false;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let pid_marker = format!(",\"{}\",", pid);
        return stdout.contains(&pid_marker) || stdout.contains(&format!(",\"{}\"", pid));
    }

    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

#[cfg(unix)]
fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn process_identity_matches(info: &ProcessInfo) -> bool {
    runtime_identity_matches(&info.runtime, read_process_commandline(info.pid).as_deref())
}

fn runtime_identity_matches(runtime: &str, commandline: Option<&str>) -> bool {
    if !runtime.eq_ignore_ascii_case("nacelle") {
        return true;
    }

    let Some(commandline) = commandline else {
        return false;
    };

    is_expected_nacelle_commandline(commandline)
}

fn is_expected_nacelle_commandline(commandline: &str) -> bool {
    let normalized = commandline.to_ascii_lowercase();
    normalized.contains("nacelle") || normalized.contains("capsule open")
}

fn read_process_commandline(pid: i32) -> Option<String> {
    if pid <= 0 {
        return None;
    }

    #[cfg(target_os = "linux")]
    {
        let proc_path = format!("/proc/{pid}/cmdline");
        if let Ok(raw) = fs::read(proc_path) {
            if !raw.is_empty() {
                let mut out = String::new();
                for byte in raw {
                    if byte == 0 {
                        out.push(' ');
                    } else {
                        out.push(byte as char);
                    }
                }
                let trimmed = out.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }

    #[cfg(unix)]
    {
        let output = Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "command="])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let cmd = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if cmd.is_empty() {
            None
        } else {
            Some(cmd)
        }
    }

    #[cfg(windows)]
    {
        let output = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout.lines().next()?.trim();
        if line.is_empty() || line.starts_with("INFO:") {
            return None;
        }
        let image = line
            .split(',')
            .next()
            .map(|v| v.trim_matches('"'))
            .unwrap_or("")
            .trim();
        if image.is_empty() {
            None
        } else {
            Some(image.to_string())
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

fn wait_for_process_exit(pid: i32, timeout_secs: u64) -> Result<()> {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if !is_process_alive(pid) {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    anyhow::bail!(
        "Process {} did not exit within {} seconds",
        pid,
        timeout_secs
    )
}

fn terminate_process(pid: i32, force: bool) -> Result<bool> {
    if pid <= 0 {
        return Ok(false);
    }

    #[cfg(unix)]
    {
        let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
        let result = unsafe { libc::kill(pid, signal) };
        if result == 0 {
            return Ok(true);
        }

        let err = errno();
        if err == libc::ESRCH {
            Ok(false)
        } else {
            Err(anyhow::anyhow!("Failed to send signal to process {}", pid))
        }
    }

    #[cfg(windows)]
    {
        let mut command = Command::new("taskkill");
        command.arg("/PID").arg(pid.to_string());
        if force {
            command.arg("/F");
        }
        let status = command
            .status()
            .with_context(|| format!("Failed to execute taskkill for PID {}", pid))?;

        if status.success() {
            return Ok(true);
        }

        if !is_process_alive(pid) {
            Ok(false)
        } else {
            Err(anyhow::anyhow!("Failed to terminate process {}", pid))
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = force;
        Err(anyhow::anyhow!(
            "Process termination is not supported on this platform"
        ))
    }
}

pub fn get_process_uptime(start_time: SystemTime) -> Result<std::time::Duration> {
    let now = SystemTime::now();
    now.duration_since(start_time)
        .with_context(|| "Process start time is in the future")
}

pub fn format_duration(duration: std::time::Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

impl Default for ProcessManager {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| {
            let run_dir = dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(RUN_DIR);
            Self { run_dir }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_status_display() {
        assert_eq!(ProcessStatus::Running.to_string(), "running");
        assert_eq!(ProcessStatus::Stopped.to_string(), "stopped");
        assert_eq!(ProcessStatus::Unknown.to_string(), "unknown");
    }

    #[test]
    fn test_format_duration() {
        let one_hour = std::time::Duration::from_secs(3661);
        assert_eq!(format_duration(one_hour), "1h 1m 1s");

        let thirty_min = std::time::Duration::from_secs(1800);
        assert_eq!(format_duration(thirty_min), "30m 0s");

        let forty_five_sec = std::time::Duration::from_secs(45);
        assert_eq!(format_duration(forty_five_sec), "45s");

        let zero_sec = std::time::Duration::from_secs(0);
        assert_eq!(format_duration(zero_sec), "0s");
    }

    #[test]
    fn test_process_info_serialization() {
        let info = ProcessInfo {
            id: "test-123".to_string(),
            name: "my-capsule".to_string(),
            pid: 12345,
            status: ProcessStatus::Running,
            runtime: "nacelle".to_string(),
            start_time: SystemTime::UNIX_EPOCH,
            manifest_path: Some(PathBuf::from("/path/to/capsule.toml")),
        };

        let serialized = toml::to_string(&info).expect("Failed to serialize");
        let deserialized: ProcessInfo = toml::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(info.id, deserialized.id);
        assert_eq!(info.name, deserialized.name);
        assert_eq!(info.pid, deserialized.pid);
        assert_eq!(info.status, deserialized.status);
        assert_eq!(info.runtime, deserialized.runtime);
        assert_eq!(info.manifest_path, deserialized.manifest_path);
    }

    #[test]
    fn test_process_info_without_manifest() {
        let info = ProcessInfo {
            id: "test-456".to_string(),
            name: "another-capsule".to_string(),
            pid: 67890,
            status: ProcessStatus::Stopped,
            runtime: "nacelle".to_string(),
            start_time: SystemTime::UNIX_EPOCH,
            manifest_path: None,
        };

        let serialized = toml::to_string(&info).expect("Failed to serialize");
        let deserialized: ProcessInfo = toml::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(info.id, deserialized.id);
        assert!(deserialized.manifest_path.is_none());
    }

    #[test]
    fn test_pid_file_extension() {
        assert_eq!(PID_FILE_EXT, ".pid");
    }

    #[test]
    fn nacelle_identity_matches_expected_commandline() {
        assert!(runtime_identity_matches(
            "nacelle",
            Some("/usr/local/bin/nacelle run ...")
        ));
        assert!(runtime_identity_matches(
            "nacelle",
            Some("/usr/bin/ato capsule open ./sample")
        ));
        assert!(!runtime_identity_matches(
            "nacelle",
            Some("/usr/sbin/launchd")
        ));
    }

    #[test]
    fn nacelle_identity_fails_closed_when_commandline_missing() {
        assert!(!runtime_identity_matches("nacelle", None));
    }

    #[test]
    fn non_nacelle_runtime_skips_strict_identity_check() {
        assert!(runtime_identity_matches("host", None));
        assert!(runtime_identity_matches(
            "host",
            Some("/usr/bin/python app.py")
        ));
    }
}
