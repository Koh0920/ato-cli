use std::path::Path;

use crate::error::CapsuleError;

pub fn validate_manifest_for_build(
    manifest_path: &Path,
    target_label: &str,
) -> Result<(), CapsuleError> {
    let raw_text = std::fs::read_to_string(manifest_path).map_err(CapsuleError::Io)?;
    let raw: toml::Value = toml::from_str(&raw_text)
        .map_err(|e| manifest_err(manifest_path, format!("Failed to parse manifest TOML: {e}")))?;
    let manifest_dir = manifest_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let target = raw
        .get("targets")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get(target_label))
        .and_then(|v| v.as_table())
        .ok_or_else(|| manifest_err(manifest_path, format!("targets.{target_label} is missing")))?;

    let entrypoint = target
        .get("entrypoint")
        .and_then(|v| v.as_str())
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!("targets.{target_label}.entrypoint is required"),
            )
        })?;

    let clean_entrypoint = entrypoint.trim_start_matches("./");
    if clean_entrypoint.contains('/') || clean_entrypoint.contains('\\') {
        let path_in_root = manifest_dir.join(clean_entrypoint);
        let path_in_source = manifest_dir.join("source").join(clean_entrypoint);
        if !path_in_root.exists() && !path_in_source.exists() {
            return Err(manifest_err(
                manifest_path,
                format!(
                    "entrypoint not found: targets.{target_label}.entrypoint='{}'. Checked '{}' and '{}'",
                    entrypoint,
                    path_in_root.display(),
                    path_in_source.display()
                ),
            ));
        }
    }

    if let Some(port_raw) = target.get("port") {
        let port = port_raw.as_integer().ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!("targets.{target_label}.port must be an integer"),
            )
        })?;
        if !(1..=65535).contains(&port) {
            return Err(manifest_err(
                manifest_path,
                format!("targets.{target_label}.port must be between 1 and 65535"),
            ));
        }
    }

    if let Some(smoke) = target.get("smoke") {
        let smoke = smoke.as_table().ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!("targets.{target_label}.smoke must be a table"),
            )
        })?;

        if let Some(timeout) = smoke.get("startup_timeout_ms") {
            let timeout = timeout.as_integer().ok_or_else(|| {
                manifest_err(
                    manifest_path,
                    format!("targets.{target_label}.smoke.startup_timeout_ms must be an integer"),
                )
            })?;
            if timeout <= 0 {
                return Err(manifest_err(
                    manifest_path,
                    format!(
                        "targets.{target_label}.smoke.startup_timeout_ms must be greater than 0"
                    ),
                ));
            }
        }

        if let Some(commands) = smoke.get("check_commands") {
            let commands = commands.as_array().ok_or_else(|| {
                manifest_err(
                    manifest_path,
                    format!("targets.{target_label}.smoke.check_commands must be an array"),
                )
            })?;
            for (idx, cmd) in commands.iter().enumerate() {
                let cmd = cmd.as_str().ok_or_else(|| {
                    manifest_err(
                        manifest_path,
                        format!(
                            "targets.{target_label}.smoke.check_commands[{idx}] must be a string"
                        ),
                    )
                })?;
                if cmd.trim().is_empty() {
                    return Err(manifest_err(
                        manifest_path,
                        format!(
                            "targets.{target_label}.smoke.check_commands[{idx}] must not be empty"
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn manifest_err(path: &Path, message: String) -> CapsuleError {
    CapsuleError::Manifest(path.to_path_buf(), message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_smoke_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "x"
version = "0.1.0"
default_target = "cli"

[targets.cli]
runtime = "source"
entrypoint = "main.py"

[targets.cli.smoke]
startup_timeout_ms = 0
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("main.py"), "print('ok')").unwrap();

        assert!(validate_manifest_for_build(&manifest_path, "cli").is_err());
    }

    #[test]
    fn accepts_valid_smoke_block() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "x"
version = "0.1.0"
default_target = "cli"

[targets.cli]
runtime = "source"
entrypoint = "main.py"

[targets.cli.smoke]
startup_timeout_ms = 1500
check_commands = ["python -V"]
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("main.py"), "print('ok')").unwrap();

        assert!(validate_manifest_for_build(&manifest_path, "cli").is_ok());
    }
}
