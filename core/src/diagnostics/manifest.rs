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
    validate_pack_config(manifest_path, &raw)?;

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

    let runtime = target
        .get("runtime")
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let driver = target
        .get("driver")
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_ascii_lowercase());

    let clean_entrypoint = entrypoint.trim_start_matches("./");
    if runtime == "web" {
        let driver = driver.ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!(
                    "targets.{target_label}.driver is required for runtime=web (static|node|deno|python)"
                ),
            )
        })?;
        if matches!(driver.as_str(), "browser_static" | "browser-static") {
            return Err(manifest_err(
                manifest_path,
                format!(
                    "targets.{target_label}.driver='{}' is not supported. Use 'static'",
                    driver
                ),
            ));
        }
        if !matches!(driver.as_str(), "static" | "node" | "deno" | "python") {
            return Err(manifest_err(
                manifest_path,
                format!(
                    "targets.{target_label}.driver='{}' is invalid for runtime=web (allowed: static|node|deno|python)",
                    driver
                ),
            ));
        }
        if target.get("public").is_some() {
            return Err(manifest_err(
                manifest_path,
                format!("targets.{target_label}.public is no longer supported"),
            ));
        }

        if !is_safe_relative_path(clean_entrypoint) {
            return Err(manifest_err(
                manifest_path,
                format!(
                    "targets.{target_label}.entrypoint='{}' must be a safe relative path",
                    entrypoint
                ),
            ));
        }

        let port_raw = target.get("port").ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!("targets.{target_label}.port is required for runtime=web"),
            )
        })?;
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

        let path_in_root = manifest_dir.join(clean_entrypoint);
        let path_in_source = manifest_dir.join("source").join(clean_entrypoint);
        match driver.as_str() {
            "static" => {
                if !path_in_root.exists() || !path_in_root.is_dir() {
                    return Err(manifest_err(
                        manifest_path,
                        format!(
                            "targets.{target_label}.entrypoint='{}' must be an existing directory under project root ('{}')",
                            entrypoint,
                            path_in_root.display()
                        ),
                    ));
                }
            }
            "node" | "deno" | "python" => {
                if entrypoint.split_whitespace().count() > 1 {
                    return Err(manifest_err(
                        manifest_path,
                        format!(
                            "targets.{target_label}.entrypoint must be a script file path (shell command strings are not allowed)"
                        ),
                    ));
                }
                if (!path_in_root.exists() || !path_in_root.is_file())
                    && (!path_in_source.exists() || !path_in_source.is_file())
                {
                    return Err(manifest_err(
                        manifest_path,
                        format!(
                            "entrypoint file not found: targets.{target_label}.entrypoint='{}'. Checked '{}' and '{}'",
                            entrypoint,
                            path_in_root.display(),
                            path_in_source.display()
                        ),
                    ));
                }

                if let Some(runtime_tools) = target.get("runtime_tools") {
                    let tools_table = runtime_tools.as_table().ok_or_else(|| {
                        manifest_err(
                            manifest_path,
                            format!("targets.{target_label}.runtime_tools must be a table"),
                        )
                    })?;
                    for (tool, version) in tools_table {
                        let version = version.as_str().map(str::trim).ok_or_else(|| {
                            manifest_err(
                                manifest_path,
                                format!(
                                    "targets.{target_label}.runtime_tools.{tool} must be a non-empty string"
                                ),
                            )
                        })?;
                        if version.is_empty() {
                            return Err(manifest_err(
                                manifest_path,
                                format!(
                                    "targets.{target_label}.runtime_tools.{tool} must be a non-empty string"
                                ),
                            ));
                        }
                    }
                }

                // Deno orchestrator targets require explicit runtime pins and tool pins.
                // We detect orchestrator intent by conventional entrypoint name.
                if driver == "deno"
                    && std::path::Path::new(clean_entrypoint)
                        .file_name()
                        .and_then(|v| v.to_str())
                        .map(|v| v.eq_ignore_ascii_case("ato-entry.ts"))
                        .unwrap_or(false)
                {
                    let runtime_version_ok = target
                        .get("runtime_version")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .is_some();
                    if !runtime_version_ok {
                        return Err(manifest_err(
                            manifest_path,
                            format!(
                                "targets.{target_label}.runtime_version is required for deno orchestrator targets"
                            ),
                        ));
                    }

                    let tools_table = target
                        .get("runtime_tools")
                        .and_then(|v| v.as_table())
                        .ok_or_else(|| {
                            manifest_err(
                                manifest_path,
                                format!(
                                    "targets.{target_label}.runtime_tools is required for deno orchestrator targets"
                                ),
                            )
                        })?;
                    for required_tool in ["node", "python"] {
                        let ok = tools_table
                            .get(required_tool)
                            .and_then(|v| v.as_str())
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .is_some();
                        if !ok {
                            return Err(manifest_err(
                                manifest_path,
                                format!(
                                    "targets.{target_label}.runtime_tools.{required_tool} is required for deno orchestrator targets"
                                ),
                            ));
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    } else {
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

fn is_safe_relative_path(path: &str) -> bool {
    use std::path::Component;
    !Path::new(path).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    })
}

fn validate_pack_config(manifest_path: &Path, raw: &toml::Value) -> Result<(), CapsuleError> {
    let Some(pack) = raw.get("pack") else {
        return Ok(());
    };
    let pack = pack
        .as_table()
        .ok_or_else(|| manifest_err(manifest_path, "pack must be a table".to_string()))?;

    for field in ["include", "exclude"] {
        let Some(value) = pack.get(field) else {
            continue;
        };
        let arr = value.as_array().ok_or_else(|| {
            manifest_err(
                manifest_path,
                format!("pack.{field} must be an array of strings"),
            )
        })?;

        for (idx, pattern) in arr.iter().enumerate() {
            let pattern = pattern.as_str().ok_or_else(|| {
                manifest_err(
                    manifest_path,
                    format!("pack.{field}[{idx}] must be a non-empty string"),
                )
            })?;
            if pattern.trim().is_empty() {
                return Err(manifest_err(
                    manifest_path,
                    format!("pack.{field}[{idx}] must be a non-empty string"),
                ));
            }
        }
    }

    Ok(())
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

    #[test]
    fn web_static_requires_existing_directory_entrypoint() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "web-static"
version = "0.1.0"
default_target = "static"

[targets.static]
runtime = "web"
driver = "static"
entrypoint = "dist"
port = 8080
"#,
        )
        .unwrap();

        let err = validate_manifest_for_build(&manifest_path, "static").unwrap_err();
        assert!(err
            .to_string()
            .contains("must be an existing directory under project root"));

        std::fs::create_dir_all(dir.path().join("dist")).unwrap();
        assert!(validate_manifest_for_build(&manifest_path, "static").is_ok());
    }

    #[test]
    fn web_dynamic_rejects_shell_style_entrypoint() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "web-node"
version = "0.1.0"
default_target = "web"

[targets.web]
runtime = "web"
driver = "node"
entrypoint = "npm run start"
port = 3000
"#,
        )
        .unwrap();

        let err = validate_manifest_for_build(&manifest_path, "web").unwrap_err();
        assert!(err
            .to_string()
            .contains("entrypoint must be a script file path"));
    }

    #[test]
    fn rejects_empty_pack_patterns() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("capsule.toml");
        std::fs::write(
            &manifest_path,
            r#"
schema_version = "0.2"
name = "pack-test"
version = "0.1.0"
default_target = "web"

[pack]
include = ["", "apps/**"]

[targets.web]
runtime = "web"
driver = "deno"
entrypoint = "ato-entry.ts"
port = 4173
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("ato-entry.ts"), "console.log('ok');").unwrap();

        let err = validate_manifest_for_build(&manifest_path, "web").unwrap_err();
        assert!(err.to_string().contains("pack.include[0]"));
    }
}
