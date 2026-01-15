use anyhow::{Context, Result};
use jsonschema::{Draft, JSONSchema};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const CONFIG_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigJson {
    pub version: String,
    pub services: HashMap<String, ServiceSpec>,
    pub sandbox: SandboxConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub executable: String,
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<UserConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signals: Option<SignalsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheck>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<HashMap<String, u16>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalsConfig {
    pub stop: String,
    pub kill: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_connect: Option<String>,
    pub port: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filesystem: Option<FilesystemConfig>,
    pub network: NetworkConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub development_mode: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_write: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_domains: Option<Vec<String>>,
    pub enforcement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<EgressConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressConfig {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<EgressRuleEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressRuleEntry {
    #[serde(rename = "type")]
    pub rule_type: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_manifest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub umask: Option<u16>,
}

type CommandResolution = (
    String,
    Vec<String>,
    Option<HashMap<String, String>>,
    Option<SignalsConfig>,
);

#[derive(Debug, Clone, Deserialize)]
struct ManifestService {
    entrypoint: String,
    #[serde(default)]
    depends_on: Option<Vec<String>>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    readiness_probe: Option<ManifestReadinessProbe>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestReadinessProbe {
    #[serde(default)]
    http_get: Option<String>,
    #[serde(default)]
    tcp_connect: Option<String>,
    port: String,
}

pub fn generate_and_write_config(
    manifest_path: &Path,
    enforcement_override: Option<String>,
) -> Result<PathBuf> {
    let manifest_content = std::fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;

    let manifest: toml::Value = toml::from_str(&manifest_content)
        .with_context(|| format!("Failed to parse manifest TOML: {}", manifest_path.display()))?;

    let config = build_config_json(&manifest, &manifest_content, enforcement_override)?;
    validate_config_json(&config)?;

    let manifest_dir = manifest_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let output_path = manifest_dir.join("config.json");

    let json = serde_json::to_string_pretty(&config).context("Failed to serialize config.json")?;
    std::fs::write(&output_path, json)
        .with_context(|| format!("Failed to write config.json: {}", output_path.display()))?;

    Ok(output_path)
}

fn build_config_json(
    manifest: &toml::Value,
    manifest_raw: &str,
    enforcement_override: Option<String>,
) -> Result<ConfigJson> {
    let mut services = HashMap::new();
    let entrypoint = read_entrypoint(manifest)?;
    let (executable, args, env, signals) = resolve_command(&entrypoint, manifest);
    let main_spec = ServiceSpec {
        executable,
        args,
        cwd: Some("source".to_string()),
        env,
        user: None,
        signals,
        depends_on: None,
        health_check: read_health_check(manifest),
        ports: None,
    };
    services.insert("main".to_string(), main_spec);

    let manifest_services = manifest
        .get("services")
        .and_then(|s| s.as_table())
        .map(|tbl| {
            tbl.iter()
                .filter_map(|(k, v)| {
                    let svc: Option<ManifestService> = v.clone().try_into().ok();
                    svc.map(|s| (k.to_string(), s))
                })
                .collect::<HashMap<String, ManifestService>>()
        })
        .unwrap_or_default();

    if !manifest_services.is_empty() {
        for (name, svc) in &manifest_services {
            if name == "main" {
                anyhow::bail!(
                    "services.main conflicts with execution entrypoint; remove services.main or execution"
                );
            }

            let (executable, args, env, signals) = resolve_command(&svc.entrypoint, manifest);
            let health_check = svc.readiness_probe.as_ref().map(|p| HealthCheck {
                http_get: p.http_get.clone(),
                tcp_connect: p.tcp_connect.clone(),
                port: p.port.clone(),
                interval_secs: None,
                timeout_secs: None,
            });

            let spec = ServiceSpec {
                executable,
                args,
                cwd: Some("source".to_string()),
                env: merge_envs(env, svc.env.clone()),
                user: None,
                signals,
                depends_on: svc.depends_on.clone(),
                health_check,
                ports: None,
            };
            services.insert(name.clone(), spec);
        }
    }

    validate_services_dag(&services)?;

    let (allow_domains, egress_rules) = build_egress(manifest)?;

    let sandbox = SandboxConfig {
        enabled: true,
        filesystem: read_filesystem(manifest),
        network: NetworkConfig {
            enabled: true,
            allow_domains,
            enforcement: enforcement_override.unwrap_or_else(|| "best_effort".to_string()),
            egress: egress_rules,
        },
        development_mode: None,
    };

    let metadata = MetadataConfig {
        name: manifest
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        version: manifest
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        generated_at: None,
        generated_by: Some(format!("capsule-cli v{}", env!("CARGO_PKG_VERSION"))),
        source_manifest: Some(format!("sha256:{}", sha256_hex(manifest_raw.as_bytes()))),
    };

    Ok(ConfigJson {
        version: CONFIG_VERSION.to_string(),
        services,
        sandbox,
        metadata: Some(metadata),
        annotations: None,
    })
}

fn validate_config_json(config: &ConfigJson) -> Result<()> {
    let schema_json: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../schema/config-schema.json"
    )))
    .context("Failed to parse config schema")?;
    let schema_json = Box::leak(Box::new(schema_json));

    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(schema_json)
        .context("Failed to compile config schema")?;

    let instance = serde_json::to_value(config).context("Failed to convert config to JSON")?;
    if let Err(errors) = compiled.validate(&instance) {
        let details: Vec<String> = errors.map(|e| e.to_string()).collect();
        anyhow::bail!(
            "config.json schema validation failed: {}",
            details.join("; ")
        );
    }

    Ok(())
}

fn read_entrypoint(manifest: &toml::Value) -> Result<String> {
    let entrypoint = manifest
        .get("targets")
        .and_then(|t| t.get("source"))
        .and_then(|s| s.get("entrypoint"))
        .and_then(|e| e.as_str())
        .or_else(|| {
            manifest.get("execution").and_then(|e| {
                e.get("release")
                    .and_then(|r| r.get("entrypoint"))
                    .or_else(|| e.get("release").and_then(|r| r.get("command")))
                    .or_else(|| e.get("entrypoint"))
                    .or_else(|| e.get("command"))
                    .and_then(|v| v.as_str())
            })
        })
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("No entrypoint defined in capsule.toml"))?;

    Ok(entrypoint.to_string())
}

fn read_health_check(manifest: &toml::Value) -> Option<HealthCheck> {
    let execution = manifest.get("execution")?;
    let http_get = execution
        .get("health_check")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let port = execution
        .get("port")
        .and_then(|v| v.as_integer())
        .map(|p| p.to_string());

    if http_get.is_none() || port.is_none() {
        return None;
    }

    Some(HealthCheck {
        http_get,
        tcp_connect: None,
        port: port?,
        interval_secs: None,
        timeout_secs: None,
    })
}

fn read_filesystem(manifest: &toml::Value) -> Option<FilesystemConfig> {
    let fs = manifest
        .get("sandbox")
        .and_then(|s| s.get("filesystem"))
        .or_else(|| {
            manifest
                .get("isolation")
                .and_then(|i| i.get("sandbox"))
                .and_then(|s| s.get("filesystem"))
        })?;

    let read_only = fs.get("read_only").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<String>>()
    });

    let read_write = fs.get("read_write").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<String>>()
    });

    if read_only.is_none() && read_write.is_none() {
        return None;
    }

    Some(FilesystemConfig {
        read_only,
        read_write,
    })
}

fn build_egress(manifest: &toml::Value) -> Result<(Option<Vec<String>>, Option<EgressConfig>)> {
    let mut allow_domains: Vec<String> = manifest
        .get("network")
        .and_then(|n| n.get("egress_allow"))
        .or_else(|| manifest.get("sandbox").and_then(|s| s.get("egress_allow")))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();
    allow_domains.sort();
    allow_domains.dedup();
    let allow_domains = if allow_domains.is_empty() {
        None
    } else {
        Some(allow_domains)
    };

    let mut rules: Vec<EgressRuleEntry> = Vec::new();
    let mut seen_ips: HashSet<String> = HashSet::new();
    let mut seen_cidrs: HashSet<String> = HashSet::new();

    if let Some(id_allow) = manifest
        .get("network")
        .and_then(|n| n.get("egress_id_allow"))
        .and_then(|v| v.as_array())
    {
        for rule in id_allow {
            let rule_type = rule.get("type").and_then(|v| v.as_str());
            let value = rule.get("value").and_then(|v| v.as_str());
            match (rule_type, value) {
                (Some("ip"), Some(val)) => {
                    if seen_ips.insert(val.to_string()) {
                        rules.push(EgressRuleEntry {
                            rule_type: "ip".to_string(),
                            value: val.to_string(),
                        });
                    }
                }
                (Some("cidr"), Some(val)) => {
                    if seen_cidrs.insert(val.to_string()) {
                        rules.push(EgressRuleEntry {
                            rule_type: "cidr".to_string(),
                            value: val.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
    }

    if rules.is_empty() && allow_domains.is_none() {
        return Ok((None, None));
    }

    let egress = EgressConfig {
        mode: "allowlist".to_string(),
        rules: if rules.is_empty() { None } else { Some(rules) },
    };

    Ok((allow_domains, Some(egress)))
}

fn resolve_command(entrypoint: &str, manifest: &toml::Value) -> CommandResolution {
    let tokens = shell_words::split(entrypoint).unwrap_or_else(|_| vec![entrypoint.to_string()]);
    let program = tokens
        .first()
        .cloned()
        .unwrap_or_else(|| entrypoint.to_string());

    let language = read_language(manifest)
        .or_else(|| detect_language_from_program(&program))
        .or_else(|| detect_language_from_entrypoint(entrypoint));

    let mut env = HashMap::new();
    let execution_env = manifest
        .get("execution")
        .and_then(|e| e.get("env"))
        .and_then(|e| e.as_table())
        .map(|tbl| {
            tbl.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.to_string(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    env.extend(execution_env);

    let (executable, mut args) = if let Some(lang) = language.as_deref() {
        match lang {
            "python" => {
                env.insert("PYTHONHOME".to_string(), "runtime/python".to_string());
                env.insert("PYTHONPATH".to_string(), "source".to_string());
                let mut args = tokens.get(1..).unwrap_or(&[]).to_vec();
                if tokens.len() <= 1 {
                    args = vec![program];
                }
                ("runtime/python/bin/python3".to_string(), args)
            }
            "node" => {
                let mut args = tokens.get(1..).unwrap_or(&[]).to_vec();
                if tokens.len() <= 1 {
                    args = vec![program];
                }
                ("runtime/node/bin/node".to_string(), args)
            }
            "deno" => {
                let mut args = tokens.get(1..).unwrap_or(&[]).to_vec();
                if tokens.len() <= 1 {
                    args = vec![program];
                }
                ("runtime/deno/bin/deno".to_string(), args)
            }
            "bun" => {
                let mut args = tokens.get(1..).unwrap_or(&[]).to_vec();
                if tokens.len() <= 1 {
                    args = vec![program];
                }
                ("runtime/bun/bin/bun".to_string(), args)
            }
            _ => (
                normalize_program(&program),
                tokens.get(1..).unwrap_or(&[]).to_vec(),
            ),
        }
    } else {
        (
            normalize_program(&program),
            tokens.get(1..).unwrap_or(&[]).to_vec(),
        )
    };

    if !args.is_empty() {
        args = args.into_iter().map(|a| normalize_arg(&a)).collect();
    }

    let signals = manifest
        .get("execution")
        .and_then(|e| e.get("signals"))
        .and_then(|s| s.as_table())
        .map(|tbl| SignalsConfig {
            stop: tbl
                .get("stop")
                .and_then(|v| v.as_str())
                .unwrap_or("SIGTERM")
                .to_string(),
            kill: tbl
                .get("kill")
                .and_then(|v| v.as_str())
                .unwrap_or("SIGKILL")
                .to_string(),
        });

    (
        executable,
        args,
        if env.is_empty() { None } else { Some(env) },
        signals,
    )
}

fn read_language(manifest: &toml::Value) -> Option<String> {
    manifest
        .get("targets")
        .and_then(|t| t.get("source"))
        .and_then(|s| s.get("language"))
        .and_then(|l| l.as_str())
        .map(normalize_language)
}

fn detect_language_from_program(program: &str) -> Option<String> {
    match program {
        "python" | "python3" => Some("python".to_string()),
        "node" | "nodejs" => Some("node".to_string()),
        "deno" => Some("deno".to_string()),
        "bun" => Some("bun".to_string()),
        _ => None,
    }
}

fn detect_language_from_entrypoint(entrypoint: &str) -> Option<String> {
    let lower = entrypoint.to_ascii_lowercase();
    if lower.ends_with(".py") {
        return Some("python".to_string());
    }
    if lower.ends_with(".js") || lower.ends_with(".mjs") || lower.ends_with(".cjs") {
        return Some("node".to_string());
    }
    if lower.ends_with(".ts") || lower.ends_with(".tsx") {
        return Some("bun".to_string());
    }
    None
}

fn normalize_language(lang: &str) -> String {
    match lang.trim().to_ascii_lowercase().as_str() {
        "python3" => "python".to_string(),
        "nodejs" => "node".to_string(),
        other => other.to_string(),
    }
}

fn normalize_program(program: &str) -> String {
    let p = program.trim();
    if p.is_empty() {
        return program.to_string();
    }

    if p.starts_with('/') || p.starts_with("runtime/") || p.starts_with("source/") {
        return p.to_string();
    }

    if p.starts_with("./") {
        return format!("source/{}", p.trim_start_matches("./"));
    }

    if p.contains('/') || p.contains('.') {
        return format!("source/{p}");
    }

    p.to_string()
}

fn normalize_arg(arg: &str) -> String {
    let a = arg.trim();
    if a.is_empty() || a.starts_with('-') {
        return arg.to_string();
    }

    if a.starts_with("source/") || a.starts_with("runtime/") || a.starts_with('/') {
        return a.to_string();
    }

    if a.starts_with("./") {
        return format!("source/{}", a.trim_start_matches("./"));
    }

    if a.contains('/')
        || a.ends_with(".py")
        || a.ends_with(".js")
        || a.ends_with(".mjs")
        || a.ends_with(".cjs")
        || a.ends_with(".ts")
        || a.ends_with(".tsx")
        || a.ends_with(".wasm")
    {
        return format!("source/{a}");
    }

    a.to_string()
}

fn merge_envs(
    base: Option<HashMap<String, String>>,
    extra: Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let mut out = base.unwrap_or_default();
    if let Some(extra) = extra {
        out.extend(extra);
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn validate_services_dag(services: &HashMap<String, ServiceSpec>) -> Result<()> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut visiting: HashSet<String> = HashSet::new();

    fn visit(
        name: &str,
        services: &HashMap<String, ServiceSpec>,
        visited: &mut HashSet<String>,
        visiting: &mut HashSet<String>,
        stack: &mut Vec<String>,
    ) -> Result<()> {
        if visited.contains(name) {
            return Ok(());
        }
        if visiting.contains(name) {
            stack.push(name.to_string());
            anyhow::bail!("Circular dependency detected: {}", stack.join(" -> "));
        }

        let spec = services
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Unknown service '{}' (depends_on)", name))?;

        visiting.insert(name.to_string());
        stack.push(name.to_string());

        if let Some(deps) = &spec.depends_on {
            for dep in deps {
                if !services.contains_key(dep) {
                    anyhow::bail!("Service '{}' depends on unknown service '{}'", name, dep);
                }
                visit(dep, services, visited, visiting, stack)?;
            }
        }

        stack.pop();
        visiting.remove(name);
        visited.insert(name.to_string());
        Ok(())
    }

    let mut names: Vec<&String> = services.keys().collect();
    names.sort();
    for name in names {
        let mut stack = Vec::new();
        visit(name, services, &mut visited, &mut visiting, &mut stack)?;
    }

    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn generates_valid_config_json() {
        let tmp = tempdir().unwrap();
        let manifest_path = tmp.path().join("capsule.toml");

        let manifest = r#"
name = "demo"
version = "0.1.0"

[execution]
runtime = "source"
entrypoint = "main.py"

[execution.env]
MODEL = "demo"

[network]
egress_allow = ["1.1.1.1"]
"#;

        std::fs::write(&manifest_path, manifest).unwrap();

        let config_path = generate_and_write_config(&manifest_path, None).unwrap();
        let config_raw = std::fs::read_to_string(config_path).unwrap();
        let config: serde_json::Value = serde_json::from_str(&config_raw).unwrap();

        assert_eq!(config["version"], CONFIG_VERSION);
        assert!(config["services"].get("main").is_some());
        assert!(config["sandbox"]["network"]["egress"].is_object());
    }
}
