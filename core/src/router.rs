use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::debug;

use crate::manifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Oci,
    Wasm,
    Source,
    Web,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProfile {
    Dev,
    Release,
}

#[derive(Debug, Clone)]
pub struct ManifestData {
    pub manifest: toml::Value,
    pub manifest_path: PathBuf,
    pub manifest_dir: PathBuf,
    pub profile: ExecutionProfile,
    pub selected_target: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeDecision {
    pub kind: RuntimeKind,
    pub reason: String,
    pub plan: ManifestData,
}

pub fn route_manifest(
    manifest_path: &Path,
    profile: ExecutionProfile,
    target_label: Option<&str>,
) -> Result<RuntimeDecision> {
    let loaded = manifest::load_manifest(manifest_path)?;
    let manifest = loaded.raw;
    let manifest_dir = loaded.dir.clone();
    let selected_target = resolve_target_label(&manifest, target_label)?;

    let plan = ManifestData {
        manifest,
        manifest_path: loaded.path,
        manifest_dir,
        profile,
        selected_target,
    };

    let runtime = plan.execution_runtime().ok_or_else(|| {
        anyhow!(
            "Target '{}' is missing required field: runtime",
            plan.selected_target
        )
    })?;

    let chosen = parse_runtime_kind(&runtime).ok_or_else(|| {
        anyhow!(
            "Unsupported runtime '{}' for target '{}'",
            runtime,
            plan.selected_target
        )
    })?;

    let reason = format!(
        "targets.{}.runtime={}",
        plan.selected_target,
        runtime.to_ascii_lowercase()
    );

    debug!(
        "RuntimeRouter: chosen={:?}, reason={}, target={}",
        chosen, reason, plan.selected_target
    );

    Ok(RuntimeDecision {
        kind: chosen,
        reason,
        plan,
    })
}

impl ManifestData {
    pub fn execution_entrypoint(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "entrypoint"])
    }

    pub fn execution_runtime(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "runtime"])
    }

    pub fn execution_driver(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "driver"])
    }

    pub fn execution_language(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "language"])
    }

    pub fn execution_image(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "image"])
    }

    pub fn execution_env(&self) -> HashMap<String, String> {
        self.get_table(&["targets", &self.selected_target, "env"])
            .map(table_to_map)
            .unwrap_or_default()
    }

    pub fn manifest_name(&self) -> Option<String> {
        self.get_str(&["name"])
    }

    pub fn manifest_version(&self) -> Option<String> {
        self.get_str(&["version"])
    }

    pub fn execution_port(&self) -> Option<u16> {
        self.get_value(&["targets", &self.selected_target, "port"])
            .or_else(|| self.get_value(&["port"]))
            .and_then(|v| v.as_integer())
            .and_then(|v| u16::try_from(v).ok())
    }

    pub fn execution_working_dir(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "working_dir"])
    }

    pub fn execution_preference(&self) -> Option<Vec<RuntimeKind>> {
        let pref = self.get_array(&["targets", "preference"])?;

        let mut out = Vec::new();
        for value in pref {
            if let Some(name) = value.as_str() {
                if let Some(kind) = parse_runtime_kind(name) {
                    out.push(kind);
                }
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    pub fn targets_oci_image(&self) -> Option<String> {
        let runtime = self.execution_runtime()?;
        if !runtime.eq_ignore_ascii_case("oci") {
            return None;
        }
        self.get_str(&["targets", &self.selected_target, "image"])
            .or_else(|| self.execution_entrypoint())
    }

    pub fn targets_oci_cmd(&self) -> Vec<String> {
        self.get_array(&["targets", &self.selected_target, "cmd"])
            .map(|a| array_to_vec(a))
            .unwrap_or_default()
    }

    pub fn targets_oci_env(&self) -> HashMap<String, String> {
        self.get_table(&["targets", &self.selected_target, "env"])
            .map(table_to_map)
            .unwrap_or_default()
    }

    pub fn targets_oci_working_dir(&self) -> Option<String> {
        self.get_str(&["targets", &self.selected_target, "working_dir"])
    }

    pub fn targets_wasm_component(&self) -> Option<String> {
        let runtime = self.execution_runtime()?;
        if !runtime.eq_ignore_ascii_case("wasm") {
            return None;
        }
        self.get_str(&["targets", &self.selected_target, "component"])
            .or_else(|| self.get_str(&["targets", &self.selected_target, "path"]))
            .or_else(|| self.execution_entrypoint())
    }

    pub fn targets_wasm_args(&self) -> Vec<String> {
        self.get_array(&["targets", &self.selected_target, "args"])
            .or_else(|| self.get_array(&["targets", &self.selected_target, "cmd"]))
            .map(|a| array_to_vec(a))
            .unwrap_or_default()
    }

    pub fn targets_web_public(&self) -> Vec<String> {
        self.get_array(&["targets", &self.selected_target, "public"])
            .map(|a| array_to_vec(a))
            .unwrap_or_default()
    }

    pub fn selected_target_label(&self) -> &str {
        &self.selected_target
    }

    pub fn build_gpu(&self) -> bool {
        self.get_value(&["build", "gpu"])
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn build_context(&self) -> Option<String> {
        self.get_str(&["build", "context"])
    }

    pub fn build_dockerfile(&self) -> Option<String> {
        self.get_str(&["build", "dockerfile"])
    }

    pub fn build_image(&self) -> Option<String> {
        self.get_str(&["build", "image"])
    }

    pub fn build_tag(&self) -> Option<String> {
        self.get_str(&["build", "tag"])
    }

    pub fn build_target(&self) -> Option<String> {
        self.get_str(&["build", "target"])
    }

    #[allow(dead_code)]
    pub fn requirements_vram_min(&self) -> Option<String> {
        self.get_str(&["requirements", "vram_min"])
    }

    pub fn resolve_path(&self, raw: &str) -> PathBuf {
        let p = PathBuf::from(raw);
        if p.is_absolute() {
            p
        } else {
            self.manifest_dir.join(p)
        }
    }

    fn get_value<'a>(&'a self, path: &[&str]) -> Option<&'a toml::Value> {
        let mut current = &self.manifest;
        for key in path {
            let table = current.as_table()?;
            current = table.get(*key)?;
        }
        Some(current)
    }

    fn get_table<'a>(&'a self, path: &[&str]) -> Option<&'a toml::value::Table> {
        self.get_value(path)?.as_table()
    }

    fn get_array<'a>(&'a self, path: &[&str]) -> Option<&'a Vec<toml::Value>> {
        self.get_value(path)?.as_array()
    }

    fn get_str(&self, path: &[&str]) -> Option<String> {
        self.get_value(path)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }
}

fn parse_runtime_kind(value: &str) -> Option<RuntimeKind> {
    match value.to_ascii_lowercase().as_str() {
        "oci" | "docker" | "youki" | "runc" => Some(RuntimeKind::Oci),
        "wasm" => Some(RuntimeKind::Wasm),
        "source" | "native" => Some(RuntimeKind::Source),
        "web" => Some(RuntimeKind::Web),
        _ => None,
    }
}

fn resolve_target_label(manifest: &toml::Value, target_label: Option<&str>) -> Result<String> {
    let targets = manifest
        .get("targets")
        .and_then(|v| v.as_table())
        .ok_or_else(|| anyhow!("Missing required [targets] table"))?;

    let default_target = manifest
        .get("default_target")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("Missing required field: default_target"))?;

    let selected = target_label
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(default_target);

    if !targets.contains_key(selected) {
        return Err(anyhow!("Target '{}' not found under [targets]", selected));
    }

    Ok(selected.to_string())
}

fn table_to_map(table: &toml::value::Table) -> HashMap<String, String> {
    table
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

fn array_to_vec(values: &[toml::Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect()
}
