use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::debug;

use crate::manifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Oci,
    Wasm,
    Source,
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
}

#[derive(Debug, Clone)]
pub struct RuntimeDecision {
    pub kind: RuntimeKind,
    pub reason: String,
    pub plan: ManifestData,
}

pub fn route_manifest(manifest_path: &Path, profile: ExecutionProfile) -> Result<RuntimeDecision> {
    let loaded = manifest::load_manifest(manifest_path)?;
    let manifest = loaded.raw;
    let manifest_dir = loaded.dir.clone();

    let plan = ManifestData {
        manifest,
        manifest_path: loaded.path,
        manifest_dir,
        profile,
    };

    let entrypoint = plan.execution_entrypoint();
    let runtime = plan.execution_runtime();
    let image = plan.targets_oci_image().or_else(|| plan.execution_image());
    let has_wasm_target = plan.get_value(&["targets", "wasm"]).is_some();
    let has_source_target = plan.get_value(&["targets", "source"]).is_some();
    let entrypoint_wasm = entrypoint.as_deref().is_some_and(is_wasm_path);

    let runtime_is_oci = runtime.as_deref().is_some_and(is_oci_runtime);
    let runtime_is_wasm = runtime.as_deref().is_some_and(is_wasm_runtime);
    let runtime_is_source = runtime.as_deref().is_some_and(is_source_runtime);

    let mut candidates = Vec::new();
    if image.is_some() || runtime_is_oci {
        candidates.push(RuntimeKind::Oci);
    }
    if has_wasm_target || runtime_is_wasm || entrypoint_wasm {
        candidates.push(RuntimeKind::Wasm);
    }
    if has_source_target || candidates.is_empty() || runtime_is_source {
        candidates.push(RuntimeKind::Source);
    }

    let default_order = [RuntimeKind::Oci, RuntimeKind::Wasm, RuntimeKind::Source];
    let explicit_runtime = if runtime_is_oci {
        Some(RuntimeKind::Oci)
    } else if runtime_is_wasm {
        Some(RuntimeKind::Wasm)
    } else if runtime_is_source {
        Some(RuntimeKind::Source)
    } else {
        None
    };

    let preference = plan.execution_preference();
    let chosen = if let Some(explicit) = explicit_runtime {
        explicit
    } else if let Some(pref) = preference {
        pref.into_iter()
            .find(|k| candidates.contains(k))
            .or_else(|| {
                default_order
                    .iter()
                    .copied()
                    .find(|k| candidates.contains(k))
            })
            .unwrap_or_else(|| candidates[0])
    } else {
        default_order
            .iter()
            .copied()
            .find(|k| candidates.contains(k))
            .unwrap_or_else(|| candidates[0])
    };

    let reason = match chosen {
        RuntimeKind::Oci => {
            if image.is_some() {
                "targets.oci.image or execution.image detected".to_string()
            } else if runtime_is_oci {
                "execution.runtime=oci".to_string()
            } else {
                "OCI candidate selected".to_string()
            }
        }
        RuntimeKind::Wasm => {
            if has_wasm_target {
                "targets.wasm detected".to_string()
            } else if entrypoint_wasm {
                "execution.entrypoint ends with .wasm/.component".to_string()
            } else {
                "execution.runtime=wasm".to_string()
            }
        }
        RuntimeKind::Source => {
            if runtime_is_source {
                "execution.runtime=source".to_string()
            } else if has_source_target {
                "targets.source detected".to_string()
            } else {
                "default to source runtime".to_string()
            }
        }
    };

    debug!(
        "RuntimeRouter: chosen={:?}, reason={}, runtime={:?}",
        chosen, reason, runtime
    );

    Ok(RuntimeDecision {
        kind: chosen,
        reason,
        plan,
    })
}

impl ManifestData {
    pub fn execution_entrypoint(&self) -> Option<String> {
        let profile_key = match self.profile {
            ExecutionProfile::Dev => "dev",
            ExecutionProfile::Release => "release",
        };

        self.get_str(&["execution", profile_key, "entrypoint"])
            .or_else(|| self.get_str(&["execution", "entrypoint"]))
    }

    pub fn execution_runtime(&self) -> Option<String> {
        self.get_str(&["execution", "runtime"])
    }

    pub fn execution_image(&self) -> Option<String> {
        self.get_str(&["execution", "image"])
    }

    pub fn execution_env(&self) -> HashMap<String, String> {
        self.get_table(&["execution", "env"])
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
        self.get_value(&["execution", "port"])
            .and_then(|v| v.as_integer())
            .and_then(|v| u16::try_from(v).ok())
    }

    pub fn execution_working_dir(&self) -> Option<String> {
        self.get_str(&["execution", "working_dir"])
    }

    pub fn execution_preference(&self) -> Option<Vec<RuntimeKind>> {
        let pref = self
            .get_array(&["execution", "preference"])
            .or_else(|| self.get_array(&["targets", "preference"]))?;

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
        self.get_str(&["targets", "oci", "image"])
    }

    pub fn targets_oci_cmd(&self) -> Vec<String> {
        self.get_array(&["targets", "oci", "cmd"])
            .map(|a| array_to_vec(a))
            .unwrap_or_default()
    }

    pub fn targets_oci_env(&self) -> HashMap<String, String> {
        self.get_table(&["targets", "oci", "env"])
            .map(table_to_map)
            .unwrap_or_default()
    }

    pub fn targets_oci_working_dir(&self) -> Option<String> {
        self.get_str(&["targets", "oci", "working_dir"])
    }

    pub fn targets_wasm_component(&self) -> Option<String> {
        self.get_str(&["targets", "wasm", "component"])
            .or_else(|| self.get_str(&["targets", "wasm", "path"]))
    }

    pub fn targets_wasm_args(&self) -> Vec<String> {
        self.get_array(&["targets", "wasm", "args"])
            .map(|a| array_to_vec(a))
            .unwrap_or_default()
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
        _ => None,
    }
}

fn is_oci_runtime(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "oci" | "docker" | "youki" | "runc"
    )
}

fn is_wasm_runtime(value: &str) -> bool {
    value.eq_ignore_ascii_case("wasm")
}

fn is_source_runtime(value: &str) -> bool {
    value.eq_ignore_ascii_case("source") || value.eq_ignore_ascii_case("native")
}

fn is_wasm_path(value: &str) -> bool {
    value.ends_with(".wasm") || value.ends_with(".component")
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
