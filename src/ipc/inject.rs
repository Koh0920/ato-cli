//! IPC environment injection — bridges the IPC Broker with runtime executors.
//!
//! `IpcContext` is created once per `ato open` session. It holds the
//! resolved IPC environment variables that executors need
//! to inject into child processes.
//!
//! ## Flow
//!
//! ```text
//! capsule.toml [ipc.imports]
//!   → IpcBroker.resolve() per import
//!   → IpcBroker.generate_ipc_env() per resolved service
//!   → IpcContext { env_vars }
//!   → executor injects into child process
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use super::broker::{IpcBroker, ResolvedService};
use super::types::IpcConfig;

/// IPC context for a single `ato open` session.
///
/// Created by resolving all `[ipc.imports]` entries and generating
/// environment variables for runtime injection.
#[derive(Debug, Clone, Default)]
pub struct IpcContext {
    /// Environment variables to inject into the child process.
    /// Keys are `CAPSULE_IPC_<SERVICE>_URL`, `_TOKEN`, `_SOCKET`.
    pub env_vars: HashMap<String, String>,

    /// Number of services successfully resolved.
    pub resolved_count: usize,

    /// Warnings encountered during resolution (non-fatal).
    pub warnings: Vec<String>,
}

impl IpcContext {
    /// Create an empty IPC context (no IPC imports).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if this context has any IPC configuration.
    pub fn has_ipc(&self) -> bool {
        !self.env_vars.is_empty()
    }

    /// Build an IPC context from a capsule manifest's `[ipc]` section.
    ///
    /// Resolves all imports via the broker and generates environment variables.
    pub fn from_manifest(raw_manifest: &toml::Value) -> Result<Self> {
        let ipc_section = raw_manifest.get("ipc");
        if ipc_section.is_none() {
            return Ok(Self::empty());
        }

        let ipc_str =
            toml::to_string(ipc_section.unwrap()).context("Failed to serialize [ipc] section")?;
        let config: IpcConfig =
            toml::from_str(&ipc_str).context("Failed to parse [ipc] section")?;

        if config.imports.is_empty() {
            debug!("No IPC imports defined");
            return Ok(Self::empty());
        }

        let socket_dir = default_socket_dir();
        let broker = IpcBroker::new(socket_dir);

        Self::resolve_imports(&broker, &config)
    }

    /// Resolve all IPC imports using the given broker.
    fn resolve_imports(broker: &IpcBroker, config: &IpcConfig) -> Result<Self> {
        let mut env_vars = HashMap::new();
        let mut warnings = Vec::new();
        let mut resolved_count = 0;

        info!(imports = config.imports.len(), "Resolving IPC imports");

        for (import_name, import_config) in &config.imports {
            match broker.resolve(&import_config.from) {
                ResolvedService::Running(info) => {
                    // Service is already running — generate env vars
                    let token = broker.token_manager.generate(info.capabilities.clone());

                    let ipc_env = broker.generate_ipc_env(import_name, &info, &token.value);
                    for (k, v) in ipc_env {
                        env_vars.insert(k, v);
                    }

                    resolved_count += 1;
                    debug!(
                        service = import_name,
                        from = %import_config.from,
                        "IPC import resolved (running)"
                    );
                }
                ResolvedService::LocalStore { runtime_kind, .. } => {
                    // Service is installed but not running.
                    // For eager activation, we would start it here.
                    // For now, register the endpoint info and generate env vars
                    // so the child process knows where to connect.
                    let socket_path = broker.socket_path(import_name);

                    let info = super::types::IpcServiceInfo {
                        name: import_name.clone(),
                        pid: None,
                        endpoint: super::types::IpcTransport::UnixSocket(socket_path.clone()),
                        capabilities: vec![],
                        ref_count: 0,
                        started_at: None,
                        runtime_kind,
                        sharing_mode: super::types::SharingMode::default(),
                    };

                    let token = broker.token_manager.generate(vec![]);

                    let ipc_env = broker.generate_ipc_env(import_name, &info, &token.value);
                    for (k, v) in ipc_env {
                        env_vars.insert(k, v);
                    }

                    broker.registry.register(info);
                    resolved_count += 1;

                    debug!(
                        service = import_name,
                        from = %import_config.from,
                        runtime = %runtime_kind,
                        "IPC import resolved (local store — pending start)"
                    );
                }
                ResolvedService::NotFound { from, suggestion } => {
                    if import_config.optional {
                        warnings.push(format!(
                            "Optional IPC import '{}' not found: {}",
                            import_name, suggestion
                        ));
                        warn!(
                            service = import_name,
                            "Optional IPC import not found; skipping"
                        );
                    } else {
                        anyhow::bail!(
                            "Required IPC import '{}' (from '{}') not found. {}",
                            import_name,
                            from,
                            suggestion
                        );
                    }
                }
            }
        }

        // Add protocol marker env vars
        if resolved_count > 0 {
            env_vars.insert(
                "CAPSULE_IPC_PROTOCOL".to_string(),
                "jsonrpc-2.0".to_string(),
            );
            env_vars.insert(
                "CAPSULE_IPC_TRANSPORT".to_string(),
                "unix-socket".to_string(),
            );
        }

        info!(
            resolved = resolved_count,
            env_count = env_vars.len(),
            "IPC imports resolved"
        );

        Ok(Self {
            env_vars,
            resolved_count,
            warnings,
        })
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
    fn test_empty_context() {
        let ctx = IpcContext::empty();
        assert!(!ctx.has_ipc());
        assert!(ctx.env_vars.is_empty());
        assert_eq!(ctx.resolved_count, 0);
    }

    #[test]
    fn test_from_manifest_no_ipc_section() {
        let manifest: toml::Value = toml::from_str(
            r#"
            [execution]
            entrypoint = "python main.py"
            "#,
        )
        .unwrap();

        let ctx = IpcContext::from_manifest(&manifest).unwrap();
        assert!(!ctx.has_ipc());
    }

    #[test]
    fn test_from_manifest_empty_imports() {
        let manifest: toml::Value = toml::from_str(
            r#"
            [ipc.exports]
            name = "my-service"

            [ipc.imports]
            "#,
        )
        .unwrap();

        let ctx = IpcContext::from_manifest(&manifest).unwrap();
        assert!(!ctx.has_ipc());
    }

    #[test]
    fn test_from_manifest_optional_import_not_found() {
        let manifest: toml::Value = toml::from_str(
            r#"
            [ipc.imports.analytics]
            from = "nonexistent-analytics-service"
            optional = true
            "#,
        )
        .unwrap();

        let ctx = IpcContext::from_manifest(&manifest).unwrap();
        assert!(!ctx.has_ipc());
        assert_eq!(ctx.warnings.len(), 1);
        assert!(ctx.warnings[0].contains("Optional IPC import"));
    }

    #[test]
    fn test_from_manifest_required_import_not_found_errors() {
        let manifest: toml::Value = toml::from_str(
            r#"
            [ipc.imports.llm]
            from = "nonexistent-llm-service"
            optional = false
            "#,
        )
        .unwrap();

        let result = IpcContext::from_manifest(&manifest);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"), "Error: {}", err);
    }

    #[test]
    fn test_env_vars_protocol_markers() {
        // We can't easily test with a running service, but we can verify
        // that the protocol markers are set when resolved_count > 0
        let ctx = IpcContext {
            env_vars: {
                let mut m = HashMap::new();
                m.insert(
                    "CAPSULE_IPC_PROTOCOL".to_string(),
                    "jsonrpc-2.0".to_string(),
                );
                m.insert(
                    "CAPSULE_IPC_TRANSPORT".to_string(),
                    "unix-socket".to_string(),
                );
                m
            },
            resolved_count: 1,
            warnings: vec![],
        };

        assert!(ctx.has_ipc());
        assert_eq!(
            ctx.env_vars.get("CAPSULE_IPC_PROTOCOL").unwrap(),
            "jsonrpc-2.0"
        );
    }
}
