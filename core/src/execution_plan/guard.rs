use std::path::{Path, PathBuf};

use crate::execution_plan::derive::derive_tier;
use crate::execution_plan::error::AtoExecutionError;
use crate::execution_plan::model::{
    ExecutionDriver, ExecutionPlan, ExecutionRuntime, ExecutionTier,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredLock {
    CapsuleLock,
    DenoLockOrPackageLock,
    PackageLock,
    UvLock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    Deno,
    NodeCompat,
    Native,
    Wasm,
    BrowserStatic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeGuardResult {
    pub requires_unsafe_opt_in: bool,
    pub required_lock: Option<RequiredLock>,
    pub executor_kind: ExecutorKind,
}

pub fn evaluate(
    plan: &ExecutionPlan,
    manifest_dir: &Path,
    enforcement: &str,
    unsafe_mode: bool,
) -> Result<RuntimeGuardResult, AtoExecutionError> {
    let runtime = plan.target.runtime;
    let driver = plan.target.driver;

    let tier = derive_tier(runtime, driver)?;
    if matches!(tier, ExecutionTier::Tier1) && !resolve_capsule_lock_path(manifest_dir).exists() {
        return Err(AtoExecutionError::lock_incomplete(
            "capsule.lock is required for Tier1 execution",
            Some("capsule.lock"),
        ));
    }

    let required_lock = resolve_required_lock(runtime, driver)?;
    match required_lock {
        Some(RequiredLock::DenoLockOrPackageLock) => {
            if resolve_deno_dependency_lock_path(manifest_dir).is_none() {
                return Err(AtoExecutionError::lock_incomplete(
                    "deno.lock or package-lock.json is required for source/deno execution",
                    Some("deno.lock"),
                ));
            }
        }
        Some(RequiredLock::PackageLock) => {
            if resolve_package_lock_path(manifest_dir).is_none() {
                return Err(AtoExecutionError::lock_incomplete(
                    "package-lock.json is required for source/node Tier1 execution",
                    Some("package-lock.json"),
                ));
            }
        }
        Some(RequiredLock::UvLock) => {
            if resolve_uv_lock_path(manifest_dir).is_none() {
                return Err(AtoExecutionError::lock_incomplete(
                    "uv.lock is required for source/python execution",
                    Some("uv.lock"),
                ));
            }
        }
        Some(RequiredLock::CapsuleLock) | None => {}
    }

    let requires_unsafe_opt_in = matches!(
        (runtime, driver),
        (ExecutionRuntime::Source, ExecutionDriver::Native)
            | (ExecutionRuntime::Source, ExecutionDriver::Python)
    );

    if requires_unsafe_opt_in && !unsafe_mode {
        return Err(AtoExecutionError::policy_violation(
            "source/native|python execution requires explicit --unsafe opt-in",
        ));
    }

    if requires_unsafe_opt_in && enforcement != "strict" {
        return Err(AtoExecutionError::policy_violation(
            "source/native|python execution requires strict sandbox enforcement",
        ));
    }

    let executor_kind = resolve_executor_kind(runtime, driver)?;

    Ok(RuntimeGuardResult {
        requires_unsafe_opt_in,
        required_lock,
        executor_kind,
    })
}

fn resolve_executor_kind(
    runtime: ExecutionRuntime,
    driver: ExecutionDriver,
) -> Result<ExecutorKind, AtoExecutionError> {
    match (runtime, driver) {
        (ExecutionRuntime::Web, ExecutionDriver::BrowserStatic) => Ok(ExecutorKind::BrowserStatic),
        (ExecutionRuntime::Wasm, ExecutionDriver::Wasmtime) => Ok(ExecutorKind::Wasm),
        (ExecutionRuntime::Source, ExecutionDriver::Deno) => Ok(ExecutorKind::Deno),
        (ExecutionRuntime::Source, ExecutionDriver::Node) => Ok(ExecutorKind::NodeCompat),
        (ExecutionRuntime::Source, ExecutionDriver::Native)
        | (ExecutionRuntime::Source, ExecutionDriver::Python) => Ok(ExecutorKind::Native),
        _ => Err(AtoExecutionError::policy_violation(format!(
            "unsupported runtime/driver pair for guard: runtime='{}' driver='{}'",
            runtime.as_str(),
            driver.as_str()
        ))),
    }
}

fn resolve_required_lock(
    runtime: ExecutionRuntime,
    driver: ExecutionDriver,
) -> Result<Option<RequiredLock>, AtoExecutionError> {
    match (runtime, driver) {
        (ExecutionRuntime::Web, ExecutionDriver::BrowserStatic)
        | (ExecutionRuntime::Wasm, ExecutionDriver::Wasmtime) => {
            Ok(Some(RequiredLock::CapsuleLock))
        }
        (ExecutionRuntime::Source, ExecutionDriver::Deno) => {
            Ok(Some(RequiredLock::DenoLockOrPackageLock))
        }
        (ExecutionRuntime::Source, ExecutionDriver::Node) => Ok(Some(RequiredLock::PackageLock)),
        (ExecutionRuntime::Source, ExecutionDriver::Python) => Ok(Some(RequiredLock::UvLock)),
        (ExecutionRuntime::Source, ExecutionDriver::Native) => Ok(None),
        _ => Err(AtoExecutionError::policy_violation(format!(
            "unsupported runtime/driver pair for lock policy: runtime='{}' driver='{}'",
            runtime.as_str(),
            driver.as_str()
        ))),
    }
}

fn resolve_capsule_lock_path(manifest_dir: &Path) -> PathBuf {
    manifest_dir.join("capsule.lock")
}

fn resolve_deno_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        manifest_dir.join("deno.lock"),
        manifest_dir.join("source").join("deno.lock"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn resolve_package_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        manifest_dir.join("package-lock.json"),
        manifest_dir.join("source").join("package-lock.json"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn resolve_deno_dependency_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    resolve_deno_lock_path(manifest_dir).or_else(|| resolve_package_lock_path(manifest_dir))
}

fn resolve_uv_lock_path(manifest_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        manifest_dir.join("uv.lock"),
        manifest_dir.join("source").join("uv.lock"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plan::model::{
        CapsuleRef, Consent, ConsentKey, NonInteractiveBehavior, Platform, Provisioning,
        ProvisioningNetwork, Reproducibility, Runtime, RuntimeFilesystemPolicy,
        RuntimeNetworkPolicy, RuntimePolicy, RuntimeSecretsPolicy, SecretDelivery, TargetRef,
    };

    fn sample_plan(runtime: ExecutionRuntime, driver: ExecutionDriver) -> ExecutionPlan {
        ExecutionPlan {
            schema_version: "1".to_string(),
            capsule: CapsuleRef {
                scoped_id: "local/sample".to_string(),
                version: "1.0.0".to_string(),
            },
            target: TargetRef {
                label: "cli".to_string(),
                runtime,
                driver,
                language: None,
            },
            provisioning: Provisioning {
                network: ProvisioningNetwork {
                    allow_registry_hosts: Vec::new(),
                },
                lock_required: true,
                integrity_required: true,
                allowed_registries: Vec::new(),
            },
            runtime: Runtime {
                policy: RuntimePolicy {
                    network: RuntimeNetworkPolicy {
                        allow_hosts: Vec::new(),
                    },
                    filesystem: RuntimeFilesystemPolicy {
                        read_only: Vec::new(),
                        read_write: Vec::new(),
                    },
                    secrets: RuntimeSecretsPolicy {
                        allow_secret_ids: Vec::new(),
                        delivery: SecretDelivery::Fd,
                    },
                    args: Vec::new(),
                },
                fail_closed: true,
                non_interactive_behavior: NonInteractiveBehavior::DenyIfUnconsented,
            },
            consent: Consent {
                key: ConsentKey {
                    scoped_id: "local/sample".to_string(),
                    version: "1.0.0".to_string(),
                    target_label: "cli".to_string(),
                },
                policy_segment_hash: "blake3:policy".to_string(),
                provisioning_policy_hash: "blake3:provisioning".to_string(),
                mount_set_algo_id: "lockfile_mountset_v1".to_string(),
                mount_set_algo_version: 1,
            },
            reproducibility: Reproducibility {
                platform: Platform {
                    os: "darwin".to_string(),
                    arch: "arm64".to_string(),
                    libc: "unknown".to_string(),
                },
            },
        }
    }

    #[test]
    fn node_tier1_does_not_require_unsafe_when_locks_exist() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("capsule.lock"), "").expect("write capsule.lock");
        std::fs::write(tmp.path().join("package-lock.json"), "{}").expect("write package-lock");

        let plan = sample_plan(ExecutionRuntime::Source, ExecutionDriver::Node);
        let result = evaluate(&plan, tmp.path(), "strict", false).expect("guard pass");
        assert!(!result.requires_unsafe_opt_in);
        assert_eq!(result.required_lock, Some(RequiredLock::PackageLock));
        assert_eq!(result.executor_kind, ExecutorKind::NodeCompat);
    }

    #[test]
    fn python_tier2_requires_unsafe() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("uv.lock"), "").expect("write uv.lock");
        let plan = sample_plan(ExecutionRuntime::Source, ExecutionDriver::Python);

        let err = evaluate(&plan, tmp.path(), "strict", false).expect_err("must reject");
        assert_eq!(err.code, "ATO_ERR_POLICY_VIOLATION");
        assert!(err.message.contains("--unsafe"));
    }

    #[test]
    fn node_requires_package_lock_only() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("capsule.lock"), "").expect("write capsule.lock");
        std::fs::write(tmp.path().join("source").join("package-lock.json"), "{}").unwrap_or_else(
            |_| {
                std::fs::create_dir_all(tmp.path().join("source")).expect("create source");
                std::fs::write(tmp.path().join("source").join("package-lock.json"), "{}")
                    .expect("write package-lock in source");
            },
        );

        let plan = sample_plan(ExecutionRuntime::Source, ExecutionDriver::Node);
        let result = evaluate(&plan, tmp.path(), "strict", false).expect("guard pass");
        assert_eq!(result.required_lock, Some(RequiredLock::PackageLock));
    }

    #[test]
    fn tier1_requires_capsule_lock() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("package-lock.json"), "{}").expect("write package-lock");

        let plan = sample_plan(ExecutionRuntime::Source, ExecutionDriver::Node);
        let err = evaluate(&plan, tmp.path(), "strict", false).expect_err("must reject");
        assert_eq!(err.code, "ATO_ERR_PROVISIONING_LOCK_INCOMPLETE");
        assert!(err.message.contains("capsule.lock"));
    }
}
