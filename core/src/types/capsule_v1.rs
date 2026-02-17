//! Capsule Manifest v0.2 Schema
//!
//! Implements the "Everything is a Capsule" paradigm for Gumball v0.3.0.
//! Supports both TOML (human-authored) and JSON (machine-generated) formats.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::error::CapsuleError;
use super::utils::parse_memory_string;
use crate::schema_registry::SchemaRegistry;

/// Capsule Type - defines the fundamental nature of the Capsule
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CapsuleType {
    /// AI model inference (MLX, vLLM, etc.)
    Inference,
    /// Utility tool (RAG, code interpreter, etc.)
    Tool,
    /// Application (agent, workflow, etc.)
    #[default]
    App,
}

/// Runtime Type - how the Capsule is executed
///
/// UARC V1.1.0 defines three runtime classes:
/// - `Source`: Interpreted source code (Python, JS, etc.)
/// - `Wasm`: WebAssembly Component Model
/// - `Oci`: OCI Container Image (Docker, Youki, etc.)
///
/// Legacy types (Docker, Native, Youki) are deprecated and mapped to Oci.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeType {
    /// Interpreted source code runtime (Python, Node.js, Ruby, etc.)
    /// UARC V1.1.0: Primary runtime for scripting workloads
    #[default]
    Source,

    /// WebAssembly Component Model runtime
    /// UARC V1.1.0: Portable, sandboxed bytecode for edge/latency-sensitive workloads
    Wasm,

    /// OCI Container Image runtime (youki, runc, containerd)
    /// UARC V1.1.0: Fallback for legacy/GPU applications
    Oci,

    /// Static web runtime for browser sandbox / playground.
    Web,

    // === Legacy types (deprecated, for backward compatibility) ===
    // These will be removed in UARC v0.2.0
    /// Docker container (deprecated: use `oci` instead)
    #[deprecated(since = "1.1.0", note = "Use `oci` runtime type instead")]
    #[serde(rename = "docker")]
    Docker,

    /// Native binary (deprecated: not supported in UARC V1)
    #[deprecated(
        since = "1.1.0",
        note = "Native runtime is not supported in UARC V1 for security reasons"
    )]
    #[serde(rename = "native")]
    Native,

    /// Youki OCI runtime (deprecated: use `oci` instead)
    #[deprecated(since = "1.1.0", note = "Use `oci` runtime type instead")]
    #[serde(rename = "youki")]
    Youki,
}

impl RuntimeType {
    /// Normalize legacy runtime types to UARC V1.1.0 types
    pub fn normalize(&self) -> RuntimeType {
        #[allow(deprecated)]
        match self {
            RuntimeType::Docker => RuntimeType::Oci,
            RuntimeType::Youki => RuntimeType::Oci,
            RuntimeType::Native => RuntimeType::Source, // Best-effort fallback
            other => other.clone(),
        }
    }

    /// Check if this is a legacy (deprecated) runtime type
    #[allow(deprecated)]
    pub fn is_legacy(&self) -> bool {
        matches!(
            self,
            RuntimeType::Docker | RuntimeType::Native | RuntimeType::Youki
        )
    }

    /// Parse a v0.2 named target runtime label.
    #[allow(deprecated)]
    pub fn from_target_runtime(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "source" => Some(RuntimeType::Source),
            "wasm" => Some(RuntimeType::Wasm),
            "oci" => Some(RuntimeType::Oci),
            "web" => Some(RuntimeType::Web),
            "docker" => Some(RuntimeType::Docker),
            "native" => Some(RuntimeType::Native),
            "youki" => Some(RuntimeType::Youki),
            _ => None,
        }
    }
}

/// Routing Weight - determines local vs cloud routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RouteWeight {
    /// Small models, quick tasks - prefer local
    #[default]
    Light,
    /// Large models, heavy compute - consider cloud
    Heavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Quantization {
    Fp16,
    Bf16,
    #[serde(rename = "8bit")]
    Bit8,
    #[serde(rename = "4bit")]
    Bit4,
}

/// Platform target
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Platform {
    DarwinArm64,
    DarwinX86_64,
    LinuxAmd64,
    LinuxArm64,
}

/// Transparency enforcement level for source code validation
///
/// Controls how strictly the runtime enforces source code transparency requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TransparencyLevel {
    /// Source code required, no binaries allowed except explicitly allowlisted.
    /// Most restrictive: .pyc, .class, native binaries all forbidden unless allowlisted.
    Strict,
    /// Binaries allowed if in allowlist or are known bytecode (.pyc, .class).
    /// Practical default for most use cases.
    #[default]
    Loose,
    /// No transparency enforcement (legacy/Docker compatibility mode).
    Off,
}

/// Transparency enforcement configuration
///
/// Enforces UARC's "no binary-only" philosophy by validating that capsules
/// contain source code and not just compiled binaries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransparencyConfig {
    /// Enforcement level
    #[serde(default)]
    pub level: TransparencyLevel,

    /// Glob patterns for allowed binary files
    ///
    /// Examples: "lib/**/*.so", "venv/bin/*", "node_modules/**/*.node"
    #[serde(default)]
    pub allowed_binaries: Vec<String>,
}

/// Build configuration (packaging-time behavior)
///
/// These settings affect how capsules are packaged (e.g. bundle/source archive).
/// They do not change runtime behavior directly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildConfig {
    /// Glob patterns to exclude from packaged artifacts.
    ///
    /// Typical uses:
    /// - Exclude large ML libraries (torch, jaxlib, etc.) for "Thin Capsule on Fat Container"
    /// - Exclude host-provided dynamic libs when using passthrough
    #[serde(default)]
    pub exclude_libs: Vec<String>,

    /// Sugar syntax: GPU-oriented packaging defaults.
    ///
    /// When true, tooling may apply recommended defaults (e.g. docker scaffold template
    /// and optional exclude patterns) but should remain opt-in.
    #[serde(default)]
    pub gpu: bool,

    /// Build task lifecycle for CI/build pipelines.
    #[serde(default)]
    pub lifecycle: Option<BuildLifecycleConfig>,

    /// Build inputs used for reproducibility and provenance.
    #[serde(default)]
    pub inputs: Option<BuildInputsConfig>,

    /// Build outputs expected by registry/store verification.
    #[serde(default)]
    pub outputs: Option<BuildOutputsConfig>,

    /// Publish-time verification policy.
    #[serde(default)]
    pub policy: Option<BuildPolicyConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildLifecycleConfig {
    #[serde(default)]
    pub prepare: Option<String>,
    #[serde(default)]
    pub build: Option<String>,
    #[serde(default)]
    pub package: Option<String>,
    #[serde(default)]
    pub verify: Option<String>,
    #[serde(default)]
    pub publish: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildInputsConfig {
    #[serde(default)]
    pub lockfiles: Vec<String>,
    #[serde(default)]
    pub toolchain: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub allow_network: Option<bool>,
    #[serde(default)]
    pub reproducibility: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildOutputsConfig {
    #[serde(default)]
    pub capsule: Option<String>,
    #[serde(default)]
    pub sha256: Option<bool>,
    #[serde(default)]
    pub blake3: Option<bool>,
    #[serde(default)]
    pub attestation: Option<bool>,
    #[serde(default)]
    pub signature: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildPolicyConfig {
    #[serde(default)]
    pub require_attestation: Option<bool>,
    #[serde(default)]
    pub require_did_signature: Option<bool>,
}

/// Isolation configuration (runtime-time behavior)
///
/// This section controls what host environment data is allowed to pass into the
/// capsule at runtime. This is a security-sensitive opt-in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IsolationConfig {
    /// Host environment variables to pass through.
    ///
    /// Examples: ["LD_LIBRARY_PATH", "CUDA_HOME", "HF_TOKEN"].
    #[serde(default)]
    pub allow_env: Vec<String>,
}

/// Service specification for Supervisor Mode (multi-process orchestration).
///
/// This is intentionally minimal in Step 1: schema + dependency graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpec {
    /// Command line to execute.
    ///
    /// Accept both `entrypoint` (preferred) and `command` (alias) for compatibility
    /// with early drafts.
    #[serde(alias = "command")]
    pub entrypoint: String,

    /// Service dependencies by name.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,

    /// Placeholders to allocate and inject as ports (Step 2).
    #[serde(default)]
    pub expose: Option<Vec<String>>,

    /// Environment variables to inject into this service.
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,

    /// Readiness probe (Step 2/3).
    #[serde(default)]
    pub readiness_probe: Option<ReadinessProbe>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessProbe {
    #[serde(default)]
    pub http_get: Option<String>,

    #[serde(default)]
    pub tcp_connect: Option<String>,

    /// Placeholder name that resolves to a concrete port (e.g., "PORT").
    pub port: String,
}

/// Capsule Manifest v0.2
///
/// The primary configuration format for all Capsules in Gumball v0.3.0+
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapsuleManifestV1 {
    /// Schema version (must be "0.2")
    #[serde(default = "default_schema_version")]
    pub schema_version: String,

    /// Unique capsule identifier (kebab-case)
    pub name: String,

    /// Semantic version
    pub version: String,

    /// Capsule type
    #[serde(rename = "type")]
    pub capsule_type: CapsuleType,

    /// Default target label used when no explicit target is selected.
    #[serde(default)]
    pub default_target: String,

    /// Human-readable metadata
    #[serde(default)]
    pub metadata: CapsuleMetadataV1,

    /// Capsule capabilities (for inference type)
    #[serde(default)]
    pub capabilities: Option<CapsuleCapabilities>,

    /// System requirements
    #[serde(default)]
    pub requirements: CapsuleRequirements,

    /// Execution configuration
    #[serde(default, skip_serializing)]
    pub execution: CapsuleExecution,

    /// Persistent storage volumes
    #[serde(default)]
    pub storage: CapsuleStorage,

    /// Routing configuration
    #[serde(default)]
    pub routing: CapsuleRouting,

    /// Network configuration
    #[serde(default)]
    pub network: Option<NetworkConfig>,

    /// Model configuration (for inference type)
    #[serde(default)]
    pub model: Option<ModelConfig>,

    /// Transparency enforcement configuration
    #[serde(default)]
    pub transparency: Option<TransparencyConfig>,

    /// Pre-warmed container pool configuration
    #[serde(default)]
    pub pool: Option<PoolConfig>,

    /// Build configuration (packaging-time)
    #[serde(default)]
    pub build: Option<BuildConfig>,

    /// Isolation configuration (runtime-time)
    #[serde(default)]
    pub isolation: Option<IsolationConfig>,

    /// Polymorphism configuration (implements schema hashes)
    #[serde(default)]
    pub polymorphism: Option<PolymorphismConfig>,

    /// Multi-target execution configuration (UARC V1.1.0)
    ///
    /// Allows capsules to specify multiple runtime targets (wasm, source, oci).
    /// Engine performs runtime resolution to select the most appropriate target.
    #[serde(default)]
    pub targets: Option<TargetsConfig>,

    /// Supervisor Mode: Multi-service definition.
    ///
    /// Optional and dev-first: absence means single-process execution via `execution`.
    #[serde(default)]
    pub services: Option<HashMap<String, ServiceSpec>>,
}

/// Polymorphism configuration
///
/// Allows capsules to declare which schema hashes they implement.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolymorphismConfig {
    #[serde(default)]
    pub implements: Vec<String>,
}

fn default_schema_version() -> String {
    "0.2".to_string()
}

/// Pre-warmed container pool configuration
///
/// Enables ultra-low latency container startup by maintaining a pool of
/// frozen containers that can be instantly thawed and assigned.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Whether pool is enabled for this capsule
    #[serde(default)]
    pub enabled: bool,

    /// Number of containers to keep pre-warmed (default: 3)
    #[serde(default = "default_pool_size")]
    pub size: u16,

    /// Minimum threshold before triggering replenishment (default: 1)
    #[serde(default = "default_min_threshold")]
    pub min_threshold: u16,

    /// Replenish check interval in milliseconds (default: 5000)
    #[serde(default = "default_replenish_interval_ms")]
    pub replenish_interval_ms: u32,

    /// Maximum time a container can be assigned in seconds (default: 300)
    #[serde(default = "default_max_assignment_duration_secs")]
    pub max_assignment_duration_secs: u32,
}

fn default_pool_size() -> u16 {
    3
}
fn default_min_threshold() -> u16 {
    1
}
fn default_replenish_interval_ms() -> u32 {
    5000
}
fn default_max_assignment_duration_secs() -> u32 {
    300
}

/// Persistent storage configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapsuleStorage {
    #[serde(default)]
    pub volumes: Vec<StorageVolume>,
    /// Use thin provisioning by default for all volumes in this capsule
    #[serde(default)]
    pub use_thin_provisioning: bool,
}

/// A named persistent volume mounted into the container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageVolume {
    pub name: String,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
    /// Size in bytes (0 = use engine default)
    #[serde(default)]
    pub size_bytes: u64,
    /// Use thin provisioning for this volume (overrides CapsuleStorage.use_thin_provisioning)
    #[serde(default)]
    pub use_thin: Option<bool>,
    /// Enable encryption for this volume
    #[serde(default)]
    pub encrypted: bool,
}

/// Human-readable metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapsuleMetadataV1 {
    /// Display name for UI
    #[serde(default)]
    pub display_name: Option<String>,

    /// Description
    #[serde(default)]
    pub description: Option<String>,

    /// Author or organization
    #[serde(default)]
    pub author: Option<String>,

    /// Icon URL
    #[serde(default)]
    pub icon: Option<String>,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Capsule capabilities (for inference type)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapsuleCapabilities {
    /// Supports chat completions
    #[serde(default)]
    pub chat: bool,

    /// Supports function/tool calling
    #[serde(default)]
    pub function_calling: bool,

    /// Supports vision/image input
    #[serde(default)]
    pub vision: bool,

    /// Maximum context window size
    #[serde(default)]
    pub context_length: Option<u32>,
}

/// System requirements
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapsuleRequirements {
    /// Supported platforms
    #[serde(default)]
    pub platform: Vec<Platform>,

    /// Minimum VRAM required (e.g., "6GB")
    #[serde(default)]
    pub vram_min: Option<String>,

    /// Recommended VRAM (e.g., "8GB")
    #[serde(default)]
    pub vram_recommended: Option<String>,

    /// Disk space required (e.g., "5GB")
    #[serde(default)]
    pub disk: Option<String>,

    /// Other Capsule dependencies
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl CapsuleRequirements {
    /// Parse vram_min into bytes
    pub fn vram_min_bytes(&self) -> Result<Option<u64>, CapsuleError> {
        match &self.vram_min {
            Some(s) => {
                Ok(Some(parse_memory_string(s).map_err(|e| {
                    CapsuleError::InvalidMemoryString(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    /// Parse vram_recommended into bytes
    pub fn vram_recommended_bytes(&self) -> Result<Option<u64>, CapsuleError> {
        match &self.vram_recommended {
            Some(s) => {
                Ok(Some(parse_memory_string(s).map_err(|e| {
                    CapsuleError::InvalidMemoryString(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    /// Parse disk into bytes
    pub fn disk_bytes(&self) -> Result<Option<u64>, CapsuleError> {
        match &self.disk {
            Some(s) => {
                Ok(Some(parse_memory_string(s).map_err(|e| {
                    CapsuleError::InvalidMemoryString(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }
}

/// Signal configuration for graceful shutdown
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalConfig {
    /// Signal for graceful stop (default: SIGTERM)
    #[serde(default = "default_stop_signal")]
    pub stop: String,

    /// Signal for force kill (default: SIGKILL)
    #[serde(default = "default_kill_signal")]
    pub kill: String,
}

fn default_stop_signal() -> String {
    "SIGTERM".to_string()
}

fn default_kill_signal() -> String {
    "SIGKILL".to_string()
}

/// Execution configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CapsuleExecution {
    /// Runtime type
    pub runtime: RuntimeType,

    /// Entry point (script, binary, or Docker image)
    pub entrypoint: String,

    /// Port the service listens on
    #[serde(default)]
    pub port: Option<u16>,

    /// Health check endpoint
    #[serde(default)]
    pub health_check: Option<String>,

    /// Startup timeout in seconds
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout: u32,

    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Signal configuration
    #[serde(default)]
    pub signals: SignalConfig,
}

fn default_startup_timeout() -> u32 {
    60
}

/// Routing configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapsuleRouting {
    /// Weight for routing decision
    #[serde(default)]
    pub weight: RouteWeight,

    /// Whether to fallback to cloud when local resources are insufficient
    #[serde(default = "default_true")]
    pub fallback_to_cloud: bool,

    /// Cloud Capsule ID to use as fallback
    #[serde(default)]
    pub cloud_capsule: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Model configuration (for inference Capsules)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Model source (e.g., "hf:org/model")
    #[serde(default)]
    pub source: Option<String>,

    /// Quantization format
    #[serde(default)]
    pub quantization: Option<Quantization>,
}

/// Network configuration for Egress Control
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// List of allowlisted domains (L7/Proxy)
    #[serde(default)]
    pub egress_allow: Vec<String>,

    /// List of allowlisted IPs/CIDRs (L3/Firewall)
    #[serde(default)]
    pub egress_id_allow: Vec<EgressIdRule>,
}

/// Rule for L3 Egress Control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressIdRule {
    /// Type of rule (ip, cidr, spiffe - though spiffe might be L7, treating as ID here)
    #[serde(rename = "type")]
    pub rule_type: EgressIdType,

    /// Value (e.g., "192.168.1.1", "10.0.0.0/8")
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EgressIdType {
    Ip,
    Cidr,
    /// SPIFFE ID (future use, currently placeholder for L3 mapping)
    Spiffe,
}

// ============================================================================
// Multi-Target Execution Configuration (UARC V1.1.0)
// ============================================================================

/// Multi-target execution configuration
///
/// Allows capsules to provide multiple runtime targets (wasm, source, oci).
/// The Engine performs runtime resolution to select the most appropriate target
/// based on platform capabilities and the preference order.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TargetsConfig {
    /// Preferred resolution order (e.g., ["wasm", "source", "oci"])
    ///
    /// If not specified, the default order is: wasm → source → oci
    #[serde(default)]
    pub preference: Vec<String>,

    /// SHA256 digest of the source code archive for L1 policy verification (UARC V1.1.0)
    ///
    /// Format: "sha256:<hash>" pointing to the source archive in CAS.
    /// Required when source target is specified.
    /// The Engine verifies this digest against CAS during L1 Source Policy checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_digest: Option<String>,

    /// Port the service listens on (global for all targets)
    #[serde(default)]
    pub port: Option<u16>,

    /// Startup timeout in seconds (global for all targets)
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout: u32,

    /// Environment variables (global for all targets)
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Health check endpoint (global for all targets)
    #[serde(default)]
    pub health_check: Option<String>,

    /// WebAssembly Component Model target
    #[serde(default)]
    pub wasm: Option<WasmTarget>,

    /// Source code target (interpreted languages)
    #[serde(default)]
    pub source: Option<SourceTarget>,

    /// OCI container target
    #[serde(default)]
    pub oci: Option<OciTarget>,

    /// Named target entries for v0.2 (e.g. [targets.cli], [targets.static]).
    #[serde(flatten)]
    pub named: HashMap<String, NamedTarget>,
}

/// v0.2 named target definition under [targets.<label>].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamedTarget {
    /// Runtime kind for this target (`source`, `web`, `wasm`, `oci`).
    #[serde(default)]
    pub runtime: String,

    /// Entrypoint path for the target.
    #[serde(default)]
    pub entrypoint: String,

    /// Optional command arguments.
    #[serde(default)]
    pub cmd: Vec<String>,

    /// Optional environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Public asset allowlist for web runtime.
    #[serde(default)]
    pub public: Vec<String>,

    /// Optional working directory.
    #[serde(default)]
    pub working_dir: Option<String>,
}

/// WebAssembly Component Model target configuration
///
/// For capsules that can run as Wasm components using the wasi:cli/command world.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmTarget {
    /// CAS digest of the Wasm component binary
    ///
    /// Format: "sha256:<hash>" pointing to the .wasm file in CAS
    pub digest: String,

    /// WIT world interface (e.g., "wasi:cli/command", "uarc:v1/http-handler")
    #[serde(default = "default_wasm_world")]
    pub world: String,

    /// Optional: component-specific configuration as key-value pairs
    #[serde(default)]
    pub config: HashMap<String, String>,
}

fn default_wasm_world() -> String {
    "wasi:cli/command".to_string()
}

/// Source code target configuration
///
/// For capsules that run directly from source code using an interpreter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTarget {
    /// Language runtime (e.g., "python", "node", "deno")
    pub language: String,

    /// Version constraint (e.g., "^3.11", ">=18.0")
    #[serde(default)]
    pub version: Option<String>,

    /// Entry point file (relative to source root)
    pub entrypoint: String,

    /// Dependencies file (e.g., "requirements.txt", "package.json")
    #[serde(default)]
    pub dependencies: Option<String>,

    /// Optional: runtime-specific arguments
    #[serde(default)]
    pub args: Vec<String>,

    /// Development mode - disables sandboxing for easier debugging.
    /// WARNING: Only honored when Engine's allow_insecure_dev_mode is true.
    /// UARC V1.1.0: (manifest.dev_mode) AND (engine.allow_insecure_dev_mode)
    #[serde(default)]
    pub dev_mode: bool,
}

/// OCI container target configuration
///
/// For capsules that run as Docker/OCI containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciTarget {
    /// OCI image reference (e.g., "python:3.11-slim", "ghcr.io/org/image:tag")
    pub image: String,

    /// Image digest for immutability (e.g., "sha256:<hash>")
    #[serde(default)]
    pub digest: Option<String>,

    /// Command to execute (overrides image CMD)
    #[serde(default)]
    pub cmd: Vec<String>,

    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,
}

impl TargetsConfig {
    /// Check if any target is defined
    pub fn has_any_target(&self) -> bool {
        self.wasm.is_some()
            || self.source.is_some()
            || self.oci.is_some()
            || !self.named.is_empty()
    }

    /// Get the preference order, using defaults if not specified
    pub fn preference_order(&self) -> Vec<&str> {
        if self.preference.is_empty() {
            // Default order: wasm → source → oci
            vec!["wasm", "source", "oci"]
        } else {
            self.preference.iter().map(|s| s.as_str()).collect()
        }
    }

    /// Validates that source_digest is present when source target is defined (UARC V1.1.0 L1 requirement)
    pub fn validate_source_digest(&self) -> Result<(), String> {
        if self.source.is_some() && self.source_digest.is_none() {
            return Err(
                "source_digest is required when source target is defined (UARC V1.1.0 L1)"
                    .to_string(),
            );
        }
        if let Some(ref digest) = self.source_digest {
            if !digest.starts_with("sha256:") {
                return Err(format!(
                    "source_digest must start with 'sha256:', got: {}",
                    digest
                ));
            }
            // Validate hex length (SHA256 = 64 hex chars)
            let hash_part = digest.strip_prefix("sha256:").unwrap();
            if hash_part.len() != 64 || !hash_part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!(
                    "source_digest has invalid SHA256 hash format: {}",
                    digest
                ));
            }
        }
        Ok(())
    }

    /// Returns a v0.2 named target by label.
    pub fn named_target(&self, label: &str) -> Option<&NamedTarget> {
        self.named.get(label)
    }

    /// Returns all named targets.
    pub fn named_targets(&self) -> &HashMap<String, NamedTarget> {
        &self.named
    }
}

impl CapsuleManifestV1 {
    /// Parse from TOML string
    pub fn from_toml(content: &str) -> Result<Self, CapsuleError> {
        let raw: toml::Value = toml::from_str(content)
            .map_err(|e| CapsuleError::ParseError(format!("TOML parse error: {}", e)))?;

        if raw.get("execution").is_some() {
            return Err(CapsuleError::ParseError(
                "legacy [execution] section is not supported in schema_version=0.2".to_string(),
            ));
        }

        toml::from_str(content)
            .map_err(|e| CapsuleError::ParseError(format!("TOML parse error: {}", e)))
    }

    /// Parse from JSON string
    pub fn from_json(content: &str) -> Result<Self, CapsuleError> {
        let raw: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| CapsuleError::ParseError(format!("JSON parse error: {}", e)))?;
        if raw.get("execution").is_some() {
            return Err(CapsuleError::ParseError(
                "legacy [execution] section is not supported in schema_version=0.2".to_string(),
            ));
        }

        serde_json::from_str(content)
            .map_err(|e| CapsuleError::ParseError(format!("JSON parse error: {}", e)))
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String, CapsuleError> {
        serde_json::to_string_pretty(self).map_err(|e| CapsuleError::SerializeError(e.to_string()))
    }

    /// Serialize to TOML
    pub fn to_toml(&self) -> Result<String, CapsuleError> {
        toml::to_string_pretty(self).map_err(|e| CapsuleError::SerializeError(e.to_string()))
    }

    /// Resolve the effective v0.2 target from `default_target`.
    pub fn resolve_default_target(&self) -> Result<&NamedTarget, CapsuleError> {
        let targets = self.targets.as_ref().ok_or_else(|| {
            CapsuleError::ValidationError(
                "at least one [targets.<label>] section is required".to_string(),
            )
        })?;
        if self.default_target.trim().is_empty() {
            return Err(CapsuleError::ValidationError(
                "default_target is required".to_string(),
            ));
        }
        targets
            .named_targets()
            .get(self.default_target.trim())
            .ok_or_else(|| {
                CapsuleError::ValidationError(format!(
                    "default_target '{}' does not exist under [targets]",
                    self.default_target
                ))
            })
    }

    /// Resolve runtime from the effective v0.2 target.
    pub fn resolve_default_runtime(&self) -> Result<RuntimeType, CapsuleError> {
        let target = self.resolve_default_target()?;
        RuntimeType::from_target_runtime(&target.runtime)
            .map(|runtime| runtime.normalize())
            .ok_or_else(|| CapsuleError::ValidationError(format!("Invalid target '{}': runtime and entrypoint are required", self.default_target)))
    }

    /// Check whether this capsule implements the given schema identifier.
    ///
    /// The schema identifier may be a sha256 hash or a registry alias.
    pub fn implements_schema(
        &self,
        schema_id: &str,
        registry: &SchemaRegistry,
    ) -> Result<bool, CapsuleError> {
        let Some(poly) = &self.polymorphism else {
            return Ok(false);
        };

        let target = registry
            .resolve_schema_hash(schema_id)
            .map_err(|e| CapsuleError::ValidationError(e.to_string()))?;

        for entry in &poly.implements {
            let resolved = registry
                .resolve_schema_hash(entry)
                .map_err(|e| CapsuleError::ValidationError(e.to_string()))?;
            if resolved == target {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Load from file (auto-detects format by extension)
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, CapsuleError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|e| CapsuleError::IoError(e.to_string()))?;

        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        match ext {
            "toml" => Self::from_toml(&content),
            "json" => Self::from_json(&content),
            _ => {
                // Try TOML first, then JSON
                Self::from_toml(&content).or_else(|_| Self::from_json(&content))
            }
        }
    }

    /// Validate the manifest
    pub fn validate(&self) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Schema version must be "0.2"
        if self.schema_version != "0.2" {
            errors.push(ValidationError::InvalidSchemaVersion(
                self.schema_version.clone(),
            ));
        }

        // Name must be kebab-case
        if !is_kebab_case(&self.name) {
            errors.push(ValidationError::InvalidName(self.name.clone()));
        }

        // Name length bounds (frozen v1.0)
        if !(3..=64).contains(&self.name.len()) {
            errors.push(ValidationError::InvalidName(self.name.clone()));
        }

        // Version must be semver
        if !is_semver(&self.version) {
            errors.push(ValidationError::InvalidVersion(self.version.clone()));
        }

        // Requirements memory strings must be parseable if present
        if let Some(v) = &self.requirements.vram_min {
            if parse_memory_string(v).is_err() {
                errors.push(ValidationError::InvalidMemoryString {
                    field: "requirements.vram_min",
                    value: v.clone(),
                });
            }
        }
        if let Some(v) = &self.requirements.vram_recommended {
            if parse_memory_string(v).is_err() {
                errors.push(ValidationError::InvalidMemoryString {
                    field: "requirements.vram_recommended",
                    value: v.clone(),
                });
            }
        }
        if let Some(v) = &self.requirements.disk {
            if parse_memory_string(v).is_err() {
                errors.push(ValidationError::InvalidMemoryString {
                    field: "requirements.disk",
                    value: v.clone(),
                });
            }
        }

        // Inference type should have capabilities
        if self.capsule_type == CapsuleType::Inference && self.capabilities.is_none() {
            errors.push(ValidationError::MissingCapabilities);
        }

        // Inference type should have model config
        if self.capsule_type == CapsuleType::Inference && self.model.is_none() {
            errors.push(ValidationError::MissingModelConfig);
        }

        // default_target must point to an existing named target.
        let named_targets = self
            .targets
            .as_ref()
            .map(|t| t.named_targets())
            .cloned()
            .unwrap_or_default();
        if self.default_target.trim().is_empty() {
            errors.push(ValidationError::MissingDefaultTarget);
        }
        if named_targets.is_empty() {
            errors.push(ValidationError::MissingTargets);
        } else if !self.default_target.trim().is_empty()
            && !named_targets.contains_key(self.default_target.trim())
        {
            errors.push(ValidationError::DefaultTargetNotFound(
                self.default_target.clone(),
            ));
        }

        for (label, target) in named_targets {
            let runtime = target.runtime.trim().to_ascii_lowercase();
            let entrypoint = target.entrypoint.trim();
            if label.trim().is_empty()
                || runtime.is_empty()
                || entrypoint.is_empty()
                || !matches!(runtime.as_str(), "source" | "wasm" | "oci" | "web")
            {
                errors.push(ValidationError::InvalidTarget(label));
                continue;
            }
            if runtime == "web" {
                if target.public.is_empty() {
                    errors.push(ValidationError::InvalidWebTarget(
                        label.clone(),
                        "public is required for runtime=web".to_string(),
                    ));
                } else if !target.public.iter().any(|p| p == entrypoint) {
                    errors.push(ValidationError::InvalidWebTarget(
                        label.clone(),
                        "entrypoint must be included in public allowlist".to_string(),
                    ));
                }
            }
        }

        // Storage volumes (minimal): require at least one OCI target.
        let has_oci_target = self.targets.as_ref().is_some_and(|targets| {
            targets
                .named_targets()
                .values()
                .any(|t| t.runtime.eq_ignore_ascii_case("oci"))
                || targets.oci.is_some()
        });
        if !self.storage.volumes.is_empty() {
            if !has_oci_target {
                errors.push(ValidationError::StorageOnlyForDocker);
            }

            let mut names = std::collections::HashSet::new();
            for vol in &self.storage.volumes {
                if vol.name.trim().is_empty() {
                    errors.push(ValidationError::InvalidStorageVolume);
                    continue;
                }
                if !names.insert(vol.name.trim().to_string()) {
                    errors.push(ValidationError::InvalidStorageVolume);
                }
                let mp = vol.mount_path.trim();
                if mp.is_empty() || !mp.starts_with('/') || mp.contains("..") {
                    errors.push(ValidationError::InvalidStorageVolume);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Check if this Capsule can run on the current platform
    pub fn supports_current_platform(&self) -> bool {
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        {
            self.requirements.platform.is_empty()
                || self.requirements.platform.contains(&Platform::DarwinArm64)
        }
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        {
            self.requirements.platform.is_empty()
                || self.requirements.platform.contains(&Platform::DarwinX86_64)
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            self.requirements.platform.is_empty()
                || self.requirements.platform.contains(&Platform::LinuxAmd64)
        }
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        {
            self.requirements.platform.is_empty()
                || self.requirements.platform.contains(&Platform::LinuxArm64)
        }
        #[cfg(not(any(
            all(target_os = "macos", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64")
        )))]
        {
            false
        }
    }

    /// Get effective display name
    pub fn display_name(&self) -> &str {
        self.metadata.display_name.as_deref().unwrap_or(&self.name)
    }

    /// Check if this is an inference Capsule
    pub fn is_inference(&self) -> bool {
        self.capsule_type == CapsuleType::Inference
    }

    /// Check if cloud fallback is enabled
    pub fn can_fallback_to_cloud(&self) -> bool {
        self.routing.fallback_to_cloud && self.routing.cloud_capsule.is_some()
    }
}

/// Validation error types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    InvalidSchemaVersion(String),
    InvalidName(String),
    InvalidMemoryString { field: &'static str, value: String },
    InvalidVersion(String),
    MissingCapabilities,
    MissingModelConfig,
    InvalidPort(u16),
    StorageOnlyForDocker,
    InvalidStorageVolume,
    MissingDefaultTarget,
    MissingTargets,
    DefaultTargetNotFound(String),
    InvalidTarget(String),
    InvalidWebTarget(String, String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::InvalidSchemaVersion(v) => {
                write!(f, "Invalid schema_version '{}', expected '0.2'", v)
            }
            ValidationError::InvalidName(n) => {
                write!(f, "Invalid name '{}', must be kebab-case", n)
            }
            ValidationError::InvalidMemoryString { field, value } => {
                write!(f, "Invalid memory string for {}: '{}'", field, value)
            }
            ValidationError::InvalidVersion(v) => {
                write!(f, "Invalid version '{}', must be semver (e.g., 1.0.0)", v)
            }
            ValidationError::MissingCapabilities => {
                write!(f, "Inference Capsule must have capabilities defined")
            }
            ValidationError::MissingModelConfig => {
                write!(f, "Inference Capsule must have model config defined")
            }
            ValidationError::InvalidPort(p) => {
                write!(f, "Invalid port {}", p)
            }
            ValidationError::StorageOnlyForDocker => {
                write!(
                    f,
                    "Storage volumes are only supported for execution.runtime=docker"
                )
            }
            ValidationError::InvalidStorageVolume => {
                write!(
                    f,
                    "Invalid storage volume (requires unique name and absolute mount_path)"
                )
            }
            ValidationError::MissingDefaultTarget => {
                write!(f, "default_target is required")
            }
            ValidationError::MissingTargets => {
                write!(f, "At least one [targets.<label>] entry is required")
            }
            ValidationError::DefaultTargetNotFound(target) => {
                write!(f, "default_target '{}' does not exist under [targets]", target)
            }
            ValidationError::InvalidTarget(label) => {
                write!(
                    f,
                    "Invalid target '{}': runtime and entrypoint are required",
                    label
                )
            }
            ValidationError::InvalidWebTarget(label, message) => {
                write!(f, "Invalid web target '{}': {}", label, message)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Check if string is kebab-case
fn is_kebab_case(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let chars: Vec<char> = s.chars().collect();
    // Must start and end with alphanumeric
    if !chars[0].is_ascii_lowercase() && !chars[0].is_ascii_digit() {
        return false;
    }
    if !chars.last().unwrap().is_ascii_lowercase() && !chars.last().unwrap().is_ascii_digit() {
        return false;
    }
    // Only lowercase, digits, and hyphens allowed
    chars
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
}

/// Check if string is valid semver
fn is_semver(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    let version_part = parts[0];
    let version_nums: Vec<&str> = version_part.split('.').collect();

    if version_nums.len() != 3 {
        return false;
    }

    version_nums.iter().all(|n| n.parse::<u32>().is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
schema_version = "0.2"
name = "mlx-qwen3-8b"
version = "1.0.0"
type = "inference"
default_target = "cli"

[metadata]
display_name = "Qwen3 8B (MLX)"
description = "Local inference on Apple Silicon"
author = "gumball-official"
tags = ["llm", "mlx"]

[capabilities]
chat = true
function_calling = true
vision = false
context_length = 128000

[requirements]
platform = ["darwin-arm64"]
vram_min = "6GB"
vram_recommended = "8GB"
disk = "5GB"

[targets]
port = 8081
health_check = "/health"
startup_timeout = 120

[targets.cli]
runtime = "source"
entrypoint = "server.py"

[targets.cli.env]
GUMBALL_MODEL = "qwen3-8b"

[routing]
weight = "light"
fallback_to_cloud = true
cloud_capsule = "vllm-qwen3-8b"

[model]
source = "hf:org/model"
quantization = "4bit"
"#;

    #[test]
    fn test_parse_valid_toml() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();

        assert_eq!(manifest.name, "mlx-qwen3-8b");
        assert_eq!(manifest.version, "1.0.0");
        assert_eq!(manifest.capsule_type, CapsuleType::Inference);
        assert_eq!(manifest.targets.as_ref().and_then(|t| t.port), Some(8081));
        assert_eq!(manifest.resolve_default_runtime().unwrap(), RuntimeType::Source);
        assert!(manifest.capabilities.as_ref().unwrap().chat);
        assert_eq!(manifest.routing.weight, RouteWeight::Light);
    }

    #[test]
    fn test_validate_valid_manifest() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_schema_version() {
        let toml = VALID_TOML.replace("schema_version = \"0.2\"", "schema_version = \"2.0\"");
        let manifest = CapsuleManifestV1::from_toml(&toml).unwrap();
        let errors = manifest.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidSchemaVersion(_))));
    }

    #[test]
    fn test_validate_invalid_memory_string() {
        let toml = VALID_TOML.replace("vram_min = \"6GB\"", "vram_min = \"6XB\"");
        let manifest = CapsuleManifestV1::from_toml(&toml).unwrap();
        let errs = manifest.validate().unwrap_err();
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidMemoryString { .. })));
    }

    #[test]
    fn test_validate_invalid_name() {
        let toml = VALID_TOML.replace("name = \"mlx-qwen3-8b\"", "name = \"Invalid Name!\"");
        let manifest = CapsuleManifestV1::from_toml(&toml).unwrap();
        let errors = manifest.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidName(_))));
    }

    #[test]
    fn test_to_json_roundtrip() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();
        let json = manifest.to_json().unwrap();
        let manifest2 = CapsuleManifestV1::from_json(&json).unwrap();

        assert_eq!(manifest.name, manifest2.name);
        assert_eq!(manifest.version, manifest2.version);
    }

    #[test]
    fn test_parse_build_and_isolation_sections() {
        let toml = format!(
            "{}\n\n[build]\nexclude_libs = [\"**/site-packages/torch/**\"]\ngpu = true\n\n[build.lifecycle]\nprepare = \"npm ci\"\nbuild = \"npm run build\"\npackage = \"ato pack\"\n\n[build.inputs]\nlockfiles = [\"package-lock.json\"]\ntoolchain = \"node:20\"\n\n[build.outputs]\ncapsule = \"dist/*.capsule\"\nsha256 = true\nblake3 = true\nattestation = true\nsignature = true\n\n[build.policy]\nrequire_attestation = true\nrequire_did_signature = true\n\n[isolation]\nallow_env = [\"LD_LIBRARY_PATH\", \"HF_TOKEN\"]\n",
            VALID_TOML
        );

        let manifest = CapsuleManifestV1::from_toml(&toml).unwrap();

        let build = manifest.build.as_ref().expect("build section should exist");
        assert!(build.gpu);
        assert_eq!(build.exclude_libs, vec!["**/site-packages/torch/**"]);
        assert_eq!(
            build
                .lifecycle
                .as_ref()
                .and_then(|v| v.prepare.as_deref()),
            Some("npm ci")
        );
        assert_eq!(
            build.inputs.as_ref().and_then(|v| v.toolchain.as_deref()),
            Some("node:20")
        );
        assert_eq!(
            build
                .outputs
                .as_ref()
                .and_then(|v| v.capsule.as_deref()),
            Some("dist/*.capsule")
        );
        assert_eq!(
            build
                .policy
                .as_ref()
                .and_then(|v| v.require_attestation),
            Some(true)
        );

        let isolation = manifest
            .isolation
            .as_ref()
            .expect("isolation section should exist");
        assert_eq!(isolation.allow_env, vec!["LD_LIBRARY_PATH", "HF_TOKEN"]);
    }

    #[test]
    fn test_display_name() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();
        assert_eq!(manifest.display_name(), "Qwen3 8B (MLX)");
    }

    #[test]
    fn test_can_fallback_to_cloud() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();
        assert!(manifest.can_fallback_to_cloud());
    }

    #[test]
    fn test_vram_parsing() {
        let manifest = CapsuleManifestV1::from_toml(VALID_TOML).unwrap();
        let vram_min = manifest.requirements.vram_min_bytes().unwrap();
        assert_eq!(vram_min, Some(6 * 1024 * 1024 * 1024)); // 6GB
    }

    #[test]
    fn test_rejects_legacy_execution_section_toml() {
        let legacy_manifest = r#"
schema_version = "0.2"
name = "legacy-app"
version = "0.1.0"
type = "app"
default_target = "cli"

[execution]
runtime = "source"
entrypoint = "main.py"

[targets.cli]
runtime = "source"
entrypoint = "main.py"
"#;

        let error = CapsuleManifestV1::from_toml(legacy_manifest).unwrap_err();
        assert!(error
            .to_string()
            .contains("legacy [execution] section is not supported in schema_version=0.2"));
    }

    #[test]
    fn test_rejects_legacy_execution_section_json() {
        let legacy_manifest = r#"{
  "schema_version": "0.2",
  "name": "legacy-app",
  "version": "0.1.0",
  "type": "app",
  "default_target": "cli",
  "execution": {
    "runtime": "source",
    "entrypoint": "main.py"
  },
  "targets": {
    "cli": {
      "runtime": "source",
      "entrypoint": "main.py"
    }
  }
}"#;

        let error = CapsuleManifestV1::from_json(legacy_manifest).unwrap_err();
        assert!(error
            .to_string()
            .contains("legacy [execution] section is not supported in schema_version=0.2"));
    }

    #[test]
    fn test_is_kebab_case() {
        assert!(is_kebab_case("valid-name"));
        assert!(is_kebab_case("name123"));
        assert!(is_kebab_case("a1"));
        assert!(!is_kebab_case("Invalid"));
        assert!(!is_kebab_case("-invalid"));
        assert!(!is_kebab_case("invalid-"));
        assert!(!is_kebab_case(""));
    }

    #[test]
    fn test_is_semver() {
        assert!(is_semver("1.0.0"));
        assert!(is_semver("0.1.0"));
        assert!(is_semver("1.0.0-alpha"));
        assert!(!is_semver("1.0"));
        assert!(!is_semver("v1.0.0"));
    }
}
