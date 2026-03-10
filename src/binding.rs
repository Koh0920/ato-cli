use anyhow::Result;
use std::net::IpAddr;
use std::path::Path;

use crate::ingress_proxy;
use crate::registry_store::{NewServiceBindingRecord, RegistryStore, ServiceBindingRecord};
use capsule_core::types::CapsuleManifest;

pub const SERVICE_BINDING_KIND_INGRESS: &str = "ingress";
pub const SERVICE_BINDING_KIND_SERVICE: &str = "service";
pub const SERVICE_BINDING_ADAPTER_REVERSE_PROXY: &str = "reverse_proxy";
pub const SERVICE_BINDING_ADAPTER_LOCAL_SERVICE: &str = "local_service";
pub const SERVICE_BINDING_TLS_MODE_DISABLED: &str = "disabled";
pub const SERVICE_BINDING_TLS_MODE_EXPLICIT: &str = "explicit";

#[derive(Debug, Clone)]
struct ServiceBindingContract {
    owner_scope: String,
    service_name: String,
    binding_kind: String,
    transport_kind: String,
    adapter_kind: String,
    tls_mode: String,
    allowed_callers: Vec<String>,
    target_hint: Option<String>,
}

pub fn open_binding_store() -> Result<RegistryStore> {
    let store_dir = capsule_core::config::config_dir()?.join("state");
    RegistryStore::open(&store_dir)
}

pub fn parse_binding_reference(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    trimmed.starts_with("binding-").then_some(trimmed)
}

pub fn host_service_binding_scope(manifest: &CapsuleManifest) -> Result<String> {
    manifest.host_service_binding_scope().ok_or_else(|| {
        anyhow::anyhow!(
            "manifest name or service_binding_scope is required before host-side service binding can be registered"
        )
    })
}

pub fn list_bindings(
    owner_scope: Option<&str>,
    service_name: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = open_binding_store()?;
    let records = store.list_service_bindings(owner_scope, service_name)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&records)?);
        return Ok(());
    }

    if records.is_empty() {
        println!("No host-side service bindings registered.");
        return Ok(());
    }

    println!(
        "{:<40} {:<20} {:<16} {:<10} {:<8} ENDPOINT",
        "BINDING ID", "OWNER SCOPE", "SERVICE", "KIND", "TLS"
    );
    for record in records {
        println!(
            "{:<40} {:<20} {:<16} {:<10} {:<8} {}",
            record.binding_id,
            record.owner_scope,
            record.service_name,
            record.binding_kind,
            record.tls_mode,
            record.endpoint_locator,
        );
    }
    Ok(())
}

pub fn inspect_binding(binding_ref: &str, json: bool) -> Result<()> {
    let binding_id = parse_binding_reference(binding_ref).unwrap_or(binding_ref);
    let store = open_binding_store()?;
    let record = store
        .find_service_binding_by_id(binding_id)?
        .ok_or_else(|| {
            anyhow::anyhow!("host-side service binding '{}' was not found", binding_id)
        })?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    println!("Binding ID: {}", record.binding_id);
    println!("Owner Scope: {}", record.owner_scope);
    println!("Service Name: {}", record.service_name);
    println!("Binding Kind: {}", record.binding_kind);
    println!("Transport Kind: {}", record.transport_kind);
    println!("Adapter Kind: {}", record.adapter_kind);
    println!("Endpoint Locator: {}", record.endpoint_locator);
    println!("TLS Mode: {}", record.tls_mode);
    if let Some(tls) = ingress_proxy::load_tls_bootstrap(&record.binding_id)? {
        println!("TLS Bootstrap: ready");
        println!("TLS Cert Path: {}", tls.cert_path.display());
        println!(
            "TLS Trust Installed: {}",
            if tls.system_trust_installed {
                "yes"
            } else {
                "no"
            }
        );
    } else if record.tls_mode == SERVICE_BINDING_TLS_MODE_EXPLICIT {
        println!("TLS Bootstrap: pending");
    }
    if !record.allowed_callers.is_empty() {
        println!("Allowed Callers: {}", record.allowed_callers.join(", "));
    }
    if let Some(target_hint) = record.target_hint.as_deref() {
        println!("Target Hint: {}", target_hint);
    }
    println!("Created At: {}", record.created_at);
    println!("Updated At: {}", record.updated_at);
    Ok(())
}

pub fn resolve_binding(
    owner_scope: &str,
    service_name: &str,
    binding_kind: &str,
    caller_service: Option<&str>,
    json: bool,
) -> Result<()> {
    let record = resolve_binding_record(owner_scope, service_name, binding_kind, caller_service)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    println!("Resolved Binding: {}", record.binding_id);
    println!("Owner Scope: {}", record.owner_scope);
    println!("Service Name: {}", record.service_name);
    println!("Binding Kind: {}", record.binding_kind);
    println!("Transport Kind: {}", record.transport_kind);
    println!("Endpoint Locator: {}", record.endpoint_locator);
    if let Some(caller_service) = caller_service.filter(|value| !value.trim().is_empty()) {
        println!("Caller Service: {}", caller_service.trim());
    }
    if !record.allowed_callers.is_empty() {
        println!("Allowed Callers: {}", record.allowed_callers.join(", "));
    }
    Ok(())
}

pub fn resolve_binding_record(
    owner_scope: &str,
    service_name: &str,
    binding_kind: &str,
    caller_service: Option<&str>,
) -> Result<ServiceBindingRecord> {
    let record = open_binding_store()?
        .resolve_service_binding(owner_scope, service_name, binding_kind, caller_service)?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "host-side service binding '{}:{}:{}' was not found",
                owner_scope,
                service_name,
                binding_kind
            )
        })?;
    Ok(record)
}

pub fn register_ingress_binding_from_manifest(
    manifest_path: &Path,
    service_name: &str,
    url: &str,
    json: bool,
) -> Result<()> {
    let record = register_ingress_binding(manifest_path, service_name, url)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    println!(
        "✅ Registered host-side ingress binding {}",
        record.binding_id
    );
    println!("   owner_scope: {}", record.owner_scope);
    println!("   service_name: {}", record.service_name);
    println!("   endpoint_locator: {}", record.endpoint_locator);
    println!("   tls_mode: {}", record.tls_mode);
    Ok(())
}

pub fn register_ingress_binding(
    manifest_path: &Path,
    service_name: &str,
    url: &str,
) -> Result<ServiceBindingRecord> {
    let manifest = load_manifest(manifest_path)?;
    let endpoint = normalize_endpoint_locator(url)?;
    let contract = ingress_binding_contract(&manifest, service_name, &endpoint)?;
    open_binding_store()?.register_service_binding(&NewServiceBindingRecord {
        owner_scope: contract.owner_scope,
        service_name: contract.service_name,
        binding_kind: contract.binding_kind,
        transport_kind: contract.transport_kind,
        adapter_kind: contract.adapter_kind,
        endpoint_locator: endpoint,
        tls_mode: contract.tls_mode,
        allowed_callers: contract.allowed_callers,
        target_hint: contract.target_hint,
    })
}

pub fn register_service_binding_from_manifest(
    manifest_path: &Path,
    service_name: &str,
    url: &str,
    json: bool,
) -> Result<()> {
    let record = register_service_binding(manifest_path, service_name, url)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    println!("✅ Registered local service binding {}", record.binding_id);
    println!("   owner_scope: {}", record.owner_scope);
    println!("   service_name: {}", record.service_name);
    println!("   endpoint_locator: {}", record.endpoint_locator);
    println!("   tls_mode: {}", record.tls_mode);
    Ok(())
}

pub fn register_service_binding(
    manifest_path: &Path,
    service_name: &str,
    url: &str,
) -> Result<ServiceBindingRecord> {
    let manifest = load_manifest(manifest_path)?;
    let endpoint = normalize_local_service_locator(url)?;
    let contract = local_service_binding_contract(&manifest, service_name, &endpoint)?;
    open_binding_store()?.register_service_binding(&NewServiceBindingRecord {
        owner_scope: contract.owner_scope,
        service_name: contract.service_name,
        binding_kind: contract.binding_kind,
        transport_kind: contract.transport_kind,
        adapter_kind: contract.adapter_kind,
        endpoint_locator: endpoint,
        tls_mode: contract.tls_mode,
        allowed_callers: contract.allowed_callers,
        target_hint: contract.target_hint,
    })
}

pub fn bootstrap_ingress_tls(
    binding_ref: &str,
    install_system_trust: bool,
    yes: bool,
    json: bool,
) -> Result<()> {
    let binding_id = parse_binding_reference(binding_ref).unwrap_or(binding_ref);
    let record = open_binding_store()?
        .find_service_binding_by_id(binding_id)?
        .ok_or_else(|| {
            anyhow::anyhow!("host-side service binding '{}' was not found", binding_id)
        })?;
    if record.binding_kind != SERVICE_BINDING_KIND_INGRESS {
        anyhow::bail!(
            "TLS bootstrap currently supports only ingress bindings (got '{}')",
            record.binding_kind
        );
    }
    if record.tls_mode != SERVICE_BINDING_TLS_MODE_EXPLICIT {
        anyhow::bail!(
            "binding '{}' does not require explicit TLS bootstrap because tls_mode={}.",
            record.binding_id,
            record.tls_mode
        );
    }

    let endpoint = reqwest::Url::parse(&record.endpoint_locator)?;
    let endpoint_host = endpoint.host_str().ok_or_else(|| {
        anyhow::anyhow!("binding '{}' endpoint is missing a host", record.binding_id)
    })?;
    let tls =
        ingress_proxy::bootstrap_tls(&record.binding_id, endpoint_host, install_system_trust, yes)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&tls)?);
        return Ok(());
    }

    println!("✅ Bootstrapped ingress TLS for {}", record.binding_id);
    println!("   endpoint_host: {}", tls.endpoint_host);
    println!("   cert_path: {}", tls.cert_path.display());
    println!("   key_path: {}", tls.key_path.display());
    println!(
        "   system_trust_installed: {}",
        if tls.system_trust_installed {
            "yes"
        } else {
            "no"
        }
    );
    Ok(())
}

pub fn serve_ingress_binding(
    binding_ref: &str,
    manifest_path: &Path,
    upstream_url: Option<&str>,
) -> Result<()> {
    let binding_id = parse_binding_reference(binding_ref).unwrap_or(binding_ref);
    let record = open_binding_store()?
        .find_service_binding_by_id(binding_id)?
        .ok_or_else(|| {
            anyhow::anyhow!("host-side service binding '{}' was not found", binding_id)
        })?;
    if record.binding_kind != SERVICE_BINDING_KIND_INGRESS {
        anyhow::bail!(
            "host-side proxy serving currently supports only ingress bindings (got '{}')",
            record.binding_kind
        );
    }

    let manifest = load_manifest(manifest_path)?;
    let upstream_locator = match upstream_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => normalize_endpoint_locator(value)?,
        None => derive_service_upstream_locator(&manifest, &record.service_name)?,
    };

    println!(
        "▶️  Serving ingress binding {} on {} -> {}",
        record.binding_id, record.endpoint_locator, upstream_locator
    );
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(ingress_proxy::serve(ingress_proxy::IngressProxyConfig {
        binding_id: record.binding_id,
        endpoint_locator: record.endpoint_locator,
        upstream_locator,
        tls_mode: record.tls_mode,
    }))
}

fn load_manifest(path: &Path) -> Result<CapsuleManifest> {
    let manifest_path = if path.is_dir() {
        path.join("capsule.toml")
    } else {
        path.to_path_buf()
    };

    if !manifest_path.exists() {
        anyhow::bail!("capsule.toml not found at {}", manifest_path.display());
    }

    CapsuleManifest::load_from_file(&manifest_path).map_err(Into::into)
}

fn normalize_endpoint_locator(raw: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(raw.trim())?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed.to_string()),
        scheme => anyhow::bail!(
            "host-side service binding endpoint must use http or https scheme (got '{}')",
            scheme
        ),
    }
}

fn normalize_local_service_locator(raw: &str) -> Result<String> {
    let normalized = normalize_endpoint_locator(raw)?;
    let parsed = reqwest::Url::parse(&normalized)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("local service binding endpoint is missing a host"))?;
    let is_loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
    if !is_loopback {
        anyhow::bail!(
            "local service binding endpoint must use a loopback host such as localhost or 127.0.0.1"
        );
    }
    Ok(normalized)
}

fn service_contract(
    manifest: &CapsuleManifest,
    service_name: &str,
) -> Result<ServiceBindingContract> {
    let service = manifest
        .services
        .as_ref()
        .and_then(|services| services.get(service_name))
        .ok_or_else(|| {
            anyhow::anyhow!("service '{}' is not declared in the manifest", service_name)
        })?;

    Ok(ServiceBindingContract {
        owner_scope: host_service_binding_scope(manifest)?,
        service_name: service_name.to_string(),
        binding_kind: String::new(),
        transport_kind: String::new(),
        adapter_kind: String::new(),
        tls_mode: String::new(),
        allowed_callers: service
            .network
            .as_ref()
            .map(|network| network.allow_from.clone())
            .unwrap_or_default(),
        target_hint: service.target.clone(),
    })
}

fn ingress_binding_contract(
    manifest: &CapsuleManifest,
    service_name: &str,
    endpoint_locator: &str,
) -> Result<ServiceBindingContract> {
    let service = manifest
        .services
        .as_ref()
        .and_then(|services| services.get(service_name))
        .ok_or_else(|| {
            anyhow::anyhow!("service '{}' is not declared in the manifest", service_name)
        })?;

    let is_publishable = service_name == "main"
        || service
            .network
            .as_ref()
            .map(|network| network.publish)
            .unwrap_or(false);
    if !is_publishable {
        anyhow::bail!(
            "service '{}' is not marked for host-side publication; set services.{}.network.publish = true or use 'main'",
            service_name,
            service_name
        );
    }

    let transport_kind = if endpoint_locator.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let tls_mode = if transport_kind == "https" {
        SERVICE_BINDING_TLS_MODE_EXPLICIT
    } else {
        SERVICE_BINDING_TLS_MODE_DISABLED
    };

    let mut contract = service_contract(manifest, service_name)?;
    contract.binding_kind = SERVICE_BINDING_KIND_INGRESS.to_string();
    contract.transport_kind = transport_kind.to_string();
    contract.adapter_kind = SERVICE_BINDING_ADAPTER_REVERSE_PROXY.to_string();
    contract.tls_mode = tls_mode.to_string();
    Ok(contract)
}

fn local_service_binding_contract(
    manifest: &CapsuleManifest,
    service_name: &str,
    endpoint_locator: &str,
) -> Result<ServiceBindingContract> {
    let transport_kind = if endpoint_locator.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let tls_mode = if transport_kind == "https" {
        SERVICE_BINDING_TLS_MODE_EXPLICIT
    } else {
        SERVICE_BINDING_TLS_MODE_DISABLED
    };

    let mut contract = service_contract(manifest, service_name)?;
    contract.binding_kind = SERVICE_BINDING_KIND_SERVICE.to_string();
    contract.transport_kind = transport_kind.to_string();
    contract.adapter_kind = SERVICE_BINDING_ADAPTER_LOCAL_SERVICE.to_string();
    contract.tls_mode = tls_mode.to_string();
    Ok(contract)
}

fn derive_service_upstream_locator(
    manifest: &CapsuleManifest,
    service_name: &str,
) -> Result<String> {
    let service = manifest
        .services
        .as_ref()
        .and_then(|services| services.get(service_name))
        .ok_or_else(|| {
            anyhow::anyhow!("service '{}' is not declared in the manifest", service_name)
        })?;
    let target_label = service
        .target
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(manifest.default_target.trim());
    if target_label.is_empty() {
        anyhow::bail!(
            "service '{}' does not resolve to a target with a listening port",
            service_name
        );
    }
    let port = manifest
        .targets
        .as_ref()
        .and_then(|targets| {
            targets
                .named
                .get(target_label)
                .and_then(|target| target.port)
                .or(targets.port)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "service '{}' target '{}' does not declare a listening port required for ingress proxying",
                service_name,
                target_label
            )
        })?;
    Ok(format!("http://127.0.0.1:{port}/"))
}

#[cfg(test)]
mod tests {
    use super::{
        derive_service_upstream_locator, ingress_binding_contract, local_service_binding_contract,
        normalize_endpoint_locator, normalize_local_service_locator, parse_binding_reference,
        SERVICE_BINDING_KIND_SERVICE,
    };
    use capsule_core::types::CapsuleManifest;

    #[test]
    fn parse_binding_reference_accepts_bare_binding_id() {
        assert_eq!(
            parse_binding_reference("binding-demo"),
            Some("binding-demo")
        );
        assert_eq!(parse_binding_reference("https://example.com"), None);
    }

    #[test]
    fn normalize_endpoint_locator_requires_http_or_https() {
        assert_eq!(
            normalize_endpoint_locator("https://example.com/api").expect("normalize https"),
            "https://example.com/api"
        );
        assert!(normalize_endpoint_locator("tcp://127.0.0.1:8080").is_err());
    }

    #[test]
    fn normalize_local_service_locator_requires_loopback_host() {
        assert_eq!(
            normalize_local_service_locator("http://127.0.0.1:8080/").expect("loopback"),
            "http://127.0.0.1:8080/"
        );
        assert!(normalize_local_service_locator("https://example.com/api").is_err());
    }

    #[test]
    fn ingress_binding_contract_carries_allow_from_metadata() {
        let manifest = CapsuleManifest::from_toml(
            r#"
schema_version = "0.2"
name = "demo-app"
version = "0.1.0"
type = "app"
default_target = "app"

[targets.app]
runtime = "oci"
image = "ghcr.io/example/app:latest"

[services.api]
target = "app"
network = { publish = true, allow_from = ["web", "worker"] }
"#,
        )
        .expect("manifest");

        let contract =
            ingress_binding_contract(&manifest, "api", "https://demo.local/").expect("contract");
        assert_eq!(contract.allowed_callers, vec!["web", "worker"]);
    }

    #[test]
    fn local_service_binding_contract_allows_non_published_services() {
        let manifest = CapsuleManifest::from_toml(
            r#"
schema_version = "0.2"
name = "demo-app"
version = "0.1.0"
type = "app"
default_target = "app"

[targets.app]
runtime = "oci"
image = "ghcr.io/example/app:latest"

[services.api]
target = "app"
network = { allow_from = ["web"] }
"#,
        )
        .expect("manifest");

        let contract = local_service_binding_contract(&manifest, "api", "http://127.0.0.1:4310/")
            .expect("contract");
        assert_eq!(contract.binding_kind, SERVICE_BINDING_KIND_SERVICE);
        assert_eq!(contract.allowed_callers, vec!["web"]);
    }

    #[test]
    fn derive_service_upstream_locator_uses_target_port() {
        let manifest = CapsuleManifest::from_toml(
            r#"
schema_version = "0.2"
name = "demo-app"
version = "0.1.0"
type = "app"
default_target = "app"

[targets.app]
runtime = "oci"
image = "ghcr.io/example/app:latest"
port = 4310

[services.main]
target = "app"
network = { publish = true }
"#,
        )
        .expect("manifest");

        let upstream = derive_service_upstream_locator(&manifest, "main").expect("upstream");
        assert_eq!(upstream, "http://127.0.0.1:4310/");
    }
}
