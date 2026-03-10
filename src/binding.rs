use anyhow::Result;
use std::path::Path;

use crate::registry_store::{NewServiceBindingRecord, RegistryStore, ServiceBindingRecord};
use capsule_core::types::CapsuleManifest;

pub const SERVICE_BINDING_KIND_INGRESS: &str = "ingress";
pub const SERVICE_BINDING_ADAPTER_REVERSE_PROXY: &str = "reverse_proxy";
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
    let contract = service_binding_contract(&manifest, service_name, &endpoint)?;
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
            "host-side ingress endpoint must use http or https scheme (got '{}')",
            scheme
        ),
    }
}

fn service_binding_contract(
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

    Ok(ServiceBindingContract {
        owner_scope: host_service_binding_scope(manifest)?,
        service_name: service_name.to_string(),
        binding_kind: SERVICE_BINDING_KIND_INGRESS.to_string(),
        transport_kind: transport_kind.to_string(),
        adapter_kind: SERVICE_BINDING_ADAPTER_REVERSE_PROXY.to_string(),
        tls_mode: tls_mode.to_string(),
        allowed_callers: service
            .network
            .as_ref()
            .map(|network| network.allow_from.clone())
            .unwrap_or_default(),
        target_hint: service.target.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{normalize_endpoint_locator, parse_binding_reference, service_binding_contract};
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
    fn service_binding_contract_carries_allow_from_metadata() {
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
            service_binding_contract(&manifest, "api", "https://demo.local/").expect("contract");
        assert_eq!(contract.allowed_callers, vec!["web", "worker"]);
    }
}
