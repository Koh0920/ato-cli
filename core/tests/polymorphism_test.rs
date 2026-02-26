use capsule_core::schema_registry::SchemaRegistry;
use capsule_core::types::capsule_v1::{CapsuleManifestV1, PolymorphismConfig};
use serde_json::json;

#[test]
fn implements_schema_resolves_aliases() {
    let mut registry = SchemaRegistry::default();
    let schema_hash = SchemaRegistry::hash_schema_value(&json!({"type":"todo"})).unwrap();
    registry.register_alias("std.todo.v1", &schema_hash);

    let mut manifest = CapsuleManifestV1::from_json(
        r#"{
        "schema_version":"1.0",
        "name":"todo-app",
        "version":"0.1.0",
        "type":"app",
        "execution":{"runtime":"source","entrypoint":"main.py","env":{},"startup_timeout":30},
        "requirements":{"platform":[],"dependencies":[]},
        "routing":{"weight":"light","fallback_to_cloud":false},
        "storage":{"volumes":[],"use_thin_provisioning":false},
        "polymorphism":{"implements":["std.todo.v1"]}
        }"#,
    )
    .unwrap();

    manifest.polymorphism = Some(PolymorphismConfig {
        implements: vec!["std.todo.v1".to_string()],
    });

    let is_match = manifest
        .implements_schema("std.todo.v1", &registry)
        .unwrap();
    assert!(is_match);
}
