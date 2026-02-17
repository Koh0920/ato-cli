//! Guest E2E tests
//!
//! These tests verify the Guest protocol implementation for `.sync` file operations.
//! They test permission enforcement, payload read/write, context operations, and WASM execution.

use assert_cmd::Command;
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use sync_runtime::{
    encode_payload_base64, GuestAction, GuestContext, GuestContextRole, GuestMode, GuestPermission,
    GuestRequest, GuestResponse, GUEST_PROTOCOL_VERSION,
};
use tempfile::TempDir;
use wat::parse_str as wat_parse;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

fn create_test_sync_file(
    temp_dir: &PathBuf,
    payload: &[u8],
    write_allowed: bool,
    wasm_bytes: Option<Vec<u8>>,
) -> PathBuf {
    let manifest_toml = format!(
        r#"
[sync]
version = "1.2"
content_type = "application/octet-stream"
display_ext = "bin"

[meta]
created_by = "Capsule Guest E2E"
created_at = "2099-01-23T12:00:00Z"
hash_algo = "blake3"

[policy]
ttl = 3600
timeout = 30

[permissions]
allow_hosts = []
allow_env = []

[ownership]
owner_capsule = "did:key:test"
write_allowed = {}
"#,
        if write_allowed { "true" } else { "false" }
    );

    let sync_path = temp_dir.join("guest-e2e.sync");
    let file = File::create(&sync_path).unwrap();
    let mut zip = ZipWriter::new(file);

    let options: FileOptions<()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    zip.start_file("manifest.toml", options).unwrap();
    zip.write_all(manifest_toml.as_bytes()).unwrap();

    zip.start_file("payload", options).unwrap();
    zip.write_all(payload).unwrap();

    zip.start_file("context.json", options).unwrap();
    zip.write_all(br#"{"ok":true}"#).unwrap();

    if let Some(wasm) = wasm_bytes {
        zip.start_file("sync.wasm", options).unwrap();
        zip.write_all(&wasm).unwrap();
    }

    zip.finish().unwrap();

    sync_path
}

fn run_guest(sync_path: &PathBuf, request: &GuestRequest) -> GuestResponse {
    let request_json = serde_json::to_string(request).unwrap();
    let mut cmd = Command::cargo_bin("ato").unwrap();
    let output = cmd
        .arg("guest")
        .arg(sync_path)
        .write_stdin(request_json)
        .output()
        .unwrap();

    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn guest_read_payload_returns_base64() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![0, 1, 2, 255, 16, 32];
    let wasm_bytes = wat_parse(
        r#"(module
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 8) "ok")
          (func (export "_start")
            (i32.store (i32.const 0) (i32.const 8))
            (i32.store (i32.const 4) (i32.const 2))
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
            drop)
        )"#,
    )
    .unwrap();
    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        &payload,
        false,
        Some(wasm_bytes),
    );

    let context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Consumer,
        permissions: GuestPermission {
            can_read_payload: true,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "req-1".to_string(),
        action: GuestAction::ReadPayload,
        context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    if !response.ok {
        panic!("execute_wasm failed: {:?}", response.error);
    }

    let result = response.result.unwrap();
    let payload_b64 = result.as_str().unwrap();
    assert_eq!(payload_b64, encode_payload_base64(&payload));
}

#[test]
fn guest_write_payload_accepts_base64() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![1, 2, 3];
    let wasm_bytes = wat_parse(
        r#"(module
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 8) "ok")
          (func (export "_start")
            (i32.store (i32.const 0) (i32.const 8))
            (i32.store (i32.const 4) (i32.const 2))
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
            drop)
        )"#,
    )
    .unwrap();
    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        &payload,
        true,
        Some(wasm_bytes),
    );

    let new_payload = vec![9, 8, 7, 6, 5];
    let payload_b64 = encode_payload_base64(&new_payload);

    let context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: true,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "req-2".to_string(),
        action: GuestAction::WritePayload,
        context,
        input: Value::String(payload_b64),
    };

    let response = run_guest(&sync_path, &request);
    if !response.ok {
        panic!("execute_wasm failed: {:?}", response.error);
    }

    let file = File::open(&sync_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();
    let mut payload_file = archive.by_name("payload").unwrap();
    let mut buffer = Vec::new();
    payload_file.read_to_end(&mut buffer).unwrap();

    assert_eq!(buffer, new_payload);
}

#[test]
fn guest_execute_wasm_runs_sync_module() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![7, 7, 7];

    let wat = r#"
(module
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory 1)
  (export "memory" (memory 0))
  (data (i32.const 8) "ok")
  (func (export "_start")
    (i32.store (i32.const 0) (i32.const 8))
    (i32.store (i32.const 4) (i32.const 2))
    (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
    drop)
)
"#;
    let wasm_bytes = wat_parse(wat).unwrap();

    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        &payload,
        true,
        Some(wasm_bytes),
    );

    let context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: true,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "req-wasm".to_string(),
        action: GuestAction::ExecuteWasm,
        context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    if !response.ok {
        panic!("execute_wasm failed: {:?}", response.error);
    }

    let result = response.result.unwrap();
    let payload_b64 = result.as_str().unwrap();
    assert_eq!(payload_b64, encode_payload_base64(b"ok"));
}

#[test]
fn guest_update_payload_requires_write_allowed() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![1, 2, 3];
    let sync_path = create_test_sync_file(&temp_dir.path().to_path_buf(), &payload, false, None);

    let new_payload = vec![9, 9, 9];
    let payload_b64 = encode_payload_base64(&new_payload);

    let context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: true,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "req-update".to_string(),
        action: GuestAction::UpdatePayload,
        context,
        input: Value::String(payload_b64),
    };

    let response = run_guest(&sync_path, &request);
    assert!(!response.ok);
}

#[test]
fn guest_read_context_respects_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let wasm_bytes = wat_parse(
        r#"(module
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 8) "ok")
          (func (export "_start")
            (i32.store (i32.const 0) (i32.const 8))
            (i32.store (i32.const 4) (i32.const 2))
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
            drop)
        )"#,
    )
    .unwrap();
    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        b"payload",
        false,
        Some(wasm_bytes),
    );

    let denied_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Consumer,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "read-context-denied".to_string(),
        action: GuestAction::ReadContext,
        context: denied_context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    assert!(!response.ok);

    let allowed_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Consumer,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: true,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "read-context-allowed".to_string(),
        action: GuestAction::ReadContext,
        context: allowed_context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    assert!(response.ok);
    let result = response.result.unwrap();
    assert!(result.get("ok").is_some());
}

#[test]
fn guest_write_context_respects_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let wasm_bytes = wat_parse(
        r#"(module
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 8) "ok")
          (func (export "_start")
            (i32.store (i32.const 0) (i32.const 8))
            (i32.store (i32.const 4) (i32.const 2))
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
            drop)
        )"#,
    )
    .unwrap();
    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        b"payload",
        false,
        Some(wasm_bytes),
    );

    let denied_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "write-context-denied".to_string(),
        action: GuestAction::WriteContext,
        context: denied_context,
        input: serde_json::json!({"updated": true}),
    };

    let response = run_guest(&sync_path, &request);
    assert!(!response.ok);

    let allowed_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: true,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "write-context-allowed".to_string(),
        action: GuestAction::WriteContext,
        context: allowed_context,
        input: serde_json::json!({"updated": true}),
    };

    let response = run_guest(&sync_path, &request);
    assert!(response.ok);
}

#[test]
fn guest_execute_wasm_requires_owner_and_permission() {
    let temp_dir = TempDir::new().unwrap();
    let payload = vec![7, 7, 7];

    let wat = r#"
(module
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory 1)
  (export "memory" (memory 0))
  (data (i32.const 8) "ok")
  (func (export "_start")
    (i32.store (i32.const 0) (i32.const 8))
    (i32.store (i32.const 4) (i32.const 2))
    (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12))
    drop)
)
"#;
    let wasm_bytes = wat_parse(wat).unwrap();

    let sync_path = create_test_sync_file(
        &temp_dir.path().to_path_buf(),
        &payload,
        true,
        Some(wasm_bytes),
    );

    let denied_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Consumer,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: true,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "execute-wasm-denied".to_string(),
        action: GuestAction::ExecuteWasm,
        context: denied_context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    assert!(!response.ok);

    let denied_permission_context = GuestContext {
        mode: GuestMode::Headless,
        role: GuestContextRole::Owner,
        permissions: GuestPermission {
            can_read_payload: false,
            can_read_context: false,
            can_write_payload: false,
            can_write_context: false,
            can_execute_wasm: false,
            allowed_hosts: Vec::new(),
            allowed_env: Vec::new(),
        },
        sync_path: sync_path.to_string_lossy().to_string(),
        host_app: None,
    };

    let request = GuestRequest {
        version: GUEST_PROTOCOL_VERSION.to_string(),
        request_id: "execute-wasm-denied-perm".to_string(),
        action: GuestAction::ExecuteWasm,
        context: denied_permission_context,
        input: Value::Null,
    };

    let response = run_guest(&sync_path, &request);
    assert!(!response.ok);
}
