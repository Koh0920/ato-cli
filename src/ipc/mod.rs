//! Capsule IPC Broker — Inter-Process Communication for Capsule Services
//!
//! This module implements the IPC Broker Core (Phase 13b), which coordinates
//! communication between capsule workloads across all runtimes (Source/OCI/Wasm).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    ato-cli (IPC Broker)                  │
//! ├─────────────────────────────────────────────────────────────┤
//! │  registry   — Service discovery and lifecycle tracking       │
//! │  token      — Bearer token generation, validation, revocation│
//! │  schema     — JSON Schema input validation                   │
//! │  jsonrpc    — JSON-RPC 2.0 wire protocol                     │
//! │  refcount   — Reference counting and idle-timeout management │
//! │  dag        — DAG integration for IPC dependency ordering     │
//! │  broker     — Main orchestrator (resolve → start → connect)  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Design Principles
//!
//! - **Smart Build, Dumb Runtime**: All IPC validation and orchestration
//!   happens here in ato-cli. nacelle only provides sandbox passthrough.
//! - **Universal Runtime**: IPC works across Source, OCI, and Wasm runtimes.
//! - **Process Boundary Pattern**: JSON-RPC 2.0 over Unix Domain Sockets.

// IPC modules will be fully integrated in Phase 13b.6+.
// Suppress dead_code warnings until then.
#[allow(dead_code)]
pub mod broker;
#[allow(dead_code)]
pub mod dag;
#[allow(dead_code)]
pub mod inject;
#[allow(dead_code)]
pub mod jsonrpc;
#[allow(dead_code)]
pub mod refcount;
#[allow(dead_code)]
pub mod registry;
#[allow(dead_code)]
pub mod schema;
#[allow(dead_code)]
pub mod token;
#[allow(dead_code)]
pub mod types;
#[allow(dead_code)]
pub mod validate;
