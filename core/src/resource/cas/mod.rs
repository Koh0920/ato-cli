//! Content-Addressable Storage (CAS) client abstraction.
//!
//! Provides a unified interface for fetching and storing content-addressed blobs,
//! supporting both local filesystem and remote HTTP backends.

mod client;

pub use client::{create_cas_client_from_env, CasClient, HttpCasClient, LocalCasClient};
