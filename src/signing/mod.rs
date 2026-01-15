//! Signing Module for Capsule CLI
//!
//! Migrated from nacelle/src/verification/signing.rs
//! Handles Ed25519 signature creation and verification.

pub mod sign;
pub mod verify;

// Re-export common types
pub use sign::sign_bundle;
pub use verify::verify_bundle;
