#![allow(unused_imports)]
//! Signing Module for Capsule CLI
//!
//! Migrated from nacelle/src/verification/signing.rs
//! Handles Ed25519 signature creation and verification.

pub mod sign;
pub mod verify;
pub mod legacy_signer;
#[cfg(feature = "manifest-signing")]
pub mod manifest_verifier;

// Re-export common types
pub use sign::{sign_artifact, sign_bundle};
pub use legacy_signer::CapsuleSigner;
#[cfg(feature = "manifest-signing")]
pub use manifest_verifier::ManifestVerifier;
