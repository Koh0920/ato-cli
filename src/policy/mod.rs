//! Policy Module for Capsule CLI
//!
//! Handles policy resolution (e.g., DNS → IP for egress rules)

pub mod egress_resolver;

pub use egress_resolver::resolve_egress_policy;
