//! IPC Bearer Token management — generation, validation, and revocation.
//!
//! Each IPC client receives a short-lived bearer token scoped to specific
//! capabilities. Tokens are validated using constant-time comparison to
//! prevent timing attacks.
//!
//! ## Token Lifecycle
//!
//! 1. Client requests connection → Broker generates token
//! 2. Client includes token in every `capsule/invoke` call
//! 3. On client disconnect → token is revoked
//! 4. After TTL expiry → token is automatically invalid

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Default token TTL: 24 hours.
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Token length in bytes (32 bytes = 256 bits of entropy).
const TOKEN_LENGTH: usize = 32;

/// An IPC bearer token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcToken {
    /// The token value (hex-encoded).
    pub value: String,
    /// Capabilities this token is scoped to.
    pub scoped_capabilities: Vec<String>,
    /// When the token was created.
    #[serde(skip)]
    pub created_at: Option<Instant>,
    /// Time-to-live duration.
    #[serde(skip)]
    pub ttl: Duration,
}

impl IpcToken {
    /// Check if this token has expired.
    pub fn is_expired(&self) -> bool {
        self.created_at
            .map(|t| t.elapsed() > self.ttl)
            .unwrap_or(true)
    }
}

/// Token manager — thread-safe token store.
#[derive(Debug, Clone)]
pub struct TokenManager {
    inner: Arc<Mutex<TokenManagerInner>>,
}

#[derive(Debug)]
struct TokenManagerInner {
    /// Active tokens: value → IpcToken
    tokens: HashMap<String, IpcToken>,
    /// Default TTL for new tokens
    default_ttl: Duration,
}

impl TokenManager {
    /// Create a new token manager with default TTL.
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TOKEN_TTL)
    }

    /// Create a new token manager with custom TTL.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TokenManagerInner {
                tokens: HashMap::new(),
                default_ttl: ttl,
            })),
        }
    }

    /// Generate a new token with the given capabilities.
    ///
    /// Returns the generated `IpcToken`.
    pub fn generate(&self, capabilities: Vec<String>) -> IpcToken {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; TOKEN_LENGTH];
        rng.fill(&mut bytes);
        let value = hex::encode(bytes);

        let inner = self.inner.lock().expect("token manager lock poisoned");
        let ttl = inner.default_ttl;
        drop(inner);

        let token = IpcToken {
            value: value.clone(),
            scoped_capabilities: capabilities,
            created_at: Some(Instant::now()),
            ttl,
        };

        let mut inner = self.inner.lock().expect("token manager lock poisoned");
        inner.tokens.insert(value, token.clone());
        debug!(
            capabilities = ?token.scoped_capabilities,
            ttl_secs = ttl.as_secs(),
            "Generated new IPC token"
        );

        token
    }

    /// Validate a token value.
    ///
    /// Uses constant-time comparison to prevent timing attacks.
    /// Returns the matching `IpcToken` if valid and not expired.
    pub fn validate(&self, token_value: &str) -> Option<IpcToken> {
        let inner = self.inner.lock().expect("token manager lock poisoned");

        // Constant-time lookup: iterate all tokens to prevent timing leaks
        let mut found: Option<&IpcToken> = None;
        for (stored_value, token) in &inner.tokens {
            if constant_time_eq(stored_value.as_bytes(), token_value.as_bytes()) {
                found = Some(token);
            }
        }

        match found {
            Some(token) if !token.is_expired() => {
                debug!("IPC token validated successfully");
                Some(token.clone())
            }
            Some(_) => {
                warn!("IPC token has expired");
                None
            }
            None => {
                warn!("IPC token not found or invalid");
                None
            }
        }
    }

    /// Revoke a token by its value.
    ///
    /// Returns `true` if the token was found and removed.
    pub fn revoke(&self, token_value: &str) -> bool {
        let mut inner = self.inner.lock().expect("token manager lock poisoned");
        let removed = inner.tokens.remove(token_value).is_some();
        if removed {
            debug!("IPC token revoked");
        }
        removed
    }

    /// Revoke all tokens.
    pub fn revoke_all(&self) {
        let mut inner = self.inner.lock().expect("token manager lock poisoned");
        let count = inner.tokens.len();
        inner.tokens.clear();
        debug!(count, "All IPC tokens revoked");
    }

    /// Remove expired tokens (garbage collection).
    ///
    /// Returns the number of tokens removed.
    pub fn gc(&self) -> usize {
        let mut inner = self.inner.lock().expect("token manager lock poisoned");
        let before = inner.tokens.len();
        inner.tokens.retain(|_, token| !token.is_expired());
        let removed = before - inner.tokens.len();
        if removed > 0 {
            debug!(removed, "Garbage collected expired IPC tokens");
        }
        removed
    }

    /// Number of active tokens.
    pub fn active_count(&self) -> usize {
        let inner = self.inner.lock().expect("token manager lock poisoned");
        inner.tokens.len()
    }
}

impl Default for TokenManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Constant-time byte comparison to prevent timing attacks.
///
/// Unlike a naive implementation, this function does NOT early-return
/// on length mismatch. Instead, it pads both inputs to a fixed width
/// (TOKEN_COMPARE_WIDTH) so that comparison time is independent of
/// both content and length, eliminating the length-oracle side channel.
const TOKEN_COMPARE_WIDTH: usize = 256;

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Length mismatch contributes to the result but does NOT cause
    // an early return — the full TOKEN_COMPARE_WIDTH loop always runs.
    let len_match: u8 = if a.len() == b.len() { 0 } else { 1 };

    let mut result: u8 = len_match;
    for i in 0..TOKEN_COMPARE_WIDTH {
        // Use the same sentinel (0x00) for both when past-end so that
        // the padding region itself does not produce XOR differences.
        // Length difference is already captured by `len_match` above.
        let x = if i < a.len() { a[i] } else { 0x00 };
        let y = if i < b.len() { b[i] } else { 0x00 };
        result |= x ^ y;
    }
    result == 0
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_token() {
        let manager = TokenManager::new();
        let token = manager.generate(vec!["greet".to_string()]);

        assert_eq!(token.value.len(), TOKEN_LENGTH * 2); // hex-encoded
        assert_eq!(token.scoped_capabilities, vec!["greet"]);
        assert!(!token.is_expired());
    }

    #[test]
    fn test_validate_valid_token() {
        let manager = TokenManager::new();
        let token = manager.generate(vec!["greet".to_string()]);

        let validated = manager.validate(&token.value);
        assert!(validated.is_some());
        assert_eq!(validated.unwrap().scoped_capabilities, vec!["greet"]);
    }

    #[test]
    fn test_validate_invalid_token() {
        let manager = TokenManager::new();
        manager.generate(vec!["greet".to_string()]);

        let validated = manager.validate("invalid_token_value");
        assert!(validated.is_none());
    }

    #[test]
    fn test_validate_expired_token() {
        // Use 1ms TTL to ensure immediate expiry
        let manager = TokenManager::with_ttl(Duration::from_millis(1));
        let token = manager.generate(vec!["greet".to_string()]);

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(10));

        let validated = manager.validate(&token.value);
        assert!(validated.is_none(), "Expired token should be invalid");
    }

    #[test]
    fn test_revoke_token() {
        let manager = TokenManager::new();
        let token = manager.generate(vec!["greet".to_string()]);

        assert!(manager.revoke(&token.value));
        assert!(manager.validate(&token.value).is_none());
    }

    #[test]
    fn test_revoke_nonexistent_token() {
        let manager = TokenManager::new();
        assert!(!manager.revoke("nonexistent"));
    }

    #[test]
    fn test_revoke_all() {
        let manager = TokenManager::new();
        manager.generate(vec!["a".to_string()]);
        manager.generate(vec!["b".to_string()]);
        manager.generate(vec!["c".to_string()]);

        assert_eq!(manager.active_count(), 3);
        manager.revoke_all();
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_gc_removes_expired() {
        let manager = TokenManager::with_ttl(Duration::from_millis(1));
        manager.generate(vec!["a".to_string()]);
        manager.generate(vec!["b".to_string()]);

        std::thread::sleep(Duration::from_millis(10));

        let removed = manager.gc();
        assert_eq!(removed, 2);
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_constant_time_eq_no_length_oracle() {
        // Different-length inputs must NOT early-return; they must
        // still do the full constant-time comparison loop.
        assert!(!constant_time_eq(b"short", b"a_much_longer_string"));
        assert!(!constant_time_eq(b"a_much_longer_string", b"short"));
        // Same content but one is truncated
        assert!(!constant_time_eq(b"abcdef", b"abcde"));
        assert!(!constant_time_eq(b"abcde", b"abcdef"));
    }

    #[test]
    fn test_validate_different_length_token_rejected() {
        // Ensure that tokens of different length from stored tokens
        // are rejected without timing information leak.
        let manager = TokenManager::new();
        let _token = manager.generate(vec!["greet".to_string()]);
        // Try validating with a short string
        assert!(manager.validate("abc").is_none());
        // Try validating with a very long string
        assert!(manager.validate(&"x".repeat(1000)).is_none());
    }

    #[test]
    fn test_token_uniqueness() {
        let manager = TokenManager::new();
        let t1 = manager.generate(vec![]);
        let t2 = manager.generate(vec![]);
        assert_ne!(t1.value, t2.value, "Each token should be unique");
    }
}
