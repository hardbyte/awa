//! HTTP callback ingress contract — wire types, signing, header names.
//!
//! Shared between:
//! - `awa-worker`'s [`HttpWorker`](../../../awa_worker/http_worker/index.html)
//!   which signs the outgoing callback id when posting to a function endpoint.
//! - `awa-ui`'s built-in callback handler, which verifies incoming requests.
//! - User-owned callback receivers (FastAPI/axum/etc.) that want to share the
//!   exact same authentication and payload contract — see ADR-027.
//!
//! See ADR-018 (HTTP worker) and ADR-027 (callback ingress as a deployable
//! surface).
//!
//! The signature scheme is BLAKE3 keyed-hash over the UTF-8 bytes of the
//! callback id, rendered as lowercase hex. Verification uses the constant-time
//! `PartialEq` implementation on `blake3::Hash`.
//!
//! ```no_run
//! use awa_model::callback_contract::{sign, verify, SIGNATURE_HEADER};
//!
//! let secret = [7u8; 32];
//! let id = "550e8400-e29b-41d4-a716-446655440000";
//! let signature = sign(&secret, id);
//! assert!(verify(&secret, id, &signature));
//! assert_eq!(SIGNATURE_HEADER, "X-Awa-Signature");
//! ```

use serde::{Deserialize, Serialize};

/// HTTP header carrying the hex-encoded BLAKE3 keyed-hash signature of the
/// callback id.
pub const SIGNATURE_HEADER: &str = "X-Awa-Signature";

/// Default heartbeat timeout in seconds (1 hour). Used when an inbound
/// heartbeat request omits `timeout_seconds`.
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: f64 = 3600.0;

/// Sign a callback id with the shared secret. Returns lowercase hex.
pub fn sign(secret: &[u8; 32], callback_id: &str) -> String {
    blake3::keyed_hash(secret, callback_id.as_bytes())
        .to_hex()
        .to_string()
}

/// Verify a provided signature against the expected one. Returns `true` only
/// when the signature is well-formed hex and equals the keyed hash of the
/// callback id. Comparison is constant-time.
pub fn verify(secret: &[u8; 32], callback_id: &str, signature: &str) -> bool {
    let expected = blake3::keyed_hash(secret, callback_id.as_bytes());
    blake3::Hash::from_hex(signature).is_ok_and(|h| expected == h)
}

/// Body of `POST /api/callbacks/{id}/complete`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CompletePayload {
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

/// Body of `POST /api/callbacks/{id}/fail`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FailPayload {
    pub error: String,
}

/// Body of `POST /api/callbacks/{id}/heartbeat`. `timeout_seconds` defaults to
/// [`DEFAULT_HEARTBEAT_TIMEOUT_SECS`] when omitted.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HeartbeatPayload {
    #[serde(default = "default_heartbeat_timeout")]
    pub timeout_seconds: f64,
}

impl Default for HeartbeatPayload {
    fn default() -> Self {
        Self {
            timeout_seconds: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
        }
    }
}

fn default_heartbeat_timeout() -> f64 {
    DEFAULT_HEARTBEAT_TIMEOUT_SECS
}

/// Response body returned by the built-in callback handler. User-owned
/// receivers SHOULD mirror this shape so generic clients work against any
/// receiver implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackResponse {
    pub id: i64,
    pub state: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let secret = [3u8; 32];
        let id = "abc";
        let sig = sign(&secret, id);
        assert!(verify(&secret, id, &sig));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let secret = [3u8; 32];
        let other = [4u8; 32];
        let id = "abc";
        let sig = sign(&secret, id);
        assert!(!verify(&other, id, &sig));
    }

    #[test]
    fn verify_rejects_wrong_id() {
        let secret = [3u8; 32];
        let sig = sign(&secret, "abc");
        assert!(!verify(&secret, "abd", &sig));
    }

    #[test]
    fn verify_rejects_malformed_signature() {
        let secret = [3u8; 32];
        assert!(!verify(&secret, "abc", "not-hex"));
        assert!(!verify(&secret, "abc", ""));
    }

    #[test]
    fn heartbeat_default_uses_constant() {
        let payload: HeartbeatPayload =
            serde_json::from_str("{}").expect("empty object should deserialize");
        assert_eq!(payload.timeout_seconds, DEFAULT_HEARTBEAT_TIMEOUT_SECS);
    }
}
