//! Python bindings for `awa_model::callback_contract`.
//!
//! These wrappers exist so user-owned callback receivers written in Python
//! (FastAPI, Starlette, Flask, etc.) verify signatures and build URLs against
//! the exact same Rust implementation the worker side uses. See ADR-027 and
//! `docs/callback-receivers.md`.

use awa_model::callback_contract;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

fn secret_array(secret: &Bound<'_, PyBytes>) -> PyResult<[u8; 32]> {
    let bytes = secret.as_bytes();
    if bytes.len() != 32 {
        return Err(PyValueError::new_err(format!(
            "callback secret must be exactly 32 bytes (got {})",
            bytes.len()
        )));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    Ok(out)
}

/// Sign a callback id with the shared secret. Returns the lowercase hex of
/// the BLAKE3 keyed-hash, identical to what the worker side embeds in the
/// `X-Awa-Signature` header.
#[pyfunction]
pub fn sign_callback(secret: &Bound<'_, PyBytes>, callback_id: &str) -> PyResult<String> {
    let key = secret_array(secret)?;
    Ok(callback_contract::sign(&key, callback_id))
}

/// Verify that `signature` is the BLAKE3 keyed-hash of `callback_id` under
/// the shared secret. Returns `True` only when the signature is well-formed
/// hex and equals the expected value; comparison is constant-time.
#[pyfunction]
pub fn verify_callback(
    secret: &Bound<'_, PyBytes>,
    callback_id: &str,
    signature: &str,
) -> PyResult<bool> {
    let key = secret_array(secret)?;
    Ok(callback_contract::verify(&key, callback_id, signature))
}

/// Build the receiver URL for a given callback action. Output is normalized
/// so callers cannot accidentally produce double slashes or drop the leading
/// `/`. `action` should be `"complete"`, `"fail"`, or `"heartbeat"`.
#[pyfunction]
pub fn build_callback_url(base: &str, prefix: &str, callback_id: &str, action: &str) -> String {
    callback_contract::callback_url(base, prefix, callback_id, action)
}
