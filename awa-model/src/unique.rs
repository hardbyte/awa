/// Compute a BLAKE3 unique key for job deduplication.
///
/// Hash inputs: kind + optional(queue) + optional(args_json) + optional(period_bucket).
/// Truncated to 16 bytes (128 bits) — birthday bound ~18 quintillion.
pub fn compute_unique_key(
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind.as_bytes());

    if let Some(queue) = queue {
        hasher.update(b"\x00");
        hasher.update(queue.as_bytes());
    }

    if let Some(args) = args {
        hasher.update(b"\x00");
        // serde_json::Value uses Map<String, Value> backed by BTreeMap,
        // so keys iterate in sorted order. This is an implementation detail
        // of serde_json, not a guarantee. Python uses json.dumps(sort_keys=True)
        // explicitly. Cross-language golden tests verify consistency.
        let canonical = serde_json::to_vec(args).expect("JSON serialization should not fail");
        hasher.update(&canonical);
    }

    if let Some(period_bucket) = period_bucket {
        hasher.update(b"\x00");
        hasher.update(&period_bucket.to_le_bytes());
    }

    hasher.finalize().as_bytes()[..16].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_key_deterministic() {
        let key1 = compute_unique_key("send_email", None, None, None);
        let key2 = compute_unique_key("send_email", None, None, None);
        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 16);
    }

    #[test]
    fn test_unique_key_different_kinds() {
        let key1 = compute_unique_key("send_email", None, None, None);
        let key2 = compute_unique_key("process_payment", None, None, None);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_unique_key_with_queue() {
        let key1 = compute_unique_key("send_email", Some("default"), None, None);
        let key2 = compute_unique_key("send_email", Some("priority"), None, None);
        let key3 = compute_unique_key("send_email", None, None, None);
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_unique_key_with_args() {
        let args1 = serde_json::json!({"to": "a@b.com"});
        let args2 = serde_json::json!({"to": "c@d.com"});
        let key1 = compute_unique_key("send_email", None, Some(&args1), None);
        let key2 = compute_unique_key("send_email", None, Some(&args2), None);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_unique_key_with_period() {
        let key1 = compute_unique_key("send_email", None, None, Some(1000));
        let key2 = compute_unique_key("send_email", None, None, Some(1001));
        assert_ne!(key1, key2);
    }
}
