# ADR-002: BLAKE3 Truncated to 16 Bytes for Unique Key Hashing

## Status

Accepted

## Context

Awa supports job deduplication via a uniqueness constraint (PRD section 6.6). When a job is inserted with `UniqueOpts`, a `unique_key` is computed from a combination of: kind, optional queue, optional args (canonical JSON), and optional time-period bucket. This key is stored as `BYTEA` and enforced via a conditional unique index in Postgres.

The hash function must satisfy:

1. **Deterministic across languages:** Rust and Python must produce identical keys for identical inputs. Awa has three locations that derive kind strings (proc-macro, `awa-model::kind`, `awa-python::args`) and the unique key computation runs in both Rust and Python.
2. **Low collision probability:** The system relies on hash uniqueness for correctness -- a collision means a legitimate job is rejected as a duplicate.
3. **Fast:** Unique key computation runs in the insert hot path.
4. **Compact:** The key is stored per-row and indexed; smaller is better for index performance.

Alternatives considered:

- **FNV-1a (64-bit):** Fast but only 64 bits. Birthday bound at ~2^32 (4 billion) insertions gives a 50% collision probability. For a queue processing millions of jobs per day, this is uncomfortably close.
- **SHA-256 (32 bytes):** Well-established, available everywhere, but 32 bytes per row is larger than necessary. Also slower than BLAKE3 by a factor of ~3-6x on modern hardware.
- **BLAKE3 full output (32 bytes):** Overkill for this use case. The birthday bound at 256 bits is astronomically high.

## Decision

Use BLAKE3 truncated to 16 bytes (128 bits) for unique key computation.

The implementation in `awa-model/src/unique.rs`:

```rust
pub fn compute_unique_key(kind, queue, args, period_bucket) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind.as_bytes());
    // ... domain separator bytes + optional fields ...
    hasher.finalize().as_bytes()[..16].to_vec()
}
```

### Birthday Bound Analysis

At 128 bits, the birthday bound is approximately 2^64 (~18.4 quintillion). To reach a 50% collision probability, you would need ~2^64 unique jobs. Even at 1 million jobs per second, it would take ~584,000 years to reach this threshold. At 10,000 jobs per second (a more realistic ceiling for a Postgres-backed queue), the timeline exceeds 58 million years.

For reference, at 16 bytes the probability of at least one collision among `n` keys is approximately `n^2 / (2 * 2^128)`:

| Jobs | Collision probability |
|---|---|
| 1 billion (10^9) | ~1.5 x 10^-21 |
| 1 trillion (10^12) | ~1.5 x 10^-15 |
| 1 quadrillion (10^15) | ~1.5 x 10^-9 |

This is far beyond the operational lifetime of any job queue.

### Cross-Language Consistency

BLAKE3 has official implementations for Rust (`blake3` crate) and Python (`blake3` PyPI package), both producing identical output for identical input. The canonical JSON serialization uses sorted keys in both languages:

- **Rust:** `serde_json::Value` uses `BTreeMap` for object keys, which iterates in sorted order.
- **Python:** `json.dumps(sort_keys=True)` is used explicitly.

Golden tests (PRD section 9.2) verify that Rust and Python produce identical unique keys for the same inputs.

Domain separator bytes (`\x00`) between fields prevent ambiguous concatenation (e.g., kind="ab" + queue="cd" vs kind="abc" + queue="d").

## Consequences

### Positive

- **16 bytes per row** is compact enough for excellent index performance. The `idx_awa_jobs_unique` partial index stays small.
- **BLAKE3 is fast:** ~3-6x faster than SHA-256, with SIMD acceleration on modern CPUs. Key computation adds negligible latency to inserts.
- **Birthday bound is astronomical:** 128 bits provides a collision probability that is effectively zero for any realistic workload.
- **Cross-language consistency:** Both Rust and Python use BLAKE3 with the same canonical serialization, verified by golden tests.

### Negative

- **Additional dependency:** The `blake3` crate is added to `awa-model`. However, it has no transitive dependencies of concern and is widely used.
- **Not a standard cryptographic commitment:** BLAKE3 truncated to 128 bits is not suitable for adversarial collision resistance (an attacker with ~2^64 work could find a collision). This is irrelevant for Awa -- uniqueness keys are not security-critical, and the input space (job kind + args) is not adversarially controlled.
- **Truncation discards bits:** Using the full 256-bit output would be "safer," but the 16 extra bytes per row and per index entry are wasted storage for no practical benefit.
