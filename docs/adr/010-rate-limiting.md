# ADR-010: Per-Queue Rate Limiting

## Status

Accepted

## Context

Dispatchers claim and execute jobs as fast as concurrency permits allow. Some downstream systems (APIs, email providers, payment gateways) impose rate limits. Without dispatch-side throttling, the worker saturates the external service, causing cascading retries and wasted work.

Operators currently work around this by setting `max_workers` very low, but that couples concurrency (how many jobs can run in parallel) with throughput (how many jobs start per second). A queue processing slow API calls might need 50 concurrent workers but only 10 new dispatches per second.

## Decision

Add an optional per-queue token bucket rate limiter. When `rate_limit` is set on a `QueueConfig`, the dispatcher gates the effective batch size by the number of available tokens before claiming from the database.

### Token Bucket

The implementation is a hand-rolled token bucket (~25 lines) with no external dependencies:

- **`max_rate`** (f64): sustained dispatch rate in jobs per second.
- **`burst`** (u32): maximum burst size. Defaults to `ceil(max_rate)` if 0.
- Tokens refill continuously based on elapsed wall-clock time.
- The bucket is local to each `Dispatcher` — single-threaded access, no contention.

### Integration

Rate limiting is applied in `poll_once()` after acquiring concurrency permits but before claiming from the database:

```
permits = acquire_permits()          // non-blocking
rate_available = token_bucket.available()
batch_size = min(permits, rate_available, 10)
if batch_size == 0: return           // rate limited, try next poll
jobs = claim(batch_size)             // DB claim
token_bucket.consume(jobs.len())
dispatch(jobs)
```

When `rate_limit` is `None`, the code path is a single `Option::map` — effectively zero cost.

### Configuration

```rust
pub struct QueueConfig {
    pub rate_limit: Option<RateLimit>,  // Default: None (unlimited)
    // ...
}

pub struct RateLimit {
    pub max_rate: f64,   // jobs/second
    pub burst: u32,      // max burst (0 = auto)
}
```

Build-time validation rejects `max_rate <= 0.0`.

## Consequences

### Positive

- **Decouples concurrency from throughput:** A queue can have 50 concurrent workers but limit new dispatches to 10/sec.
- **Zero dependencies:** No external crate needed.
- **Zero overhead when unused:** A single `Option` check per poll cycle.
- **Composable:** Works with both hard-reserved and weighted concurrency modes.

### Negative

- **Per-worker, not global:** Each worker instance has its own token bucket. With N workers, the effective global rate is `N * max_rate`. This is acceptable for most use cases; global rate limiting would require distributed coordination (Redis, advisory locks).
- **Approximate under low poll intervals:** The bucket refills on each poll. If `poll_interval` is longer than `1/max_rate`, tokens accumulate between polls and dispatch in bursts equal to the burst size. This is by design — the burst parameter controls this behavior.

## Relationship to ADR-019

Rate limiting sits above the storage engine: token consumption happens in
the dispatcher before the claim query fires, and composes identically with
canonical and queue-storage backends. The per-worker token bucket is
storage-plane-agnostic. See [ADR-019](019-queue-storage-redesign.md).
