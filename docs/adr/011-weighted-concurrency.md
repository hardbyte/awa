# ADR-011: Weighted/Opportunistic Queue Concurrency

## Status

Accepted

## Context

In the default hard-reserved mode, each queue owns an independent semaphore with `max_workers` permits. This is simple but wasteful: if queue A is idle, its 50 reserved permits sit unused while queue B is overloaded and capped at its own 10.

Operators want a **global worker pool** where queues share capacity, with:
1. **Minimum guarantees** — each queue gets at least N workers even under contention.
2. **Work-conserving allocation** — idle capacity flows to loaded queues.
3. **Weighted fairness** — overflow capacity is split proportionally to configured weights.

## Decision

### Two Concurrency Modes

The system supports two modes, selected at build time based on whether `global_max_workers` is set on `ClientBuilder`:

1. **HardReserved** (default): Each queue has its own `Semaphore(max_workers)`. Current behavior, unchanged.
2. **Weighted**: Each queue has a local `Semaphore(min_workers)` for guaranteed permits, plus access to a shared `OverflowPool` for additional capacity.

### OverflowPool

The `OverflowPool` is a centralized, work-conserving, weighted fair-share allocator:

```
overflow_capacity = global_max_workers - sum(min_workers)
```

Each dispatcher calls `try_acquire(queue, wanted)` to request overflow permits. The pool tracks:
- **held**: how many overflow permits each queue currently holds
- **demand**: how many each queue last requested (updated every poll)
- **weights**: configured per-queue weight (immutable)

Fair share is computed as:
```
contending_weight = sum of weights for queues with demand > 0 OR held > 0
my_fair_share = ceil(total * my_weight / contending_weight)
granted = min(wanted, available, fair_share - my_held)
```

This is work-conserving: when only one queue has demand, its fair share equals the entire pool. When multiple queues contend, capacity is split proportionally to weights.

### Permit-Before-Claim

The critical invariant is that every job marked `running` in the database must have a reserved execution slot. The previous design (snapshot available permits → claim → acquire) was racy with a shared pool.

The new `poll_once()` flow:
1. **Pre-acquire permits** (non-blocking `try_acquire_owned` + `OverflowPool::try_acquire`)
2. **Apply rate limit** (truncate permits if rate-limited)
3. **Claim from DB** (only as many as held permits)
4. **Release excess** if DB returned fewer jobs than permits
5. **Clear demand** if no jobs found (prevents empty queues from appearing as contenders)
6. **Dispatch** with one pre-acquired permit per job

### DispatchPermit

A `DispatchPermit` enum wraps the different permit types so the correct resource is released on drop:

- `Hard(OwnedSemaphorePermit)` — released by tokio on drop
- `Local(OwnedSemaphorePermit)` — released by tokio on drop
- `Overflow { pool, queue }` — calls `pool.release(queue, 1)` on drop

This ensures crash-safety: if a job task panics, the permit is still released.

### Configuration

```rust
pub struct QueueConfig {
    pub max_workers: u32,    // hard-reserved mode (default: 50)
    pub min_workers: u32,    // weighted mode floor (default: 0)
    pub weight: u32,         // weighted mode weight (default: 1)
    // ...
}

impl ClientBuilder {
    pub fn global_max_workers(self, max: u32) -> Self;
}
```

Build-time validation:
- `sum(min_workers) > global_max_workers` → `BuildError::MinWorkersExceedGlobal`
- `weight == 0` → `BuildError::InvalidWeight`

### Health Check

`QueueHealth` now includes a `QueueCapacity` enum:

```rust
pub enum QueueCapacity {
    HardReserved { max_workers: u32 },
    Weighted { min_workers: u32, weight: u32, overflow_held: u32 },
}
```

## Consequences

### Positive

- **Work-conserving:** Idle capacity is not wasted. One loaded queue uses the entire pool.
- **Fair under contention:** Weighted fair share prevents starvation.
- **Backward compatible:** Default behavior is unchanged (hard-reserved mode).
- **Non-blocking:** No dispatcher ever blocks waiting for another. `try_acquire` is always non-blocking.
- **Crash-safe:** Permits are released on drop, including panic unwind.

### Negative

- **Complexity:** The OverflowPool adds ~100 lines of synchronized state. The demand-tracking mechanism requires understanding to reason about correctness.
- **Per-instance only:** The overflow pool is local to a single worker process. Multiple worker instances each have their own pool — there is no cross-instance coordination. This is the same limitation as hard-reserved mode.
- **Convergence lag:** After a new queue becomes active, it takes a few poll cycles for the demand signal to propagate and for the incumbents' holdings to drain to their fair share. This is acceptable — convergence happens within seconds.
