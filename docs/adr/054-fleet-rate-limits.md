# ADR-054: Fleet rate limits

## Status

Accepted. Tracked in [#718](https://github.com/hardbyte/awa/issues/718). The runtime behaviour,
migration v065 and the CLI are implemented; the HTTP API, admin UI and Python bindings follow. Amends
[ADR-010](010-rate-limiting.md), which scoped rate limits to one runtime, and
[ADR-038](038-queue-runtime-overrides.md), whose first guard rail this removes for rate limits.

## Context

An operator protecting a downstream API wants to say "this queue starts at most 50 jobs a second"
and change that number during an incident without a restart. Awa cannot express either part.

ADR-010's token bucket lives in each runtime's dispatcher, shared by that runtime's claimers for the
queue. With N runtimes serving a queue the fleet starts up to N times `max_rate`, and the figure
moves whenever the fleet scales. A `PartitionedQueue` multiplies it again by its partition count.

ADR-038 made the rate overridable, but only as a retune: the bucket exists only when the builder
configured a limit, so a queue deployed without one cannot be throttled live, which is the incident
case. Burst cannot be overridden; it is re-derived as `ceil(rate)`. An override records only when it
was set, not who set it or why, and no runtime reports which value it applied: snapshots publish the
builder `QueueConfig`, so neither the admin UI nor `config_mismatch` can show the limit in force.

The constraint from #169 and ADR-026 still holds: no hot mutable row on the claim path. A token
bucket row updated by every claim would serialize every claimer of the queue on one row and leave a
dead tuple per claim under a pinned MVCC horizon.

## Decision

### Two limits, both optional

A queue has a **runtime rate limit**, ADR-010's existing `rate_limit`, and a new **fleet rate
limit**. Both are jobs started per second with a burst. Each runtime enforces

```text
effective rate = min(runtime rate limit, fleet rate limit x share)
```

with either term absent when its limit is unset. The fleet limit is the one an operator reaches for;
the runtime limit stays for capacity a single process owns, such as a per-host connection budget.

### Each runtime enforces its share

A runtime's share of a queue's fleet limit is its weight divided by the total weight of the live
runtimes serving the queue:

```text
share(r) = weight(r) / sum(weight(s) for s live and serving the queue)
```

`weight(r)` is the runtime's worker capacity for the queue: `max_workers` in hard-reserved mode, and
`min_workers` plus the overflow pool total in weighted mode. Tokens go where permits can spend them;
an equal split would starve a large runtime next to a small one. Each runtime publishes its weight
per queue in its snapshot in `awa.runtime_instances`.

A runtime is live when its snapshot is within the existing stale cutoff and it is not shutting
down. A runtime always counts itself with its current weight, whether or not its own snapshot is
visible yet. Shares are recomputed on the ADR-038 override refresh, once per queue per runtime, from
one read of `awa.runtime_instances`.

The fleet burst divides the same way, floored at one token per runtime:

```text
burst(r) = max(1, ceil(fleet burst x share(r)))
```

A bucket that can never hold a whole token would never start a job. The floor means that with more
runtimes than burst tokens, an instantaneous fleet burst can reach one job per runtime.

### Tolerance

Every runtime enforces locally, so the bound depends on runtimes agreeing on the membership:

- **Steady state:** shares sum to one, and the fleet starts at most the fleet rate, plus each
  runtime's floored burst at the start of an interval.
- **A runtime joins:** it counts itself at once, while the others count it only after its first
  snapshot and their next refresh. For up to one snapshot interval plus one refresh interval (20 s
  at the defaults), the fleet can start up to the newcomer's share above the limit.
- **A runtime shuts down gracefully:** its snapshot is marked shutting down, and the others take up
  its share within one refresh interval. Until then the fleet runs under the limit.
- **A runtime dies:** the others count it until its snapshot passes the stale cutoff (30 s at the
  defaults), then take up its share on their next refresh. Until then the fleet runs under the
  limit by that share.

The limit is exceeded only by the shares of runtimes that joined within the last window, and only
for that window.
Under-admission after a crash is the safe direction for a limit that protects a downstream API.

### Limits change live

Each runtime keeps one limiter per queue that can hold no bucket at all. An override can enable a
limit on a queue built without one, retune it, or clear it; clearing reverts to the builder value,
including to no limit. Retuning keeps the bucket's tokens, capped at the new burst, so a change never
grants a fresh burst. This removes ADR-038's first guard rail for rate limits. Its deadline guard
rail is unchanged.

### Override storage, audit and revision

`awa.queue_meta` gains `override_fleet_rate_limit`, `override_fleet_rate_burst` and
`override_rate_burst` next to ADR-038's `override_rate_limit`, plus:

- `overrides_revision`, incremented by every set and clear, so a runtime can say which write it
  applied;
- `overrides_updated_by` and `overrides_reason`, recorded with every write, as `paused_by` is for
  pause.

The builder value stays the base and a non-NULL override wins, as in ADR-038. A builder may set a
base fleet limit through `QueueConfig`; runtimes that disagree on it show as a `config_mismatch`.

### Applied state

Each runtime adds to its per-queue snapshot the override revision it applied, its weight, its share,
and its effective rate and burst. These sit beside the queue's runtime counters, not in its
configuration snapshot, so a share that differs between runtimes is not a configuration mismatch.

`awa queue overrides show` and the admin API report, for each queue, the base, the override, its
revision, who set it and why, the effective fleet rate, and how many of the live runtimes report the
current revision applied.

### Operator surfaces

- **CLI:** `awa queue overrides set` gains `--fleet-rate-limit`, `--fleet-rate-burst`,
  `--rate-burst` and `--reason`. `show` prints base, override, effective value and applied counts.
- **HTTP:** `GET`, `PUT` and `DELETE /api/queues/{queue}/overrides`, guarded by `require_writable`.
- **Admin UI:** the queue page shows the base, override, effective fleet rate, each runtime's share
  and "applied by N of M runtimes", with an edit form that requires a reason.
- **Python:** admin bindings for set, clear and show.

### Scope

- **Partitioned queues:** limits stay per physical queue. A fleet limit set on a partitioned queue's
  partitions applies to each partition. A limit on the logical queue needs a recorded mapping from
  logical to physical queues and is deferred.
- **Over-limit behaviour:** unchanged. An over-limit queue leaves jobs unclaimed; it never claims and
  reschedules them, which would spend attempts. A handler that meets an upstream 429 snoozes, which
  does not spend an attempt either.
- **Permissions:** override writes stay plain DML on `awa.queue_meta`. Routing operator writes
  through definer functions waits for #677.
- **0.6:** not backported. 0.6 has no ADR-038 overrides.

## Alternatives considered

### Token bucket row updated in the claim transaction

Rejected. Every claim on every shard of the queue would serialize on one row, undoing ADR-025, and
every claim would leave a dead tuple (#169, ADR-026).

### GCRA timestamp in a sequence under a per-queue advisory lock

Not chosen now. It avoids dead tuples but is still one serialization point per queue on the claim
path. Worth benchmarking only if share-based limits prove too loose.

### Budget handed out through claimer leases

Deferred. `queue_claimer_state` and `queue_claimer_leases` are already one coordinated row per
claimer slot, written at lease frequency, and could carry a rate budget with tighter membership than
snapshots. It covers queue storage only, not the canonical engine.

### Equal shares

Rejected. A runtime with one worker would hold as many tokens as one with fifty, and the fleet would
run under the limit while the large runtime waits for tokens.

## Validation

- A queue with a fleet limit, served by 1, 2 and 4 runtimes, starts jobs at the configured rate
  within the steady-state tolerance on PostgreSQL 17 and 18. Timing bounds go through
  `awa/tests/ci_timing.rs`.
- An operator enables, retunes and clears a fleet limit on a queue built without one, from the CLI,
  and every runtime applies it within one refresh interval, reported by the applied revision.
- A runtime joining, shutting down and going stale moves the other runtimes' shares within the
  stated windows.
- Retuning never grants a fresh burst.

## Relationship to other ADRs

- **ADR-010:** the runtime rate limit keeps its meaning. Its "per-worker, not global" negative is
  answered by the fleet limit.
- **ADR-011:** a runtime's weight comes from the same capacity model.
- **ADR-033:** Tier 1 per-key rates reuse this share model and the override, audit and
  applied-state plumbing, with a key dimension added.
- **ADR-038:** this extends its override row and removes its first guard rail for rate limits.
