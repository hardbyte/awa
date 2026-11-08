# AWA — Validation Test Plan

Tests run against real Postgres 15+ (not managed services) using a dedicated test database. Most Rust/Python rows below are automated in CI; heavyweight soak, failover, benchmark, TLA+, and managed-service checks are run manually or in scheduled lanes when they need special infrastructure.

## Test Matrix

**Rust** = Rust integration test, **Py** = Python test, **TLA+** = TLC model check, **Both** = cross-language.

### Correctness & Crash Recovery

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T1 | No duplicate processing (100k jobs) | ✓ |  |
| T4 | `kill -9` crash recovery | ✓ |  |
| T5 | Deadline rescue (hung job) | ✓ |  |
| T6 | Transactional atomicity | ✓ |  |
| T7 | Transactional atomicity (Python) |  | ✓ |
| T8 | Uniqueness under contention | ✓ | ✓ |
| T9 | Hash cross-language consistency | ✓ |  |
| T10 | Priority ordering | ✓ |  |
| T12 | Queue isolation | ✓ |  |
| T18 | Backoff timing | ✓ |  |
| T19 | Snooze semantics | ✓ |  |
| T19a | `RetryAfter` bounded by `max_attempts`: exhausts to failed/DLQ (`max_attempts_exhausted`) on the final attempt, exactly `max_attempts` executions, `Retried` semantics preserved below the bound | ✓ |  |
| T20 | Terminal error semantics | ✓ |  |
| T21 | Deserialization failure | ✓ |  |
| B1 | Late completion after rescue is no-op | ✓ |  |
| B2 | Late completion after cancel is no-op | ✓ |  |
| B3 | Shutdown waits for in-flight jobs | ✓ |  |
| B4 | Heartbeat alive during shutdown drain | ✓ |  |
| B5 | Deadline rescue signals ctx.is_cancelled() | ✓ |  |
| B6 | UniqueConflict.constraint field | ✓ |  |
| B7 | Admin cancel signals in-flight handler cancellation | ✓ | ✓ |
| B8 | Shutdown finalizes attempts whose handler returned during the drain, so a process exit cannot leave them for rescue | ✓ |  |
| T75 | Priority aging maintenance task promotes long-waiting low-priority jobs | ✓ |  |

### Uniqueness (Python)

| #    | Test                                                  | Rust | Py  |
| ---- | ----------------------------------------------------- | ---- | --- |
| T8   | Uniqueness under contention (10 concurrent producers) | ✓    |     |
| PU1  | Unique insert happy path                              |      | ✓   |
| PU2  | Duplicate rejected (UniqueConflict)                   |      | ✓   |
| PU3  | by_queue: different queues allowed                    |      | ✓   |
| PU4  | by_queue: same queue rejected                         |      | ✓   |
| PU5  | by_args: different args allowed                       |      | ✓   |
| PU6  | by_period: same bucket rejected                       |      | ✓   |
| PU7  | by_period: different bucket allowed                   |      | ✓   |
| PU8  | Unicode args hash consistently                        |      | ✓   |
| PU9  | Nested dict args hash consistently                    |      | ✓   |
| PU10 | Empty args hash consistently                          |      | ✓   |
| PU11 | Large args (~100KB) hash consistently                 |      | ✓   |
| PU12 | No unique_opts allows duplicates                      |      | ✓   |
| PU13 | Sync insert with unique_opts                          |      | ✓   |

### Admin & Resilience

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T13 | Queue pause/resume | ✓ |  |
| T22 | Pool exhaustion resilience | ✓ |  |
| T26 | Migration idempotency | ✓ |  |
| T27 | Admin ops under load | ✓ |  |
| T72 | Runtime recovers after terminating Postgres backends | ✓ |  |
| T71 | Mixed Rust/Python workers share queue | Both | Both |
| T73 | Sustained mixed workload survives node failure (see below) | Both | Both |
| T74 | Hot-standby promotion | ✓ |  |
| HA1 | Postgres failover smoke | ✓ |  |

**T73 ordering invariant:** Jobs are inserted _before_ Rust clients start so that the Python worker exclusively claims `simple_chaos_job` rows. This avoids a race where Rust's instant `CompleteWorker` steals all simple jobs before the slower Python worker (400 ms/job) can claim any. The test confirms Python is mid-execution via its `START` stdout line, then kills it. Heartbeat backdating triggers rescue, and the `max_simple_attempt >= 2` assertion verifies re-processing by a surviving Rust worker.

### External Callbacks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| E1 | register_callback + WaitForCallback → waiting_external | ✓ | ✓ |
| E2 | complete_external → completed | ✓ | ✓ |
| E3 | fail_external → failed | ✓ | ✓ |
| E4 | retry_external → available | ✓ | ✓ |
| E5/PE1-2 | Callback timeout rescue (retryable + failed) | ✓ | ✓ |
| E6/PE3 | Double completion → CallbackNotFound | ✓ | ✓ |
| E7 | Wrong callback_id → CallbackNotFound | ✓ | ✓ |
| E8/PE4 | Admin cancel while waiting_external | ✓ | ✓ |
| E9/PE5 | Admin retry while waiting_external | ✓ | ✓ |
| E10/PE6 | drain_queue includes waiting_external | ✓ | ✓ |
| E11/PE7 | Race: complete during running (before WaitForCallback) | ✓ | ✓ |
| E12 | Crash clears stale callback | ✓ |  |
| E13 | Uniqueness during waiting_external | ✓ |  |
| E15 | resolve_callback accepts running state | ✓ |  |
| E16/PE8 | Stale callback rejected after rescue | ✓ | ✓ |
| E17 | cancel_callback clears fields | ✓ |  |
| E18 | cancel_callback wrong lease is noop | ✓ |  |
| PE9 | Callback timeout rescued end-to-end by live runtime |  | ✓ |

### CEL Callback Expressions

| #       | Test                                               | Rust | Py  |
| ------- | -------------------------------------------------- | ---- | --- |
| C1-C2   | resolve_callback default actions (complete/ignore) | ✓    | ✓   |
| C3-C4   | Filter expressions (pass/fail)                     | ✓    |     |
| C5-C6   | on_fail takes precedence over on_complete          | ✓    |     |
| C7      | Transform payload                                  | ✓    |     |
| C8-C9   | Invalid CEL fail-open                              | ✓    |     |
| C10     | Fallthrough to default action                      | ✓    |     |
| C11     | Invalid CEL at registration → validation error     | ✓    |     |
| C12     | Double resolve → CallbackNotFound                  | ✓    |     |
| C14-C15 | Deeply nested / missing field (fail-open)          | ✓    |     |
| C16     | resolve_callback accepts running state             | ✓    |     |
| C17     | Concurrent resolve (FOR UPDATE prevents race)      | ✓    |     |
| C18-C19 | CEL disabled: registration + resolution errors     | ✓    |     |

### Sequential Callbacks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| SC1 | wait_for_callback: suspend + resume | ✓ | ✓ (SC14) |
| SC2 | Two sequential callbacks via admin API | ✓ | ✓ (SC15) |
| SC3 | Timeout during second callback → retryable | ✓ |  |
| SC4/SC16 | Heartbeat extends timeout during wait | ✓ | ✓ |
| SC5 | Concurrent resume: exactly one succeeds | ✓ |  |
| SC6 | Resume with wrong run_lease rejected | ✓ |  |
| SC7 | Crash/rescue after resume | ✓ |  |
| SC8 | Admin cancel after resume | ✓ |  |
| SC9 | Resume preserves metadata | ✓ | ✓ |
| SC10-SC12 | fail/resolve/retry on second callback | ✓ |  |
| SC13 | resume_external transitions to running with payload |  | ✓ |
| SC17 | Double resume fails with CallbackNotFound |  | ✓ |

### HTTP Worker

| #   | Test                               | Rust | Py  |
| --- | ---------------------------------- | ---- | --- |
| HW1 | Sync mode 200 → completed          | ✓    | —   |
| HW2 | Sync mode 500 → retryable          | ✓    | —   |
| HW3 | Sync mode 400 → terminal fail      | ✓    | —   |
| HW4 | Async mode 202 → callback complete | ✓    | —   |
| HW5 | Async mode 503 → retryable         | ✓    | —   |
| HW6 | Async unreachable → retryable      | ✓    | —   |
| HW7 | Custom headers                     | ✓    | —   |
| HW8 | BLAKE3 callback signature          | ✓    | —   |
| HW9 | Callback URL construction          | ✓    | —   |

_HTTPWorker is a Rust-only feature (ADR-018: serverless function dispatch). Not exposed to Python._

### Python SDK

| #       | Test                                                         | Py  |
| ------- | ------------------------------------------------------------ | --- |
| T58-T70 | Sync API variants (insert, cancel, retry, drain, list, etc.) | ✓   |
| P12-P16 | start() config validation (dict, tuple, weighted, errors)    | ✓   |
| P17     | start() accepts all maintenance interval kwargs              | ✓   |
| PP1-PP5 | Progress tracking (set, merge, flush, checkpoint)            | ✓   |
| BR4     | Bridge: asyncpg/psycopg/SQLAlchemy/Django insert             | ✓   |

### Cron / Periodic Jobs

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T34-T46 | Cron: migration, upsert, atomic CTE, dedup, backfill, DST | ✓ |  |
| T40-T41 | End-to-end periodic + metadata propagation | ✓ |  |
| CRP1-CRP9 | Cron pause / resume: state transitions, CTE guard, upsert preserves pause, manual trigger bypass, queue interaction | ✓ |  |

### COPY Batch Ingestion

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T47-T57a | COPY: empty, single, 1000, special chars, tags, unique, atomic | ✓ | ✓ |
| QSC1-QSC2 | Queue storage COPY producer: ready/deferred direct COPY, unique-conflict rollback | ✓ | ✓ |

### Rate Limiting & Weighted Concurrency

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| RL1-RL6 | Rate limit: fast, throttle, burst, bottleneck, invalid | ✓ |  |
| W5-W13 | Weighted: backward compat, overflow, floor, cap, proportionality | ✓ |  |

### Observability

| #       | Test                         | Rust |
| ------- | ---------------------------- | ---- |
| T28-T30 | Tracing spans + OTel metrics | ✓    |
| OT1-OT4 | OTLP export end-to-end       | ✓    |

### Bridge Adapters

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| BR1-BR3 | tokio-postgres bridge: atomicity, lease guard, unique conflict | ✓ |  |
| BR4 | Python bridge: asyncpg/psycopg/SQLAlchemy/Django |  | ✓ |

### UI (Admin)

| # | Test | Rust |
| --- | --- | --- |
| RO1-RO3 | Read-only serve: capabilities, mutations blocked, UI disables | ✓ |
| UIB1-UIB3 | Batch operations page: set-priority preview/submit, move-queue payload, cancellation request | ✓ |
| UII1 | Instance identity: `--instance-name`/`--instance-color`/`--peer` parse and validate (hex color, http(s) peers only), surfaced via `/api/capabilities` (#437) | ✓ |
| UII2 | Frontend identity helpers: tab title, hex-color normalization (style-injection rejected), tinted favicon data URI (vitest) | ✓ |

### Admin Metadata Guardrails

| #   | Test                                                         | Rust |
| --- | ------------------------------------------------------------ | ---- |
| AM1 | Heartbeat/progress-only UPDATEs do not dirty queues or kinds | ✓    |
| AM2 | flush_dirty_admin_metadata() drains backlog > 100 keys       | ✓    |
| AM3 | Enqueues inside application transactions never deadlock through dirty marks; ADR-045 lock contract over every trigger on `jobs_hot`/`scheduled_jobs`: allowlisted write targets only, no `ON CONFLICT`/`LOCK`/row locks, mark tables unconstrained, maintenance never `TRUNCATE`s | ✓    |
| AM4 | Drain leaves marks of uncommitted transitions for the next pass (exact counts); concurrent re-schedules with drain + refresh never fail | ✓    |

### Benchmarks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T31-T33 | Throughput + latency + insert speed | ✓ |  |
| CL1-CL2 | Concurrent multi-queue lifecycle | ✓ |  |
| SP1-SP2 | Scheduled promotion at scale | ✓ |  |
| FB1-FB7 | Failure modes: terminal, retryable, callback, deadline, mixed | ✓ |  |
| FB8 | Failure modes (Python) |  | ✓ |
| PB1 | Portable cross-system enqueue throughput (Awa vs River vs Oban, 10k and 50k jobs) |  |  |
| PB2 | Portable cross-system worker throughput (50/100/200 workers, no-op jobs) |  |  |
| PB3 | Portable cross-system pickup latency (single job to idle queue) |  |  |

### Dirty-key trigger overhead (measured v0.5.1, debug build, Docker PG 17)

Concurrent lifecycle benchmark (1 queue × 128 workers, 20K jobs):

| Config                  | Throughput | Overhead |
| ----------------------- | ---------- | -------- |
| No triggers (baseline)  | 1963/s     | —        |
| Noop PL/pgSQL trigger   | 1855/s     | 5.5%     |
| Full dirty-key triggers | 1832/s     | **~7%**  |

### Formal Models (TLA+)

| # | Model | Invariants |
| --- | --- | --- |
| TLA1 | AwaCore | Lease-guarded finalization, stale completion rejected |
| TLA2 | AwaExtended | Shutdown/rescue/permit/fairness protocol |
| TLA3 | AwaCbk | At-most-once callback resolution, sequential resume |
| TLA4 | AwaCron | No duplicate fire under leader failover; paused schedules block enqueue across a snapshot-pause-CAS race; liveness preserved under fair Resume |
| TLA5 | AwaBatcher | At-most-once completion, DirectCompleteFail recovery |
| TLA6 | AwaDispatchClaim with NewClaim config | Dispatch claim safety |
| TLA7 | AwaSegmentedStorage | Segmented storage safety, waiting flow, optional attempt-state, prune safety |
| TLA8 | AwaSegmentedStorageInterleavings | Two-worker segmented-storage interleavings |
| TLA9 | AwaShardedPrune | Cross-shard ready/terminal prune matching by `enqueue_shard` |
| TLA10 | AwaSegmentedStorageRaces | Claim-vs-rotate/prune race exposure and checked-commit safety |
| TLA11 | AwaSegmentedStorageTrace | Runtime trace acceptance for snooze, receipt rescue, cancel, callback wait, DLQ retry, and DLQ purge paths |
| TLA12 | AwaStorageLockOrder | Postgres lock ordering across claim, complete, cancel, rescue, rotate, and prune |
| TLA13 | AwaStorageTransition | Storage-transition prepare, mixed-entry, finalize, and abort gates |
| TLA14 | AwaDeadTupleContract | Hot-table reclaim-kind and partition-truncate contract |
| TLA15 | AwaCanonicalDirtyMarks | Canonical dirty-key triggers never block a job transition and cannot close a waits-for cycle; `OnConflict` and `Incident` configs keep the v006 deadlock (including the 2026-09-12 production re-schedule pair) as expected counterexamples |

The formal suite includes passing configs and expected-counterexample configs. The expected-counterexample configs keep historical bugs executable: old dispatch claim, old view trigger, naive segment race, old storage-transition gate, shard-ignorant prune, and deliberate lock-order cycles. Trace configs use a `TraceIncomplete` invariant as a positive witness: a valid trace violates that invariant after TLC consumes every event.

## 0.7 Planned Validation

Planned test matrix for the 0.7 cycle, mapped to the [0.7 roadmap](0.7-roadmap.md) and the release gates on the [#383 tracker](https://github.com/hardbyte/awa/issues/383). Rows move into the matrix above as they are implemented.

Rows V10 and V20–V25 are not 0.7 gates: their issues (#118, #14, #340, #341 and #342) moved to 0.8 in the [scope revision](0.7-roadmap.md#scope-revision-2026-11-03).

V25 adds a focused `AwaKeyedExecution` model for grant safety, partial-legacy coverage scope, and the drain/epoch/verify/resume/activate-or-cancel lowering protocol; it also extends `AwaStorageLockOrder` for the short policy transition windows. V26 adds `AwaCallerOwnedCompletion` for completion-versus-rescue atomicity. These names are planned deliverables, not claims that model files already exist.

### Harness & compatibility

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V1 | Broad integration suite green under `AWA_TEST_ENGINE=queue_storage` | ✓ | ✓ | #360 |
| V2 | Engine guard rejects canonical-only raw-SQL helpers under queue_storage | ✓ |  | #360 |
| V3 | Pinned 0.6.2 binary: enqueue→claim→complete→cancel against the 0.7 schema in columns authority, then fail after flip | | ✓ | #367/#427 — nightly `scripts/compat-matrix.sh`; the 0.6.0 wheel remains an additional data-plane regression leg |
| V3a | Released 0.6.3 plus current under live traffic: migrate v040→v043, mixed completions, old-only final rotation, guarded flip/reconciliation, returning-old fence, exact drain, and hard-killed old-leader deadline rescue | ✓ | ✓ | #427 — nightly `rolling_upgrade_rehearsal_test` |
| V3b | Live v043 migration deadlock rolls back and retries the complete atomic transaction | ✓ | | #427 — `migration_test::test_live_migration_retries_hot_table_deadlock` |
| V3c | Binary-first order: current runtime refuses the v040 schema loudly (naming `awa migrate`), released fleet keeps draining through the refusal, the roll completes unaided after migration, and a hard-killed released worker's in-flight work is rescued | ✓ | ✓ | #427 — nightly `rolling_upgrade_rehearsal_test` binary-first cell |
| V3d | Overlapped order: a crash-looping current deployment races the live v041→v043 migration, comes up unaided at commit, and both versions complete concurrently before the flip | ✓ | ✓ | #427 — nightly `rolling_upgrade_rehearsal_test` overlap cell |
| V3e | Designed-outcome workload across migrate-first: v040-banked backlog of retries, terminal/exhausted failures with exact DLQ depth, snoozes, handler and admin cancellations, callback waits, scheduled/deadline/multi-second jobs, and cron fires on both sides of the flip, each reconciled to its exact terminal state and attempt | ✓ | ✓ | #427 — nightly `rolling_upgrade_rehearsal_test` workload cell |
| V4 | Newest binary against a pre-migrate old schema fails loudly (message + exit code asserted) | ✓ |  | #367 — asserted against a 0.5.7 schema with canonical work; the 0.6-schema variant activates with the first 0.7 migration |
| V5 | `awa migrate` refuses non-`active` storage state, names finalize steps | ✓ |  | #370 — `migration_test` gate tests + the nightly backward-guard leg |

### Deployment & operations

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V6 | `/readyz` flips 503 on DB-down / schema-mismatch / stalled claim loop | ✓ |  | #368 |
| V7 | Maintenance-only role: promotes/rescues/rotates, never claims or dispatches | ✓ | ✓ | #282 |
| V8 | Chaos topology with zero general workers (maintenance-only + callback ingress + HttpWorker) | ✓ |  | #282, #372 |
| V9 | Callback contract parity: embedded router vs `awa callbacks serve` (shared contract tests) | ✓ | ✓ | #372 |
| V10 | `tick()` bounded steps; scheduler-driven demo promotes/rescues with no resident worker | ✓ | ✓ | #118 |
| V11 | Unauthenticated non-loopback serve → read-only + banner; token auth constant-time; mutation audit line | ✓ |  | #343 |
| V12 | `awa doctor` checks cover every troubleshooting.md scenario; `--json` schema asserted | ✓ |  | #373 |
| V13 | Integration suite through pgbouncer session mode; transaction mode engages NOTIFY fallback + warning | ✓ | ✓ | #374 |
| V14 | Helm chart kind smoke: four-surface topology installs, probes pass, migration hook runs | ✓ |  | #344 |

### CLI contexts

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| CTX1 | Config parsing: exactly one of `url`/`url_env` per context, unknown keys and unknown `default_context` rejected loudly | ✓ |  | #437 |
| CTX2 | Resolution order: `--database-url` > `--context` > `DATABASE_URL` env > `default_context`; no config file → pre-context behaviour unchanged | ✓ |  | #437 |
| CTX3 | Read/write asymmetry: with >1 contexts, mutating commands refuse `DATABASE_URL`/`default_context` fallback; read-only commands and single-context configs are unaffected | ✓ |  | #437 |
| CTX4 | Access classification matrix: the #437 destructive set (migrate apply, storage transitions/flip, dlq retry/purge/move, queue drain/pause, batch-ops submit, cron remove) and all other writes classify as mutating; status/list/`--check` variants as read-only | ✓ |  | #437 |
| CTX5 | Target echo redaction: userinfo password and `password=` query param stripped; unparseable URLs never echoed | ✓ |  | #437 |
| CTX6 | Production gate: `production = true` + mutation prompts y/N; `--yes` skips; non-interactive stdin without `--yes` refuses | ✓ |  | #437 |
| CTX7 | Plaintext-password `url` in a non-0600 config file warns (secret not echoed); `url_env` indirection never warns | ✓ |  | #437 |

### Observability

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V15 | Trace context: enqueue→claim→finalize one connected trace; retries/callbacks/cron linked | ✓ |  | #110 |
| V16 | Trace propagation across the PyO3 boundary (Python handler is a child span) |  | ✓ | #110 |
| V17 | Tracing overhead A/B within the E8 gate (<2% sampled enqueue cost) | ✓ |  | #110 |
| V18 | Attempt-timeline API assembles ordered events across retries incl. callback park/resume | ✓ |  | #375 |
| V19 | Alert pack rules lint/import clean | ✓ |  | #376 |

### Flow & tenancy

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V20 | Per-key Tier 1: worker-local cap holds exactly; fleet approximation matches documented formula | ✓ | ✓ | #340 |
| V21 | Per-key fairness under Zipf-skewed tenants (Jain index bound, no starvation) | ✓ |  | #340 |
| V22 | A→B promotion is transactional with parent finalize; `on_parent_failure` policies (TLA+ witness + integration) | ✓ | ✓ | #14 |
| V23 | Backpressure: `Signal` surfaces pressure, `Reject` returns typed error, transactional enqueue unaffected by default | ✓ | ✓ | #341 |
| V24 | SQL contract conformance script green; cross-language vectors cover BLAKE3 unique-key derivation, the domain-separated SHA-256 concurrency-key digest (two algorithms by design — see ADR-033), concurrency-key-to-enqueue-shard mapping at every test width, and explicit ordering-key-over-concurrency-key precedence for SQL, direct binary, and bulk producers; PostgreSQL computes public-SQL routing authoritatively and rejects any supplied expectation that disagrees | ✓ | ✓ | #342 |
| V25 | Per-key Tier 2: `AwaKeyedExecution` and concurrent claimers never exceed the grant limit; limit lowering uses short epoch fences plus bounded out-of-lock verification, refuses incomplete/invariant-breaking updates without stalling claimers, cancellation restores the prior limit only for the matching epoch while admission stays closed until commit, and resume revalidates the pending epoch/target, keeps admission closed, and reruns verification without creating a second pending transition; every exit closes only its exact grant, with callback park/resume retaining it; gated heads never advance and cannot report false idle while an admissible lane is inside the configured probe set; capped rotating probes provide bounded eventual pickup; keyed-only routing stays shard-local; complete coverage rejects legacy rows until drained/backfilled while explicit partial mode admits them ungoverned with non-green diagnostics/metrics; grant closure wakes session listeners while poll-only pickup is bounded; E5 compares row-local, separate-ledger, and proved parking shapes | ✓ | ✓ | #340, ADR-033 |
| V26 | Caller-owned completion: `AwaCallerOwnedCompletion` proves completion-versus-rescue atomicity; distinct handler return type; app rows + terminal/conditional grant closure commit or roll back together; receipt/lease tokens reconcile; bounded ambiguity preserves durable authority and pool reserve; the 0.7 bounded-owner/manifest slice leaves the finalizer role only hardened function execution with no inherited or transitive `SET ROLE` path to protected owners on the oldest and newest supported PostgreSQL majors (straddling PG16's membership-option change); transaction-pooler conformance | ✓ | ✓ | #401, ADR-042 |

### Storage & performance (gate evidence, benchmark harness)

| # | Test | Ref |
| --- | --- | --- |
| V27 | 0.6 pinned-MVCC long-horizon shape still passes on 0.7 main (798/s through 60-min pin, bounded depth, full reclaim) | #383 Gate 2 |
| V28 | #246 shape: ≤5% rescue-ON overhead at 1×256, or named mechanism + mitigation | #246 / E2 |
| V29 | Ring-state dead tuples reduced to noise on the long-horizon idle phase | #371 |
| V30 | Partitioned-queue preset ≥9k jobs/s e2e on the reference 24-CPU harness | Not a 0.7 gate (moved to 0.8) |
| V31 | Gate A evidence: allocator bake-off (E1) + WAL/job decomposition (E3) run ids recorded on #295 | #295 |

## Persona soak

Durable loss detection covers every lifecycle-observed job and a deterministic `job_id % 64 = 0` sample of `NO_EVENTS` workloads. The other 63/64 returned `NO_EVENTS` jobs remain in enqueue/execution accounting, but their terminal commits are not proven. A bulk observer polls sampled terminal rows and eventful attempts whose handlers returned without terminal proof once per second (2 s query timeout, 100,000-row cap) and keeps the run lease in the client ledger, so captured proof survives pruning. A handler return is never terminal proof. Missing sampled proof after pruning or a failed invariant query makes the report `INCONCLUSIVE` and fails the test; it does not assert job loss or establish a pass. Reports include sampled, confirmed and excluded counts.

On CI, assertion waits and heartbeat staleness use `ci_timing.rs` scaling; load durations, kill schedules and measurement windows retain their configured values.

`awa/tests/persona_soak_test.rs` (harness in `awa/tests/persona_soak/`, Python side in `awa/tests/persona_soak/python/`) runs several application-shaped workloads concurrently against one database, the way different applications share an `awa` schema, and fails the run on any invariant violation.

| Persona | Workload | Stresses |
| --- | --- | --- |
| Mia, ML pipeline | jobs of 0.1-60 s with per-job deadlines, progress updates, retries into the DLQ, handler cancels, latency spikes | deadline and stale-heartbeat rescues racing heartbeats and completions, DLQ |
| Tom, multi-tenant | 100 queues at a low rate each, unique jobs, scheduled jobs, cron every 10 s on two runtimes, mixed priorities with aging | lane and queue cardinality, per-queue maintenance, cron fencing |
| Priya, firehose | one hot queue, 100k-row `insert_many` bursts, COPY batches, steady small batches, tiny jobs, three runtimes | bind-parameter limit, ring rotation and prune, claim contention |
| Sam, ordered streams | a queue with 4 enqueue shards and 64 ordering keys, a 4-stripe queue (`queue_stripe_count`), scheduled jobs, a rate-limited queue | routing, promotion, fairness |
| Alex, operator | pause/resume, drain, cancel and bulk cancel, move and reprioritise batch operations, DLQ retry and purge, the admin read queries, rolling restarts of other personas' runtimes | admin paths under live load |
| Chaos | kills a runtime or the Python process without a shutdown handshake, terminates the maintenance leader's connection and random runtime connections, exhausts a runtime's pool, holds an idle-in-transaction session | recovery paths |
| Watchers | six dashboard tabs polling overview, queues, counts, timeseries, runtime and storage at the UI's refresh interval, some also the kinds, queue-runtime and cron pages | admin read queries under load, through the real `awa serve` HTTP server |
| Support engineer | searches jobs by kind, queue, state and tag, pages through large result sets with `before_id`, opens job detail and follows a job at the detail page's poll rate, pages the DLQ, opens runtime, cron and kind pages | search and paging cost against retained history and backlog |
| UI operator | the UI's mutations over HTTP: pause and resume queues and cron schedules, cancel and bulk-cancel, DLQ retry and purge (single and by filter), batch operation preview and submit | mutation endpoints racing live work |
| Impatient user | requests abandoned after a few milliseconds, bursts of refreshes, while sampling the server's database sessions | an abandoned request must not keep a pool connection past the statement timeout |
| Fanout, replay fan-out | parent jobs whose handlers batch-insert 100-185 unique child jobs (`insert_many`, unique by queue and args) into their own queue from many concurrent handlers; some parents replay a batch with an already-claimed key, hit the unique violation and fall back to row-by-row inserts | concurrent batch enqueue into one lane, unique claims, inserting from inside a handler |
| Shard operator | an operator changes `enqueue_shards` on two hot queues while producers, promotion and claimers run: 1 → 4 → 8, down to 2 and back to 1, sometimes after `prepare_queue` and sometimes not, restarting some runtimes and producer `QueueStorage` handles before others so cached shard counts disagree. Producers use `insert_many`, `insert_many_copy` and `QueueStorage::enqueue_params_batch`, with scheduled jobs, retries (promotion into shards) and `ordering_key` jobs. The actors run as non-superuser roles: a schema owner that migrates and runs `prepare_queue`, a migrator role with default privileges, and two application roles in a runtime group role with no default privileges, granted as in [Database roles](security/database-roles.md). For some raises the producers are held until the maintenance leader's runtime has created the new lanes, so a different role is the next to use them | live shard-count changes (ADR-025), lane provisioning by different roles, stale per-process shard counts, promotion into new shards |
| Claim and abandon | a separate process (the harness binary started again with one test selected) that claims as much as it can: `max_workers` equal to the target on two queues, one with zero deadlines (compact claim batches) and one with a deadline, jobs held until the process dies, heartbeats running. It is killed with SIGKILL, repeatedly, and replaced after a few seconds by a process that claims the rescued jobs again; some jobs have `max_attempts` 2 so repeated kills dead-letter them. A surviving in-process runtime leads maintenance and serves a canary queue of short jobs, and the other personas keep working; after the last kill an in-process runtime finishes the rescued jobs | rescue of a large abandoned-claim backlog, claim-ring rotation while slots hold abandoned claims, false rescues of live owners, `max_attempts` and the DLQ under repeated rescue |
| Python users | a separate process using `awa-python`: single inserts, a 6,500-row `insert_many` in one transaction and a COPY batch, enqueues in transactions that sometimes roll back; async workers that complete, raise, `RetryAfter`, `Snooze`, report progress, overrun a queue deadline and ignore their deadline; a queue shared with a Rust runtime; a periodic migrate-then-query probe | Python bindings against live load, mixed Rust and Python runtimes on one lane, the documented slot behaviour of an ignored deadline, connections stranded after `migrate()` |

The oracle checks, while the load runs and again after it stops:

- a job attempt with more than one terminal row (`done_entries`, `dlq_entries`, compact receipt completions), a job reported completed twice, and a job that is terminal and still available, deferred or leased
- a cancelled job nobody asked to cancel, an operator cancel that overwrote a completion, and a handler that started after its operator cancel committed
- a queue with claimable jobs and idle workers whose claim cursor stops advancing (stall), and scheduled jobs overdue beyond a threshold (promotion stall)
- a lost job: enqueued, never terminal after the system quiesces, and not removed by an operator action; a cron fire that produced more than one job
- shard operations: every job enqueued before, during and after a shard-count change runs and completes exactly once (a repeat is allowed only after a stale-heartbeat rescue and is reported as a finding), every shard drains including those above a lowered count, claim cursors keep advancing on every shard that has backlog while workers are idle (the per-shard lag query from ADR-025), `ordering_key` jobs start in order per key within a window in which every runtime had observed the shard count, and no role raises a permission-denied or missing-lane error (found in the server log, with the role, lane, sequence owner and statement; needs `AWA_SOAK_PG_CONTAINER` or `AWA_SOAK_PG_LOG`, and `log_line_prefix` containing `%u@%a`, as `scripts/persona-soak.sh` sets)
- claim and abandon: after each SIGKILL the rescue backlog (open claims whose last heartbeat or claim time is older than the staleness window, or whose deadline passed, counted every few seconds from the claim tables and leases) clears within `AWA_SOAK_ABANDON_BOUND_SECONDS`, with the age and count of the oldest unrescued expired claim sampled into `abandon-backlog.csv` and the time to clear reported per kill; the claim ring does not stay still for longer than the bound once the slot it must enter next holds no claim whose owner is alive (a ring pinned by a live process is expected); no job is run again while the process that held it is alive (every start is recorded with the process that ran it, and a repeat is allowed only after that process was killed); no attempt exceeds `max_attempts`, no job is dead-lettered early, and a job abandoned on its last attempt ends in the DLQ
- a transaction the Python application rolled back that left a job in the store, and database sessions left stranded by the Python `migrate()` path
- an admin-server query that outlives its statement timeout, and sessions still running a query after every UI client has stopped
- a server crash: `pg_postmaster_start_time()` changing, or `terminated by signal`, `TRAP:`, `server process (PID` and similar lines in the Postgres log

Slow or failing admin endpoints, latency that grows with retained history or backlog, and admin queries cancelled by the statement timeout are reported as non-fatal findings in the report, next to the violations.

Terminal rows are pruned by ring rotation within seconds, so the oracle keeps a client-side ledger (job ids per persona, executions per job, lifecycle events, operator actions) and reconciles it with the database. Time-based checks are widened while a chaos action is recovering.

Environment:

| Variable | Meaning |
| --- | --- |
| `AWA_SOAK_DATABASE_URL` | required; database name must match `awa_soak[A-Za-z0-9_]*` and is dropped and recreated |
| `AWA_SOAK_PERSONAS` | comma list of `mia,tom,priya,sam,alex,chaos,watchers,support,uiops,impatient,python,fanout,shardops,abandon`, default all |
| `AWA_SOAK_ABANDON_CLAIMS`, `AWA_SOAK_ABANDON_KILL_SECONDS`, `AWA_SOAK_ABANDON_KILLS` | the claim-and-abandon persona's scale, independent of `AWA_SOAK_SCALE`: claims each killed process holds (default 4000; 25,000 to 70,000 reproduces a large backlog), seconds between kills (default 180, long enough for the process to claim everything and heartbeat for a while) and how many processes are killed (default 4; the last kill is the end of the load phase if that comes first) |
| `AWA_SOAK_ABANDON_BOUND_SECONDS`, `AWA_SOAK_ABANDON_RESTART_SECONDS` | the bound after a kill within which the rescue backlog must clear and the claim ring must be free to rotate (default 120), and the pause before the replacement process starts (default 3) |
| `AWA_SOAK_HEARTBEAT_SECONDS`, `AWA_SOAK_STALENESS_SECONDS` | heartbeat interval (default 2) and heartbeat staleness after which a claim may be rescued (default 12) of every runtime, including the claim-and-abandon process; the library's own defaults are 30 and 90. The claim-and-abandon persona also records the duration of each heartbeat pass of its process against the number of handlers running, read from the timestamps the heartbeats write (`abandon-heartbeat-passes.csv`, and percentiles by handler count in the report) |
| `AWA_SOAK_RESCUE_INTERVAL_SECONDS` | heartbeat and deadline rescue interval of every runtime; unset keeps the harness's 3 s and 2 s, the library default for heartbeat rescue is 30 s |
| `AWA_SOAK_SHARDOPS_SCALE`, `AWA_SOAK_SHARDOPS_SCHEMA_CREATE` | the shard operator's rate multiplier, independent of `AWA_SOAK_SCALE` (default 1), and whether its application roles may create objects in the schema (default 1: lanes are created on the fly; 0 withholds schema `CREATE` and the operator provisions every lane before raising a count) |
| `AWA_SOAK_SHARED_CREATOR_GRANTS` | default `0`; `1` shares application-created objects for comparisons with older binaries and requires schema `CREATE`. Keep it off when checking application-role privileges. |
| `AWA_SOAK_UI_POOL_MAX`, `AWA_SOAK_ADMIN_READ_SECONDS` | the admin server's database pool size (default 10) and the seconds between the operator persona's admin read queries (default 2); lower the load on a slow server by shrinking the pool and stretching the interval |
| `AWA_SOAK_AWA_BIN` | the `awa` binary for the UI personas (`cargo build --release -p awa-cli`); defaults to the one next to the test binary |
| `AWA_SOAK_PYTHON` | interpreter of a virtualenv with `awa-python` installed (`uv venv awa-python/.venv`, `uv pip install maturin`, `maturin develop --release` in `awa-python/`); required by the `python` persona |
| `AWA_SOAK_UI_STATEMENT_TIMEOUT_SECONDS`, `AWA_SOAK_UI_CLIENT_TIMEOUT_SECONDS` | the admin server's statement timeout (default 30) and the UI personas' request timeout (default 45); a request that hits either is reported as a finding |
| `AWA_SOAK_SECONDS` | load phase length, default 900 |
| `AWA_SOAK_SEED` | fixes the workload mix and chaos schedule; printed at start |
| `AWA_SOAK_SCALE` | multiplies rates, burst sizes and queue counts, default 1 |
| `AWA_SOAK_DRAIN_GRACE_SECONDS` | how long to wait for the system to quiesce after load stops, default 120 |
| `AWA_SOAK_PG_LOG` / `AWA_SOAK_PG_CONTAINER` / `AWA_SOAK_PG_CONTAINER_LOG` | where to read the server log for the crash scan: a file on this host, or a Docker container (with an optional log path inside it, otherwise `docker logs`) |
| `AWA_SOAK_REPORT_DIR` | where `report.md`, `report.json` and, for a failed run, `reproducer.md` are written |
| `AWA_SOAK_STALL_SECONDS`, `AWA_SOAK_PROMOTION_OVERDUE_SECONDS`, `AWA_SOAK_CHAOS_SLACK_SECONDS` | oracle thresholds |

The report records the engine, server version, `max_connections`, connections at start and the per-persona connection budget. A failed run leaves `reproducer.md` with the seed, personas and duration, the violating rows, the timeline around each violation and the relevant server log excerpt. `scripts/persona-soak.sh` runs the engine matrix (`postgres:17`, `postgres:18`, and an assert-enabled PostgreSQL 18 image) in private containers on separate ports and removes them afterwards.

After-exit retained-session findings require a successful `CLOSED` acknowledgement and a valid final session observation. Forced or unproven exits remain incomplete evidence. Pool-budget and idle-transaction checks after readiness remain independent.

## Running Tests

CI runs the Rust suite as a sharded matrix (`scripts/ci-test-shard.sh`): the
migration replay binary is partitioned per-test with `cargo-nextest`, the
other slow binaries form a `heavy` shard, and every remaining test target —
including newly added files, automatically — runs in `rest`. Locally,
`cargo test --workspace` remains the equivalent single command; individual
shards can be reproduced with e.g. `./scripts/ci-test-shard.sh heavy`.


```bash
# Start Postgres
docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test -p 15432:5432 postgres:18-alpine

# Rust tests (unit + integration + scale + validation)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --workspace

# Python tests
cd awa-python && .venv/bin/pytest tests/ -v

# Chaos recovery only (same test CI runs as a dedicated step)
cd awa-python && .venv/bin/pytest tests/test_chaos_recovery.py -v -m chaos

# Nightly chaos + benchmark lane
# GitHub Actions: .github/workflows/nightly-chaos.yml

# Rust chaos suite (mixed workload soak, sustained node-failure soak, leader failover, leader connection loss, mixed Rust/Python fleet, transient DB disconnect recovery)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_PYTHON_BIN="$PWD/awa-python/.venv/bin/python" \
  cargo test --package awa --test chaos_suite_test \
  -- --ignored --test-threads=1 --nocapture

# Postgres hot-standby promotion smoke (boots primary + replica + stable proxy endpoint via Docker Compose)
cargo test --package awa --test postgres_failover_smoke_test -- --ignored --nocapture

# COPY integration tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test copy_test -- --nocapture

# Queue-storage COPY producer tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa-model --test queue_storage_copy_test -- --nocapture

# Python sync tests
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test .venv/bin/pytest tests/test_sync.py -v

# Benchmark tests (throughput + latency, requires Postgres)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test -- --ignored --nocapture

# COPY benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test test_throughput_copy_insert -- --ignored --nocapture

# Failure-mode benchmark matrix (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test failure_benchmark_test test_failure_bench_full_matrix -- --exact --ignored --nocapture

# Stale-heartbeat rescue benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test failure_benchmark_test test_failure_bench_stale_heartbeat_rescue -- --exact --ignored --nocapture

# MVCC horizon overlap benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --release --package awa --test scheduling_benchmark_test test_mvcc_horizon_overlap_benchmark -- --exact --ignored --nocapture

# Python failure-mode benchmarks
cd awa-python && PYTHONPATH=scripts DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run python scripts/benchmark_runtime.py --scenario failures

# Sequential callback tests (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test external_wait_test -- --test-threads=1 --nocapture

# Sequential callback tests (Python)
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_sequential_callbacks.py -v

# TLA+ correctness models (requires Docker)
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
./correctness/run-tlc.sh protocol/AwaExtended.tla
./correctness/run-tlc.sh core/AwaBatcher.tla
./correctness/run-tlc.sh core/AwaBatcher.tla core/AwaBatcherLiveness.cfg
./correctness/run-tlc.sh races/AwaCbk.tla
./correctness/run-tlc.sh races/AwaCbk.tla races/AwaCbkLiveness.cfg
./correctness/run-tlc.sh races/AwaCron.tla races/AwaCronLiveness.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimOld.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimNew.cfg
./correctness/run-tlc.sh races/AwaViewTrigger.tla
./correctness/run-tlc.sh races/AwaViewTrigger.tla races/AwaViewTriggerOld.cfg

# Concurrent multi-queue lifecycle benchmarks
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --release --package awa --test concurrent_lifecycle_test -- --ignored --nocapture

# tokio-postgres bridge tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa-model --test bridge_tokio_pg_test -- --ignored --nocapture

# Bug fix tests (state guard, unique conflict field)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test executor_guard_test -- --nocapture

# Rate limiting tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test rate_limit_test -- --nocapture

# Weighted concurrency tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test weighted_test -- --nocapture

# Python start() config validation tests
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_start_config.py -v

# Progress tests (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test progress_test -- --test-threads=1 --nocapture

# Progress tests (Python)
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_progress.py -v

# Telemetry e2e — metrics AND traces AND the published Grafana dashboards,
# same assertions as the CI telemetry-validation job (starts grafana/otel-lgtm
# itself and leaves it running for eyeballing; see the script header):
scripts/telemetry-e2e-local.sh

# Or piecewise against an already-running otel-lgtm
# (docker run -d --name awa-otel-lgtm -p 4317:4317 -p 9090:9090 -p 3000:3000 -p 3200:3200 grafana/otel-lgtm:0.22.0):
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  TEMPO_URL=http://localhost:3200 \
  cargo test -p awa --test telemetry_test -- --ignored --nocapture
scripts/validate-grafana.sh                # dashboards/alerts vs live stack
# Cross-language trace leg (needs the awa-python dev venv + built wheel):
cd awa-python && uv run --with opentelemetry-exporter-otlp-proto-grpc \
  python ../scripts/trace-e2e-python.py
# Cleanup:
docker rm -f awa-otel-lgtm

# Repeat 20 times to detect flakes
for i in $(seq 1 20); do echo "=== Run $i ===" && cargo test --workspace 2>&1 | tail -1; done
```
