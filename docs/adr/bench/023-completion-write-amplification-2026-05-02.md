# ADR-023 Completion Write-Amplification Follow-Up (2026-05-02)

This note records a focused completion-path follow-up to [ADR-023](../023-receipt-plane-ring-partitioning.md). The experiment targeted three costs visible in the portable harness:

- terminal write amplification for default successful completions
- round trips inside receipt-backed completion transactions
- partition locality for closure and terminal writes

## Changes Tested

- Keep `done_entries.payload` `NULL` when the terminal runtime payload is empty or unchanged from the corresponding ready row.
- Skip the ready-payload lookup entirely when every row in a done batch has an empty terminal payload.
- Merge receipt closure insertion and `attempt_state` cleanup into one data-modifying CTE.
- Sort completion batches by `claim_slot`, then `(job_id, run_lease)`, before dispatching them to queue storage.

The changes preserved ADR-023's correctness shape for that branch: receipt-backed completion still closed the exact `(claim_slot, job_id, run_lease)` claim and stale writers still lost via the closure conflict / materialized lease fallback. At the time of this artifact, the default successful path still appended a `done_entries` row. ADR-026 later superseded that physical detail for compact successful receipt completions while keeping synchronous terminal history and exact queue counts.

## Focused Harness Runs

All runs used the extracted portable harness from `hardbyte/postgresql-job-queue-benchmarking` with:

```text
PRODUCER_BATCH_MAX=128 PRODUCER_BATCH_MS=25 \
uv run bench run --systems awa --producer-rate 5000 --worker-count 64 \
  --phase warmup=warmup:10s --phase clean=clean:75s --sample-every 5
```

The earlier comparison run used the same Awa adapter settings as part of an `awa,pgque` comparison. Systems are run independently with a fresh Postgres container, so the Awa phase is comparable for a directional signal.

| run id | median completion/s | peak completion/s | median backlog | peak backlog | median e2e p99 |
| --- | --: | --: | --: | --: | --: |
| `custom-20260502T022357Z-14b91e` | 3,251 | 4,002 | 57,005 | 116,812 | 11.6 s |
| `custom-20260502T024341Z-674839` | 4,272 | 5,204 | 7,315 | 28,098 | 2.0 s |
| `custom-20260502T024606Z-6a2d02` | 4,621 | 5,861 | 3,780 | 31,988 | 1.9 s |

The patched repeats were both above the comparison run by 31-42% on median completion throughput. The backlog and end-to-end p99 reductions are larger because the producer target was 5,000/s: once completion gets closer to the offered rate, the queue stops running away during the short clean phase.

## Wait Profile

The wait profile did not move to relation or transaction locks. It remained dominated by CPU, `WALWriteLock`, `ClientRead`, and `WalSync`, which matches the write-amplification model from issue #207: fewer completion statements and less terminal payload work improve throughput, but the binding resource is still the WAL/fsync plane.

## Later Follow-Up: ADR-026

ADR-026 shipped the safe version of the narrower terminal-history idea. It does not remove `done_entries`; it keeps a durable terminal fact and hydrates duplicated immutable body fields from the retained `ready_entries` row for ready-backed terminal transitions.

The design keeps enough information for:

- queue counts after ready partitions rotate
- terminal retention and pruning
- `load_job` / admin inspection of completed jobs
- retry/replay tooling that expects terminal job identity and timing

Transitions without a claimed attempt snapshot, such as cancelling an unclaimed available job, still write a wide `done_entries` row even though the ready lane itself is tombstoned and reclaimed later by queue prune.
