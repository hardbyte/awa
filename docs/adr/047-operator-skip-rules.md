# ADR-047: Operator skip rules

## Status

Proposed. Needs one migration, numbered when it lands, because the next numbers are contested by open PRs. Builds on [ADR-038](038-queue-runtime-overrides.md) runtime overrides and on the poison-job bounds from #546.

## Context

A poison job is one whose handler panics, hangs or crashes its worker. #546 bounded what one costs: a panic is a failed attempt, and heartbeat and deadline rescues now count against `max_attempts`. A bounded poison job can still do harm while it spends those attempts. Each crash takes down every job running in that process, and a kind with many bad jobs keeps a queue busy failing them.

When an operator spots the pattern, for example `ResizeImage` jobs whose `args.width` exceeds 20000, the controls they have are:

| Control | Scope | Gap |
| --- | --- | --- |
| Queue pause (`awa queue pause`, `queue_meta.paused`) | Every job on the queue | Stops healthy kinds sharing the queue |
| Cron pause | One schedule | Only cron-produced jobs |
| Batch cancel or move ([ADR-030](030-batch-operations.md)) | Matching jobs that exist now | Misses jobs enqueued afterwards, and cannot see running ones |
| DLQ purge or retry ([ADR-020](020-dead-letter-queue.md)) | Jobs already failed | After the damage |
| A feature flag in the handler | Whatever the code checks | Needs a deploy, and runs after argument decoding, which may be what crashes |

The job-lifecycle docs answer "stop a kind" with "put risky kinds on their own queue and pause it". That works only if the queue layout anticipated the problem. What is missing is a standing rule, effective within seconds and without a deploy: *don't run jobs of this kind, optionally only those matching this predicate, until I say so.*

## Decision

Add **skip rules**: operator-managed rows that the worker checks after it claims a job and before it runs the handler.

### Rule shape

A small control-plane table, `awa.job_skip_rules`, holds one row per rule:

| Column | Meaning |
| --- | --- |
| `id` | Identity, shown in logs, metrics and the job's error history |
| `kind` | Required. Rules are always scoped to one kind |
| `queue` | Optional; `NULL` means every queue |
| `expression` | Optional CEL predicate. `NULL` matches every job of the kind |
| `action` | `hold`, `dead_letter` or `cancel` |
| `reason` | Required free text, shown wherever the rule's effect is |
| `created_by`, `created_at` | Audit |
| `expires_at` | Optional. An expired rule stops matching |
| `disabled_at` | Set when the rule is removed. Rows are kept for audit |

Operators change rules rarely, and nothing on the job path writes to the table, so it is not a hot mutable table.

### Matching

Workers load the enabled, unexpired rules on the same slow refresh as queue overrides (ADR-038, 10 s). They compile each expression once per refresh and index the rules by kind. For each claimed job:

1. **No rule for its kind.** One hash lookup, then the job runs as today.
2. **A rule matches.** Its `queue` is `NULL` or equal to the job's queue, and its expression is absent or evaluates to `true`. The worker applies the rule's action instead of running the handler. If several rules match, the most restrictive action wins: `cancel` over `dead_letter` over `hold`.

The CEL environment is the job's `args`, `metadata`, `kind`, `queue`, `attempt`, `priority` and `tags`. It is evaluated on the worker with the CEL support that callback filters already use. A rule whose expression fails to compile is rejected when it is created.

An expression that errors when evaluated (for example `args.width` on a job without a `width`) **does not match**. The job runs as it would have without the rule. The error is counted in `awa.job.skip_rule_errors{rule}` and recorded on the rule as `last_error`. The other choice, treating an error as a match, would let one malformed expression quarantine healthy jobs. CEL's `has()` lets a rule author handle absent fields explicitly.

### Actions

Each action reuses an existing outcome path. None is a new kind of state transition.

- **`hold`, the default.** The job is snoozed: rescheduled without spending an attempt, with the rule ID recorded under `metadata."awa:held_by"`. It is held until the rule's `expires_at` or for 24 hours, whichever is sooner, not for a short interval. A short interval would re-claim every held job each cycle, costing one claim and one snooze transaction per job per interval. When a rule is removed, `awa job skip remove` releases the jobs it held with one batch operation (ADR-030) that sets `run_at = now()` for scheduled jobs carrying that `awa:held_by`. A held job that reaches its horizon while the rule is still active is claimed and held again, at most once a day.
- **`dead_letter`.** The job moves to the dead-letter queue with reason `skipped`, keeping its args so a DLQ retry can re-run it after a fix. The attempt is spent: the job did not run, but it was not deferred either.
- **`cancel`.** The job is cancelled with reason `skipped by rule <id>: <reason>`.

Hooks and registered follow-ups behave as if the handler had returned the corresponding result. A hold is a snooze, so nothing fires. `dead_letter` fires `Exhausted` and `on_exhausted_enqueue`. `cancel` fires `Cancelled` and `on_cancelled_enqueue`. A compensation registered for "this job will not run" therefore runs whichever way that came about. The handler never runs, so nothing is staged (ADR-046).

A rule never touches an attempt that is already running. To stop those as well, cancel them with the admin cancel, which signals the running handler.

### Operator surface

- **CLI:** `awa job skip add --kind K [--queue Q] [--where EXPR] [--action hold|dead_letter|cancel] --reason R [--expires-in D] [--dry-run]`, plus `list` and `remove`.
- **Preview.** `--dry-run`, and the confirmation step without it, report how many available, scheduled and retryable jobs the rule would match. ADR-030 rejected CEL as a filter for bulk mutation because counts must come from the database. Here the preview narrows by `kind`, `queue` and `state` in SQL, then evaluates the expression on at most 10,000 of those rows. Past that cap it reports "at least N". Enforcement is per claimed job and never depends on the preview, so an approximate count cannot widen what the rule does.
- **Admin UI:** the rules list, each rule's matches and held count, and a remove button. It uses the same API as the CLI.
- **Metrics:**
  - `awa.job.skipped{kind, queue, action, rule}`;
  - `awa.job.skip_rule_errors{rule}`;
  - a held-jobs gauge per rule, so a hold nobody is watching shows up on a dashboard.

### Rollout

Phase 1 ships rules with `kind`, `queue` and an action, without expressions. That covers "stop this kind on a shared queue" with no CEL dependency. Phase 2 adds `expression`, gated by the `cel` feature that callback filters already use. A worker built without that feature does not match rules that have an expression, and it logs a warning once per rule.

In a mixed fleet, workers older than the migration ignore rules. `awa job skip add` warns when live runtimes report an older version, using the runtime snapshot that the admin overview already shows.

## Fit with the rest of Awa

- **Control plane in the database ([ADR-038](038-queue-runtime-overrides.md)).** Rules follow the queue-override pattern: a small table, read on a slow refresh, applied by every worker without a deploy.
- **The claim path is untouched ([ADR-033](033-per-key-execution-control.md)).** ADR-033 rules out skipping a row at claim time "as if it were claimed", because that breaks the cursor-monotonicity and prune proofs. A skipped job here is claimed normally and then finalized through the snooze, DLQ or cancel path, each of which the storage models already cover. The cost is one claim per skipped job, paid only by jobs that match a rule.
- **CEL ([ADR-030](030-batch-operations.md)).** Enforcement evaluates CEL against one job at a time, as callback filters do, which is the use ADR-030 called CEL good for. Only the preview reads in bulk, and it is bounded in SQL by kind and capped, as ADR-030 requires.
- **Not a workflow engine.** A rule decides whether a job runs at all, the same decision a queue pause makes at a finer grain. It does not decide which work follows, and it keeps no relation between jobs, so it passes the test in ADR-046 ([#556](https://github.com/hardbyte/awa/pull/556)). Rules are an operator control with an audit trail and an expiry. They are not a routing API: they are not offered on `ClientBuilder` or `InsertOpts`.

## Consequences

### Positive

- An operator can quarantine a poison pattern within seconds of spotting it, scoped to one kind and optionally to the arguments that trigger it, without pausing healthy kinds or deploying.
- `hold` loses nothing: the jobs keep their attempts and run once the rule is removed.
- Matching happens before the handler runs, so arguments that crash deserialization can be held too.
- Every effect names its rule in metrics, logs and the job's error history.

### Negative

- A claim, then a finalize, for each skipped job. A rule that matches a large backlog churns through it once, and `hold` keeps that to once per held job per horizon.
- Up to 10 seconds between adding a rule and every worker applying it. Jobs claimed in that window run. An urgent stop is still a queue pause.
- Another place where a job can go "missing". The held-jobs gauge and the `awa:held_by` metadata exist so that a hold is visible.
- A forgotten hold delays jobs indefinitely. Expiry is optional because some fixes take longer than any default, but the CLI asks for confirmation when `--expires-in` is omitted.

## Alternatives considered

### Filter in the claim query

Push the rules into claim SQL so that matching jobs are never claimed. Rejected for three reasons:
- it changes the claim path that ADR-019, ADR-023 and ADR-033 constrain;
- CEL cannot run in SQL, which ADR-030 already settled;
- every claim on every queue would pay for the rules, not only jobs of the affected kinds.

### Reject at enqueue

Refuse new jobs that match a rule. It misses jobs already queued. Rejecting an enqueue inside a caller's transaction also turns an operational problem into an application failure, which is the tension [ADR-035](035-backpressure-flow-control.md) describes. A hold at claim time protects the workers without failing producers.

### Pause by kind

Add per-kind pause to `queue_meta`. This is Phase 1 with a single action and no audit or expiry. A rules table carries that subset at the same cost and leaves room for expressions.

### A skip check in user code

A handler can check a feature flag and return `Snooze`. That works today and needs no engine change. But every kind needs its own check and a deploy to add one, and it runs after argument decoding.

### Make skipped jobs a new state

A `held` state would make holds visible without metadata. It was rejected because it touches every state machine, both engines, the admin UI and the TLA+ models, whereas a hold that is a snooze with a marker needs no new state.

## Open questions

- **Should `hold` be the default?** It loses nothing, but a pile of held jobs is easy to ignore. The gauge and the expiry prompt are the mitigation; `dead_letter` makes the cost visible but spends an attempt.
- **Should a rule match on `errors`,** such as the last error message? It would let an operator quarantine "jobs that failed with OOM" without knowing the argument pattern, at the cost of a larger CEL environment.
- **A faster path than the 10-second refresh,** for example a `NOTIFY` on rule changes, if operators need an immediate stop and a queue pause is too coarse.
