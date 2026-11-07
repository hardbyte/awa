> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-051: Durable runtime halt

## Status

Proposed.

## Context

An awa runtime can become maintenance leader for every queue in its database, including queues for which it has no handlers. Queue pause prevents new claims but leaves maintenance running. Restarting an embedded runtime's host can disrupt unrelated application work.

Incident containment needs a live control that survives restarts and remains usable when worker connections or maintenance are blocked.

## Decision

Add a durable halt covering all awa runtimes sharing a database, with an independent local runtime stop handle. Database scope matches maintenance leadership and covers new queues and restarted runtimes.

A halt blocks new claims, handler launches, cron scheduling and maintenance, requests cancellation, and permits a bounded drain. Enqueue remains available and queued work is preserved. Control observation and local stop must operate independently of worker-pool capacity and maintenance progress.

Runtimes require fresh database control permission for those activities. Losing it suspends them; fresh permission can recover that suspension. An operator halt requires explicit resume against the current halt. Resume cannot clear newer halts or independent local restrictions and queue pauses.

## Alternatives considered

**Queue pause** is appropriate for stopping dispatch on selected queues, but cannot contain maintenance faults.

**Startup configuration and restarts** need less machinery but offer no live fleet control and couple incident response to application replacement.

**An external feature-flag service as sole authority** reuses application infrastructure but makes library guarantees depend on provider defaults, overrides and integration coverage. Applications may impose additional restrictions without making that service mandatory for awa.

## Consequences

Operators gain persistent fleet intent without stopping host applications. Independent control observation costs connections; control outages can suspend healthy workers. Paused maintenance delays promotion, rescue and retention, allowing backlog and storage growth.

Requested halt and observed cessation are distinct. Cancellation cannot guarantee that handlers, external effects or server-side SQL have stopped. Unresolved work must remain durably visible after heartbeat expiry and block resumption, including automatic recovery, until reconciled or independently fenced.

A local stop that cannot be persisted protects only that runtime's lifetime; restart protection requires a durable halt or external restriction.

Fleet containment requires compatible clients. Obsolete binaries need an enforced database or deployment boundary; without it, the guarantee covers cooperating runtimes only. Compatibility must follow [ADR-041](../041-rolling-upgrade-policy/index.md), and control privileges must preserve producer access. This proposal defines the contract; integrated fleet behavior remains unverified.

## Relationship to ADR-028

[ADR-028](../028-maintenance-only-runtime-role/index.md) proposes a runtime role that runs maintenance without executing jobs. That role, whether standalone or embedded, must obey the same database halt, fresh-permission and local-stop contract as job-executing runtimes. Stopping one maintenance instance locally can hand leadership to another eligible runtime; the database halt must constrain successor leaders too.
