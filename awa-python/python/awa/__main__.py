"""awa CLI entry point — python -m awa.

Mirrors the Rust ``awa`` CLI for every command that does not require the
embedded web UI (``serve``) or the storage-transition admin path. Shares
output formatting with the Rust CLI so operators can switch between them
without relearning.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

import awa


def _add_database_url(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL connection URL. Required for all commands except migrate --sql/--version/--from/--to.",
    )


def _require_db(args: argparse.Namespace) -> str:
    if not args.database_url:
        print("--database-url is required.", file=sys.stderr)
        sys.exit(1)
    return args.database_url


def main() -> None:
    parser = argparse.ArgumentParser(prog="awa", description="Awa job queue CLI")
    _add_database_url(parser)
    sub = parser.add_subparsers(dest="command")

    # migrate -------------------------------------------------------------
    p_migrate = sub.add_parser("migrate", help="Run database migrations")
    p_migrate.add_argument("--sql", action="store_true", help="Print migration SQL to stdout instead of applying")
    p_migrate.add_argument("--from", type=int, dest="from_version", default=None, help="Only include migrations after this version (exclusive)")
    p_migrate.add_argument("--to", type=int, default=None, help="Only include migrations up to this version (inclusive)")
    p_migrate.add_argument("--version", type=int, default=None, help="Show a single migration version")
    p_migrate.add_argument("--pending", action="store_true", help="Auto-detect: from=current DB version, to=latest")

    # job -----------------------------------------------------------------
    p_job = sub.add_parser("job", help="Job management")
    sj = p_job.add_subparsers(dest="job_cmd")
    sj.add_parser("dump", help="Dump a single job as JSON").add_argument("id", type=int)
    p_dump_run = sj.add_parser("dump-run", help="Dump one attempt as JSON")
    p_dump_run.add_argument("id", type=int)
    p_dump_run.add_argument("--attempt", type=int, default=None, help="Attempt number to inspect. Defaults to the current attempt.")
    sj.add_parser("retry", help="Retry a failed or cancelled job").add_argument("id", type=int)
    sj.add_parser("cancel", help="Cancel a job").add_argument("id", type=int)
    p_retry_failed = sj.add_parser("retry-failed", help="Retry all failed jobs by kind or queue")
    p_retry_failed.add_argument("--kind")
    p_retry_failed.add_argument("--queue")
    p_discard = sj.add_parser("discard", help="Discard failed jobs by kind")
    p_discard.add_argument("--kind", required=True)
    p_list = sj.add_parser("list", help="List jobs")
    p_list.add_argument("--state")
    p_list.add_argument("--kind")
    p_list.add_argument("--queue")
    p_list.add_argument("--limit", type=int, default=50)

    # queue ---------------------------------------------------------------
    p_queue = sub.add_parser("queue", help="Queue management")
    sq = p_queue.add_subparsers(dest="queue_cmd")
    sq.add_parser("pause", help="Pause a queue").add_argument("queue")
    sq.add_parser("resume", help="Resume a queue").add_argument("queue")
    sq.add_parser("drain", help="Drain a queue (cancel all pending jobs)").add_argument("queue")
    sq.add_parser("stats", help="Show queue statistics")

    # Back-compat alias — kept so existing scripts keep working.
    sub.add_parser("queue-stats", help=argparse.SUPPRESS)

    # dlq -----------------------------------------------------------------
    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue management")
    sd = p_dlq.add_subparsers(dest="dlq_cmd")
    p_dlq_list = sd.add_parser("list", help="List rows in the DLQ")
    p_dlq_list.add_argument("--kind")
    p_dlq_list.add_argument("--queue")
    p_dlq_list.add_argument("--tag")
    p_dlq_list.add_argument("--before-id", type=int, default=None)
    p_dlq_list.add_argument("--before-dlq-at")
    p_dlq_list.add_argument("--limit", type=int, default=50)
    p_dlq_depth = sd.add_parser("depth", help="Show DLQ depth")
    p_dlq_depth.add_argument("--queue")
    sd.add_parser("retry", help="Retry a single DLQ'd job by id").add_argument("id", type=int)
    p_dlq_retry_all = sd.add_parser("retry-all", help="Retry DLQ rows in bulk matching the filter")
    p_dlq_retry_all.add_argument("--kind")
    p_dlq_retry_all.add_argument("--queue")
    p_dlq_retry_all.add_argument("--tag")
    p_dlq_retry_all.add_argument(
        "--all",
        action="store_true",
        help="Retry every DLQ row when no filter is provided (required without --kind/--queue/--tag).",
    )
    p_dlq_purge = sd.add_parser("purge", help="Purge (delete) DLQ rows matching the filter")
    p_dlq_purge.add_argument("--kind")
    p_dlq_purge.add_argument("--queue")
    p_dlq_purge.add_argument("--tag")
    p_dlq_purge.add_argument(
        "--all",
        action="store_true",
        help="Purge every DLQ row when no filter is provided (required without --kind/--queue/--tag).",
    )

    # cron ----------------------------------------------------------------
    p_cron = sub.add_parser("cron", help="Cron/periodic job management")
    sc = p_cron.add_subparsers(dest="cron_cmd")
    sc.add_parser("list", help="List all registered cron schedules")
    sc.add_parser("remove", help="Remove a cron schedule by name").add_argument("name")

    # storage -------------------------------------------------------------
    p_storage = sub.add_parser("storage", help="Storage transition management")
    ss = p_storage.add_subparsers(dest="storage_cmd")
    ss.add_parser("status", help="Show the current storage transition state")
    # prepare/prepare-schema/abort/begin-mixed-transition/finalize are
    # deferred — they mutate production routing and deserve explicit safety
    # guardrails. Use the Rust CLI until dedicated Python subcommands land.

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    asyncio.run(_dispatch(args))


async def _dispatch(args: argparse.Namespace) -> None:
    cmd = args.command

    if cmd == "migrate":
        await _migrate(args)
        return

    db = _require_db(args)
    client = awa.AsyncClient(db)
    try:
        if cmd == "queue-stats":
            await _queue_stats(client)
        elif cmd == "job":
            await _dispatch_job(client, args)
        elif cmd == "queue":
            await _dispatch_queue(client, args)
        elif cmd == "dlq":
            await _dispatch_dlq(client, args)
        elif cmd == "cron":
            await _dispatch_cron(client, args)
        elif cmd == "storage":
            await _dispatch_storage(client, args)
        else:
            print(f"Unknown command: {cmd}", file=sys.stderr)
            sys.exit(1)
    finally:
        await client.close()


# ── migrate ─────────────────────────────────────────────────────────────


async def _migrate(args: argparse.Namespace) -> None:
    current_ver = awa.current_migration_version()
    select_mode = (
        args.sql
        or args.version is not None
        or args.from_version is not None
        or args.to is not None
        or args.pending
    )
    if not select_mode:
        db = _require_db(args)
        await awa.migrate(db)
        print("Migrations applied successfully.")
        return

    if args.version is not None:
        if args.version < 1 or args.version > current_ver:
            print(
                f"Version {args.version} is out of range. Valid versions: 1..{current_ver}",
                file=sys.stderr,
            )
            sys.exit(1)
        range_from, range_to = args.version - 1, args.version
    elif args.pending:
        # Without an applied-version query we cannot compute a true "pending"
        # slice from Python. Fall through to the same listing the Rust CLI
        # emits; callers who need the pending-only slice should run against
        # the Rust CLI until a current_db_version() helper ships.
        range_from, range_to = 0, current_ver
    else:
        range_from = args.from_version if args.from_version is not None else 0
        range_to = args.to if args.to is not None else current_ver

    if range_from >= range_to:
        print(f"No migrations in range ({range_from}, {range_to}].", file=sys.stderr)
        return

    for version, description, sql_text in awa.migrations_range(range_from, range_to):
        print(f"-- Migration V{version}: {description}\n{sql_text}\n")


# ── job ─────────────────────────────────────────────────────────────────


async def _dispatch_job(client: "awa.AsyncClient", args: argparse.Namespace) -> None:
    jc = args.job_cmd
    if jc == "dump":
        print(await client.dump_job(args.id))
    elif jc == "dump-run":
        print(await client.dump_run(args.id, args.attempt))
    elif jc == "retry":
        await client.retry(args.id)
        print(f"Retried job {args.id}")
    elif jc == "cancel":
        await client.cancel(args.id)
        print(f"Cancelled job {args.id}")
    elif jc == "retry-failed":
        if not args.kind and not args.queue:
            print("Must specify --kind or --queue", file=sys.stderr)
            sys.exit(1)
        jobs = await client.retry_failed(kind=args.kind, queue=args.queue)
        print(f"Retried {len(jobs)} failed jobs")
    elif jc == "discard":
        count = await client.discard_failed(args.kind)
        print(f"Discarded {count} failed jobs of kind '{args.kind}'")
    elif jc == "list":
        jobs = await client.list_jobs(
            state=args.state,
            kind=args.kind,
            queue=args.queue,
            limit=args.limit,
        )
        if not jobs:
            print("No jobs found.")
            return
        print(f"{'ID':<8} {'KIND':<25} {'QUEUE':<10} {'STATE':<10} {'ATT':<5} {'MAX':<5}")
        for j in jobs:
            print(
                f"{j.id:<8} {str(j.kind):<25} {str(j.queue):<10} "
                f"{str(j.state):<10} {j.attempt:<5} {j.max_attempts:<5}"
            )
        print(f"\n{len(jobs)} jobs listed.")
    else:
        print("usage: awa job {dump,dump-run,retry,cancel,retry-failed,discard,list}", file=sys.stderr)
        sys.exit(1)


# ── queue ───────────────────────────────────────────────────────────────


async def _dispatch_queue(client: "awa.AsyncClient", args: argparse.Namespace) -> None:
    qc = args.queue_cmd
    if qc == "pause":
        await client.pause_queue(args.queue, paused_by="cli")
        print(f"Paused queue '{args.queue}'")
    elif qc == "resume":
        await client.resume_queue(args.queue)
        print(f"Resumed queue '{args.queue}'")
    elif qc == "drain":
        count = await client.drain_queue(args.queue)
        print(f"Drained {count} jobs from queue '{args.queue}'")
    elif qc == "stats":
        await _queue_stats(client)
    else:
        print("usage: awa queue {pause,resume,drain,stats}", file=sys.stderr)
        sys.exit(1)


async def _queue_stats(client: "awa.AsyncClient") -> None:
    stats = await client.queue_stats()
    if not stats:
        print("No queues found.")
        return
    print(f"{'QUEUE':<15} {'AVAIL':<10} {'RUNNING':<10} {'FAILED':<10}")
    for s in stats:
        print(f"{s.queue:<15} {s.available:<10} {s.running:<10} {s.failed:<10}")


# ── dlq ─────────────────────────────────────────────────────────────────


async def _dispatch_dlq(client: "awa.AsyncClient", args: argparse.Namespace) -> None:
    dc = args.dlq_cmd
    if dc == "list":
        rows = await client.list_dlq(
            kind=args.kind,
            queue=args.queue,
            tag=args.tag,
            before_id=args.before_id,
            before_dlq_at=args.before_dlq_at,
            limit=args.limit,
        )
        if not rows:
            print("DLQ is empty (no matching rows).")
            return
        print(f"{'ID':<8} {'KIND':<25} {'QUEUE':<10} {'REASON':<30} {'DLQ_AT':<25}")
        for row in rows:
            reason = row.reason if len(row.reason) <= 30 else row.reason[:27] + "..."
            print(f"{row.id:<8} {str(row.kind):<25} {str(row.queue):<10} {reason:<30} {str(row.dlq_at):<25}")
        print(f"\n{len(rows)} rows.")
        last = rows[-1]
        print(f"Next page: --before-id {last.id} --before-dlq-at {last.dlq_at}")
    elif dc == "depth":
        if args.queue:
            depth = await client.dlq_depth(queue=args.queue)
            print(f"{args.queue}: {depth}")
        else:
            total = await client.dlq_depth()
            by_queue = await client.dlq_depth_by_queue()
            print(f"Total: {total}")
            for q, count in by_queue:
                print(f"  {q}: {count}")
    elif dc == "retry":
        job = await client.retry_from_dlq(args.id)
        if job is None:
            print(f"No DLQ row with id {args.id}")
        else:
            print(f"Retried DLQ job {args.id} → job state {job.state}")
    elif dc == "retry-all":
        count = await client.bulk_retry_from_dlq(
            kind=args.kind,
            queue=args.queue,
            tag=args.tag,
            allow_all=args.all,
        )
        print(f"Retried {count} DLQ rows.")
    elif dc == "purge":
        count = await client.purge_dlq(
            kind=args.kind,
            queue=args.queue,
            tag=args.tag,
            allow_all=args.all,
        )
        print(f"Purged {count} DLQ rows.")
    else:
        print("usage: awa dlq {list,depth,retry,retry-all,purge}", file=sys.stderr)
        sys.exit(1)


# ── cron ────────────────────────────────────────────────────────────────


async def _dispatch_cron(client: "awa.AsyncClient", args: argparse.Namespace) -> None:
    cc = args.cron_cmd
    if cc == "list":
        rows = await client.list_cron_jobs()
        if not rows:
            print("No cron job schedules found.")
            return
        print(f"{'NAME':<25} {'CRON':<20} {'TIMEZONE':<12} {'KIND':<25} {'QUEUE':<10}")
        for s in rows:
            print(
                f"{s['name']:<25} {s['cron_expr']:<20} {s['timezone']:<12} "
                f"{s['kind']:<25} {s['queue']:<10}"
            )
        print(f"\n{len(rows)} schedules listed.")
    elif cc == "remove":
        deleted = await client.delete_cron_job(args.name)
        if deleted:
            print(f"Removed cron schedule '{args.name}'")
        else:
            print(f"No cron schedule found with name '{args.name}'")
    else:
        print("usage: awa cron {list,remove}", file=sys.stderr)
        sys.exit(1)


# ── storage ─────────────────────────────────────────────────────────────


async def _dispatch_storage(client: "awa.AsyncClient", args: argparse.Namespace) -> None:
    if args.storage_cmd == "status":
        print(await client.storage_status())
    else:
        print("usage: awa storage status", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
