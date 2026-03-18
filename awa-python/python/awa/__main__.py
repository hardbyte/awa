"""awa CLI entry point — python -m awa.

Delegates to the Rust CLI binary when available, otherwise provides
basic migration and admin commands directly from Python.
"""

import argparse
import asyncio
import sys

import awa


def main():
    parser = argparse.ArgumentParser(prog="awa", description="Awa job queue CLI")
    parser.add_argument("--database-url", required=True, help="PostgreSQL connection URL")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("migrate", help="Run database migrations")

    stats_parser = sub.add_parser("queue-stats", help="Show queue statistics")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    asyncio.run(_run(args))


async def _run(args):
    if args.command == "migrate":
        await awa.migrate(args.database_url)
        print("Migrations applied successfully.")
    elif args.command == "queue-stats":
        client = awa.Client(args.database_url)
        stats = await client.queue_stats()
        if not stats:
            print("No queues found.")
        else:
            print(f"{'QUEUE':<15} {'AVAIL':<10} {'RUNNING':<10} {'FAILED':<10}")
            for s in stats:
                print(f"{s['queue']:<15} {s['available']:<10} {s['running']:<10} {s['failed']:<10}")


if __name__ == "__main__":
    main()
