"""Released-wheel worker and producer for the 0.6.9 -> 0.6.10 patch rehearsal.

Runs under an installed `awa-pg` wheel (the released N-1), never the source
tree, so the rehearsal exercises the bytes users actually run. Every line on
stdout is an event the Rust harness parses:

    READY pid=<pid> version=<awa version>
    START pid=<pid> job_id=<id> attempt=<n> seq=<seq> mode=<mode>
    RETRY pid=<pid> job_id=<id> attempt=<n>
    COMPLETE pid=<pid> job_id=<id> attempt=<n> seq=<seq>
    ENQUEUED pid=<pid> job_id=<id> seq=<seq> mode=<mode> queue=<queue>
    PRODUCED pid=<pid> count=<n>

Modes (REHEARSAL_MODE):
    worker   serve REHEARSAL_QUEUES (comma separated) until killed
    produce  insert REHEARSAL_COUNT jobs into REHEARSAL_QUEUE, then exit
"""

import asyncio
import datetime
import os
from dataclasses import dataclass
from importlib import metadata

import awa
from _subprocess_exit import run_async_main_without_finalizers


@dataclass
class PatchRehearsalJob:
    seq: int
    mode: str


def version() -> str:
    try:
        return metadata.version("awa-pg")
    except metadata.PackageNotFoundError:
        return "unknown"


async def run_worker(client: "awa.AsyncClient") -> None:
    queues = [queue for queue in os.environ["REHEARSAL_QUEUES"].split(",") if queue]
    workers = int(os.environ.get("REHEARSAL_WORKERS", "4"))
    slow_ms = int(os.environ.get("REHEARSAL_SLOW_MS", "1500"))
    pid = os.getpid()

    @client.task(PatchRehearsalJob, queue=queues[0])
    async def handle(job):
        print(
            f"START pid={pid} job_id={job.id} attempt={job.attempt} "
            f"seq={job.args.seq} mode={job.args.mode}",
            flush=True,
        )
        if job.args.mode == "fail_once" and job.attempt == 1:
            print(f"RETRY pid={pid} job_id={job.id} attempt={job.attempt}", flush=True)
            return awa.RetryAfter(seconds=0.5)
        if job.args.mode == "slow":
            await asyncio.sleep(slow_ms / 1000)
        else:
            await asyncio.sleep(0.005)
        print(
            f"COMPLETE pid={pid} job_id={job.id} attempt={job.attempt} seq={job.args.seq}",
            flush=True,
        )
        return None

    await client.start(
        [(queue, workers) for queue in queues],
        poll_interval_ms=50,
        leader_election_interval_ms=200,
        heartbeat_interval_ms=100,
        promote_interval_ms=100,
        heartbeat_rescue_interval_ms=250,
        heartbeat_staleness_ms=2000,
        queue_storage_schema=os.environ["REHEARSAL_SCHEMA"],
        # A long ready-ring rotation keeps completed jobs readable for the
        # harness's final accounting.
        queue_storage_queue_rotate_interval_ms=3_600_000,
    )
    print(f"READY pid={pid} version={version()}", flush=True)
    await asyncio.Event().wait()


async def run_producer(client: "awa.AsyncClient") -> None:
    queue = os.environ["REHEARSAL_QUEUE"]
    count = int(os.environ["REHEARSAL_COUNT"])
    seq_start = int(os.environ["REHEARSAL_SEQ_START"])
    modes = os.environ.get("REHEARSAL_JOB_MODES", "ok").split(",")
    delay_s = float(os.environ.get("REHEARSAL_DELAY_S", "0"))
    pause_ms = int(os.environ.get("REHEARSAL_PAUSE_MS", "0"))
    pid = os.getpid()
    for index in range(count):
        seq = seq_start + index
        mode = modes[index % len(modes)]
        run_at = None
        if delay_s > 0:
            run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=delay_s
            )
        job = await client.insert(
            PatchRehearsalJob(seq=seq, mode=mode),
            queue=queue,
            max_attempts=5,
            run_at=run_at,
        )
        print(
            f"ENQUEUED pid={pid} job_id={job.id} seq={seq} mode={mode} queue={queue}",
            flush=True,
        )
        if pause_ms:
            await asyncio.sleep(pause_ms / 1000)
    print(f"PRODUCED pid={pid} count={count}", flush=True)


async def main() -> None:
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    mode = os.environ["REHEARSAL_MODE"]
    if mode == "worker":
        await run_worker(client)
    elif mode == "produce":
        await run_producer(client)
    else:
        raise SystemExit(f"unknown REHEARSAL_MODE={mode}")


if __name__ == "__main__":
    # PyO3/tokio teardown can crash while Python unloads modules; these are
    # single-purpose subprocesses, so skip finalizers on exit.
    run_async_main_without_finalizers(main)
