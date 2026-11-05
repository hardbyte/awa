"""Python side of the persona soak: producers, async workers and the migrate path.

Started and stopped by the Rust harness. Every enqueue, handler start and handler end
is appended to a JSON-lines ledger that the harness folds into its own ledger, so the
oracle checks Python jobs like any others.

Environment:
  SOAK_DATABASE_URL   database to use (an awa_soak* database)
  SOAK_LEDGER         ledger file, appended to
  SOAK_SEED, SOAK_SCALE, SOAK_PACE
  SOAK_ROLE           "full" (default) or "migrate_probe"
  SOAK_QUEUE_WORK, SOAK_QUEUE_DEADLINE, SOAK_QUEUE_SHARED   queue names

Signals: SIGUSR1 stops producing (workers keep running); SIGTERM exits at once.
"""

import asyncio
import json
import os
import random
import signal
import sys
import time
from dataclasses import dataclass

import awa

URL = os.environ["SOAK_DATABASE_URL"]
ROLE = os.environ.get("SOAK_ROLE", "full")
SEED = int(os.environ.get("SOAK_SEED", "1"))
SCALE = float(os.environ.get("SOAK_SCALE", "1"))
PACE = float(os.environ.get("SOAK_PACE", "1"))
QUEUE_WORK = os.environ.get("SOAK_QUEUE_WORK", "awa_soak_py_work")
QUEUE_DEADLINE = os.environ.get("SOAK_QUEUE_DEADLINE", "awa_soak_py_deadline")
QUEUE_SHARED = os.environ.get("SOAK_QUEUE_SHARED", "awa_soak_py_shared")
WORK_WORKERS = 8
DEADLINE_WORKERS = 6
SHARED_WORKERS = 6
DEADLINE_MS = 5000

# Outcomes a job may end in, matching the harness (1 completed, 2 failed, 4 cancelled).
COMPLETED, FAILED, CANCELLED = 1, 2, 4

ledger = open(os.environ["SOAK_LEDGER"], "a", buffering=1) if "SOAK_LEDGER" in os.environ else None


def log(**event):
    if ledger is not None:
        ledger.write(json.dumps(event, separators=(",", ":")) + "\n")


@dataclass
class PyJob:
    seq: int
    behavior: str
    work_ms: int
    born: float = 0.0


@dataclass
class PySharedJob:
    seq: int
    behavior: str
    work_ms: int


active = 0
max_active = 0
started = 0
enqueued = 0
errors = 0


async def handle(job):
    """One handler for every behaviour; see `plan` for what each one is for."""
    global active, max_active, started
    args = job.args
    attempt = job.attempt
    active += 1
    max_active = max(max_active, active)
    started += 1
    log(e="start", id=job.id, attempt=attempt, q=job.queue)
    result = "completed"
    try:
        behavior = args.behavior
        if behavior == "ok":
            await asyncio.sleep(args.work_ms / 1000)
        elif behavior == "progress":
            for step in range(1, 5):
                await asyncio.sleep(args.work_ms / 4000)
                job.set_progress(step * 25, f"step {step}")
                await job.flush_progress()
        elif behavior == "retry":
            await asyncio.sleep(0.05)
            if attempt < 3:
                result = "raise"
                raise RuntimeError("transient failure")
        elif behavior == "doomed":
            result = "raise"
            raise RuntimeError("permanent failure")
        elif behavior == "retry_after":
            if attempt == 1:
                result = "retry_after"
                return awa.RetryAfter(seconds=1.0)
        elif behavior == "snooze":
            # A snooze does not count as an attempt, so the wait is keyed on the clock.
            if time.time() < args.born + 3.0:
                result = "snooze"
                return awa.Snooze(seconds=1.0)
        elif behavior == "cancel":
            result = "cancel"
            return awa.Cancel("rejected")
        elif behavior == "long":
            # Overruns the queue deadline on the first attempt only.
            await asyncio.sleep(args.work_ms / 1000 if attempt == 1 else 0.1)
        elif behavior == "ignore_deadline":
            # Documented behaviour: after the deadline rescue the worker slot is freed
            # but this coroutine keeps running, so more handlers than max_workers run.
            await asyncio.sleep(args.work_ms / 1000)
        else:
            await asyncio.sleep(args.work_ms / 1000)
        return None
    except Exception:
        raise
    finally:
        active -= 1
        log(e="end", id=job.id, attempt=attempt, r=result)


def plan(rng, scale_rows=None):
    """Returns (queue, behavior, work_ms, max_attempts, allowed_outcomes, kind_class)."""
    roll = rng.random() * 100
    if roll < 40:
        return QUEUE_WORK, "ok", rng.randint(20, 400), 5, COMPLETED
    if roll < 48:
        return QUEUE_WORK, "progress", rng.randint(800, 2500), 5, COMPLETED
    if roll < 56:
        return QUEUE_WORK, "retry", 50, 5, COMPLETED | FAILED
    if roll < 60:
        return QUEUE_WORK, "doomed", 50, 3, FAILED
    if roll < 66:
        return QUEUE_WORK, "retry_after", 50, 5, COMPLETED | FAILED
    if roll < 72:
        return QUEUE_WORK, "snooze", 50, 5, COMPLETED
    if roll < 75:
        return QUEUE_WORK, "cancel", 50, 3, CANCELLED
    if roll < 80:
        return QUEUE_DEADLINE, "long", rng.randint(8000, 12000), 3, COMPLETED | FAILED
    if roll < 83:
        return QUEUE_DEADLINE, "ignore_deadline", rng.randint(9000, 14000), 2, COMPLETED | FAILED
    return QUEUE_SHARED, rng.choice(["ok", "ok", "retry", "progress"]), rng.randint(20, 300), 5, COMPLETED | FAILED


def kind_for(queue):
    return PySharedJob if queue == QUEUE_SHARED else PyJob


def make_args(queue, rng, behavior, work_ms):
    seq = rng.randint(1, 1 << 40)
    if queue == QUEUE_SHARED:
        return PySharedJob(seq=seq, behavior=behavior, work_ms=work_ms)
    return PyJob(seq=seq, behavior=behavior, work_ms=work_ms, born=time.time())


def kind_name(queue):
    return "py_shared_job" if queue == QUEUE_SHARED else "py_job"


def record_enqueued(job, queue, behavior, allowed, max_attempts):
    global enqueued
    enqueued += 1
    log(e="enq", id=job.id, q=queue, b=behavior, x=allowed, ma=max_attempts)


async def producer_singles(client, rng, stop):
    """Single inserts, one or two at a time."""
    period = 0.25 / max(SCALE, 0.1)
    while not stop.is_set():
        for _ in range(rng.randint(1, 2)):
            queue, behavior, work_ms, max_attempts, allowed = plan(rng)
            args = make_args(queue, rng, behavior, work_ms)
            try:
                job = await client.insert(args, kind=kind_name(queue), queue=queue, max_attempts=max_attempts)
                record_enqueued(job, queue, behavior, allowed, max_attempts)
            except Exception as error:  # connection killed by chaos, pool timeout
                note_error("insert", error)
        await asyncio.sleep(period)


async def producer_batches(client, rng, stop):
    """insert_many above the old 5,461-row bind-parameter limit, and COPY."""
    rows = int(6500 * SCALE) if SCALE >= 1 else 6500
    interval = max(20.0, 75.0 * PACE)
    await asyncio.sleep(min(15.0, interval))
    flip = 0
    while not stop.is_set():
        flip += 1
        args = [PyJob(seq=i, behavior="ok", work_ms=0, born=time.time()) for i in range(rows)]
        try:
            started_at = time.monotonic()
            if flip % 3 == 0:
                jobs = await client.insert_many_copy(args, kind="py_job", queue=QUEUE_WORK, max_attempts=3)
                how = "copy"
            else:
                # One transaction, one insert_many call far above the old bind-parameter limit.
                tx = await client.transaction()
                try:
                    jobs = await tx.insert_many(args, kind="py_job", queue=QUEUE_WORK, max_attempts=3)
                    await tx.commit()
                except Exception:
                    await tx.rollback()
                    raise
                how = "insert_many"
            for job in jobs:
                record_enqueued(job, QUEUE_WORK, "ok", COMPLETED, 3)
            print(f"BATCH how={how} rows={len(jobs)} seconds={time.monotonic() - started_at:.2f}", flush=True)
        except Exception as error:
            note_error(f"batch of {rows}", error)
        await wait_or_stop(stop, interval)


async def producer_transactions(client, rng, stop):
    """Enqueue inside an application transaction that sometimes rolls back."""
    while not stop.is_set():
        count = rng.randint(1, 20)
        plans = [plan(rng) for _ in range(count)]
        rollback = rng.random() < 0.35
        try:
            tx = await client.transaction()
            try:
                created = []
                for queue, behavior, work_ms, max_attempts, allowed in plans:
                    args = make_args(queue, rng, behavior, work_ms)
                    job = await tx.insert(args, kind=kind_name(queue), queue=queue, max_attempts=max_attempts)
                    created.append((job, queue, behavior, allowed, max_attempts))
                ids = [item[0].id for item in created]
                if rollback:
                    await tx.rollback()
                    log(e="rolled_back", ids=ids)
                else:
                    await tx.commit()
                    for job, queue, behavior, allowed, max_attempts in created:
                        record_enqueued(job, queue, behavior, allowed, max_attempts)
            except Exception:
                try:
                    await tx.rollback()
                except Exception:
                    pass
                raise
        except Exception as error:
            note_error("transaction", error)
        await asyncio.sleep(1.5 / max(SCALE, 0.1))


def note_error(what, error):
    global errors
    errors += 1
    log(e="error", what=what, error=str(error)[:200])
    if errors <= 5:
        print(f"ERROR {what}: {str(error)[:200]}", flush=True)


async def wait_or_stop(stop, seconds):
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def reporter(stop):
    while True:
        await asyncio.sleep(2)
        print(f"STAT active={active} max_active={max_active} started={started} enqueued={enqueued} errors={errors}", flush=True)


async def migrate_and_probe(client, label):
    """What an application does at startup: migrate, then query."""
    await client.migrate()
    stats = await client.queue_stats()
    jobs = await client.list_jobs(limit=5)
    print(f"MIGRATED {label} queues={len(stats)} sample={len(jobs)}", flush=True)


async def migrate_probe_main():
    client = awa.AsyncClient(URL, max_connections=4)
    await migrate_and_probe(client, "probe")
    # Idle with the pool open: connections stranded by migrate would show up as
    # sessions beyond the pool size, or as idle-in-transaction.
    await asyncio.sleep(4)
    print("IDLE", flush=True)
    await asyncio.sleep(4)
    await client.close()
    print("CLOSED", flush=True)


async def main():
    if ROLE == "migrate_probe":
        await migrate_probe_main()
        return
    rng = random.Random(SEED)
    client = awa.AsyncClient(URL, max_connections=14)
    await migrate_and_probe(client, "startup")
    client.task(PyJob, kind="py_job", queue=QUEUE_WORK)(handle)
    client.task(PySharedJob, kind="py_shared_job", queue=QUEUE_SHARED)(handle)
    await client.start(
        [
            {"name": QUEUE_WORK, "max_workers": WORK_WORKERS},
            {"name": QUEUE_DEADLINE, "max_workers": DEADLINE_WORKERS, "deadline_duration_ms": DEADLINE_MS},
            {"name": QUEUE_SHARED, "max_workers": SHARED_WORKERS},
        ],
        heartbeat_interval_ms=2000,
        heartbeat_staleness_ms=12000,
        heartbeat_rescue_interval_ms=3000,
        deadline_rescue_interval_ms=2000,
        promote_interval_ms=250,
    )
    # The deadline queue serves a second kind through the same handler.
    print(f"READY pid={os.getpid()}", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, stop.set)
    tasks = [
        asyncio.create_task(producer_singles(client, rng, stop)),
        asyncio.create_task(producer_batches(client, rng, stop)),
        asyncio.create_task(producer_transactions(client, rng, stop)),
        asyncio.create_task(reporter(stop)),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.Event().wait()


def exit_now(signum, _frame):
    os._exit(128 + signum)


if __name__ == "__main__":
    # PyO3/tokio teardown can crash while Python unloads modules; exit without finalizers.
    signal.signal(signal.SIGTERM, exit_now)
    signal.signal(signal.SIGINT, exit_now)
    try:
        asyncio.run(main())
    finally:
        sys.stdout.flush()
        os._exit(0)
