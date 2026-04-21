import asyncio
import contextlib
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
WORKER_SCRIPT = Path(__file__).with_name("chaos_worker.py")


@dataclass
class ChaosProbe:
    marker: str


def chaos_timeout_multiplier() -> float:
    raw = os.environ.get("AWA_CHAOS_TIMEOUT_MULTIPLIER")
    if raw is not None:
        try:
            return max(float(raw), 1.0)
        except ValueError:
            pass

    return 3.0 if os.environ.get("CI") else 1.0


def scaled_timeout(timeout: float) -> float:
    return timeout * chaos_timeout_multiplier()


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    tx = await c.transaction()
    await tx.execute(
        """
        CREATE TABLE IF NOT EXISTS awa_test_chaos_markers (
            job_id BIGINT NOT NULL,
            attempt INTEGER NOT NULL,
            marker TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'chaos_%'")
    await tx.execute("DELETE FROM awa.queue_meta WHERE queue LIKE 'chaos_%'")
    await tx.execute(
        "DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
    )
    await tx.execute("DELETE FROM awa.runtime_instances")
    await tx.execute(
        """
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        """
    )
    await tx.execute("DELETE FROM awa_test_chaos_markers")
    await tx.commit()
    yield c
    await c.close()


async def _active_queue_storage_schema(client: awa.AsyncClient) -> str | None:
    tx = await client.transaction()
    try:
        row = await tx.fetch_one("SELECT awa.active_queue_storage_schema() AS schema_name")
        return row["schema_name"]
    finally:
        await tx.rollback()


async def _backdate_running_deadline(client: awa.AsyncClient, job_id: int) -> None:
    schema = await _active_queue_storage_schema(client)
    tx = await client.transaction()
    try:
        if schema:
            await tx.execute(
                f"UPDATE {schema}.leases "
                "SET deadline_at = now() - interval '1 second' "
                "WHERE job_id = $1 AND state = 'running'",
                job_id,
            )
        else:
            await tx.execute(
                "UPDATE awa.jobs SET deadline_at = now() - interval '1 second' WHERE id = $1",
                job_id,
            )
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_worker_sigkill_job_is_rescued_and_completed(client):
    queue = f"chaos_{uuid.uuid4().hex[:8]}"
    worker_a = await _start_worker(queue, "hang")
    worker_b = None

    try:
        await _wait_for_line(worker_a, "READY role=hang", timeout=10)

        job = await client.insert(ChaosProbe(marker="ci"), queue=queue)
        await _wait_for_job(
            client,
            job.id,
            lambda row: row is not None
            and row["state"] == "running"
            and row["attempt"] == 1
            and row["heartbeat_at"] is not None,
            timeout=10,
            worker=worker_a,
            description="running attempt=1 with heartbeat",
        )

        await _backdate_running_deadline(client, job.id)

        worker_a.kill()
        await asyncio.wait_for(worker_a.wait(), timeout=5)

        worker_b = await _start_worker(queue, "complete")
        await _wait_for_line(worker_b, "READY role=complete", timeout=10)

        row = await _wait_for_job(
            client,
            job.id,
            lambda current: current is not None
            and current["state"] == "completed"
            and current["attempt"] == 2
            and current["finalized_at"] is not None,
            timeout=45,
            worker=worker_b,
            description="completed attempt=2",
        )
        assert row["attempt"] == 2
        assert row["finalized_at"] is not None
    finally:
        await _stop_process(worker_a)
        await _stop_process(worker_b)


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_worker_hang_is_cancelled_by_deadline_rescue_and_retried(client):
    queue = f"chaos_{uuid.uuid4().hex[:8]}"
    worker = await _start_worker(queue, "hang_until_cancel")

    try:
        await _wait_for_line(worker, "READY role=hang_until_cancel", timeout=10)

        job = await client.insert(ChaosProbe(marker="deadline"), queue=queue)
        await _wait_for_job(
            client,
            job.id,
            lambda row: row is not None
            and row["state"] == "running"
            and row["attempt"] == 1
            and row["heartbeat_at"] is not None,
            timeout=10,
            worker=worker,
            description="running attempt=1 with heartbeat",
        )

        await _backdate_running_deadline(client, job.id)

        cancelled = await _wait_for_cancel_marker(
            client,
            job.id,
            attempt=1,
            timeout=10,
            worker=worker,
        )
        assert cancelled["marker"] == "cancel_observed"

        row = await _wait_for_job(
            client,
            job.id,
            lambda current: current is not None
            and current["state"] == "completed"
            and current["attempt"] == 2
            and current["finalized_at"] is not None,
            timeout=10,
            worker=worker,
            description="completed attempt=2",
        )
        assert row["attempt"] == 2
        assert row["finalized_at"] is not None
    finally:
        await _stop_process(worker)


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_callback_timeout_is_rescued_and_retried(client):
    queue = f"chaos_{uuid.uuid4().hex[:8]}"
    worker = await _start_worker(queue, "callback_wait")

    try:
        await _wait_for_line(worker, "READY role=callback_wait", timeout=10)

        job = await client.insert(ChaosProbe(marker="callback"), queue=queue)
        row = await _wait_for_job(
            client,
            job.id,
            lambda current: current is not None
            and current["state"] == "waiting_external"
            and current["attempt"] == 1,
            timeout=10,
            worker=worker,
            description="waiting_external attempt=1",
        )
        assert row["attempt"] == 1

        final_row = await _wait_for_job(
            client,
            job.id,
            lambda current: current is not None
            and current["state"] == "completed"
            and current["attempt"] == 2
            and current["finalized_at"] is not None,
            timeout=10,
            worker=worker,
            description="completed attempt=2 after callback timeout rescue",
        )
        assert final_row["attempt"] == 2
        assert final_row["finalized_at"] is not None
    finally:
        await _stop_process(worker)


class ChaosWorker:
    """Wraps a subprocess and continuously drains its stdout into an
    in-memory list, so the test can match lines at its own pace without
    the pipe backing up.

    Why this exists: the worker prints heartbeat/maintenance/tracing
    output at ~50+ lines/sec. Linux's default pipe capacity is 64 KiB —
    roughly nine seconds of output. If the test matches an expected line
    and then stops reading while it does a DB poll, the pipe fills, the
    worker's next write blocks on stdout, and any tokio worker thread
    holding the write-blocked logger stalls. With the completion batcher
    on a blocked thread, a `running → completed` transition never gets
    committed and the test times out. Draining in the background keeps
    the pipe empty so writes never block.
    """

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.lines: list[str] = []
        self._new_line = asyncio.Event()
        self._closed = asyncio.Event()
        self._drainer = asyncio.create_task(self._drain())

    @property
    def pid(self) -> int | None:
        return self.process.pid

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    async def _drain(self) -> None:
        assert self.process.stdout is not None
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    break
                self.lines.append(line.decode().rstrip("\n"))
                self._new_line.set()
        finally:
            self._closed.set()
            self._new_line.set()

    async def wait_for_line(self, expected: str, timeout: float) -> str:
        """Return the first line (past or future) containing `expected`.

        Keeps position by scanning from index 0 every call — acceptable
        because `expected` markers embed PIDs / job IDs / attempt numbers
        that are unique per test call.
        """
        async def scan() -> str:
            cursor = 0
            while True:
                while cursor < len(self.lines):
                    line = self.lines[cursor]
                    cursor += 1
                    if expected in line:
                        return line
                if self._closed.is_set() and cursor >= len(self.lines):
                    raise AssertionError(
                        "worker exited before emitting expected output.\n"
                        + "\n".join(self.lines)
                    )
                self._new_line.clear()
                await self._new_line.wait()

        try:
            return await asyncio.wait_for(scan(), timeout=scaled_timeout(timeout))
        except TimeoutError as exc:
            raise AssertionError(
                f"timed out waiting for worker output: {expected}\n"
                + "\n".join(self.lines)
            ) from exc

    def terminate(self) -> None:
        if self.process.returncode is None:
            self.process.terminate()

    def kill(self) -> None:
        if self.process.returncode is None:
            self.process.kill()

    async def wait(self) -> int:
        return await self.process.wait()

    async def shutdown(self) -> None:
        if self.process.returncode is None:
            self.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=scaled_timeout(5))
            except TimeoutError:
                self.kill()
                await asyncio.wait_for(self.process.wait(), timeout=scaled_timeout(5))
        self._drainer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._drainer


async def _start_worker(queue: str, role: str) -> ChaosWorker:
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": DATABASE_URL,
            "CHAOS_QUEUE": queue,
            "CHAOS_ROLE": role,
            "PYTHONUNBUFFERED": "1",
        }
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(WORKER_SCRIPT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    return ChaosWorker(process)


async def _wait_for_line(worker: ChaosWorker, expected: str, timeout: float) -> str:
    return await worker.wait_for_line(expected, timeout)


async def _fetch_job_row(client: awa.AsyncClient, job_id: int):
    tx = await client.transaction()
    try:
        row = await tx.fetch_optional(
            """
            SELECT id,
                   state::text AS state,
                   attempt,
                   run_lease,
                   heartbeat_at::text AS heartbeat_at,
                   deadline_at::text AS deadline_at,
                   finalized_at::text AS finalized_at,
                   progress,
                   EXTRACT(EPOCH FROM (now() - heartbeat_at))::float AS heartbeat_age_s
            FROM awa.jobs
            WHERE id = $1
            """,
            job_id,
        )
        return row
    finally:
        await tx.rollback()


async def _wait_for_job(
    client: awa.AsyncClient,
    job_id: int,
    predicate,
    timeout: float,
    *,
    worker: "ChaosWorker | None" = None,
    description: str = "predicate satisfied",
):
    deadline = asyncio.get_running_loop().time() + scaled_timeout(timeout)

    while True:
        row = await _fetch_job_row(client, job_id)
        if predicate(row):
            return row

        if asyncio.get_running_loop().time() >= deadline:
            diag = f"job {job_id} did not reach expected condition ({description}): {row!r}"
            if worker is not None:
                tail = worker.lines[-40:]
                diag += (
                    f"\nworker pid={worker.pid} returncode={worker.returncode}"
                    f" stdout_lines={len(worker.lines)}\n"
                    "-- last 40 stdout lines --\n" + "\n".join(tail)
                )
            raise AssertionError(diag)

        await asyncio.sleep(0.2)


async def _stop_process(worker: ChaosWorker | None) -> None:
    if worker is None:
        return
    await worker.shutdown()


async def _wait_for_cancel_marker(
    client: awa.AsyncClient,
    job_id: int,
    *,
    attempt: int,
    timeout: float,
    worker: "ChaosWorker | None" = None,
):
    deadline = asyncio.get_running_loop().time() + scaled_timeout(timeout)
    while True:
        tx = await client.transaction()
        try:
            row = await tx.fetch_optional(
                """
                SELECT job_id, attempt, marker, observed_at::text AS observed_at
                FROM awa_test_chaos_markers
                WHERE job_id = $1 AND attempt = $2
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                job_id,
                attempt,
            )
        finally:
            await tx.rollback()

        if row is not None:
            return row

        if asyncio.get_running_loop().time() >= deadline:
            diag = f"job {job_id} did not persist cancel marker for attempt {attempt}"
            if worker is not None:
                tail = worker.lines[-40:]
                diag += (
                    f"\nworker pid={worker.pid} returncode={worker.returncode}"
                    f" stdout_lines={len(worker.lines)}\n"
                    "-- last 40 stdout lines --\n" + "\n".join(tail)
                )
            raise AssertionError(diag)

        await asyncio.sleep(0.2)
