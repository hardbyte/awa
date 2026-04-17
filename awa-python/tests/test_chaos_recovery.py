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
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'chaos_%'")
    await tx.execute("DELETE FROM awa.queue_meta WHERE queue LIKE 'chaos_%'")
    await tx.commit()
    yield c
    await c.close()


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_worker_sigkill_job_is_rescued_and_completed(client):
    queue = f"chaos_{uuid.uuid4().hex[:8]}"
    worker_a = await _start_worker(queue, "hang")
    worker_b = None

    try:
        await _wait_for_line(worker_a, "READY role=hang", timeout=10)

        job = await client.insert(ChaosProbe(marker="ci"), queue=queue)
        await _wait_for_line(
            worker_a,
            f"START role=hang pid={worker_a.pid} job_id={job.id} attempt=1",
            timeout=10,
        )

        tx = await client.transaction()
        await tx.execute(
            "UPDATE awa.jobs SET deadline_at = now() - interval '1 second' WHERE id = $1",
            job.id,
        )
        await tx.commit()

        worker_a.kill()
        await asyncio.wait_for(worker_a.wait(), timeout=5)

        worker_b = await _start_worker(queue, "complete")
        await _wait_for_line(worker_b, "READY role=complete", timeout=10)
        await _wait_for_line(
            worker_b,
            f"START role=complete pid={worker_b.pid} job_id={job.id} attempt=2",
            timeout=45,
        )
        await _wait_for_line(
            worker_b,
            f"COMPLETE role=complete pid={worker_b.pid} job_id={job.id} attempt=2",
            timeout=5,
        )

        row = await _wait_for_job_state(client, job.id, "completed", timeout=10)
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
        await _wait_for_line(
            worker,
            f"START role=hang_until_cancel pid={worker.pid} job_id={job.id} attempt=1",
            timeout=10,
        )

        tx = await client.transaction()
        await tx.execute(
            "UPDATE awa.jobs SET deadline_at = now() - interval '1 second' WHERE id = $1",
            job.id,
        )
        await tx.commit()

        await _wait_for_line(
            worker,
            f"CANCELLED role=hang_until_cancel pid={worker.pid} job_id={job.id} attempt=1",
            timeout=10,
        )
        await _wait_for_line(
            worker,
            f"START role=hang_until_cancel pid={worker.pid} job_id={job.id} attempt=2",
            timeout=10,
        )
        await _wait_for_line(
            worker,
            f"COMPLETE role=hang_until_cancel pid={worker.pid} job_id={job.id} attempt=2",
            timeout=5,
        )

        row = await _wait_for_job_state(client, job.id, "completed", timeout=10)
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
        await _wait_for_line(
            worker,
            f"START role=callback_wait pid={worker.pid} job_id={job.id} attempt=1",
            timeout=10,
        )
        await _wait_for_line(
            worker,
            f"WAITING role=callback_wait pid={worker.pid} job_id={job.id} attempt=1",
            timeout=5,
        )

        row = await _wait_for_job_state(client, job.id, "waiting_external", timeout=5)
        assert row["attempt"] == 1

        await _wait_for_line(
            worker,
            f"START role=callback_wait pid={worker.pid} job_id={job.id} attempt=2",
            timeout=10,
        )
        await _wait_for_line(
            worker,
            f"COMPLETE role=callback_wait pid={worker.pid} job_id={job.id} attempt=2",
            timeout=5,
        )

        final_row = await _wait_for_job_state(client, job.id, "completed", timeout=10)
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


async def _wait_for_job_state(
    client: awa.AsyncClient, job_id: int, expected_state: str, timeout: float
):
    deadline = asyncio.get_running_loop().time() + scaled_timeout(timeout)

    while True:
        tx = await client.transaction()
        row = await tx.fetch_optional(
            """
            SELECT id,
                   state::text AS state,
                   attempt,
                   finalized_at::text AS finalized_at
            FROM awa.jobs
            WHERE id = $1
            """,
            job_id,
        )
        await tx.commit()

        if row is not None and row["state"] == expected_state:
            return row

        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(
                f"job {job_id} did not reach state {expected_state!r}: {row!r}"
            )

        await asyncio.sleep(0.2)


async def _stop_process(worker: ChaosWorker | None) -> None:
    if worker is None:
        return
    await worker.shutdown()
