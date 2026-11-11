"""Cancellation must cross the Rust executor -> asyncio handler boundary."""

import asyncio
import contextvars
import os
from dataclasses import dataclass

import pytest

import awa
from awa._handler_task import HandlerRegistry

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
RUNTIME_START_KWARGS = {
    "leader_election_interval_ms": 100,
    "queue_storage_queue_rotate_interval_ms": 60_000,
}


@dataclass
class CancellationProbe:
    index: int


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    try:
        await c.migrate()
        await c.install_queue_storage(reset=True)
        yield c
    finally:
        await c.shutdown()
        await c.close()


async def wait_until(predicate, timeout=5):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def stop_remaining(tasks):
    # Also clean up when testing the old bridge, which leaves these tasks alive.
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_repeated_deadline_abandonment_cancels_python_handlers(client):
    queue = "handler_cancel_deadline"
    tasks = set()
    live = set()
    cancelled = set()
    cleaned = set()
    late_effects = []
    release = asyncio.Event()
    maximum_live = 0
    count = 6

    @client.task(CancellationProbe, queue=queue)
    async def handler(job):
        nonlocal maximum_live
        tasks.add(asyncio.current_task())
        live.add(job.id)
        maximum_live = max(maximum_live, len(live))
        try:
            await release.wait()  # Ignores job.is_cancelled(), but is cancellable.
            late_effects.append(job.id)
        except asyncio.CancelledError:
            cancelled.add(job.id)
            raise
        finally:
            await asyncio.sleep(0)
            live.remove(job.id)
            cleaned.add(job.id)

    for index in range(count):
        await client.insert(CancellationProbe(index), queue=queue, max_attempts=1)
    try:
        await client.start(
            [{"name": queue, "max_workers": 1, "deadline_duration_ms": 100}],
            poll_interval_ms=10,
            deadline_rescue_interval_ms=50,
            **RUNTIME_START_KWARGS,
        )
        await wait_until(lambda: len(cleaned) == count)
        assert len(cancelled) == count
        assert not live
        # Cancellation callbacks can overlap the next dispatch by one loop turn.
        assert maximum_live <= 2
        release.set()
        await asyncio.sleep(0)
        assert not late_effects
    finally:
        await client.shutdown()
        await stop_remaining(tasks)


@pytest.mark.asyncio
async def test_forced_shutdown_cancels_python_handler_and_runs_cleanup(client):
    queue = "handler_cancel_shutdown"
    started = asyncio.Event()
    cleaned = asyncio.Event()
    cancelled = asyncio.Event()
    tasks = set()

    @client.task(CancellationProbe, queue=queue)
    async def handler(job):
        tasks.add(asyncio.current_task())
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            await asyncio.sleep(0.05)
            cleaned.set()

    await client.insert(CancellationProbe(0), queue=queue)
    try:
        await client.start(
            [{"name": queue, "max_workers": 1, "deadline_duration_ms": 0}],
            **RUNTIME_START_KWARGS,
        )
        await asyncio.wait_for(started.wait(), 2)
        await client.shutdown(timeout_ms=25)
        assert cleaned.is_set(), "shutdown returned before asynchronous cleanup"
        assert cancelled.is_set()
        assert all(task.done() for task in tasks)
    finally:
        await client.shutdown()
        await stop_remaining(tasks)


@pytest.mark.asyncio
async def test_shutdown_only_cancels_its_own_clients_handlers(client):
    other = awa.AsyncClient(DATABASE_URL)
    started = [asyncio.Event(), asyncio.Event()]
    cleaned = [asyncio.Event(), asyncio.Event()]
    releases = [asyncio.Event(), asyncio.Event()]
    tasks = set()

    def register(c, index):
        queue = f"handler_cancel_isolation_{index}"

        @c.task(CancellationProbe, queue=queue)
        async def handler(job):
            tasks.add(asyncio.current_task())
            started[index].set()
            try:
                await releases[index].wait()
            finally:
                await asyncio.sleep(0.05)
                cleaned[index].set()

        return queue

    try:
        queues = [register(client, 0), register(other, 1)]
        for c, queue in zip((client, other), queues):
            await c.insert(CancellationProbe(0), queue=queue)
            await c.start(
                [{"name": queue, "max_workers": 1, "deadline_duration_ms": 0}],
                **RUNTIME_START_KWARGS,
            )
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 3)
        await client.shutdown(timeout_ms=25)
        assert cleaned[0].is_set()
        assert not cleaned[1].is_set(), "one client's shutdown cancelled another's handler"
        releases[1].set()
        await other.shutdown()
        assert cleaned[1].is_set()
    finally:
        for release in releases:
            release.set()
        await client.shutdown()
        await other.shutdown()
        await stop_remaining(tasks)
        await other.close()


@pytest.mark.asyncio
async def test_shutdown_bounds_cleanup_for_handler_suppressing_cancellation(client):
    queue = "handler_cancel_stubborn"
    started = asyncio.Event()
    cleaned = asyncio.Event()
    release = asyncio.Event()
    tasks = set()

    @client.task(CancellationProbe, queue=queue)
    async def handler(job):
        tasks.add(asyncio.current_task())
        started.set()
        try:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
        finally:
            cleaned.set()

    try:
        await client.insert(CancellationProbe(0), queue=queue)
        await client.start(
            [{"name": queue, "max_workers": 1, "deadline_duration_ms": 0}],
            **RUNTIME_START_KWARGS,
        )
        await asyncio.wait_for(started.wait(), 2)
        with pytest.warns(RuntimeWarning, match="Python handler.*may still be running"):
            await asyncio.wait_for(client.shutdown(timeout_ms=25), 3)
        assert not cleaned.is_set()
        release.set()
        await asyncio.wait_for(cleaned.wait(), 2)
    finally:
        release.set()
        await client.shutdown()
        await stop_remaining(tasks)


@pytest.mark.asyncio
async def test_cancelled_shutdown_waiter_keeps_cleanup_and_start_gate(client):
    queue = "handler_cancel_waiter"
    started = asyncio.Event()
    cleaning = asyncio.Event()
    cleaned = asyncio.Event()
    release = asyncio.Event()
    tasks = set()

    @client.task(CancellationProbe, queue=queue)
    async def handler(job):
        tasks.add(asyncio.current_task())
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    config = [{"name": queue, "max_workers": 1, "deadline_duration_ms": 0}]
    try:
        await client.insert(CancellationProbe(0), queue=queue)
        await client.start(config, **RUNTIME_START_KWARGS)
        await asyncio.wait_for(started.wait(), 2)
        shutdown = asyncio.create_task(client.shutdown(timeout_ms=25))
        await asyncio.wait_for(cleaning.wait(), 2)
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert not cleaned.is_set()
        with pytest.raises(awa.AwaError, match="already running|shutting down"):
            await client.start(config, **RUNTIME_START_KWARGS)
        release.set()
        await client.shutdown()  # Joins the accepted, serialized shutdown.
        assert cleaned.is_set()
        assert all(task.done() for task in tasks)
    finally:
        release.set()
        await client.shutdown()
        await stop_remaining(tasks)


@pytest.mark.asyncio
async def test_idle_shutdown_keeps_start_gated_until_cleanup_finishes(client, monkeypatch):
    queue = "handler_cancel_idle_shutdown"
    draining = asyncio.Event()
    release = asyncio.Event()
    original_drain = HandlerRegistry.drain

    async def gated_drain(registry):
        draining.set()
        await release.wait()
        await original_drain(registry)

    monkeypatch.setattr(HandlerRegistry, "drain", gated_drain)

    @client.task(CancellationProbe, queue=queue)
    async def handler(job):
        return None

    shutdown = asyncio.create_task(client.shutdown())
    try:
        await asyncio.wait_for(draining.wait(), 2)
        with pytest.raises(awa.AwaError, match="already running|shutting down"):
            await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    finally:
        release.set()
        await shutdown
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await client.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_result", [False, True])
async def test_normal_handler_preserves_registration_context_and_result(
    client, cancel_result
):
    queue = "handler_cancel_normal"
    context = contextvars.ContextVar("handler_cancellation_probe", default="missing")
    observed = []
    registration = context.set("registered")
    try:
        @client.task(CancellationProbe, queue=queue)
        async def handler(job):
            observed.append((context.get(), job.args.index))
            await asyncio.sleep(0)
            return awa.Cancel(reason="normal result") if cancel_result else None
    finally:
        context.reset(registration)

    job = await client.insert(CancellationProbe(42), queue=queue)
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    expected = awa.JobState.Cancelled if cancel_result else awa.JobState.Completed

    async def wait_for_result():
        while (await client.get_job(job.id)).state != expected:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_for_result(), 3)
    assert observed == [("registered", 42)]
