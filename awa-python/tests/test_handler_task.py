"""Deterministic task-creation races without a database or Rust runtime."""

import asyncio
import inspect
import threading

import pytest

from awa._handler_task import HandlerRegistry, HandlerTask


@pytest.mark.asyncio
@pytest.mark.parametrize("schedule_before_cancel", [False, True])
async def test_cancel_before_first_poll_never_starts_handler(schedule_before_cancel):
    started = []

    async def handler():
        started.append(True)

    coro = handler()
    owner = HandlerTask(coro, asyncio.get_running_loop())
    if schedule_before_cancel:
        task = asyncio.ensure_future(owner)
    thread = threading.Thread(target=owner.cancel)
    thread.start()
    thread.join()  # Do not yield: the scheduled task has not had its first poll.
    if not schedule_before_cancel:
        task = asyncio.ensure_future(owner)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not started
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_cancellation_from_another_thread_runs_finally():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    owner = HandlerTask(handler(), asyncio.get_running_loop())
    task = asyncio.ensure_future(owner)
    await started.wait()
    await asyncio.to_thread(owner.cancel)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()


@pytest.mark.asyncio
async def test_cancel_before_first_poll_cancels_existing_handler_task():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    handler_task = asyncio.create_task(handler())
    await started.wait()
    owner = HandlerTask(handler_task, asyncio.get_running_loop())
    wrapper = asyncio.ensure_future(owner)
    thread = threading.Thread(target=owner.cancel)
    thread.start()
    thread.join()
    try:
        with pytest.raises(asyncio.CancelledError):
            await wrapper
        with pytest.raises(asyncio.CancelledError):
            await handler_task
        assert cleaned.is_set()
    finally:
        handler_task.cancel()
        await asyncio.gather(handler_task, return_exceptions=True)


def test_cancellation_after_loop_close_closes_unstarted_coroutine():
    async def handler():
        raise AssertionError("must not run")

    loop = asyncio.new_event_loop()
    coro = handler()
    owner = HandlerTask(coro, loop)
    loop.close()
    thread = threading.Thread(target=owner.cancel)
    thread.start()
    thread.join()
    owner.cancel()  # Repeated cancellation is harmless.
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


def test_cancellation_on_stopped_loop_closes_coroutine_before_loop_close():
    async def handler():
        raise AssertionError("must not run")

    loop = asyncio.new_event_loop()
    coro = handler()
    registry = HandlerRegistry()
    owner = HandlerTask(coro, loop, registry)
    owner.cancel()
    loop.close()
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
    assert owner.completion.done()


@pytest.mark.asyncio
async def test_registry_drain_waits_for_cleanup_from_another_loop():
    registry = HandlerRegistry()
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.05)
            cleaned.set()

    owner = HandlerTask(handler(), asyncio.get_running_loop(), registry)
    task = asyncio.ensure_future(owner)
    await started.wait()
    await asyncio.to_thread(lambda: asyncio.run(registry.drain()))
    assert cleaned.is_set()
    assert task.done()
    assert owner.completion.done()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancelled_drain_does_not_interrupt_cleanup_or_completion_record():
    registry = HandlerRegistry()
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    owner = HandlerTask(handler(), asyncio.get_running_loop(), registry)
    task = asyncio.ensure_future(owner)
    await started.wait()
    drain = asyncio.create_task(registry.drain())
    await cleaning.wait()
    drain.cancel()
    with pytest.raises(asyncio.CancelledError):
        await drain
    assert not owner.completion.done()
    owner.cancel()  # Repeated cancellation must not interrupt this finally.
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()
    assert owner.completion.done()


@pytest.mark.asyncio
async def test_registry_drain_waits_for_existing_tasks_actual_cleanup():
    registry = HandlerRegistry()
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.05)
            cleaned.set()

    original = asyncio.create_task(handler())
    await started.wait()
    owner = HandlerTask(original, asyncio.get_running_loop(), registry)
    wrapper = asyncio.ensure_future(owner)
    owner.cancel()
    await registry.drain()
    assert cleaned.is_set()
    assert original.done()
    with pytest.raises(asyncio.CancelledError):
        await wrapper


@pytest.mark.asyncio
async def test_handler_registered_after_drain_never_starts_until_reopened():
    started = []

    async def handler():
        started.append(True)

    registry = HandlerRegistry()
    await registry.drain()

    late = handler()
    task = asyncio.ensure_future(HandlerTask(late, asyncio.get_running_loop(), registry))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not started
    assert inspect.getcoroutinestate(late) == inspect.CORO_CLOSED

    registry.reopen()
    await HandlerTask(handler(), asyncio.get_running_loop(), registry)
    assert started == [True]


def test_cancel_supports_future_without_python_311_cancelling_method():
    class LegacyFuture:
        cancelled = False

        def cancel(self):
            self.cancelled = True

    future = LegacyFuture()
    HandlerTask._request_cancel(future)
    assert future.cancelled


@pytest.mark.asyncio
async def test_handler_exception_propagates():
    error = ValueError("handler failed")

    async def handler():
        raise error

    with pytest.raises(ValueError) as raised:
        await HandlerTask(handler(), asyncio.get_running_loop())
    assert raised.value is error
