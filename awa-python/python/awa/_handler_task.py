"""Cancellation ownership for Python handlers awaited by the Rust executor."""

import asyncio
import concurrent.futures
import inspect
import threading
import warnings


class HandlerRegistry:
    """A client's live handlers, independent of their event-loop threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._owners = set()
        self._draining = False

    def add(self, owner):
        with self._lock:
            self._owners.add(owner)
            draining = self._draining
        # A job task can reach its handler after drain took its snapshot.
        # Nothing waits for that handler any more, so it must not start.
        if draining:
            owner.cancel()

    def reopen(self):
        """Accept handlers again; called when the worker runtime starts."""
        with self._lock:
            self._draining = False

    def discard(self, owner):
        with self._lock:
            self._owners.discard(owner)

    async def drain(self, timeout=1.0):
        with self._lock:
            self._draining = True
            owners = tuple(self._owners)
        for owner in owners:
            owner.cancel()
        if not owners:
            return
        completions = [asyncio.wrap_future(owner.completion) for owner in owners]
        # Unlike wait_for/gather cancellation, a timeout or cancelled drain must
        # not cancel the thread-safe completion records or interrupt finally.
        _, pending = await asyncio.wait(completions, timeout=timeout)
        if pending:
            warnings.warn(
                f"Awa shutdown stopped waiting after {timeout:g}s for "
                f"{len(pending)} Python handler(s) to finish cancellation cleanup; "
                "those handlers may still be running",
                RuntimeWarning,
                stacklevel=2,
            )


class HandlerTask:
    """An awaitable whose owner can request cancellation from any thread.

    The bridge schedules this awaitable with the registration's event loop and
    contextvars. Keep the handler coroutine unstarted until then, so an executor
    dropped before task creation can close it without running user code.
    """

    def __init__(self, awaitable, loop, registry=None):
        self._awaitable = awaitable
        self._loop = loop
        self._task = None
        self._cancel_requested = threading.Event()
        self._lock = threading.Lock()
        self._finished = False
        self._deferred_completion = False
        self.completion = concurrent.futures.Future()
        self._registry = registry
        if registry is not None:
            registry.add(self)

    def __await__(self):
        return self._run().__await__()

    async def _run(self):
        self._task = asyncio.current_task()
        try:
            with self._lock:
                awaitable = self._awaitable
                self._awaitable = None
            if self._cancel_requested.is_set():
                self._cancel_unstarted(awaitable)
                raise asyncio.CancelledError
            return await awaitable
        finally:
            self._task = None
            if not self._deferred_completion:
                self._finish()

    def cancel(self):
        with self._lock:
            if self._cancel_requested.is_set():
                return
            self._cancel_requested.set()
            # Closing an unclaimed native coroutine needs no running loop. Do
            # it now, before a stopped loop can discard our queued callback.
            unstarted = self._awaitable
            if inspect.iscoroutine(unstarted):
                self._awaitable = None
            else:
                unstarted = None
        if unstarted is not None:
            unstarted.close()
            self._finish()
        try:
            self._loop.call_soon_threadsafe(self._cancel_on_loop)
        except RuntimeError:
            # A closed loop cannot execute its pending creation/cancel callback.
            # Close only a coroutine that _run has not taken ownership of.
            pass

    def _cancel_on_loop(self):
        if self._task is not None:
            self._request_cancel(self._task)
        else:
            self._cancel_unstarted(self._take_unstarted())
            if not self._deferred_completion:
                self._finish()

    def _take_unstarted(self):
        with self._lock:
            awaitable = self._awaitable
            self._awaitable = None
        return awaitable

    def _cancel_unstarted(self, awaitable):
        # This path runs only on the event-loop thread. A supported handler may
        # return an already-running Task/Future rather than a native coroutine.
        if asyncio.isfuture(awaitable):
            self._deferred_completion = True
            awaitable.add_done_callback(self._complete_unstarted_future)
            self._request_cancel(awaitable)
        else:
            HandlerTask._close(awaitable)

    @staticmethod
    def _request_cancel(future):
        # Task.cancelling() was added in Python 3.11; our own request flag
        # provides one-shot cancellation on Python 3.10 as well.
        cancelling = getattr(future, "cancelling", None)
        if cancelling is None or not cancelling():
            future.cancel()

    def _complete_unstarted_future(self, future):
        try:
            future.exception()
        except asyncio.CancelledError:
            pass
        finally:
            self._finish()

    def _finish(self):
        with self._lock:
            if self._finished:
                return
            self._finished = True
        if self._registry is not None:
            self._registry.discard(self)
        try:
            self.completion.set_result(None)
        except concurrent.futures.InvalidStateError:
            # A waiter cancelled the record; the handler has finished all the same.
            pass

    @staticmethod
    def _close(awaitable):
        if inspect.iscoroutine(awaitable):
            awaitable.close()
