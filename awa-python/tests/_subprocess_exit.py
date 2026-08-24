import asyncio
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from types import FrameType
from typing import NoReturn


def _flush_std_streams() -> None:
    """`os._exit` skips stdio flushing, so callers that do not pass
    `flush=True` on every print would lose buffered output when stdout is a
    pipe. Both current callers do flush per print; this keeps the next one
    from having to know that."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass


def _exit_from_signal(signum: int, _frame: FrameType | None) -> NoReturn:
    _flush_std_streams()
    os._exit(128 + signum)


def install_exit_without_finalizers_on_signals() -> None:
    signal.signal(signal.SIGINT, _exit_from_signal)
    signal.signal(signal.SIGTERM, _exit_from_signal)


def run_async_main_without_finalizers(main: Callable[[], Awaitable[None]]) -> NoReturn:
    install_exit_without_finalizers_on_signals()
    asyncio.run(main())
    _flush_std_streams()
    os._exit(0)
