import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from types import FrameType
from typing import NoReturn


def _exit_from_signal(signum: int, _frame: FrameType | None) -> NoReturn:
    os._exit(128 + signum)


def install_exit_without_finalizers_on_signals() -> None:
    signal.signal(signal.SIGINT, _exit_from_signal)
    signal.signal(signal.SIGTERM, _exit_from_signal)


def run_async_main_without_finalizers(main: Callable[[], Awaitable[None]]) -> NoReturn:
    install_exit_without_finalizers_on_signals()
    asyncio.run(main())
    os._exit(0)
