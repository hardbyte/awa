"""Minimal released-wheel exit probe; no migration or job processing required.

DATABASE_URL=... python shutdown_probe.py
For the controlled GDB schedule in correctness/evidence/python-shutdown-2026-09-07,
pass --delay-wakeup. The delay is instrumentation, never a shutdown workaround.
"""

import argparse
import asyncio
import os
import threading
import time

import awa


async def main(delay_wakeup: bool) -> None:
    if delay_wakeup:
        loop = asyncio.get_running_loop()
        original = loop._write_to_self

        def delayed_wakeup():
            original()
            if threading.current_thread() is not threading.main_thread():
                time.sleep(0.2)

        loop._write_to_self = delayed_wakeup
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay-wakeup", action="store_true")
    asyncio.run(main(parser.parse_args().delay_wakeup))
    # Only the parent observing exit status zero can report process success.
    print("CLOSE COMPLETE", flush=True)
