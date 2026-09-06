"""Native completion callbacks must finish before CPython finalizes objects."""

import subprocess
import sys
import textwrap


def test_interpreter_exit_joins_native_completion():
    # Registration order matters: this check runs after awa's shutdown hook.
    # Hold the Rust completion callback after it wakes asyncio, reproducing the
    # handoff in the captured 0.6.2 SIGSEGV without depending on scheduler luck.
    code = textwrap.dedent('''
        import atexit, asyncio, threading, time, os
        released = threading.Event()
        finished = threading.Event()
        atexit.register(lambda: print("SAFE_EXIT" if finished.is_set() else "EARLY_EXIT", flush=True))
        import awa
        atexit.register(released.set)
        async def main():
            loop = asyncio.get_running_loop()
            original = loop._write_to_self
            def controlled_wakeup():
                original()
                if threading.current_thread() is not threading.main_thread():
                    released.wait()
                    time.sleep(0.05)
                    finished.set()
            loop._write_to_self = controlled_wakeup
            client = awa.AsyncClient(os.environ["DATABASE_URL"])
            await client.close()
        asyncio.run(main())
    ''')
    result = subprocess.run([sys.executable, "-X", "faulthandler", "-c", code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SAFE_EXIT" in result.stdout, result.stdout + result.stderr


def test_shutdown_fences_new_bridge_calls():
    code = textwrap.dedent('''
        import asyncio, os, awa
        from awa._awa import _shutdown_async_bridge
        async def main():
            client = awa.AsyncClient(os.environ["DATABASE_URL"])
            await client.close()
            _shutdown_async_bridge()
            _shutdown_async_bridge()  # idempotent; atexit will call it again
            try:
                await client.close()
            except RuntimeError as error:
                assert "shutting down" in str(error)
            else:
                raise AssertionError("bridge accepted a call after shutdown")
        asyncio.run(main())
    ''')
    result = subprocess.run([sys.executable, "-X", "faulthandler", "-c", code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
