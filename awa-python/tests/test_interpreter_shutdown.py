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


def test_interpreter_exit_joins_pending_native_future():
    # Some async bodies themselves acquire the GIL (e.g. argument conversion),
    # before they reach the completion callback. Fence/join those tasks too.
    code = textwrap.dedent('''
        import atexit, asyncio, os, threading, time
        from dataclasses import dataclass
        entered = threading.Event()
        released = threading.Event()
        finished = threading.Event()
        atexit.register(lambda: print("SAFE_EXIT" if finished.is_set() else "EARLY_EXIT", flush=True))
        import awa
        atexit.register(released.set)
        @dataclass
        class Payload:
            value: str
            def __getattribute__(self, name):
                if name == "value":
                    entered.set()
                    released.wait()
                    time.sleep(0.05)
                    finished.set()
                return object.__getattribute__(self, name)
        async def main():
            client = awa.AsyncClient(os.environ["DATABASE_URL"])
            pending = client._raw.insert(Payload("pending"), queue="shutdown_probe")
            assert await asyncio.to_thread(entered.wait, 5)
            await client.close()
            # The caller abandons pending work, but native code must not access
            # Python after interpreter finalization begins.
        asyncio.run(main())
    ''')
    result = subprocess.run([sys.executable, "-X", "faulthandler", "-c", code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SAFE_EXIT" in result.stdout, result.stdout + result.stderr


def test_rejected_bridge_operations_preserve_client_lifecycle():
    code = textwrap.dedent('''
        import asyncio, os, awa
        from awa._awa import _shutdown_async_bridge
        async def main():
            installing = awa.AsyncClient(os.environ["DATABASE_URL"])
            starting = awa.AsyncClient(os.environ["DATABASE_URL"])
            running = awa.AsyncClient(os.environ["DATABASE_URL"])
            await running.migrate()
            tx = await running.transaction()
            row = await tx.fetch_one("SELECT COALESCE((SELECT schema_name FROM awa.runtime_storage_backends WHERE backend='queue_storage'), 'awa') AS schema")
            await tx.commit()
            from dataclasses import dataclass
            @dataclass
            class Payload:
                value: int
            async def handler(job):
                return None
            for client in (starting, running):
                client.worker(Payload, queue="shutdown_lifecycle_probe")(handler)
            await running.start([("shutdown_lifecycle_probe", 1)], queue_storage_schema=row["schema"])
            _shutdown_async_bridge()
            for operation in (
                lambda: installing._raw.install_queue_storage("awa", 8, 8, False),
                lambda: starting.start([("shutdown_lifecycle_probe", 1)]),
                lambda: running.shutdown(),
            ):
                try:
                    await operation()
                except RuntimeError as error:
                    assert "shutting down" in str(error), str(error)
                else:
                    raise AssertionError("bridge accepted operation")
            for client in (installing, starting):
                try:
                    client._raw.install_queue_storage_sync("invalid-schema", 8, 8, False)
                except Exception as error:
                    assert "schema" in str(error) and "runtime" not in str(error) and "progress" not in str(error), str(error)
                else:
                    raise AssertionError("invalid schema accepted")
            assert running._raw.health_check_sync().poll_loop_alive, "rejected shutdown lost the runtime"
        asyncio.run(main())
    ''')
    result = subprocess.run([sys.executable, "-X", "faulthandler", "-c", code],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
