import os
import signal
import subprocess
import sys
import textwrap

import pytest


def _run_helper_script(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        check=False,
        cwd=os.path.dirname(__file__),
        text=True,
        capture_output=True,
    )


def test_run_async_main_without_finalizers_skips_atexit_hooks():
    result = _run_helper_script(
        """
        import atexit
        import asyncio

        from _subprocess_exit import run_async_main_without_finalizers

        atexit.register(lambda: print("atexit-ran", flush=True))

        async def main():
            print("main-ran", flush=True)

        run_async_main_without_finalizers(main)
        print("after-runner", flush=True)
        """
    )

    assert result.returncode == 0
    assert result.stdout == "main-ran\n"
    assert result.stderr == ""


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal status assertion")
def test_run_async_main_without_finalizers_exits_directly_on_sigterm():
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import atexit
                import asyncio

                from _subprocess_exit import run_async_main_without_finalizers

                atexit.register(lambda: print("atexit-ran", flush=True))

                async def main():
                    print("ready", flush=True)
                    await asyncio.Event().wait()

                run_async_main_without_finalizers(main)
                """
            ),
        ],
        cwd=os.path.dirname(__file__),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        assert process.stdout is not None
        assert process.stdout.readline() == "ready\n"
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()

    assert process.returncode == 128 + signal.SIGTERM
    assert stdout == ""
    assert stderr == ""
