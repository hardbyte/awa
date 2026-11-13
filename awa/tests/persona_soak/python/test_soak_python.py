"""Process-boundary regressions; uses a delayed fake client, never a database."""

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SCRIPT = Path(os.environ.get("SOAK_PYTHON_SCRIPT", Path(__file__).with_name("soak_python.py")))
FAKE_AWA = '''
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

class AsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def migrate(self):
        print("STARTUP_BLOCKED", flush=True)
        while not Path(os.environ["SOAK_TEST_GATE"]).exists():
            await asyncio.sleep(0.01)

    async def queue_stats(self):
        return []

    async def list_jobs(self, **kwargs):
        return []

    def task(self, *args, **kwargs):
        return lambda handler: handler

    async def start(self, *args, **kwargs):
        timings = {key: value for key, value in kwargs.items() if key.endswith("_ms")}
        Path(os.environ["SOAK_TEST_WORKERS"]).write_text(json.dumps(timings))

    async def insert(self, *args, **kwargs):
        Path(os.environ["SOAK_TEST_PRODUCED"]).touch()
        return SimpleNamespace(id=1)

    async def transaction(self):
        Path(os.environ["SOAK_TEST_PRODUCED"]).touch()
        raise RuntimeError("unexpected production during drain")
'''


class StartupDrainTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="awa-python-drain-")
        self.root = Path(self.directory.name)
        (self.root / "awa.py").write_text(FAKE_AWA)
        self.gate = self.root / "gate"
        self.workers = self.root / "workers"
        self.produced = self.root / "produced"
        self.child = None

    def tearDown(self):
        if self.child is not None:
            if self.child.poll() is None:
                self.child.terminate()
            self.child.wait(timeout=10)
            self.child.stdout.close()
            self.child.stderr.close()
        self.directory.cleanup()

    def start(self, *, draining=False, extra_env=None):
        env = os.environ | (extra_env or {}) | {
            "PYTHONPATH": str(self.root),
            "SOAK_DATABASE_URL": "unused-by-the-fake-client",
            "SOAK_ROLE": "full",
            "SOAK_DRAIN": "1" if draining else "0",
            "SOAK_TEST_GATE": str(self.gate),
            "SOAK_TEST_WORKERS": str(self.workers),
            "SOAK_TEST_PRODUCED": str(self.produced),
        }
        self.child = subprocess.Popen(
            [sys.executable, str(SCRIPT)], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self.wait_for("STARTUP_BLOCKED")

    def wait_for(self, prefix):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if select.select([self.child.stdout], [], [], 0.1)[0]:
                line = self.child.stdout.readline()
                if line.startswith(prefix):
                    return
                if not line:
                    break
        self.fail(f"did not see {prefix!r}; child status={self.child.poll()}")

    def assert_draining_workers(self):
        self.gate.touch()
        self.wait_for("READY pid=")
        time.sleep(0.1)
        self.assertIsNone(self.child.poll(), "workers must stay alive to drain")
        self.assertTrue(self.workers.exists(), "workers must start after migration")
        self.assertFalse(self.produced.exists(), "a drain restart must not enqueue new work")

    def test_stop_during_migration_is_remembered_until_workers_start(self):
        self.start()
        self.child.send_signal(signal.SIGUSR1)
        time.sleep(0.05)
        self.assertIsNone(self.child.poll(), "SIGUSR1 during startup must not terminate the worker")
        self.assert_draining_workers()

    def test_restart_during_drain_starts_workers_without_producers(self):
        self.start(draining=True)
        self.assert_draining_workers()

    def test_workers_use_the_timings_the_harness_passes(self):
        self.start(extra_env={
            "SOAK_HEARTBEAT_MS": "30000",
            "SOAK_STALENESS_MS": "90000",
            "SOAK_RESCUE_INTERVAL_MS": "7000",
        })
        self.gate.touch()
        self.wait_for("READY pid=")
        self.assertEqual(json.loads(self.workers.read_text()), {
            "heartbeat_interval_ms": 30000,
            "heartbeat_staleness_ms": 90000,
            "heartbeat_rescue_interval_ms": 7000,
            "deadline_rescue_interval_ms": 7000,
            "promote_interval_ms": 250,
        })


if __name__ == "__main__":
    unittest.main()
