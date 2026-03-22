"""
Tests for Python examples.

Run:
    cd awa-python
    DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
    .venv/bin/pytest ../examples/python/test_examples.py -v
"""

import os
import subprocess
import sys

import pytest

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

EXAMPLES_DIR = os.path.dirname(__file__)
PYTHON = sys.executable


def run_example(script_name: str, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run an example script as a subprocess with a timeout."""
    script_path = os.path.join(EXAMPLES_DIR, script_name)
    env = {**os.environ, "DATABASE_URL": DATABASE_URL}
    return subprocess.run(
        [PYTHON, script_path],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


class TestExamples:
    def test_etl_pipeline(self):
        result = run_example("etl_pipeline.py", timeout=60)
        assert result.returncode == 0, f"ETL pipeline failed:\n{result.stderr}"
        assert "All jobs completed" in result.stdout
        assert "Health: leader=True" in result.stdout

    def test_webhook_payments(self):
        result = run_example("webhook_payments.py", timeout=60)
        assert result.returncode == 0, f"Webhook payments failed:\n{result.stderr}"
        assert "All payments processed" in result.stdout
        assert "Queue stats:" in result.stdout

    def test_email_campaign(self):
        result = run_example("email_campaign.py", timeout=120)
        assert result.returncode == 0, f"Email campaign failed:\n{result.stderr}"
        assert "200 marketing emails enqueued" in result.stdout
        assert "Transactional emails will be processed first" in result.stdout
        assert "Done" in result.stdout
