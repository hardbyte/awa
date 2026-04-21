"""Smoke tests for ``awa.init_telemetry``.

We do not assert on exported metric values — that would require an OTLP
collector in CI. We verify:

* the function is exposed and idempotent
* calling it with an unreachable endpoint does not raise (the exporter
  builds lazily; errors manifest on the background export task)
* ``shutdown_telemetry`` is safe to call before init and after init
"""

import awa


def test_init_telemetry_exposed():
    assert callable(awa.init_telemetry)
    assert callable(awa.shutdown_telemetry)


def test_init_telemetry_is_idempotent():
    first = awa.init_telemetry("http://127.0.0.1:4317", "awa-test-idempotent")
    second = awa.init_telemetry("http://127.0.0.1:4317", "awa-test-idempotent")
    assert isinstance(first, bool)
    assert second is False, "second init call should be a no-op"


def test_shutdown_telemetry_before_init_is_safe():
    # Calling shutdown when no provider has been installed must not raise.
    # We cannot rely on this running first in the suite, so we just ensure
    # calling it after whatever state exists does not raise.
    awa.shutdown_telemetry()
