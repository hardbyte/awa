"""Phase 0 spike tests — prove PyO3 async interop works for Awa."""

import asyncio
import pytest
import pyo3_async_spike as spike


@pytest.mark.asyncio
async def test_spike1_call_async_handler():
    """Rust tokio can call a Python async def and await its result."""

    async def greet(name: str) -> str:
        await asyncio.sleep(0.01)  # Prove it's truly async
        return f"hello {name}"

    result = await spike.spike_call_async_handler(greet, "awa")
    assert result == "hello awa"


@pytest.mark.asyncio
async def test_spike2_heartbeat_during_handler():
    """A Rust background task heartbeats while a Python handler runs."""

    async def slow_handler():
        await asyncio.sleep(0.3)  # 300ms — heartbeat fires every 50ms
        return "done"

    counter = spike.HeartbeatCounter()
    heartbeat_count = await spike.spike_heartbeat_during_handler(slow_handler, counter)
    # Should have at least 3 heartbeats during 300ms (50ms interval)
    assert heartbeat_count >= 3, f"Expected >= 3 heartbeats, got {heartbeat_count}"
    print(f"  Heartbeats during handler: {heartbeat_count}")


@pytest.mark.asyncio
async def test_spike3_exception_propagation():
    """Python exceptions propagate to Rust with type, message, and traceback."""

    async def failing_handler():
        raise ValueError("invalid email address: missing @")

    error_type, error_message, traceback = await spike.spike_exception_propagation(
        failing_handler
    )
    assert error_type == "ValueError", f"Expected ValueError, got {error_type}"
    assert "invalid email address" in error_message
    assert "traceback" in traceback.lower() or "test_spike.py" in traceback
    print(f"  Error type: {error_type}")
    print(f"  Error message: {error_message}")
    print(f"  Has traceback: {bool(traceback)}")


@pytest.mark.asyncio
async def test_spike3b_custom_exception():
    """Custom exception types propagate correctly."""

    class JobValidationError(Exception):
        pass

    async def handler_with_custom_error():
        raise JobValidationError("args schema mismatch")

    error_type, error_message, _ = await spike.spike_exception_propagation(
        handler_with_custom_error
    )
    assert "JobValidationError" in error_type
    assert "args schema mismatch" in error_message


@pytest.mark.asyncio
async def test_spike4_cancellation():
    """ctx.is_cancelled() works from Python when Rust signals shutdown."""

    async def cancellable_handler(ctx):
        # Poll is_cancelled in a loop
        for _ in range(50):  # 50 * 10ms = 500ms max
            if ctx.is_cancelled():
                return True
            await asyncio.sleep(0.01)
        return False  # Never got cancelled

    ctx = spike.JobContext()
    was_cancelled = await spike.spike_cancellation(cancellable_handler, ctx)
    assert was_cancelled is True, "Handler should have observed cancellation"
    print("  Handler detected cancellation from Rust signal")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
