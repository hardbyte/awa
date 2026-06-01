"""Parity tests for the user-owned callback receiver contract.

The Python helpers in ``awa.callback_contract`` are PyO3 wrappers around
:mod:`awa_model::callback_contract`. These tests pin the wire contract:

- Known BLAKE3 keyed-hash test vectors stay stable.
- :func:`awa.callback_contract.sign` and the worker's signing path produce
  identical hex when given the same inputs (verified by re-decoding through
  :func:`verify`).
- :func:`awa.callback_contract.build_callback_url` normalizes prefixes the
  same way the Rust ``HttpWorker`` does.
- Exposed constants match the Rust constants.
"""

from __future__ import annotations

import pytest

from awa import _awa, callback_contract


def test_sign_and_verify_roundtrip() -> None:
    secret = bytes([7] * 32)
    callback_id = "550e8400-e29b-41d4-a716-446655440000"

    signature = callback_contract.sign(secret, callback_id)
    assert isinstance(signature, str)
    assert len(signature) == 64  # blake3 hash → 32 bytes → 64 hex chars
    assert all(c in "0123456789abcdef" for c in signature)

    assert callback_contract.verify(secret, callback_id, signature) is True


def test_verify_rejects_wrong_secret() -> None:
    secret = bytes([7] * 32)
    other_secret = bytes([8] * 32)
    callback_id = "abc"
    signature = callback_contract.sign(secret, callback_id)
    assert callback_contract.verify(other_secret, callback_id, signature) is False


def test_verify_rejects_wrong_callback_id() -> None:
    secret = bytes([7] * 32)
    signature = callback_contract.sign(secret, "abc")
    assert callback_contract.verify(secret, "abd", signature) is False


def test_verify_rejects_malformed_signature() -> None:
    secret = bytes([7] * 32)
    assert callback_contract.verify(secret, "abc", "not-hex") is False
    assert callback_contract.verify(secret, "abc", "") is False


def test_secret_must_be_32_bytes() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        callback_contract.sign(b"too short", "abc")
    with pytest.raises(ValueError, match="32 bytes"):
        callback_contract.verify(b"\x00" * 16, "abc", "ff" * 32)


def test_known_test_vector() -> None:
    """Pin a known signing output so silent algorithm changes break the test.

    These bytes are produced by ``blake3::keyed_hash(&[7u8; 32], b"abc")`` —
    the same primitive the Rust worker uses to sign and the receiver uses to
    verify.
    """
    secret = bytes([7] * 32)
    signature = callback_contract.sign(secret, "abc")
    assert signature == (
        "b1495225f01fa8cd09e410d288d09b68c1cc9fb2414686c0d3ac13fc905497d9"
    )


def test_build_callback_url_default_shape() -> None:
    url = callback_contract.build_callback_url(
        "https://awa.example.com",
        callback_contract.DEFAULT_PATH_PREFIX,
        "abc",
        "complete",
    )
    assert url == "https://awa.example.com/api/callbacks/abc/complete"


def test_build_callback_url_normalizes_slashes() -> None:
    url = callback_contract.build_callback_url(
        "https://api.example.com/",
        "/awa-cb/",
        "abc",
        "heartbeat",
    )
    assert url == "https://api.example.com/awa-cb/abc/heartbeat"


def test_build_callback_url_adds_missing_leading_slash() -> None:
    url = callback_contract.build_callback_url(
        "https://api.example.com",
        "awa-cb",
        "abc",
        "fail",
    )
    assert url == "https://api.example.com/awa-cb/abc/fail"


def test_build_callback_url_empty_prefix() -> None:
    url = callback_contract.build_callback_url(
        "https://api.example.com",
        "",
        "abc",
        "complete",
    )
    assert url == "https://api.example.com/abc/complete"


def test_constants_match_rust_module() -> None:
    """Sanity-check the Python re-exports against the raw PyO3 constants."""
    assert callback_contract.SIGNATURE_HEADER == _awa.CALLBACK_SIGNATURE_HEADER
    assert callback_contract.SIGNATURE_HEADER == "X-Awa-Signature"
    assert (
        callback_contract.DEFAULT_HEARTBEAT_TIMEOUT_SECS
        == _awa.CALLBACK_DEFAULT_HEARTBEAT_TIMEOUT_SECS
        == 3600.0
    )
    assert (
        callback_contract.DEFAULT_PATH_PREFIX
        == _awa.CALLBACK_DEFAULT_PATH_PREFIX
        == "/api/callbacks"
    )
