"""Shared HTTP callback ingress contract — signing, verification, URL building.

Mirrors :mod:`awa_model::callback_contract` on the Rust side. The underlying
implementations are the Rust ones bridged via PyO3, so signatures produced by
this module are bit-for-bit identical to what the worker emits in its
``X-Awa-Signature`` header.

Use this module when you host the callback receiver routes inside your own
Python web framework (FastAPI, Starlette, Flask, Django) — see ADR-027 and
``docs/callback-receivers.md`` for examples.

Example::

    from awa import callback_contract

    secret = bytes.fromhex(os.environ["AWA_CALLBACK_HMAC_SECRET"])

    def verify(callback_id: str, signature: str) -> bool:
        return callback_contract.verify(secret, callback_id, signature)
"""

from __future__ import annotations

from awa._awa import (
    CALLBACK_DEFAULT_HEARTBEAT_TIMEOUT_SECS as DEFAULT_HEARTBEAT_TIMEOUT_SECS,
    CALLBACK_DEFAULT_PATH_PREFIX as DEFAULT_PATH_PREFIX,
    CALLBACK_SIGNATURE_HEADER as SIGNATURE_HEADER,
    build_callback_url,
    sign_callback as sign,
    verify_callback as verify,
)

__all__ = [
    "DEFAULT_HEARTBEAT_TIMEOUT_SECS",
    "DEFAULT_PATH_PREFIX",
    "SIGNATURE_HEADER",
    "build_callback_url",
    "sign",
    "verify",
]
