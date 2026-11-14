"""Client pool options."""

import math
import os
import time

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


def _backend_pid(client: awa.Client) -> int:
    tx = client.transaction()
    try:
        row = tx.fetch_one("SELECT pg_backend_pid() AS pid")
    finally:
        tx.commit()
    return int(row["pid"])


@pytest.fixture
def short_lifetime_client():
    client = awa.Client(DATABASE_URL, max_lifetime_seconds=0.5)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def unlimited_lifetime_client():
    client = awa.Client(DATABASE_URL, max_lifetime_seconds=None)
    try:
        yield client
    finally:
        client.close()


def test_max_lifetime_recycles_connections(short_lifetime_client):
    first = _backend_pid(short_lifetime_client)
    time.sleep(1.5)
    assert _backend_pid(short_lifetime_client) != first


def test_max_lifetime_none_keeps_connections(unlimited_lifetime_client):
    first = _backend_pid(unlimited_lifetime_client)
    time.sleep(1.5)
    assert _backend_pid(unlimited_lifetime_client) == first


@pytest.fixture
async def async_client_with_lifetime():
    client = awa.AsyncClient(DATABASE_URL, max_lifetime_seconds=300)
    try:
        yield client
    finally:
        await client.close()


async def test_async_client_accepts_max_lifetime(async_client_with_lifetime):
    tx = await async_client_with_lifetime.transaction()
    try:
        row = await tx.fetch_one("SELECT 1 AS one")
    finally:
        await tx.commit()
    assert row["one"] == 1


@pytest.mark.parametrize(
    "value", [0, -1.0, math.inf, -math.inf, math.nan, 1e300]
)
def test_invalid_max_lifetime_is_rejected(value):
    with pytest.raises(awa.ValidationError, match="max_lifetime_seconds"):
        awa.Client(DATABASE_URL, max_lifetime_seconds=value)


def test_max_lifetime_is_keyword_only():
    with pytest.raises(TypeError):
        awa.Client(DATABASE_URL, 5, 300)
