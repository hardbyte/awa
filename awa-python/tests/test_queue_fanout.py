"""Tests for the Python QueueFanout helper."""

import pytest

import awa


def test_queue_fanout_default_names_and_key_routing():
    fanout = awa.QueueFanout("customer-updates", 4)

    assert fanout.logical_queue == "customer-updates"
    assert fanout.width == 4
    assert len(fanout) == 4
    assert fanout.physical_queues == [
        "customer-updates__p0",
        "customer-updates__p1",
        "customer-updates__p2",
        "customer-updates__p3",
    ]
    assert fanout.queue_for_key(b"customer-42") == "customer-updates__p0"
    assert fanout.queue_for_key("customer-42") == "customer-updates__p0"
    assert fanout.route_by_key("customer-42") == {
        "queue": "customer-updates__p0",
        "ordering_key": b"customer-42",
    }


def test_queue_fanout_round_robin_index_routing():
    fanout = awa.QueueFanout("email", 4)

    assert fanout.queue_for_index(0) == "email__p0"
    assert fanout.queue_for_index(5) == "email__p1"
    assert fanout.route_by_index(5) == {"queue": "email__p1"}

    with pytest.raises(awa.ValidationError, match="index must be >= 0"):
        fanout.queue_for_index(-1)
    with pytest.raises(awa.ValidationError, match="index must be >= 0"):
        fanout.route_by_index(-1)


def test_queue_fanout_explicit_physical_queues():
    fanout = awa.QueueFanout.from_physical_queues(
        "email", ["email-fast", "email-slow"]
    )

    assert fanout.logical_queue == "email"
    assert fanout.width == 2
    assert fanout.physical_queues == ["email-fast", "email-slow"]


def test_queue_fanout_queue_configs_hard_reserved():
    fanout = awa.QueueFanout("email", 2)

    assert fanout.queue_configs(
        max_workers_per_queue=8,
        rate_limit_per_queue=(100.0, 200),
        claimers=2,
        claim_batch_size=64,
    ) == [
        {
            "name": "email__p0",
            "max_workers": 8,
            "rate_limit": (100.0, 200),
            "claimers": 2,
            "claim_batch_size": 64,
        },
        {
            "name": "email__p1",
            "max_workers": 8,
            "rate_limit": (100.0, 200),
            "claimers": 2,
            "claim_batch_size": 64,
        },
    ]


def test_queue_fanout_queue_configs_weighted():
    fanout = awa.QueueFanout("email", 2)

    assert fanout.queue_configs(min_workers_per_queue=0, weight=3) == [
        {"name": "email__p0", "min_workers": 0, "weight": 3},
        {"name": "email__p1", "min_workers": 0, "weight": 3},
    ]


def test_queue_fanout_validation_errors():
    with pytest.raises(awa.ValidationError, match="logical queue must not be empty"):
        awa.QueueFanout("", 4)
    with pytest.raises(awa.ValidationError, match="width must be > 0"):
        awa.QueueFanout("email", 0)
    with pytest.raises(awa.ValidationError, match="width must be > 0"):
        awa.QueueFanout("email", -1)
    with pytest.raises(awa.ValidationError, match="width must be > 0"):
        awa.QueueFanout.from_physical_queues("email", [])
    with pytest.raises(awa.ValidationError, match="physical queue must not be empty"):
        awa.QueueFanout.from_physical_queues("email", ["email-a", ""])
    with pytest.raises(awa.ValidationError, match="duplicated"):
        awa.QueueFanout.from_physical_queues("email", ["email-a", "email-a"])

    fanout = awa.QueueFanout("email", 2)
    with pytest.raises(awa.ValidationError, match="ordering_key must be bytes-like or str"):
        fanout.queue_for_key(object())
    with pytest.raises(awa.ValidationError, match="ordering_key must be bytes-like or str"):
        fanout.route_by_key(object())
    with pytest.raises(awa.ValidationError, match="requires max_workers_per_queue"):
        fanout.queue_configs()
    with pytest.raises(awa.ValidationError, match="not both"):
        fanout.queue_configs(max_workers_per_queue=1, min_workers_per_queue=1)
    with pytest.raises(awa.ValidationError, match="max_workers_per_queue must be > 0"):
        fanout.queue_configs(max_workers_per_queue=0)
    with pytest.raises(awa.ValidationError, match="weight must be > 0"):
        fanout.queue_configs(min_workers_per_queue=0, weight=0)
    with pytest.raises(awa.ValidationError, match="claimers must be > 0"):
        fanout.queue_configs(max_workers_per_queue=1, claimers=0)
    with pytest.raises(awa.ValidationError, match="claim_batch_size must be > 0"):
        fanout.queue_configs(max_workers_per_queue=1, claim_batch_size=0)
