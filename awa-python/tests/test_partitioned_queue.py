"""Tests for the Python PartitionedQueue helper."""

import pytest

import awa


def test_partitioned_queue_default_names_and_key_routing():
    partitioned_queue = awa.PartitionedQueue("customer-updates", 4)

    assert partitioned_queue.logical_queue == "customer-updates"
    assert partitioned_queue.partitions == 4
    assert len(partitioned_queue) == 4
    assert partitioned_queue.physical_queues == [
        "customer-updates",
        "customer-updates__p1",
        "customer-updates__p2",
        "customer-updates__p3",
    ]
    assert partitioned_queue.queue_for_key(b"customer-42") in partitioned_queue.physical_queues
    assert partitioned_queue.queue_for_key("customer-42") == partitioned_queue.queue_for_key(
        b"customer-42"
    )
    assert partitioned_queue.route_by_key("customer-42") == {
        "queue": partitioned_queue.queue_for_key(b"customer-42"),
        "ordering_key": b"customer-42",
    }


def test_partitioned_queue_round_robin_index_routing():
    partitioned_queue = awa.PartitionedQueue("email", 4)

    assert partitioned_queue.queue_for_index(0) == "email"
    assert partitioned_queue.queue_for_index(5) == "email__p1"
    assert partitioned_queue.route_by_index(5) == {"queue": "email__p1"}

    with pytest.raises(awa.ValidationError, match="index must be >= 0"):
        partitioned_queue.queue_for_index(-1)
    with pytest.raises(awa.ValidationError, match="index must be >= 0"):
        partitioned_queue.route_by_index(-1)


def test_partitioned_queue_explicit_physical_queues():
    partitioned_queue = awa.PartitionedQueue.from_physical_queues(
        "email", ["email-fast", "email-slow"]
    )

    assert partitioned_queue.logical_queue == "email"
    assert partitioned_queue.partitions == 2
    assert partitioned_queue.physical_queues == ["email-fast", "email-slow"]


def test_partitioned_queue_queue_configs_hard_reserved():
    partitioned_queue = awa.PartitionedQueue("email", 2)

    assert partitioned_queue.queue_configs(
        max_workers_per_partition=8,
        rate_limit_per_partition=(100.0, 200),
        claimers=2,
        claim_batch_size=64,
    ) == [
        {
            "name": "email",
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


def test_partitioned_queue_queue_configs_weighted():
    partitioned_queue = awa.PartitionedQueue("email", 2)

    assert partitioned_queue.queue_configs(min_workers_per_partition=0, weight=3) == [
        {"name": "email", "min_workers": 0, "weight": 3},
        {"name": "email__p1", "min_workers": 0, "weight": 3},
    ]


def test_partitioned_queue_validation_errors():
    with pytest.raises(awa.ValidationError, match="logical queue must not be empty"):
        awa.PartitionedQueue("", 4)
    with pytest.raises(awa.ValidationError, match="partitions must be > 0"):
        awa.PartitionedQueue("email", 0)
    with pytest.raises(awa.ValidationError, match="partitions must be > 0"):
        awa.PartitionedQueue("email", -1)
    with pytest.raises(awa.ValidationError, match="partitions must be > 0"):
        awa.PartitionedQueue.from_physical_queues("email", [])
    with pytest.raises(awa.ValidationError, match="physical queue must not be empty"):
        awa.PartitionedQueue.from_physical_queues("email", ["email-a", ""])
    with pytest.raises(awa.ValidationError, match="duplicated"):
        awa.PartitionedQueue.from_physical_queues("email", ["email-a", "email-a"])

    partitioned_queue = awa.PartitionedQueue("email", 2)
    with pytest.raises(awa.ValidationError, match="ordering_key must be bytes-like or str"):
        partitioned_queue.queue_for_key(object())
    with pytest.raises(awa.ValidationError, match="ordering_key must be bytes-like or str"):
        partitioned_queue.route_by_key(object())
    with pytest.raises(awa.ValidationError, match="requires max_workers_per_partition"):
        partitioned_queue.queue_configs()
    with pytest.raises(awa.ValidationError, match="not both"):
        partitioned_queue.queue_configs(
            max_workers_per_partition=1,
            min_workers_per_partition=1,
        )
    with pytest.raises(awa.ValidationError, match="max_workers_per_partition must be > 0"):
        partitioned_queue.queue_configs(max_workers_per_partition=0)
    with pytest.raises(awa.ValidationError, match="weight must be > 0"):
        partitioned_queue.queue_configs(min_workers_per_partition=0, weight=0)
    with pytest.raises(awa.ValidationError, match="claimers must be > 0"):
        partitioned_queue.queue_configs(max_workers_per_partition=1, claimers=0)
    with pytest.raises(awa.ValidationError, match="claim_batch_size must be > 0"):
        partitioned_queue.queue_configs(max_workers_per_partition=1, claim_batch_size=0)
