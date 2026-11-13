"""The shared job filter (ADR-049) through the Python client: find_jobs,
count_jobs, the DLQ helpers and batch operations take the same dict."""

import os
from dataclasses import dataclass

import pytest

import awa
from test_dlq import _move_ready_to_failed_done

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
SCHEMA = "awa_py_dlq"


@dataclass
class FilterPyJob:
    n: int


@pytest.fixture
def client():
    client = awa.Client(DATABASE_URL)
    client.migrate()
    tx = client.transaction()
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.commit()
    client.install_queue_storage(schema=SCHEMA, reset=True)
    tx = client.transaction()
    tx.execute("DELETE FROM awa.job_unique_claims")
    tx.commit()
    try:
        yield client
    finally:
        tx = client.transaction()
        tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        tx.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        tx.commit()
        client.close()


def _seed(client, queue):
    jobs = []
    for n in range(6):
        tags = ["batch:a" if n % 2 == 0 else "batch:b", f"order:{n}"]
        jobs.append(client.insert(FilterPyJob(n=n), queue=queue, tags=tags))
    return jobs


def test_find_and_count_take_a_dict_keywords_or_both(client):
    queue = "pyfilter_find"
    jobs = _seed(client, queue)

    by_dict = client.find_jobs({"queue": queue, "tag": ["batch:a"]})
    assert sorted(j.id for j in by_dict) == sorted(j.id for j in jobs[0::2])
    assert [j.id for j in by_dict] == sorted((j.id for j in by_dict), reverse=True)

    by_keywords = client.find_jobs(queue=queue, tag=["batch:a", "order:2"])
    assert [j.id for j in by_keywords] == [jobs[2].id]

    merged = client.find_jobs({"queue": queue}, tag="order:5")
    assert [j.id for j in merged] == [jobs[5].id]
    with pytest.raises(ValueError, match="conflicts"):
        client.find_jobs({"queue": queue}, queue="elsewhere")

    assert client.count_jobs(queue=queue) == {"count": 6, "exact": True}
    assert client.count_jobs({"queue": queue, "state": "completed"}) == {
        "count": 0,
        "exact": True,
    }


def test_pages_walk_newest_first(client):
    queue = "pyfilter_pages"
    jobs = _seed(client, queue)
    seen = []
    before = None
    while True:
        page = client.find_jobs(queue=queue, before_id=before, limit=4)
        if not page:
            break
        seen.extend(j.id for j in page)
        before = page[-1].id
    assert seen == sorted((j.id for j in jobs), reverse=True)

    legacy = client.list_jobs(queue=queue, tag="batch:b", limit=10)
    assert sorted(j.id for j in legacy) == sorted(j.id for j in jobs[1::2])


def test_unknown_filter_fields_are_rejected(client):
    with pytest.raises(ValueError, match="unknown job filter field"):
        client.find_jobs({"tagz": "x"})
    with pytest.raises(ValueError, match="unknown job filter field"):
        client.preview_batch_operation(
            "set_priority", spec={"priority": 1}, filter={"queu": "x"}
        )


def test_dlq_helpers_take_the_filter(client):
    queue = "pyfilter_dlq"
    jobs = _seed(client, queue)
    for job in jobs:
        _move_ready_to_failed_done(client, job.id)
    assert client.bulk_move_failed_to_dlq(queue=queue, reason="py_filter") == 6

    listed = client.list_dlq(filter={"queue": queue, "tag": ["batch:a"]})
    assert sorted(row.job.id for row in listed) == sorted(j.id for j in jobs[0::2])

    # Every DLQ row is a dead-lettered failed job, so these don't narrow it.
    for unscoped in ({"dead_lettered": True}, {"state": "failed"}):
        with pytest.raises(Exception, match="allow_all"):
            client.purge_dlq(filter=unscoped)
    assert client.purge_dlq(filter={"queue": queue, "tag": "order:1"}) == 1
    newest = client.list_dlq(filter={"queue": queue}, limit=1)[0]
    assert (
        client.purge_dlq(
            filter={"queue": queue, "created_at_lt": "2000-01-01T00:00:00Z"},
            before_id=newest.job.id,
            before_dlq_at=newest.dlq_at,
        )
        == 0
    ), "a cursor keeps the rest of the filter"
    assert client.bulk_retry_from_dlq(queue=queue, filter={"tag": "order:3"}) == 1
    assert client.dlq_depth(queue=queue) == 4


def test_batch_preview_uses_the_same_population(client):
    queue = "pyfilter_batch"
    jobs = _seed(client, queue)
    preview = client.preview_batch_operation(
        "set_priority",
        spec={"priority": 1},
        filter={"queue": queue, "tag": ["batch:b"]},
    )
    assert preview["total_matched"] == 3
    assert {row["id"] for row in preview["sample"]} == {j.id for j in jobs[1::2]}
