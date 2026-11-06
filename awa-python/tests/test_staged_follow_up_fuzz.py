"""Fuzz ``Job.enqueue_on_completion`` (ADR-046) with seeded random options.

Whatever a handler passes, the call either raises ``awa.StagingError``, after
which the attempt fails with no follow-up, or returns, after which the
follow-up commits with the completion. No other exception escapes, even for
options of the wrong type. The ``HandlerJob`` stand-in raises for exactly the
calls the runtime raises for, so handler unit tests see what production does.

AWA_FUZZ_SEED and AWA_FUZZ_CASES change the generated cases; a failure
reports the seed and the case so it can be replayed.
"""

import asyncio
import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import awa
from awa.testing import HandlerJob

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
RUNTIME_START_KWARGS = {
    "leader_election_interval_ms": 100,
    "queue_storage_queue_rotate_interval_ms": 60_000,
}
SEED = int(os.environ.get("AWA_FUZZ_SEED", "20261027"))
CASES = int(os.environ.get("AWA_FUZZ_CASES", "150"))
QUEUE = "staged_py_fuzz"

NOW = datetime.now(timezone.utc)

# (valid, hostile) values per option. Queue names stay within 59 bytes:
# queue storage cannot insert into longer ones yet (#560).
OPTIONS: dict[str, tuple[list[Any], list[Any]]] = {
    "kind": (
        [None, "fuzz_follow_up", "k" * 200, "é" * 150],
        ["k" * 201, "a\x00b", 7, b"kind", ["kind"], 3.5],
    ),
    "queue": (
        ["fuzz_out", "q" * 59, "é" * 29],
        ["a\x00b", 1, ["q"], b"queue", 2.5],
    ),
    "priority": (
        [1, 2, 4],
        [0, 5, -1, 2**15, 2**40, 2.0, "2", None, True],
    ),
    "max_attempts": (
        [1, 25, 1000],
        [0, 1001, -3, 2**16, 3.0, "1", None],
    ),
    "tags": (
        [[], ["a"], ["a"] * 20, ("x", "y")],
        [["a"] * 21, ["a\x00"], "abc", [1, 2], None, [None]],
    ),
    "metadata": (
        [None, {}, {"k": "v"}, {"nested": {"list": [1, 2.5, None]}}],
        [
            {"big": "x" * 70_000},
            {"nul": "a\x00b"},
            {"set": {1, 2}},
            {1: "int key"},
            [1, 2],
            "text",
        ],
    ),
    "run_at": (
        [
            None,
            NOW + timedelta(hours=1),
            "2030-01-01T00:00:00Z",
            "2030-01-01T00:00:00+05:30",
            datetime(1, 1, 1, tzinfo=timezone.utc),
            datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        ],
        [
            "not a time",
            "0000-01-01T00:00:00Z",
            "2030-13-01T00:00:00Z",
            1234,
            ["2030-01-01T00:00:00Z"],
        ],
    ),
    "unique_opts": (
        [
            None,
            {},
            {"by_queue": True},
            {"by_period": 3600},
            {"by_args": False},
            {"states": 0b0001_1111},
            {"unknown": 1},
        ],
        [
            "nope",
            [],
            {"by_queue": 1},
            {"by_args": "no"},
            {"by_period": 2**70},
            {"by_period": "1"},
            {"states": 256},
            {"states": -1},
        ],
    ),
    "ordering_key": (
        [None, b"", b"key", "key", bytearray(b"key")],
        [b"x" * 70_000, 5, ["k"], 1.5],
    ),
}


@dataclass
class FuzzTrigger:
    case: int


@dataclass
class Case:
    index: int
    kwargs: dict[str, Any]
    args: dict[str, Any]
    calls: int
    raised: BaseException | None = None
    staged: int = 0
    notes: list[str] = field(default_factory=list)

    def follow_up_args(self, call: int) -> dict[str, Any]:
        return {**self.args, "fuzz_follow_up": self.index, "call": call}

    def may_deduplicate(self) -> bool:
        # Without args in the unique key, follow-ups of the same kind collapse.
        unique = self.kwargs.get("unique_opts")
        return isinstance(unique, dict) and unique.get("by_args") is False

    def describe(self) -> str:
        shown = {
            name: (repr(value)[:60] + "…" if len(repr(value)) > 60 else repr(value))
            for name, value in self.kwargs.items()
        }
        return (
            f"case {self.index} (seed {SEED}): calls={self.calls} "
            f"args+={repr(self.args)[:60]} kwargs={shown}"
        )


def generate(rng: random.Random, index: int) -> Case:
    kwargs: dict[str, Any] = {}
    for name, (valid, hostile) in OPTIONS.items():
        roll = rng.random()
        if roll < 0.5:
            continue
        kwargs[name] = rng.choice(hostile if roll < 0.62 else valid)
    args: dict[str, Any] = {}
    roll = rng.random()
    if roll < 0.04:
        args["nul"] = "a\x00b"
    elif roll < 0.08:
        args["pad"] = "x" * 70_000
    elif roll < 0.12:
        args["not_json"] = {1, 2}
    elif roll < 0.16:
        args["int_keys"] = {1: "one"}
    calls = rng.choice([1] * 12 + [3, awa.MAX_STAGED_FOLLOW_UPS, awa.MAX_STAGED_FOLLOW_UPS + 1])
    return Case(index=index, kwargs=kwargs, args=args, calls=calls)


def stand_in_raises(case: Case) -> BaseException | None:
    job = HandlerJob(
        args=FuzzTrigger(case=case.index),
        id=case.index,
        attempt=1,
        max_attempts=1,
        queue=QUEUE,
        priority=2,
        deadline=None,
        metadata={},
        tags=[],
    )
    try:
        for call in range(case.calls):
            job.enqueue_on_completion(case.follow_up_args(call), **case.kwargs)
    except Exception as e:  # recorded, not raised: parity is checked below
        return e
    return None


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    await c.install_queue_storage(reset=True)
    try:
        yield c
    finally:
        await c.shutdown()
        await c.close()


async def follow_up_count(client, index: int) -> int:
    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint AS n FROM awa.jobs WHERE args->>'fuzz_follow_up' = $1",
        str(index),
    )
    await tx.commit()
    return row["n"]


async def wait_until_settled(client, job_ids: list[int], timeout: float = 60.0):
    deadline = asyncio.get_running_loop().time() + timeout
    pending = set(job_ids)
    while pending:
        for job_id in list(pending):
            job = await client.get_job(job_id)
            if str(job.state) in ("completed", "failed"):
                pending.discard(job_id)
        if pending and asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"{len(pending)} trigger jobs never settled")
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_staging_either_fails_the_attempt_or_commits_the_follow_up(client):
    rng = random.Random(SEED)
    cases = [generate(rng, index) for index in range(CASES)]

    @client.task(FuzzTrigger, queue=QUEUE)
    async def stage(job):
        case = cases[job.args.case]
        try:
            for call in range(case.calls):
                job.enqueue_on_completion(case.follow_up_args(call), **case.kwargs)
                case.staged += 1
        except Exception as e:  # swallowed: the attempt must still fail
            case.raised = e
        return None

    job_ids = []
    for case in cases:
        job = await client.insert(FuzzTrigger(case=case.index), queue=QUEUE, max_attempts=1)
        job_ids.append(job.id)
    await client.start([(QUEUE, 16)], **RUNTIME_START_KWARGS)
    await wait_until_settled(client, job_ids)
    await client.shutdown()

    failures = []
    raised = 0
    for case, job_id in zip(cases, job_ids):
        state = str((await client.get_job(job_id)).state)
        count = await follow_up_count(client, case.index)
        if case.raised is not None:
            raised += 1
            if not isinstance(case.raised, awa.StagingError):
                failures.append(
                    f"{case.describe()}: raised {type(case.raised).__name__}: "
                    f"{case.raised}, not StagingError"
                )
            if state != "failed" or count != 0:
                failures.append(
                    f"{case.describe()}: staging raised but the job is {state} "
                    f"with {count} follow-ups"
                )
        else:
            # Follow-ups without args in their unique key may collapse into
            # one, or into another case's.
            expected = {case.calls} | ({0, 1} if case.may_deduplicate() else set())
            if state != "completed" or count not in expected:
                failures.append(
                    f"{case.describe()}: staging succeeded but the job is {state} "
                    f"with {count} follow-ups, expected {sorted(expected)}"
                )
        stand_in = stand_in_raises(case)
        if (stand_in is None) != (case.raised is None):
            failures.append(
                f"{case.describe()}: runtime raised {case.raised!r} "
                f"but the HandlerJob stand-in raised {stand_in!r}"
            )

    # Both outcomes must be common, or the property proves little.
    assert CASES // 5 < raised < CASES * 4 // 5, f"{raised} of {CASES} cases raised"
    assert not failures, "\n".join(failures)
