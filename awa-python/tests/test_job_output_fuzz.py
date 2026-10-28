"""Fuzz ``Job.set_output`` (ADR-048) with seeded random values.

Whatever a handler records, the call either raises ``awa.OutputError`` and
leaves no output, or returns, after which the completed job's output is the
value as the stand-in normalized it. The ``HandlerJob`` stand-in raises for
exactly the values the runtime raises for, and normalizes them the same way.

AWA_FUZZ_SEED and AWA_FUZZ_CASES change the generated cases; a failure
reports the seed and the case so it can be replayed.
"""

import asyncio
import datetime
import decimal
import enum
import math
import os
import random
from dataclasses import dataclass, field
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
SEED = int(os.environ.get("AWA_FUZZ_SEED", "20261028"))
CASES = int(os.environ.get("AWA_FUZZ_CASES", "120"))
QUEUE = "output_py_fuzz"


class Colour(enum.Enum):
    RED = "red"


@dataclass
class Point:
    x: int
    y: float
    label: str = "p"
    tags: tuple = ("a",)


@dataclass
class Nested:
    point: Point
    points: list = field(default_factory=list)


@dataclass
class OutputFuzz:
    case: int


def leaf(rng: random.Random) -> Any:
    return rng.choice(
        [
            None,
            True,
            False,
            0,
            -1,
            2**63 - 1,
            -(2**63),
            2**64 - 1,
            1.5,
            -0.0,
            1e300,
            5e-324,
            "",
            "plain",
            "é" * 3,
            "\U0001f600",
        ]
    )


def hostile(rng: random.Random) -> Any:
    return rng.choice(
        [
            math.nan,
            math.inf,
            -math.inf,
            2**64,
            -(2**63) - 1,
            {1, 2},
            frozenset(),
            b"bytes",
            object(),
            Colour.RED,
            decimal.Decimal("1.5"),
            datetime.datetime(2026, 1, 1),
            {1: "int key"},
            {("t",): "tuple key"},
            "a\x00b",
            {"k\x00": 1},
            "x" * awa.MAX_OUTPUT_BYTES,
            [0] * (awa.MAX_OUTPUT_BYTES // 2),
            Point,
        ]
    )


def value(rng: random.Random, depth: int = 0) -> Any:
    roll = rng.random()
    if roll < 0.08:
        return hostile(rng)
    if depth >= 3 or roll < 0.45:
        return leaf(rng)
    if roll < 0.6:
        return [value(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    if roll < 0.7:
        return tuple(value(rng, depth + 1) for _ in range(rng.randint(0, 3)))
    if roll < 0.85:
        return {f"k{i}": value(rng, depth + 1) for i in range(rng.randint(0, 4))}
    if roll < 0.93:
        return Point(x=rng.randint(-5, 5), y=rng.random(), tags=("a", "b"))
    return Nested(point=Point(x=1, y=0.5), points=[Point(x=2, y=1.0)])


@dataclass
class Case:
    index: int
    value: Any
    raised: BaseException | None = None
    during: Any = None


def stand_in(value: Any) -> tuple[BaseException | None, Any]:
    job = HandlerJob(
        args=OutputFuzz(case=0),
        id=0,
        attempt=1,
        max_attempts=1,
        queue=QUEUE,
        priority=2,
        deadline=None,
        metadata={},
        tags=[],
    )
    try:
        job.set_output(value)
    except Exception as e:  # recorded, not raised: parity is checked below
        return e, job.output
    return None, job.output


def same(left: Any, right: Any) -> bool:
    """JSON equality as jsonb keeps it: numbers compare by value."""
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            same(left[k], right[k]) for k in left
        )
    return left == right


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


async def wait_until_completed(client, job_ids: list[int], timeout: float = 60.0):
    deadline = asyncio.get_running_loop().time() + timeout
    pending = set(job_ids)
    while pending:
        for job_id in list(pending):
            if str((await client.get_job(job_id)).state) == "completed":
                pending.discard(job_id)
        if pending and asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"{len(pending)} jobs never completed")
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_set_output_either_raises_or_stores_what_the_stand_in_records(client):
    rng = random.Random(SEED)
    cases = [Case(index=i, value=value(rng)) for i in range(CASES)]

    @client.task(OutputFuzz, queue=QUEUE)
    async def record(job):
        case = cases[job.args.case]
        try:
            job.set_output(case.value)
        except Exception as e:  # swallowed: the attempt must still complete
            case.raised = e
        case.during = job.output

    job_ids = []
    for case in cases:
        job = await client.insert(OutputFuzz(case=case.index), queue=QUEUE)
        job_ids.append(job.id)
    await client.start([(QUEUE, 16)], **RUNTIME_START_KWARGS)
    await wait_until_completed(client, job_ids)
    await client.shutdown()

    failures = []
    raised = 0
    for case, job_id in zip(cases, job_ids):
        stored = (await client.get_job(job_id)).output
        expected_error, expected = stand_in(case.value)
        where = f"case {case.index} (seed {SEED}): {case.value!r:.80}"
        if case.raised is not None:
            raised += 1
            if not isinstance(case.raised, awa.OutputError):
                failures.append(
                    f"{where}: raised {type(case.raised).__name__}, not OutputError"
                )
            if stored is not None or case.during is not None:
                failures.append(
                    f"{where}: raised but kept {case.during!r:.60} / stored {stored!r:.60}"
                )
        elif not same(stored, expected) or not same(case.during, expected):
            failures.append(
                f"{where}: stored {stored!r:.80}, during {case.during!r:.80}, stand-in {expected!r:.80}"
            )
        if (expected_error is None) != (case.raised is None):
            failures.append(
                f"{where}: runtime raised {case.raised!r:.60} but the stand-in raised {expected_error!r:.60}"
            )

    assert CASES // 20 < raised < CASES * 3 // 4, f"{raised} of {CASES} cases raised"
    assert not failures, "\n".join(failures)
