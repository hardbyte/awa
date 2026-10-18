"""Write a report from a completed, independently auditable review packet."""

import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("pr", choices=("521", "524"))
parser.add_argument("packet", type=Path)
parser.add_argument(
    "--method", type=Path, default=Path(__file__).with_name("METHOD.md")
)
args = parser.parse_args()
results = json.loads((args.packet / "results.json").read_text())
records = {(row["build"], row["cell"]): row for row in results["measurements"]}
identities = {
    build: json.loads((args.packet / f"{build}-build.json").read_text())
    for build in ("baseline", "candidate")
}
titles = {
    "521": "Durable finalization recovery: warmed load checks",
    "524": "Post-commit cursor advance: warmed two-priority load checks",
}
introductions = {
    "521": "The reviewed runtime retains saved results across connection loss, fences replay by the original attempt/lease, drains tracked finalizers, and bounds retry awaits by an absolute deadline. Its finalizer budget also permits a flushing completion batch and a queued batch per shard. The initial limit of one result per worker prevented configured full batches from forming; `initial-worker-cap/` preserves that investigation. The full-batch SQL regression rejects that limit and verifies durable drain after backpressure.",
    "524": "The reviewed runtime restores guarded cursor advance after committed nonempty claims. Three SQL regressions exercise receipt runtime claims with uniqueness, legacy runtime claims and raw receipt claims while an enqueue reservation remains open. The two receipt regressions reject main's stale cursor; all three pass with the correction. The focused model reproduces the old empty poll that shadows another ready lane, under a successful-advance premise rather than a general fairness claim.",
}
lines = [
    f"# {titles[args.pr]}",
    "",
    introductions[args.pr],
    "",
    f"Baseline: `{identities['baseline']['head']}`. Candidate: `{identities['candidate']['head']}`.",
    "",
    f"This packet accompanies [PR #{args.pr}](https://github.com/hardbyte/awa/pull/{args.pr}). The executable identities are frozen in `*-build.json`; the source verification is linked in the PR description.",
    "",
    "## Results",
    "",
    "Values are **baseline → candidate**. These are single paired runs on a shared host; they do not establish a capacity or latency ranking.",
    "",
    "| Cell | Handler returns/s | Median window p99, ms | Worst window p99, ms | Mean queue depth |",
    "| --- | ---: | ---: | ---: | ---: |",
]
for cell, label in (
    ("reference", "800 jobs/s, 32 workers"),
    ("saturation", "Depth target 4,000, 64 workers"),
):
    first, second = records["baseline", cell], records["candidate", cell]
    values = [
        f"{first[field]:.2f} → {second[field]:.2f}"
        for field in (
            "handler_returns_per_second",
            "handler_window_p99_median_ms",
            "handler_window_p99_worst_ms",
            "queue_depth_mean",
        )
    ]
    lines.append("| " + " | ".join([label, *values]) + " |")
lines.extend(
    [
        "",
        "Both cells completed without a `Failed to complete job` log entry. This is not a count of durable acknowledgements; the boundary and shutdown limitations are described below.",
        "",
        "### Actual warmup generations",
        "",
        "Every cell requires at least 32 queue and 16 claim generations before measurement.",
        "",
        "| Build | Reference queue / claim | Saturation queue / claim |",
        "| --- | ---: | ---: |",
    ]
)
for build in ("baseline", "candidate"):
    values = [
        f"{records[build, cell]['queue_warmup_generations']:.0f} / {records[build, cell]['claim_warmup_generations']:.0f}"
        for cell in ("reference", "saturation")
    ]
    lines.append("| " + " | ".join([build.capitalize(), *values]) + " |")
method = (
    args.method.read_text()
    .replace("PR_NUMBER", args.pr)
    .replace("PRIORITY_PATTERN", "1" if args.pr == "521" else "1,2")
)
lines.extend(["", method])
(args.packet / "README.md").write_text("\n".join(lines))
print(args.packet / "README.md")
