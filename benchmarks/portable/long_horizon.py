#!/usr/bin/env python3
"""Long-horizon benchmark runner entrypoint.

Example usage:

  # Named scenario, default systems (awa, awa-python, river, procrastinate, oban).
  uv run python benchmarks/portable/long_horizon.py --scenario idle_in_tx_saturation

  # Custom phase sequence:
  uv run python benchmarks/portable/long_horizon.py \\
      --phase warmup=warmup:10m \\
      --phase clean_1=clean:60m \\
      --phase idle_1=idle-in-tx:60m \\
      --phase recovery_1=recovery:30m \\
      --systems awa

  # Developer fast path:
  uv run python benchmarks/portable/long_horizon.py \\
      --scenario idle_in_tx_saturation --fast

See benchmarks/portable/README.md for the full interpretation guide.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
# Allow running directly without pip install: put the package dir on path.
sys.path.insert(0, str(SCRIPT_DIR))

from bench_harness.orchestrator import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
