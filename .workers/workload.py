#!/usr/bin/env python3
"""Canonical workload entrypoint for the Deadline SLO seed-sweep demo."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEADLINE_WORKLOAD = ROOT / "workloads" / "11_seed_sweep_mobile_deadline.py"


def _load_deadline_workload():
    spec = importlib.util.spec_from_file_location(
        "seed_sweep_mobile_deadline",
        DEADLINE_WORKLOAD,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load workload from {DEADLINE_WORKLOAD}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    return int(_load_deadline_workload().main())


if __name__ == "__main__":
    sys.exit(main())
