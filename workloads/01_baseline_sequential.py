#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-01")
    operations = banking.make_random_operations(
        source_rng,
        120,
        key_prefix="baseline",
        amount_min=100,
        amount_max=700,
    )
    return [banking.operation_phase("sequential_collection_settlements", operations, concurrency=1)]


if __name__ == "__main__":
    sys.exit(run_workload("baseline_sequential", build()))
