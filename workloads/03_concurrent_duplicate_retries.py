#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, rng, run_workload, shuffled_duplicate_pairs


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-03")
    originals = banking.make_random_operations(
        source_rng,
        70,
        key_prefix="retry-race",
        amount_min=75,
        amount_max=450,
    )
    duplicated = shuffled_duplicate_pairs(source_rng, originals)
    return [banking.operation_phase("concurrent_duplicate_reference_retries", duplicated, concurrency=10)]


if __name__ == "__main__":
    sys.exit(run_workload("concurrent_duplicate_retries", build()))
