#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, every_nth_retry, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-10")
    originals = banking.make_random_operations(
        source_rng,
        160,
        key_prefix="retry-after-success",
        amount_min=50,
        amount_max=500,
    )
    retries = every_nth_retry(originals, 5)
    return [
        banking.operation_phase("originals_parallel", originals, concurrency=4),
        banking.operation_phase("duplicate_reference_retries_after_originals", retries, concurrency=1),
    ]


if __name__ == "__main__":
    sys.exit(run_workload("retry_after_success", build()))
