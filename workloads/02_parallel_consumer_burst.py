#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-02")
    warmup = banking.make_random_transfers(
        source_rng,
        40,
        key_prefix="burst-warmup",
        amount_min=50,
        amount_max=400,
    )
    burst = banking.make_random_transfers(
        source_rng,
        180,
        key_prefix="consumer-burst",
        amount_min=50,
        amount_max=500,
    )
    return [
        banking.transfer_phase("warmup_sequential", warmup, concurrency=1),
        banking.transfer_phase("consumer_parallel_burst", burst, concurrency=6),
    ]


if __name__ == "__main__":
    sys.exit(run_workload("parallel_consumer_burst", build()))
