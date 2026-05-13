#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-08")
    transfers = banking.make_random_transfers(
        source_rng,
        500,
        key_prefix="small-high-c",
        amount_min=1,
        amount_max=80,
    )
    return [banking.transfer_phase("many_small_transfers", transfers, concurrency=16)]


if __name__ == "__main__":
    sys.exit(run_workload("many_small_high_concurrency", build()))
