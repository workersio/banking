#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, config, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-04")
    source = source_rng.choice(config.ACCOUNTS)
    transfers = [
        banking.make_random_transfer(
            source_rng,
            index,
            key_prefix="hot-outflow",
            source=source,
            amount_min=250,
            amount_max=900,
        )
        for index in range(90)
    ]
    return [banking.transfer_phase(f"hot_account_{source}_outflow", transfers, concurrency=5)]


if __name__ == "__main__":
    sys.exit(run_workload("hot_account_outflow", build()))
