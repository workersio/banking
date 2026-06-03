#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, config, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-04")
    source = source_rng.choice(config.ACCOUNTS)
    operations = [
        banking.make_random_operation(
            source_rng,
            index,
            key_prefix="hot-outflow",
            source=source,
            amount_min=250,
            amount_max=900,
        )
        for index in range(90)
    ]
    return [banking.operation_phase(f"hot_balance_{source}_payout_outflow", operations, concurrency=5)]


if __name__ == "__main__":
    sys.exit(run_workload("hot_balance_payout_outflow", build()))
