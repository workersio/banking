#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, config, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-09")
    source = source_rng.choice(config.ACCOUNTS)
    drain = [
        banking.make_random_transfer(
            source_rng,
            index,
            key_prefix="drain",
            source=source,
            amount_min=600,
            amount_max=1100,
        )
        for index in range(45)
    ]
    cross_traffic = banking.make_random_transfers(
        source_rng,
        100,
        key_prefix="reversal-cross",
        amount_min=100,
        amount_max=800,
    )
    return [
        banking.transfer_phase(f"drain_{source}_parallel", drain, concurrency=5),
        banking.transfer_phase("cross_account_followup", cross_traffic, concurrency=4),
    ]


if __name__ == "__main__":
    sys.exit(run_workload("reversal_pressure", build()))
