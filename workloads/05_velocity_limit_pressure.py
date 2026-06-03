#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "4")

from common import banking, config, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-05")
    source = source_rng.choice(config.ACCOUNTS)
    operations = [
        banking.make_random_operation(
            source_rng,
            index,
            key_prefix="velocity",
            source=source,
            amount_min=100,
            amount_max=300,
        )
        for index in range(24)
    ]
    return [banking.operation_phase(f"velocity_limited_{source}", operations, concurrency=1)]


if __name__ == "__main__":
    sys.exit(run_workload("velocity_limit_pressure", build()))
