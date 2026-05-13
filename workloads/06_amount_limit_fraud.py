#!/usr/bin/env python3
from __future__ import annotations

import sys

from common import banking, config, rng, run_workload


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-06")
    source = source_rng.choice(config.ACCOUNTS)
    transfers = [
        banking.make_random_transfer(
            source_rng,
            index,
            key_prefix="amount-limit",
            source=source,
            amount_min=config.SINGLE_TX_LIMIT + 1,
            amount_max=config.SINGLE_TX_LIMIT + 1500,
        )
        for index in range(40)
    ]
    return [banking.transfer_phase("single_transaction_limit_probes", transfers, concurrency=4)]


if __name__ == "__main__":
    sys.exit(run_workload("amount_limit_fraud", build()))
