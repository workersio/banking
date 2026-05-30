#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, config, every_nth_retry, rng, run_workload, split_counts, validation_probe_attempts


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-07")
    counts = split_counts(420, {"routine": 0.35, "merchant": 0.50, "fraud": 0.15})

    routine = banking.make_random_transfers(
        source_rng,
        counts["routine"],
        key_prefix="mixed-routine",
        amount_min=100,
        amount_max=700,
    )
    merchant = banking.make_random_transfers(
        source_rng,
        counts["merchant"],
        key_prefix="mixed-merchant",
        amount_min=100,
        amount_max=850,
    )
    fraud = [
        banking.make_random_transfer(
            source_rng,
            index,
            key_prefix="mixed-fraud",
            amount_min=config.SINGLE_TX_LIMIT + 1,
            amount_max=config.SINGLE_TX_LIMIT + 1200,
        )
        for index in range(counts["fraud"])
    ]
    validation = validation_probe_attempts(source_rng, "mixed-validation")

    return [
        banking.transfer_phase("routine_sequential", routine, concurrency=1),
        banking.transfer_phase("routine_retries_after_success", every_nth_retry(routine, 30), concurrency=1),
        banking.transfer_phase("merchant_parallel_burst", merchant, concurrency=4),
        banking.transfer_phase("fraud_amount_probes", fraud, concurrency=4),
        banking.transfer_phase("client_validation_errors", validation, concurrency=2),
    ]


if __name__ == "__main__":
    sys.exit(run_workload("realistic_mixed_day", build()))
