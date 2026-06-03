#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")

from common import banking, config, every_nth_retry, rng, run_workload, split_counts, validation_probe_attempts


def build() -> list[banking.TransferPhase]:
    source_rng = rng("workload-07")
    counts = split_counts(420, {"routine": 0.35, "merchant": 0.50, "risk": 0.15})

    routine = banking.make_random_operations(
        source_rng,
        counts["routine"],
        key_prefix="mixed-routine",
        amount_min=100,
        amount_max=700,
    )
    merchant = banking.make_random_operations(
        source_rng,
        counts["merchant"],
        key_prefix="mixed-merchant",
        amount_min=100,
        amount_max=850,
    )
    risk = [
        banking.make_random_operation(
            source_rng,
            index,
            key_prefix="mixed-risk",
            amount_min=config.SINGLE_TX_LIMIT + 1,
            amount_max=config.SINGLE_TX_LIMIT + 1200,
        )
        for index in range(counts["risk"])
    ]
    validation = validation_probe_attempts(source_rng, "mixed-validation")

    return [
        banking.operation_phase("routine_sequential", routine, concurrency=1),
        banking.operation_phase("routine_retries_after_success", every_nth_retry(routine, 30), concurrency=1),
        banking.operation_phase("merchant_parallel_burst", merchant, concurrency=4),
        banking.operation_phase("risk_amount_probes", risk, concurrency=4),
        banking.operation_phase("api_validation_errors", validation, concurrency=2),
    ]


if __name__ == "__main__":
    sys.exit(run_workload("realistic_mixed_day", build()))
