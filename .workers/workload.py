#!/usr/bin/env python3
"""Compose a realistic payment-operations workload from client primitives."""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DB_PATH = Path("/tmp/banking.db")

WORKLOAD_ENV = {
    "BANK_RUN_ID": "payment-ops-workload",
    "BANK_SETTLE_S": "0",
    "BANK_VELOCITY_LIMIT": "1000",
}

for key, value in WORKLOAD_ENV.items():
    os.environ.setdefault(key, value)

sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import main as banking  # noqa: E402


def _split_counts(total: int, ratios: dict[str, float]) -> dict[str, int]:
    counts = {name: int(total * ratio) for name, ratio in ratios.items()}
    remainder = total - sum(counts.values())
    for name in ratios:
        if remainder <= 0:
            break
        counts[name] += 1
        remainder -= 1
    return counts


def _retries(operations: list[dict], every: int) -> list[dict]:
    if every <= 0:
        return []

    retries = []
    for index, operation in enumerate(operations):
        if index > 0 and index % every == 0:
            retries.append(banking.duplicate_operation(operation))
    return retries


def routine_collection_activity(rng: random.Random, count: int) -> list[dict]:
    operations = []
    for index in range(count):
        operations.append(banking.make_random_operation(
            rng,
            index,
            key_prefix="collection",
            amount_min=100,
            amount_max=1200,
        ))
    return operations


def payout_and_settlement_burst(rng: random.Random, count: int) -> list[dict]:
    hub = rng.choice(config.ACCOUNTS)
    counterparties = [account for account in config.ACCOUNTS if account != hub]
    operations = []

    for index in range(count):
        if index % 8 == 0:
            src = rng.choice(counterparties)
            dst = hub
        elif index % 8 == 1:
            src = hub
            dst = rng.choice(counterparties)
        else:
            src = rng.choice(config.ACCOUNTS)
            dst = banking.choose_destination(rng, src)

        operations.append(banking.make_random_operation(
            rng,
            index,
            key_prefix="payout-settlement",
            source=src,
            destination=dst,
            amount_min=100,
            amount_max=900,
        ))

    return operations


def risk_probe_attempts(rng: random.Random, count: int) -> list[dict]:
    source = rng.choice(config.ACCOUNTS)
    operations = []
    for index in range(count):
        operations.append(banking.make_random_operation(
            rng,
            index,
            key_prefix="risk-probe",
            source=source,
            amount_min=config.SINGLE_TX_LIMIT + 1,
            amount_max=config.SINGLE_TX_LIMIT + 2500,
        ))
    return operations


def validation_probe_attempts(rng: random.Random) -> list[dict]:
    source = rng.choice(config.ACCOUNTS)
    destination = banking.choose_destination(rng, source)
    return [
        banking.make_operation(source, destination, 0, "validation-zero-amount"),
        banking.make_operation(source, destination, -25, "validation-negative-amount"),
        banking.make_operation(source, source, 100, "validation-same-account"),
        banking.make_operation(source, "Z", 100, "validation-unknown-destination"),
        banking.make_operation(source, destination, 100, ""),
    ]


def build_realistic_workload() -> list[banking.TransferPhase]:
    rng = random.Random(os.environ.get("BANK_SEED", "42"))
    total = int(os.environ.get("BANK_TRANSFERS", "500"))
    burst_concurrency = int(os.environ.get("BANK_CONCURRENCY", "4"))
    duplicate_every = int(os.environ.get("BANK_DUPLICATE_EVERY", "25"))
    counts = _split_counts(total, {
        "routine": 0.30,
        "burst": 0.60,
        "risk": 0.10,
    })

    routine = routine_collection_activity(rng, counts["routine"])
    routine_retries = _retries(routine, duplicate_every)
    burst = payout_and_settlement_burst(rng, counts["burst"])
    risk = risk_probe_attempts(rng, counts["risk"])
    validation = validation_probe_attempts(rng)

    return [
        banking.operation_phase("collection_sequential_activity", routine, concurrency=1),
        banking.operation_phase("duplicate_reference_retries", routine_retries, concurrency=1),
        banking.operation_phase("payout_and_settlement_burst", burst, concurrency=burst_concurrency),
        banking.operation_phase("risk_limit_probe_attempts", risk, concurrency=burst_concurrency),
        banking.operation_phase("api_validation_errors", validation, concurrency=2),
    ]


def main() -> int:
    try:
        DB_PATH.unlink()
    except FileNotFoundError:
        pass

    phases = build_realistic_workload()
    attempted = sum(len(phase.operations) for phase in phases)
    result = banking.run_banking_app(
        phases,
        settle_s=int(os.environ["BANK_SETTLE_S"]),
        run_id=os.environ["BANK_RUN_ID"],
        plan_summary={
            "scenario": "realistic_payment_operations_day",
            "configured_operations": os.environ.get("BANK_TRANSFERS", "500"),
            "attempted_with_retries": attempted,
        },
    )
    status = "PASS" if result == 0 else "FAIL"
    print(f"WORKLOAD status={status} exit_code={result}", flush=True)
    return result


if __name__ == "__main__":
    sys.exit(main())
