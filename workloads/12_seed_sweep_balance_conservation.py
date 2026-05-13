#!/usr/bin/env python3
"""Seed-sweep demo: conservation failure under account-path tail latency.

Run this with ``09_accounts_settlement_tail`` at depth 5. The run seed
chooses a realistic operating profile. Most seeds model a normal transfer
window. A small fraction model a stressed settlement window that can leave
orphaned debits when account RPCs and rollback RPCs hit the same tail.
"""

from __future__ import annotations

import hashlib
import os
import random
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "workloads"))


def _seed_text() -> str:
    return (
        os.environ.get("BANK_SEED")
        or os.environ.get("WENV_SEED")
        or os.environ.get("FORMAL_SEED")
        or "0000000000000001"
    )


def _seed_int(seed: str) -> int:
    try:
        return int(seed, 16)
    except ValueError:
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        return int(digest[:16], 16)


SEED = _seed_text()
PROFILE_RNG = random.Random(_seed_int(SEED))
PROFILE_SCORE = PROFILE_RNG.random()
STRESSED = PROFILE_SCORE < float(os.environ.get("BANK_CONSERVATION_STRESS_RATE", "0.20"))

os.environ.setdefault("BANK_RUN_ID", f"seed-sweep-conservation-{SEED}")
os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")
os.environ.setdefault("BANK_SINGLE_TX_LIMIT", "9000")
os.environ.setdefault("BANK_FRAUD_TIMEOUT", "1.0")

if STRESSED:
    os.environ.setdefault("BANK_ACCT_TIMEOUT", "0.55")
    os.environ.setdefault("BANK_ROLLBACK_RETRIES", "1")
    os.environ.setdefault("BANK_ROLLBACK_BASE_S", "0.2")
else:
    os.environ.setdefault("BANK_ACCT_TIMEOUT", "8.0")
    os.environ.setdefault("BANK_ROLLBACK_RETRIES", "4")
    os.environ.setdefault("BANK_ROLLBACK_BASE_S", "0.2")

from common import banking, config, reset_database, run_workload  # noqa: E402


def _build_stressed_settlement(rng: random.Random) -> list[banking.TransferPhase]:
    transfers = []
    hot_sources = ["A", "B", "C"]
    for index in range(96):
        src = hot_sources[index % len(hot_sources)]
        dst = banking.choose_destination(rng, src)
        amount = rng.randint(35, 170)
        transfers.append(
            banking.make_transfer(src, dst, amount, f"conservation-stress-{SEED}-{index:04d}")
        )
    return [banking.transfer_phase("stressed_settlement_window", transfers, concurrency=24)]


def _build_normal_settlement(rng: random.Random) -> list[banking.TransferPhase]:
    transfers = []
    for index in range(32):
        src = rng.choice(config.ACCOUNTS)
        dst = banking.choose_destination(rng, src)
        amount = rng.randint(25, 150)
        transfers.append(
            banking.make_transfer(src, dst, amount, f"conservation-normal-{SEED}-{index:04d}")
        )
    return [banking.transfer_phase("normal_settlement_window", transfers, concurrency=4)]


def build() -> list[banking.TransferPhase]:
    rng = random.Random(_seed_int(SEED) ^ 0xC0115EED)
    return _build_stressed_settlement(rng) if STRESSED else _build_normal_settlement(rng)


if __name__ == "__main__":
    reset_database()
    profile = "stressed_settlement_window" if STRESSED else "normal_settlement_window"
    phases = build()
    print(
        "CONSERVATION_SCENARIO "
        f"seed={SEED} profile_score={PROFILE_SCORE:.6f} profile={profile}",
        flush=True,
    )
    sys.exit(
        run_workload(
            "seed_sweep_balance_conservation",
            phases,
            summary={
                "seed": SEED,
                "profile_score": round(PROFILE_SCORE, 6),
                "profile": profile,
                "expected_fault": "09_accounts_settlement_tail",
            },
            settle_s=0 if STRESSED else 2,
        )
    )
