from __future__ import annotations

import os
import random
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path("/tmp/banking.db")

sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import main as banking  # noqa: E402


def rng(seed: str) -> random.Random:
    return random.Random(os.environ.get("BANK_SEED", seed))


def reset_database() -> None:
    try:
        DB_PATH.unlink()
    except FileNotFoundError:
        pass


def split_counts(total: int, ratios: dict[str, float]) -> dict[str, int]:
    counts = {name: int(total * ratio) for name, ratio in ratios.items()}
    remainder = total - sum(counts.values())
    for name in ratios:
        if remainder <= 0:
            break
        counts[name] += 1
        remainder -= 1
    return counts


def every_nth_retry(transfers: list[dict[str, Any]], every: int) -> list[dict[str, Any]]:
    if every <= 0:
        return []
    return [
        banking.duplicate_transfer(transfer)
        for index, transfer in enumerate(transfers)
        if index > 0 and index % every == 0
    ]


def shuffled_duplicate_pairs(
    source_rng: random.Random,
    transfers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    paired = []
    for transfer in transfers:
        paired.append(transfer)
        paired.append(banking.duplicate_transfer(transfer))
    source_rng.shuffle(paired)
    return paired


def run_workload(
    name: str,
    phases: list[banking.TransferPhase],
    *,
    summary: dict[str, Any] | None = None,
    settle_s: int | None = None,
) -> int:
    reset_database()
    os.environ.setdefault("BANK_RUN_ID", name)
    os.environ.setdefault("BANK_SETTLE_S", "0")
    attempted = sum(len(phase.transfers) for phase in phases)
    result = banking.run_banking_app(
        phases,
        settle_s=int(os.environ["BANK_SETTLE_S"]) if settle_s is None else settle_s,
        run_id=os.environ["BANK_RUN_ID"],
        plan_summary={
            "scenario": name,
            "attempted": attempted,
            **(summary or {}),
        },
    )
    status = "PASS" if result == 0 else "FAIL"
    print(f"WORKLOAD name={name} status={status} exit_code={result}", flush=True)
    return result
