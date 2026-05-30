"""Post-run invariant checks verified by reading SQLite directly.

I1: MoneyConservation — total money in the system is constant.
I2: BalancesNeverNegative — no account balance goes below zero.
I3: TransfersAreAtomic — every debit has a matching credit or rollback.
I4: LedgerMatchesBalances — replaying the log reproduces live balances.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import config


@dataclass(frozen=True)
class CheckResult:
    """Structured result for one post-run assertion."""

    id: str
    name: str
    passed: bool
    summary: str
    details: dict[str, Any]

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def check_i1(balances: dict[str, int], total: int) -> CheckResult:
    expected = config.INITIAL_TOTAL
    delta = total - expected
    passed = delta == 0
    summary = (
        f"System total stayed at {total} cents."
        if passed
        else f"System total changed by {delta} cents: expected {expected}, observed {total}."
    )
    return CheckResult(
        id="I1",
        name="MoneyConservation",
        passed=passed,
        summary=summary,
        details={"actual_total": total, "expected_total": expected, "delta": delta},
    )


def check_i2(balances: dict[str, int]) -> CheckResult:
    negatives = {a: b for a, b in balances.items() if b < 0}
    passed = len(negatives) == 0
    summary = (
        "No account ended with a negative balance."
        if passed
        else f"{len(negatives)} account balance(s) ended negative: {', '.join(sorted(negatives)[:5])}."
    )
    return CheckResult(
        id="I2",
        name="BalancesNeverNegative",
        passed=passed,
        summary=summary,
        details={"negative_accounts": negatives},
    )


def check_i3(tx_log: list[dict]) -> CheckResult:
    by_tx: dict[str, set[str]] = {}
    for entry in tx_log:
        tx_id = entry["tx_id"]
        by_tx.setdefault(tx_id, set()).add(entry["op"])

    orphaned = []
    for tx_id, ops in by_tx.items():
        if "debit" in ops and "credit" not in ops and "rollback" not in ops:
            orphaned.append(tx_id)

    passed = len(orphaned) == 0
    summary = (
        "Every debit was matched by a credit or rollback."
        if passed
        else f"{len(orphaned)} debit(s) were missing a credit or rollback; sample tx ids: {', '.join(orphaned[:5])}."
    )
    return CheckResult(
        id="I3",
        name="TransfersAreAtomic",
        passed=passed,
        summary=summary,
        details={
            "transactions_seen": len(by_tx),
            "orphaned_debits": len(orphaned),
            "sample_tx_ids": orphaned[:5],
        },
    )


def check_i4(balances: dict[str, int], tx_log: list[dict]) -> CheckResult:
    replayed: dict[str, int] = {a: config.INITIAL_BALANCE for a in config.ACCOUNTS}
    for entry in tx_log:
        acct = entry["account_id"]
        amount = entry["amount"]
        op = entry["op"]
        if op == "debit":
            replayed[acct] -= amount
        elif op == "credit":
            replayed[acct] += amount
        elif op == "rollback":
            replayed[acct] += amount

    mismatches = {}
    for acct in config.ACCOUNTS:
        if replayed.get(acct) != balances.get(acct):
            mismatches[acct] = {"replayed": replayed.get(acct), "live": balances.get(acct)}

    passed = len(mismatches) == 0
    summary = (
        "Ledger replay matched every live account balance."
        if passed
        else f"{len(mismatches)} account balance(s) differed from ledger replay: {', '.join(sorted(mismatches)[:5])}."
    )
    return CheckResult(
        id="I4",
        name="LedgerMatchesBalances",
        passed=passed,
        summary=summary,
        details={
            "entries_replayed": len(tx_log),
            "mismatches": mismatches,
        },
    )


def run_all(balances: dict[str, int], total: int, tx_log: list[dict]) -> list[CheckResult]:
    return [
        check_i1(balances, total),
        check_i2(balances),
        check_i3(tx_log),
        check_i4(balances, tx_log),
    ]
