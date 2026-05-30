"""Post-run invariant checks verified by reading SQLite directly.

I1: MoneyConservation — total money in the system is constant.
I2: BalancesNeverNegative — no account balance goes below zero.
I3: TransfersAreAtomic — every debit has a matching credit or rollback.
I4: LedgerMatchesBalances — replaying the log reproduces live balances.
I6: LedgerTransactionShape — each transaction has one valid terminal shape.
I7: LedgerBalanceAfterConsistent — each ledger row records the actual post-op balance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

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


def _is_rollback_tombstone(entry: dict) -> bool:
    return (
        entry.get("op") == "rollback"
        and entry.get("account_id") == ""
        and entry.get("amount") == 0
    )


def _check_failed(check_id: str, name: str, exc: Exception) -> CheckResult:
    return CheckResult(
        id=check_id,
        name=name,
        passed=False,
        summary=f"Invariant check crashed instead of completing: {type(exc).__name__}: {exc}.",
        details={"exception": repr(exc)},
    )


def _run_check(check_id: str, name: str, fn: Callable[[], CheckResult]) -> CheckResult:
    try:
        return fn()
    except Exception as exc:
        return _check_failed(check_id, name, exc)


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
    malformed_rows: list[dict[str, Any]] = []
    tombstones = 0
    for entry in tx_log:
        acct = entry["account_id"]
        amount = entry["amount"]
        op = entry["op"]
        if _is_rollback_tombstone(entry):
            tombstones += 1
            continue
        if acct not in replayed:
            if len(malformed_rows) < 10:
                malformed_rows.append({
                    "tx_id": entry.get("tx_id"),
                    "op": op,
                    "account_id": acct,
                    "amount": amount,
                    "reason": "unknown_account",
                })
            continue
        if op == "debit":
            replayed[acct] -= amount
        elif op == "credit":
            replayed[acct] += amount
        elif op == "rollback":
            replayed[acct] += amount
        else:
            if len(malformed_rows) < 10:
                malformed_rows.append({
                    "tx_id": entry.get("tx_id"),
                    "op": op,
                    "account_id": acct,
                    "amount": amount,
                    "reason": "unknown_operation",
                })

    mismatches = {}
    for acct in config.ACCOUNTS:
        if replayed.get(acct) != balances.get(acct):
            mismatches[acct] = {"replayed": replayed.get(acct), "live": balances.get(acct)}

    passed = len(mismatches) == 0 and len(malformed_rows) == 0
    if passed:
        summary = "Ledger replay matched every live account balance."
    elif malformed_rows:
        summary = (
            f"Ledger replay found {len(mismatches)} balance mismatch(es) "
            f"and {len(malformed_rows)} malformed row sample(s)."
        )
    else:
        summary = (
            f"{len(mismatches)} account balance(s) differed from ledger replay: "
            f"{', '.join(sorted(mismatches)[:5])}."
        )
    return CheckResult(
        id="I4",
        name="LedgerMatchesBalances",
        passed=passed,
        summary=summary,
        details={
            "entries_replayed": len(tx_log),
            "rollback_tombstones_skipped": tombstones,
            "mismatches": mismatches,
            "malformed_row_samples": malformed_rows,
        },
    )


def check_i6(tx_log: list[dict]) -> CheckResult:
    by_tx: dict[str, dict[str, list[dict]]] = {}
    for entry in tx_log:
        tx_id = entry["tx_id"]
        op = entry["op"]
        by_tx.setdefault(tx_id, {}).setdefault(op, []).append(entry)

    malformed: dict[str, dict[str, Any]] = {}
    completed = 0
    compensated = 0
    tombstones = 0
    for tx_id, ops in by_tx.items():
        op_counts = {op: len(entries) for op, entries in sorted(ops.items())}
        known_ops = {"debit", "credit", "rollback"}
        unknown_ops = sorted(set(ops) - known_ops)
        has_debit = "debit" in ops
        has_credit = "credit" in ops
        has_rollback = "rollback" in ops

        if unknown_ops or any(count != 1 for count in op_counts.values()):
            malformed[tx_id] = {"op_counts": op_counts, "unknown_ops": unknown_ops}
        elif has_debit and has_credit and not has_rollback:
            completed += 1
        elif has_debit and has_rollback and not has_credit:
            compensated += 1
        elif has_rollback and not has_debit and not has_credit:
            rollback = ops["rollback"][0]
            if rollback["account_id"] == "" and rollback["amount"] == 0:
                tombstones += 1
            else:
                malformed[tx_id] = {"op_counts": op_counts, "reason": "rollback_without_debit"}
        else:
            malformed[tx_id] = {"op_counts": op_counts, "reason": "non_terminal_transaction"}

    passed = len(malformed) == 0
    summary = (
        "Every transaction had a valid completed, compensated, or rollback-tombstone shape."
        if passed
        else f"{len(malformed)} transaction(s) had malformed ledger shape: {', '.join(sorted(malformed)[:5])}."
    )
    return CheckResult(
        id="I6",
        name="LedgerTransactionShape",
        passed=passed,
        summary=summary,
        details={
            "transactions_seen": len(by_tx),
            "completed": completed,
            "compensated": compensated,
            "rollback_tombstones": tombstones,
            "malformed": malformed,
        },
    )


def check_i7(tx_log: list[dict]) -> CheckResult:
    replayed: dict[str, int] = {a: config.INITIAL_BALANCE for a in config.ACCOUNTS}
    mismatches: list[dict[str, Any]] = []

    for entry in tx_log:
        acct = entry["account_id"]
        op = entry["op"]
        amount = entry["amount"]
        if _is_rollback_tombstone(entry):
            continue
        if acct not in replayed:
            if len(mismatches) < 10:
                mismatches.append({
                    "tx_id": entry.get("tx_id"),
                    "op": op,
                    "account_id": acct,
                    "reason": "unknown_account",
                })
            continue
        if op == "debit":
            replayed[acct] -= amount
        elif op in ("credit", "rollback"):
            replayed[acct] += amount
        else:
            continue

        expected = replayed[acct]
        observed = entry["balance_after"]
        if expected != observed and len(mismatches) < 10:
            mismatches.append({
                "tx_id": entry["tx_id"],
                "op": op,
                "account_id": acct,
                "expected_balance_after": expected,
                "observed_balance_after": observed,
            })

    passed = len(mismatches) == 0
    summary = (
        "Every ledger row's balance_after matched per-account replay."
        if passed
        else f"{len(mismatches)} sampled ledger row(s) had inconsistent balance_after values."
    )
    return CheckResult(
        id="I7",
        name="LedgerBalanceAfterConsistent",
        passed=passed,
        summary=summary,
        details={
            "entries_checked": len(tx_log),
            "sample_mismatches": mismatches,
        },
    )


def run_all(balances: dict[str, int], total: int, tx_log: list[dict]) -> list[CheckResult]:
    return [
        _run_check("I1", "MoneyConservation", lambda: check_i1(balances, total)),
        _run_check("I2", "BalancesNeverNegative", lambda: check_i2(balances)),
        _run_check("I3", "TransfersAreAtomic", lambda: check_i3(tx_log)),
        _run_check("I4", "LedgerMatchesBalances", lambda: check_i4(balances, tx_log)),
        _run_check("I6", "LedgerTransactionShape", lambda: check_i6(tx_log)),
        _run_check("I7", "LedgerBalanceAfterConsistent", lambda: check_i7(tx_log)),
    ]
