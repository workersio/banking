"""Post-run invariant checks — verified by reading SQLite directly.

I1: Conservation — total money in the system is constant.
I2: Non-negative — no account balance goes below zero.
I3: Atomicity — every debit has a matching credit or rollback.
I4: Tx log integrity — replaying the log reproduces live balances.
"""

from __future__ import annotations

import config


def check_i1(balances: dict[str, int], total: int) -> tuple[bool, str]:
    expected = config.INITIAL_TOTAL
    ok = total == expected
    detail = f"total={total} expected={expected}"
    return ok, detail


def check_i2(balances: dict[str, int]) -> tuple[bool, str]:
    negatives = {a: b for a, b in balances.items() if b < 0}
    ok = len(negatives) == 0
    detail = f"negatives={negatives}" if negatives else ""
    return ok, detail


def check_i3(tx_log: list[dict]) -> tuple[bool, str]:
    by_tx: dict[str, set[str]] = {}
    for entry in tx_log:
        tx_id = entry["tx_id"]
        by_tx.setdefault(tx_id, set()).add(entry["op"])

    orphaned = []
    for tx_id, ops in by_tx.items():
        if "debit" in ops and "credit" not in ops and "rollback" not in ops:
            orphaned.append(tx_id)

    ok = len(orphaned) == 0
    detail = f"orphaned_debits={len(orphaned)}"
    if orphaned:
        detail += f" tx_ids={orphaned[:5]}"
    return ok, detail


def check_i4(balances: dict[str, int], tx_log: list[dict]) -> tuple[bool, str]:
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

    ok = len(mismatches) == 0
    detail = f"mismatches={mismatches}" if mismatches else ""
    return ok, detail
