#!/usr/bin/env python3
"""Driver — starts all services, fires transfers, checks invariants.

Spawns gateway, accounts, and fraud as separate processes, generates a
batch of transfers, fires them with configurable concurrency, then reads
the SQLite database directly (bypassing TCP) to verify invariants.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import sys
import threading
import time

import config
import invariants
from protocol import recv_msg, send_msg


def _rpc_to_gateway(msg: dict, timeout: float = 30.0) -> dict | None:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect(config.GATEWAY_ADDR)
        send_msg(s, msg)
        return recv_msg(s)
    except OSError:
        return None
    finally:
        try:
            s.close()
        except OSError:
            pass


def _generate_transfers(n: int, paired_keys: bool = False) -> list[dict]:
    import random
    seed_hex = os.environ.get("BANK_SEED", "")
    rng = random.Random(seed_hex if seed_hex else 42)
    transfers = []
    for i in range(n):
        src = rng.choice(config.ACCOUNTS)
        dst = rng.choice([a for a in config.ACCOUNTS if a != src])
        amount = rng.randint(100, 2000)
        if paired_keys:
            key_idx = i // 2
            idem_key = f"key-{key_idx:04d}"
        else:
            idem_key = f"key-{i:04d}"
        transfers.append({
            "op": "TRANSFER",
            "src": src,
            "dst": dst,
            "amount": amount,
            "idempotency_key": idem_key,
        })
    return transfers


def _fire_transfers(transfers: list[dict], concurrency: int) -> tuple[int, int]:
    succeeded = 0
    failed = 0
    results_lock = threading.Lock()

    def fire_one(t: dict) -> None:
        nonlocal succeeded, failed
        resp = _rpc_to_gateway(t)
        with results_lock:
            if resp is not None and resp.get("ok"):
                succeeded += 1
            else:
                failed += 1

    if concurrency <= 1:
        for t in transfers:
            fire_one(t)
    else:
        sem = threading.Semaphore(concurrency)
        threads = []
        for t in transfers:
            sem.acquire()
            def worker(transfer=t):
                try:
                    fire_one(transfer)
                finally:
                    sem.release()
            th = threading.Thread(target=worker, daemon=True)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

    return succeeded, failed


def main() -> int:
    t0 = time.monotonic_ns()
    print("BANKING_VERSION: 1", flush=True)

    from accounts import run_accounts
    from fraud import run_fraud
    from gateway import run_gateway

    stop = mp.Event()
    procs = [
        mp.Process(target=run_accounts, args=(stop,)),
        mp.Process(target=run_fraud, args=(stop,)),
        mp.Process(target=run_gateway, args=(stop,)),
    ]
    for p in procs:
        p.start()
    time.sleep(config.BIND_WAIT_S)

    paired_keys = os.environ.get("BANK_PAIRED_KEYS", "0") == "1"
    transfers = _generate_transfers(config.TRANSFERS, paired_keys=paired_keys)
    succeeded, failed = _fire_transfers(transfers, config.CONCURRENCY)

    time.sleep(config.SETTLE_S)

    stats_resp = _rpc_to_gateway({"op": "STATS"})

    stop.set()
    for p in procs:
        p.join(timeout=3.0)
        if p.is_alive():
            p.terminate()

    import sqlite3
    balances = {}
    total = 0
    tx_log = []
    try:
        conn = sqlite3.connect(config.DB_PATH)
        rows = conn.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall()
        balances = {r[0]: r[1] for r in rows}
        total = sum(balances.values())
        tx_rows = conn.execute(
            "SELECT tx_id, op, account_id, amount, balance_after, created_at "
            "FROM transactions ORDER BY created_at"
        ).fetchall()
        tx_log = [
            {"tx_id": r[0], "op": r[1], "account_id": r[2],
             "amount": r[3], "balance_after": r[4], "created_at": r[5]}
            for r in tx_rows
        ]
        conn.close()
    except Exception:
        pass

    print(f"transfers={config.TRANSFERS} succeeded={succeeded} failed={failed}", flush=True)

    if stats_resp:
        print(f"fraud_denied={stats_resp.get('fraud_denied', 0)} "
              f"fraud_timeout={stats_resp.get('fraud_timeout', 0)}", flush=True)
        print(f"debit_timeout={stats_resp.get('debit_timeout', 0)}", flush=True)
        print(f"rollback_ok={stats_resp.get('rollback_ok', 0)} "
              f"rollback_failed={stats_resp.get('rollback_failed', 0)} "
              f"rollback_retries={stats_resp.get('rollback_retries', 0)}", flush=True)

    bal_str = " ".join(f"{a}={balances.get(a, '?')}" for a in config.ACCOUNTS)
    print(f"BALANCES {bal_str} total={total}", flush=True)

    checks = [
        ("I1", "conservation",    invariants.check_i1(balances, total)),
        ("I2", "non_negative",    invariants.check_i2(balances)),
        ("I3", "atomicity",       invariants.check_i3(tx_log)),
        ("I4", "tx_log_integrity", invariants.check_i4(balances, tx_log)),
    ]
    violated = False
    for inv_id, name, (ok, detail) in checks:
        status = "PASS" if ok else "FAIL"
        print(f"INVARIANT {inv_id} {name} {status} {detail}", flush=True)
        if not ok:
            print(f"INVARIANT_VIOLATED:{inv_id}", flush=True)
            violated = True

    virt_ms = (time.monotonic_ns() - t0) // 1_000_000
    print(f"virt_total_ms={virt_ms}", flush=True)
    print(f"WORKLOAD: {'FAIL' if violated else 'PASS'}", flush=True)

    return 1 if violated else 0


if __name__ == "__main__":
    sys.exit(main())
