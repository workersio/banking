#!/usr/bin/env python3
"""Starts all services, runs transfers, and checks invariants.

Spawns gateway, accounts, and fraud as separate processes, generates a
batch of transfers, sends them with configurable concurrency, then reads
the SQLite database directly to verify invariants.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

import json
import multiprocessing as mp
import os
import random
import socket
import sqlite3
import sys
import threading
import time

import config
import invariants
from protocol import recv_msg, send_msg


SAMPLE_LIMIT = 5


@dataclass
class TransferBatchResult:
    attempted: int
    succeeded: int
    failed: int
    duration_ms: int
    error_counts: Counter[str]
    sample_failures: list[dict[str, Any]]
    success_tx_ids: set[str]
    success_tx_ids_by_key: dict[str, set[str]]
    errors_by_key: dict[str, set[str]]


@dataclass(frozen=True)
class TransferPhase:
    name: str
    transfers: list[dict[str, Any]]
    concurrency: int


@dataclass
class DatabaseSnapshot:
    balances: dict[str, int]
    total: int
    tx_log: list[dict[str, Any]]
    error: str | None


def _elapsed_ms(start_ns: int) -> int:
    return (time.monotonic_ns() - start_ns) // 1_000_000


def _format_kv_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    text = str(value)
    if text == "" or any(ch.isspace() for ch in text):
        return json.dumps(text)
    return text


def _format_kv(fields: dict[str, Any]) -> str:
    return " ".join(f"{key}={_format_kv_value(value)}" for key, value in fields.items())


def _line(prefix: str, **fields: Any) -> None:
    suffix = f" {_format_kv(fields)}" if fields else ""
    print(f"{prefix}{suffix}", flush=True)


def _section(title: str) -> None:
    print("", flush=True)
    print("=" * 72, flush=True)
    print(title, flush=True)
    print("=" * 72, flush=True)


def _event(start_ns: int, phase: str, message: str, **fields: Any) -> None:
    suffix = f" {_format_kv(fields)}" if fields else ""
    print(f"[{_elapsed_ms(start_ns):>7} ms] {phase:<12} {message}{suffix}", flush=True)


def _print_kv_table(title: str, rows: dict[str, Any]) -> None:
    print(f"{title}:", flush=True)
    width = max((len(key) for key in rows), default=0)
    for key, value in rows.items():
        if isinstance(value, (dict, list, tuple)):
            rendered = json.dumps(value, sort_keys=True)
        else:
            rendered = str(value)
        print(f"  {key:<{width}} : {rendered}", flush=True)


def _format_addr(addr: tuple[str, int]) -> str:
    return f"{addr[0]}:{addr[1]}"


def _seed_value() -> str | int:
    seed = os.environ.get("BANK_SEED")
    return seed if seed is not None else 42


def _run_id() -> str:
    return os.environ.get("BANK_RUN_ID") or f"banking-{int(time.time() * 1000)}-{os.getpid()}"


def _gateway_timeout() -> float:
    rollback_budget = sum(
        config.ROLLBACK_BASE_S * (2 ** attempt)
        for attempt in range(max(0, config.ROLLBACK_MAX_RETRIES - 1))
    )
    return config.FRAUD_TIMEOUT + (config.ACCT_TIMEOUT * 2) + rollback_budget + 1.0


def _rpc_to_gateway(msg: dict[str, Any], timeout: float | None = None) -> dict[str, Any] | None:
    s = socket.socket()
    s.settimeout(_gateway_timeout() if timeout is None else timeout)
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


def make_transfer(src: str, dst: str, amount: int, idempotency_key: str) -> dict[str, Any]:
    """Build a gateway TRANSFER request."""
    return {
        "op": "TRANSFER",
        "src": src,
        "dst": dst,
        "amount": amount,
        "idempotency_key": idempotency_key,
    }


def choose_destination(rng: random.Random, src: str) -> str:
    return rng.choice([account for account in config.ACCOUNTS if account != src])


def make_random_transfer(
    rng: random.Random,
    index: int,
    *,
    key_prefix: str,
    source: str | None = None,
    destination: str | None = None,
    amount_min: int = 100,
    amount_max: int = 2000,
) -> dict[str, Any]:
    src = source or rng.choice(config.ACCOUNTS)
    dst = destination or choose_destination(rng, src)
    if dst == src:
        dst = choose_destination(rng, src)
    amount = rng.randint(amount_min, amount_max)
    return make_transfer(src, dst, amount, f"{key_prefix}-{index:04d}")


def make_random_transfers(
    rng: random.Random,
    count: int,
    *,
    key_prefix: str,
    start_index: int = 0,
    amount_min: int = 100,
    amount_max: int = 2000,
) -> list[dict[str, Any]]:
    return [
        make_random_transfer(
            rng,
            start_index + i,
            key_prefix=key_prefix,
            amount_min=amount_min,
            amount_max=amount_max,
        )
        for i in range(count)
    ]


def duplicate_transfer(transfer: dict[str, Any]) -> dict[str, Any]:
    """Return a client retry that preserves the idempotency key."""
    return dict(transfer)


def transfer_phase(name: str, transfers: list[dict[str, Any]], concurrency: int) -> TransferPhase:
    """Build a client submission phase with its own concurrency level."""
    return TransferPhase(name=name, transfers=transfers, concurrency=concurrency)


def _summarize_transfer_plan(
    phases: list[TransferPhase],
    plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transfers = [transfer for phase in phases for transfer in phase.transfers]
    routes = Counter(f"{t['src']}->{t['dst']}" for t in transfers)
    sources = Counter(t["src"] for t in transfers)
    total_amount = sum(t["amount"] for t in transfers)
    summary = {
        "generated": len(transfers),
        "total_amount_cents": total_amount,
        "unique_routes": len(routes),
        "top_routes": dict(routes.most_common(SAMPLE_LIMIT)),
        "source_counts": dict(sorted(sources.items())),
        "phases": {phase.name: len(phase.transfers) for phase in phases},
        "phase_concurrency": {phase.name: phase.concurrency for phase in phases},
    }
    if plan:
        summary.update(plan)
    return summary


def _print_transfer_samples(transfers: list[dict[str, Any]]) -> None:
    print("Transfer samples:", flush=True)
    for idx, transfer in enumerate(transfers[:SAMPLE_LIMIT], start=1):
        print(
            f"  #{idx:03d} {transfer['src']}->{transfer['dst']} "
            f"amount={transfer['amount']} key={transfer['idempotency_key']}",
            flush=True,
        )
    remaining = len(transfers) - SAMPLE_LIMIT
    if remaining > 0:
        print(f"  ... {remaining} more transfers omitted", flush=True)


def _fire_transfers(transfers: list[dict[str, Any]], concurrency: int) -> TransferBatchResult:
    started_ns = time.monotonic_ns()
    succeeded = 0
    failed = 0
    error_counts: Counter[str] = Counter()
    sample_failures: list[dict[str, Any]] = []
    success_tx_ids: set[str] = set()
    success_tx_ids_by_key: dict[str, set[str]] = {}
    errors_by_key: dict[str, set[str]] = {}
    results_lock = threading.Lock()

    def fire_one(index: int, transfer: dict[str, Any]) -> None:
        nonlocal succeeded, failed
        t0 = time.monotonic_ns()
        resp = _rpc_to_gateway(transfer)
        latency_ms = (time.monotonic_ns() - t0) // 1_000_000
        ok = resp is not None and resp.get("ok") is True

        with results_lock:
            if ok:
                succeeded += 1
                tx_id = resp.get("tx_id")
                if tx_id:
                    tx_id_text = str(tx_id)
                    success_tx_ids.add(tx_id_text)
                    idem_key = str(transfer.get("idempotency_key", ""))
                    success_tx_ids_by_key.setdefault(idem_key, set()).add(tx_id_text)
                return

            failed += 1
            error = "gateway_unreachable" if resp is None else str(resp.get("error", "unknown_error"))
            idem_key = str(transfer.get("idempotency_key", ""))
            error_counts[error] += 1
            errors_by_key.setdefault(idem_key, set()).add(error)
            if len(sample_failures) < SAMPLE_LIMIT:
                sample_failures.append({
                    "index": index,
                    "route": f"{transfer['src']}->{transfer['dst']}",
                    "amount": transfer["amount"],
                    "key": transfer["idempotency_key"],
                    "error": error,
                    "latency_ms": latency_ms,
                })

    if concurrency <= 1:
        for idx, transfer in enumerate(transfers, start=1):
            fire_one(idx, transfer)
    else:
        sem = threading.Semaphore(concurrency)
        threads = []
        for idx, transfer in enumerate(transfers, start=1):
            sem.acquire()

            def run_transfer(i: int = idx, t: dict[str, Any] = transfer) -> None:
                try:
                    fire_one(i, t)
                finally:
                    sem.release()

            th = threading.Thread(target=run_transfer, daemon=True)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    return TransferBatchResult(
        attempted=len(transfers),
        succeeded=succeeded,
        failed=failed,
        duration_ms=duration_ms,
        error_counts=error_counts,
        sample_failures=sample_failures,
        success_tx_ids=success_tx_ids,
        success_tx_ids_by_key=success_tx_ids_by_key,
        errors_by_key=errors_by_key,
    )


def _failed_batch(transfers: list[dict[str, Any]], error: str) -> TransferBatchResult:
    return TransferBatchResult(
        attempted=len(transfers),
        succeeded=0,
        failed=len(transfers),
        duration_ms=0,
        error_counts=Counter({error: len(transfers)}),
        sample_failures=[],
        success_tx_ids=set(),
        success_tx_ids_by_key={},
        errors_by_key={},
    )


def _combine_batch_results(results: list[TransferBatchResult]) -> TransferBatchResult:
    combined_errors: Counter[str] = Counter()
    sample_failures: list[dict[str, Any]] = []
    success_tx_ids: set[str] = set()
    success_tx_ids_by_key: dict[str, set[str]] = {}
    errors_by_key: dict[str, set[str]] = {}

    for result in results:
        combined_errors.update(result.error_counts)
        sample_failures.extend(result.sample_failures[:max(0, SAMPLE_LIMIT - len(sample_failures))])
        success_tx_ids.update(result.success_tx_ids)
        for key, tx_ids in result.success_tx_ids_by_key.items():
            success_tx_ids_by_key.setdefault(key, set()).update(tx_ids)
        for key, errors in result.errors_by_key.items():
            errors_by_key.setdefault(key, set()).update(errors)

    return TransferBatchResult(
        attempted=sum(result.attempted for result in results),
        succeeded=sum(result.succeeded for result in results),
        failed=sum(result.failed for result in results),
        duration_ms=sum(result.duration_ms for result in results),
        error_counts=combined_errors,
        sample_failures=sample_failures[:SAMPLE_LIMIT],
        success_tx_ids=success_tx_ids,
        success_tx_ids_by_key=success_tx_ids_by_key,
        errors_by_key=errors_by_key,
    )


def _check_idempotency(
    transfers: list[dict[str, Any]],
    batch: TransferBatchResult,
) -> invariants.CheckResult:
    key_counts = Counter(str(t.get("idempotency_key", "")) for t in transfers)
    duplicate_keys = sorted(key for key, count in key_counts.items() if count > 1)
    violations = {}
    for key in duplicate_keys:
        tx_ids = sorted(batch.success_tx_ids_by_key.get(key, set()))
        errors = sorted(batch.errors_by_key.get(key, set()))
        if len(tx_ids) > 1 or (tx_ids and errors):
            violations[key] = {"tx_ids": tx_ids, "errors": errors}
    passed = len(violations) == 0
    summary = (
        "Duplicate idempotency keys resolved to one consistent outcome."
        if passed
        else f"{len(violations)} idempotency key(s) produced inconsistent outcomes: {', '.join(sorted(violations)[:5])}."
    )
    return invariants.CheckResult(
        id="I5",
        name="IdempotencyKeysAreConsistent",
        passed=passed,
        summary=summary,
        details={
            "duplicate_keys": len(duplicate_keys),
            "violating_keys": violations,
        },
    )


def _read_database() -> DatabaseSnapshot:
    conn: sqlite3.Connection | None = None
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
        return DatabaseSnapshot(balances=balances, total=total, tx_log=tx_log, error=None)
    except Exception as exc:
        return DatabaseSnapshot(balances={}, total=0, tx_log=[], error=repr(exc))
    finally:
        if conn is not None:
            conn.close()


def _tx_operation_counts(tx_log: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(entry.get("op", "unknown")) for entry in tx_log)
    return dict(sorted(counts.items()))


def _print_run_configuration(
    run_id: str,
    *,
    transfer_count: int,
    concurrency: int,
    settle_s: int,
) -> None:
    _print_kv_table("Run configuration", {
        "run_id": run_id,
        "pid": os.getpid(),
        "db_path": config.DB_PATH,
        "db_exists_at_start": os.path.exists(config.DB_PATH),
        "accounts": ",".join(config.ACCOUNTS),
        "initial_balance": config.INITIAL_BALANCE,
        "initial_total": config.INITIAL_TOTAL,
        "transfers": transfer_count,
        "concurrency": concurrency,
        "settle_s": settle_s,
        "fraud_timeout_s": config.FRAUD_TIMEOUT,
        "account_timeout_s": config.ACCT_TIMEOUT,
        "velocity_window_s": config.VELOCITY_WINDOW,
        "velocity_limit": config.VELOCITY_LIMIT,
        "single_tx_limit": config.SINGLE_TX_LIMIT,
        "rollback_retries": config.ROLLBACK_MAX_RETRIES,
        "rollback_base_s": config.ROLLBACK_BASE_S,
    })
    _print_kv_table("Service addresses", {
        "gateway": _format_addr(config.GATEWAY_ADDR),
        "fraud": _format_addr(config.FRAUD_ADDR),
        "accounts": _format_addr(config.ACCOUNTS_ADDR),
    })


def _print_assertion_results(checks: list[invariants.CheckResult]) -> int:
    failed = 0
    for check in checks:
        if not check.passed:
            failed += 1
        marker = "OK" if check.passed else "FAIL"
        print(f"[{marker:<4}] {check.id} {check.name}: {check.summary}", flush=True)
        if check.details:
            print(f"       details={json.dumps(check.details, sort_keys=True)}", flush=True)
        _line(
            "CHECK",
            id=check.id,
            name=check.name,
            status=check.status,
            summary=check.summary,
            details=check.details,
        )
        print(f"INVARIANT {check.id} {check.name} {check.status} {check.summary}", flush=True)
        if not check.passed:
            print(f"INVARIANT_VIOLATED:{check.id}", flush=True)
    return failed


def _record_startup_health(
    start_ns: int,
    procs: list[tuple[str, mp.Process]],
    runtime_failures: list[dict[str, Any]],
) -> None:
    for name, proc in procs:
        if proc.is_alive():
            _event(start_ns, "services", "health ok", name=name, pid=proc.pid, status="running")
            continue
        failure = {
            "stage": "startup",
            "service": name,
            "pid": proc.pid,
            "exitcode": proc.exitcode,
        }
        runtime_failures.append(failure)
        _event(
            start_ns,
            "services",
            "health failed",
            name=name,
            pid=proc.pid,
            exitcode=proc.exitcode,
        )


def _record_shutdown(
    start_ns: int,
    procs: list[tuple[str, mp.Process]],
    runtime_failures: list[dict[str, Any]],
) -> None:
    for name, proc in procs:
        proc.join(timeout=3.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)
            runtime_failures.append({
                "stage": "shutdown",
                "service": name,
                "pid": proc.pid,
                "exitcode": proc.exitcode,
                "reason": "terminated_after_timeout",
            })
            _event(start_ns, "services", "terminated", name=name, pid=proc.pid)
            continue
        if proc.exitcode not in (0, None):
            runtime_failures.append({
                "stage": "shutdown",
                "service": name,
                "pid": proc.pid,
                "exitcode": proc.exitcode,
            })
        _event(start_ns, "services", "stopped", name=name, pid=proc.pid, exitcode=proc.exitcode)


def run_banking_app(
    phases: list[TransferPhase],
    *,
    settle_s: int,
    run_id: str | None = None,
    plan_summary: dict[str, Any] | None = None,
) -> int:
    start_ns = time.monotonic_ns()
    run_id = run_id or _run_id()
    transfers = [transfer for phase in phases for transfer in phase.transfers]
    max_concurrency = max((phase.concurrency for phase in phases), default=0)
    runtime_failures: list[dict[str, Any]] = []
    print("BANKING_VERSION: 1", flush=True)

    _section("Banking Run")
    _line("RUN", id=run_id, version=1, started_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    _print_run_configuration(
        run_id,
        transfer_count=len(transfers),
        concurrency=max_concurrency,
        settle_s=settle_s,
    )

    from accounts import run_accounts
    from fraud import run_fraud
    from gateway import run_gateway

    _section("Service Startup")
    stop = mp.Event()
    procs = [
        ("accounts", mp.Process(name="accounts", target=run_accounts, args=(stop,))),
        ("fraud", mp.Process(name="fraud", target=run_fraud, args=(stop,))),
        ("gateway", mp.Process(name="gateway", target=run_gateway, args=(stop,))),
    ]
    for name, proc in procs:
        proc.start()
        _event(start_ns, "services", "started", name=name, pid=proc.pid)
    time.sleep(config.BIND_WAIT_S)
    _event(start_ns, "services", "startup wait complete", wait_s=config.BIND_WAIT_S)
    _record_startup_health(start_ns, procs, runtime_failures)
    startup_failed = len(runtime_failures) > 0

    _section("Transfer Plan")
    _print_kv_table("Generated transfers", _summarize_transfer_plan(phases, plan_summary))
    _print_transfer_samples(transfers)

    _section("Transfer Execution")
    if startup_failed:
        _event(start_ns, "transfers", "skipped because service startup failed")
        batch = _failed_batch(transfers, "startup_failed")
    else:
        phase_results = []
        for phase in phases:
            _event(
                start_ns,
                "transfers",
                "submitting phase",
                name=phase.name,
                attempted=len(phase.transfers),
                concurrency=phase.concurrency,
            )
            phase_batch = _fire_transfers(phase.transfers, phase.concurrency)
            phase_results.append(phase_batch)
            _event(
                start_ns,
                "transfers",
                "phase complete",
                name=phase.name,
                attempted=phase_batch.attempted,
                succeeded=phase_batch.succeeded,
                failed=phase_batch.failed,
                duration_ms=phase_batch.duration_ms,
            )
        batch = _combine_batch_results(phase_results)
    _event(
        start_ns,
        "transfers",
        "batch complete",
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
        duration_ms=batch.duration_ms,
    )
    if batch.error_counts:
        _print_kv_table("Transfer errors", dict(sorted(batch.error_counts.items())))
        _print_kv_table("Sample failed transfers", {
            f"failure_{idx}": failure
            for idx, failure in enumerate(batch.sample_failures, start=1)
        })
    else:
        print("Transfer errors: none", flush=True)

    if settle_s > 0:
        _event(start_ns, "settle", "waiting for services to finish async work", seconds=settle_s)
        time.sleep(settle_s)
    else:
        _event(start_ns, "settle", "skipped", seconds=0)

    _section("Gateway Stats")
    stats_resp = None if startup_failed else _rpc_to_gateway({"op": "STATS"})
    stats = stats_resp if isinstance(stats_resp, dict) else {}
    if stats:
        _print_kv_table("Gateway counters", dict(sorted(stats.items())))
        _event(start_ns, "gateway", "stats collected")
    else:
        _event(start_ns, "gateway", "stats unavailable", status="missing")

    _section("Service Shutdown")
    stop.set()
    _record_shutdown(start_ns, procs, runtime_failures)

    _section("Database Snapshot")
    snapshot = _read_database()
    if snapshot.error:
        _event(start_ns, "database", "read failed", error=snapshot.error)
    else:
        _event(
            start_ns,
            "database",
            "read complete",
            accounts=len(snapshot.balances),
            transactions=len(snapshot.tx_log),
        )
    _print_kv_table("Balances", {
        **{acct: snapshot.balances.get(acct, "?") for acct in config.ACCOUNTS},
        "total": snapshot.total,
    })
    _print_kv_table("Transaction log", {
        "entries": len(snapshot.tx_log),
        "operations": _tx_operation_counts(snapshot.tx_log),
        "unique_success_tx_ids": len(batch.success_tx_ids),
    })

    _section("Assertions")
    checks = [
        *invariants.run_all(snapshot.balances, snapshot.total, snapshot.tx_log),
        _check_idempotency(transfers, batch),
    ]
    failed_assertions = _print_assertion_results(checks)

    _section("Runtime Checks")
    service_status = "PASS" if not runtime_failures else "FAIL"
    print(f"[{service_status:<4}] services: {len(runtime_failures)} runtime failures", flush=True)
    if runtime_failures:
        print(f"       details={json.dumps(runtime_failures, sort_keys=True)}", flush=True)
    _line("RUNTIME_CHECK", name="services", status=service_status, details=runtime_failures)

    stats_status = "PASS" if stats else "FAIL"
    stats_summary = "gateway stats collected" if stats else "gateway stats unavailable"
    print(f"[{stats_status:<4}] gateway_stats: {stats_summary}", flush=True)
    _line("RUNTIME_CHECK", name="gateway_stats", status=stats_status, details=stats or {})

    elapsed_ms = _elapsed_ms(start_ns)
    result = "FAIL" if snapshot.error or failed_assertions or runtime_failures or not stats else "PASS"

    _section("Machine Summary")
    _line(
        "SUMMARY",
        run_id=run_id,
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
        transfer_duration_ms=batch.duration_ms,
        db_read="fail" if snapshot.error else "ok",
        assertions_failed=failed_assertions,
        runtime_failures=len(runtime_failures),
        gateway_stats="ok" if stats else "missing",
    )
    if batch.error_counts:
        _line("TRANSFER_ERRORS", **dict(sorted(batch.error_counts.items())))
    else:
        _line("TRANSFER_ERRORS", none=True)
    if stats:
        _line("GATEWAY_STATS", **dict(sorted(stats.items())))
    else:
        _line("GATEWAY_STATS", unavailable=True)

    print(f"transfers={batch.attempted} succeeded={batch.succeeded} failed={batch.failed}", flush=True)
    print(f"fraud_denied={stats.get('fraud_denied', 0)} "
          f"fraud_timeout={stats.get('fraud_timeout', 0)}", flush=True)
    print(f"debit_timeout={stats.get('debit_timeout', 0)}", flush=True)
    print(f"rollback_ok={stats.get('rollback_ok', 0)} "
          f"rollback_failed={stats.get('rollback_failed', 0)} "
          f"rollback_retries={stats.get('rollback_retries', 0)}", flush=True)

    bal_str = " ".join(f"{acct}={snapshot.balances.get(acct, '?')}" for acct in config.ACCOUNTS)
    print(f"BALANCES {bal_str} total={snapshot.total}", flush=True)
    print(f"elapsed_ms={elapsed_ms}", flush=True)
    print(f"RESULT: {result}", flush=True)
    _line("FINAL", run_id=run_id, status=result, elapsed_ms=elapsed_ms)

    return 1 if result == "FAIL" else 0


def main() -> int:
    rng = random.Random(_seed_value())
    transfers = make_random_transfers(rng, config.TRANSFERS, key_prefix="key")
    return run_banking_app(
        [transfer_phase("cli_smoke", transfers, config.CONCURRENCY)],
        settle_s=config.SETTLE_S,
        plan_summary={"scenario": "cli_smoke"},
    )


if __name__ == "__main__":
    sys.exit(main())
