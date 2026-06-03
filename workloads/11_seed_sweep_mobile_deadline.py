#!/usr/bin/env python3
"""Seed-sweep demo: webhook receivers under edge jitter.

This workload is meant to be run at depth 5 with one fixed fault model.
The seed changes the client mix, not the application. Most seeds model
normal payment notifications with a patient deadline. A few seeds model a
degraded webhook receiver window where receivers give up quickly while the
gateway may still finish the operation.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import hashlib
import json
import multiprocessing as mp
import os
import random
import sys
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path("/tmp/banking.db")
sys.path.insert(0, str(ROOT))

# Keep the app behavior permissive enough that this demo is about network
# webhook receiver deadlines, not risk throttles or insufficient balance.
os.environ.setdefault("BANK_LOCAL", "1")
os.environ.setdefault("BANK_VELOCITY_LIMIT", "1000")
os.environ.setdefault("BANK_SINGLE_TX_LIMIT", "9000")
os.environ.setdefault("BANK_FRAUD_TIMEOUT", "1.0")
os.environ.setdefault("BANK_ACCT_TIMEOUT", "8.0")
os.environ.setdefault("BANK_ROLLBACK_RETRIES", "4")
os.environ.setdefault("BANK_ROLLBACK_BASE_S", "0.2")

import config  # noqa: E402
import invariants  # noqa: E402
import main as banking  # noqa: E402
from accounts import run_accounts  # noqa: E402
from fraud import run_fraud  # noqa: E402
from gateway import run_gateway  # noqa: E402


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


def _reset_database() -> None:
    try:
        DB_PATH.unlink()
    except FileNotFoundError:
        pass


def _start_services(start_ns: int) -> tuple[mp.Event, list[tuple[str, mp.Process]], list[dict[str, Any]]]:
    stop = mp.Event()
    runtime_failures: list[dict[str, Any]] = []
    procs = [
        ("ledger", mp.Process(name="ledger", target=run_accounts, args=(stop,))),
        ("risk", mp.Process(name="risk", target=run_fraud, args=(stop,))),
        ("gateway", mp.Process(name="gateway", target=run_gateway, args=(stop,))),
    ]
    banking._section("Service Startup")
    for name, proc in procs:
        proc.start()
        banking._event(start_ns, "services", "started", name=name, pid=proc.pid)
    time.sleep(config.BIND_WAIT_S)
    banking._event(start_ns, "services", "startup wait complete", wait_s=config.BIND_WAIT_S)
    banking._record_startup_health(start_ns, procs, runtime_failures)
    return stop, procs, runtime_failures


def _build_mobile_burst(rng: random.Random, seed: str, count: int) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    hot_sources = config.ACCOUNTS[:3]
    for index in range(count):
        if index % 4 == 0:
            src = hot_sources[0]
        else:
            src = rng.choice(hot_sources)
        dst = banking.choose_destination(rng, src)
        amount = rng.randint(25, 160)
        operations.append(
            banking.make_operation(src, dst, amount, f"seed-sweep-{seed}-{index:04d}")
        )
    return operations


def _fire_with_deadline(
    operations: list[dict[str, Any]],
    *,
    concurrency: int,
    timeout_s: float,
) -> dict[str, Any]:
    started_ns = time.monotonic_ns()
    lock = threading.Lock()
    errors: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    succeeded = 0

    def submit(index: int, operation: dict[str, Any]) -> None:
        nonlocal succeeded
        t0 = time.monotonic_ns()
        resp = banking._rpc_to_gateway(operation, timeout=timeout_s)
        latency_ms = (time.monotonic_ns() - t0) // 1_000_000
        ok = isinstance(resp, dict) and resp.get("ok") is True
        with lock:
            if ok:
                succeeded += 1
                return
            error = "client_deadline_exceeded" if resp is None else str(resp.get("error", "unknown_error"))
            errors[error] += 1
            if len(samples) < banking.SAMPLE_LIMIT:
                samples.append({
                    "index": index,
                    "route": f"{operation['src']}->{operation['dst']}",
                    "amount": operation["amount"],
                    "key": operation["idempotency_key"],
                    "error": error,
                    "latency_ms": latency_ms,
                })

    sem = threading.Semaphore(concurrency)
    threads = []
    for index, operation in enumerate(operations, start=1):
        sem.acquire()

        def run_one(i: int = index, t: dict[str, Any] = operation) -> None:
            try:
                submit(i, t)
            finally:
                sem.release()

        th = threading.Thread(target=run_one, daemon=True)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    return {
        "attempted": len(operations),
        "succeeded": succeeded,
        "failed": len(operations) - succeeded,
        "duration_ms": (time.monotonic_ns() - started_ns) // 1_000_000,
        "error_counts": dict(sorted(errors.items())),
        "sample_failures": samples,
    }


def _deadline_check(
    *,
    profile: str,
    timeout_s: float,
    max_client_errors: int,
    batch: dict[str, Any],
    seed: str,
) -> invariants.CheckResult:
    deadline_errors = int(batch["error_counts"].get("client_deadline_exceeded", 0))
    client_errors = int(batch["failed"])
    passed = deadline_errors <= max_client_errors
    summary = (
        f"Webhook receiver deadline failures stayed within budget ({deadline_errors}/{max_client_errors})."
        if passed
        else f"Webhook receiver deadline failures exceeded budget ({deadline_errors}/{max_client_errors})."
    )
    return invariants.CheckResult(
        id="D1",
        name="WebhookDeadlineBudget",
        passed=passed,
        summary=summary,
        details={
            "seed": seed,
            "profile": profile,
            "timeout_ms": int(timeout_s * 1000),
            "attempted": batch["attempted"],
            "client_successes": batch["succeeded"],
            "client_errors": client_errors,
            "deadline_errors": deadline_errors,
            "max_client_errors": max_client_errors,
            "error_counts": batch["error_counts"],
            "sample_failures": batch["sample_failures"],
        },
    )


def main() -> int:
    seed = _seed_text()
    seed_int = _seed_int(seed)
    rng = random.Random(seed_int)
    profile_score = rng.random()

    impatient = profile_score < float(os.environ.get("BANK_DEMO_IMPATIENT_RATE", "0.20"))
    profile = "degraded_webhook_receiver" if impatient else "normal_webhook_receiver"
    concurrency = int(os.environ.get("BANK_DEMO_CONCURRENCY", "4"))
    operation_count = int(os.environ.get("BANK_DEMO_TRANSFERS", "32"))
    timeout_s = float(os.environ.get("BANK_DEMO_CLIENT_TIMEOUT", "0.09" if impatient else "2.0"))
    max_client_errors = int(os.environ.get("BANK_DEMO_MAX_CLIENT_ERRORS", "0"))
    settle_s = float(os.environ.get("BANK_DEMO_SETTLE_S", "2.0"))

    _reset_database()
    start_ns = time.monotonic_ns()
    run_id = os.environ.get("BANK_RUN_ID", f"seed-sweep-webhook-{seed}")
    operations = _build_mobile_burst(rng, seed, operation_count)

    print("PAYMENT_OPS_VERSION: 1", flush=True)
    banking._section("Payment Operations Run")
    banking._line("RUN", id=run_id, version=1, started_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    banking._print_run_configuration(
        run_id,
        operation_count=len(operations),
        concurrency=concurrency,
        settle_s=int(settle_s),
    )

    banking._section("Seed Sweep Scenario")
    banking._print_kv_table("Scenario", {
        "seed": seed,
        "profile_score": round(profile_score, 6),
        "profile": profile,
        "client_timeout_ms": int(timeout_s * 1000),
        "max_client_errors": max_client_errors,
        "expected_fault": "08_mobile_edge_jitter",
    })

    stop, procs, runtime_failures = _start_services(start_ns)
    try:
        banking._section("Scenario Plan")
        banking._print_kv_table("Generated ledger movements", {
            "scenario": "seed_sweep_mobile_deadline",
            "generated": len(operations),
            "concurrency": concurrency,
            "profile": profile,
            "seed": seed,
        })
        banking._print_operation_samples(operations)

        banking._section("Scenario Execution")
        if runtime_failures:
            batch = {
                "attempted": len(operations),
                "succeeded": 0,
                "failed": len(operations),
                "duration_ms": 0,
                "error_counts": {"startup_failed": len(operations)},
                "sample_failures": [],
            }
        else:
            batch = _fire_with_deadline(operations, concurrency=concurrency, timeout_s=timeout_s)
        banking._event(
            start_ns,
            "operations",
            "batch complete",
            attempted=batch["attempted"],
            succeeded=batch["succeeded"],
            failed=batch["failed"],
            duration_ms=batch["duration_ms"],
        )
        if batch["error_counts"]:
            banking._print_kv_table("Operation errors", batch["error_counts"])
            banking._print_kv_table(
                "Sample failed operations",
                {f"failure_{idx}": failure for idx, failure in enumerate(batch["sample_failures"], start=1)},
            )
        else:
            print("Operation errors: none", flush=True)

        banking._event(start_ns, "settle", "waiting for gateway side effects", seconds=settle_s)
        time.sleep(settle_s)
        stats_resp = None if runtime_failures else banking._rpc_to_gateway({"op": "STATS"})
        stats = stats_resp if isinstance(stats_resp, dict) else {}
        banking._section("Gateway Stats")
        banking._print_kv_table("Gateway counters", dict(sorted(stats.items())) if stats else {})
    finally:
        banking._section("Service Shutdown")
        stop.set()
        banking._record_shutdown(start_ns, procs, runtime_failures)

    banking._section("Database Snapshot")
    snapshot = banking._read_database()
    if snapshot.error:
        print(f"DB_READ_ERROR {snapshot.error}", flush=True)
    else:
        banking._print_kv_table("Balances", snapshot.balances)
        banking._print_kv_table("Transaction log operations", banking._tx_operation_counts(snapshot.tx_log))

    banking._section("Invariant Results")
    checks = invariants.run_all(snapshot.balances, snapshot.total, snapshot.tx_log)
    checks.append(
        _deadline_check(
            profile=profile,
            timeout_s=timeout_s,
            max_client_errors=max_client_errors,
            batch=batch,
            seed=seed,
        )
    )
    if runtime_failures:
        checks.append(
            invariants.CheckResult(
                id="R1",
                name="ServicesStayHealthy",
                passed=False,
                summary=f"Service runtime health failed: {len(runtime_failures)} failure(s) observed.",
                details={"failures": runtime_failures},
            )
        )
    failed = banking._print_assertion_results(checks)
    status = "PASS" if failed == 0 else "FAIL"
    banking._line(
        "SUMMARY",
        status=status,
        attempted=batch["attempted"],
        succeeded=batch["succeeded"],
        failed=batch["failed"],
        invariants_failed=failed,
        runtime_failures=len(runtime_failures),
    )
    print(f"WORKLOAD name=seed_sweep_mobile_deadline status={status} exit_code={1 if failed else 0}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
