#!/usr/bin/env python3
"""
Run the banking microservices workload under fault profiles W1–W10.

Each workload boots the 3-service banking app inside a wenv VM, fires
transfers, and checks invariants. The test parses the structured output
from main.py and asserts the expected invariant outcomes.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Failure, cli_main, open_vm, with_netem  # noqa: E402

COMMAND = "python3 /examples/banking/main.py"
FIXTURES = Path(__file__).parent / "fault_fixtures"

_SUMMARY_RE = re.compile(
    r"transfers=(?P<transfers>\d+) succeeded=(?P<succeeded>\d+) failed=(?P<failed>\d+)"
)
_BALANCE_RE = re.compile(
    r"BALANCES (?P<pairs>.+?) total=(?P<total>\d+)"
)
_INVARIANT_RE = re.compile(
    r"INVARIANT (?P<id>I\d) (?P<name>\S+) (?P<status>PASS|FAIL)\s*(?P<detail>.*)"
)
_WORKLOAD_RE = re.compile(r"WORKLOAD: (?P<verdict>PASS|FAIL)")
_STAT_FRAUD_RE = re.compile(
    r"fraud_denied=(?P<fraud_denied>\d+)\s+fraud_timeout=(?P<fraud_timeout>\d+)"
)


def parse_output(stdout: str) -> dict:
    region = stdout
    if "MARK PRE_EXEC" in stdout:
        region = stdout.split("MARK PRE_EXEC", 1)[1]
    if "TESTVMD-EXIT" in region:
        region = region.split("TESTVMD-EXIT", 1)[0]

    result: dict = {"invariants": {}, "stats": {}}

    sm = _SUMMARY_RE.search(region)
    if sm:
        result["transfers"] = int(sm["transfers"])
        result["succeeded"] = int(sm["succeeded"])
        result["failed"] = int(sm["failed"])

    bm = _BALANCE_RE.search(region)
    if bm:
        result["total"] = int(bm["total"])
        pairs = bm["pairs"]
        balances = {}
        for token in pairs.split():
            if "=" in token:
                k, v = token.split("=", 1)
                balances[k] = int(v)
        result["balances"] = balances

    for m in _INVARIANT_RE.finditer(region):
        result["invariants"][m["id"]] = m["status"]

    fm = _STAT_FRAUD_RE.search(region)
    if fm:
        result["stats"]["fraud_denied"] = int(fm["fraud_denied"])
        result["stats"]["fraud_timeout"] = int(fm["fraud_timeout"])

    wm = _WORKLOAD_RE.search(region)
    if wm:
        result["verdict"] = wm["verdict"]

    return result


def assert_invariants(label: str, parsed: dict, *invariant_ids: str) -> None:
    for iid in invariant_ids:
        status = parsed["invariants"].get(iid)
        if status is None:
            raise Failure(f"{label}: invariant {iid} not found in output")
        if status != "PASS":
            raise Failure(f"{label}: invariant {iid} FAIL")


def _load_fixture(name: str) -> Path:
    p = FIXTURES / name
    if not p.exists():
        raise Failure(f"fixture not found: {p}")
    return p


# --- W1: happy_path ---

def test_w1_happy_path(vm) -> None:
    run = vm.create(
        f"BANK_TRANSFERS=10 BANK_CONCURRENCY=1 {COMMAND}",
        timeout="3m",
    )
    parsed = parse_output(run.stdout)
    assert_invariants("W1", parsed, "I1", "I2", "I3", "I4")
    print(f"  W1 happy_path: succeeded={parsed.get('succeeded')} "
          f"total={parsed.get('total')} verdict={parsed.get('verdict')}")
    vm.assert_replay_identical(run)


# --- W2: concurrent_transfers ---

def test_w2_concurrent_transfers(vm) -> None:
    run = vm.create(
        f"BANK_TRANSFERS=50 BANK_CONCURRENCY=4 {COMMAND}",
        timeout="5m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W2 concurrent: I1={inv.get('I1')} I2={inv.get('I2')} "
          f"I3={inv.get('I3')} I4={inv.get('I4')} "
          f"succeeded={parsed.get('succeeded')} failed={parsed.get('failed')}")
    vm.assert_replay_identical(run)


# --- W3: asymmetric_partition ---

def test_w3_asymmetric_partition(vm) -> None:
    netem = _load_fixture("w3_asymmetric_partition.json")
    run = vm.create(
        f"BANK_TRANSFERS=15 BANK_CONCURRENCY=1 {COMMAND}",
        netem=netem, timeout="5m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W3 asymmetric: I1={inv.get('I1')} I3={inv.get('I3')} "
          f"succeeded={parsed.get('succeeded')} failed={parsed.get('failed')}")
    vm.assert_replay_identical(run)


# --- W4: full_partition ---

def test_w4_full_partition(vm) -> None:
    netem = _load_fixture("w4_full_partition.json")
    run = vm.create(
        f"BANK_TRANSFERS=10 BANK_CONCURRENCY=1 {COMMAND}",
        netem=netem, timeout="5m",
    )
    parsed = parse_output(run.stdout)
    if parsed.get("succeeded", 0) != 0:
        raise Failure(f"W4: expected 0 successes under full partition, got {parsed['succeeded']}")
    inv = parsed["invariants"]
    print(f"  W4 full_partition: I3={inv.get('I3')} "
          f"succeeded={parsed.get('succeeded')} total={parsed.get('total')}")
    vm.assert_replay_identical(run)


# --- W5: fraud_timeout ---

def test_w5_fraud_timeout(vm) -> None:
    netem = _load_fixture("w5_fraud_timeout.json")
    run = vm.create(
        f"BANK_TRANSFERS=20 BANK_CONCURRENCY=1 BANK_FRAUD_TIMEOUT=2 {COMMAND}",
        netem=netem, timeout="5m",
    )
    parsed = parse_output(run.stdout)
    fraud_timeout = parsed["stats"].get("fraud_timeout", 0)
    if fraud_timeout != 20:
        raise Failure(f"W5: expected fraud_timeout=20, got {fraud_timeout}")
    if parsed.get("succeeded", 0) == 0:
        raise Failure("W5: expected some transfers to succeed (fail-open)")
    print(f"  W5 fraud_timeout: succeeded={parsed.get('succeeded')} "
          f"fraud_timeout={fraud_timeout}")
    vm.assert_replay_identical(run)


# --- W6: thundering_herd ---

def test_w6_thundering_herd(vm) -> None:
    netem = _load_fixture("w6_thundering_herd.json")
    run = vm.create(
        f"BANK_TRANSFERS=30 BANK_CONCURRENCY=4 {COMMAND}",
        netem=netem, timeout="10m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W6 thundering_herd: I1={inv.get('I1')} I2={inv.get('I2')} "
          f"I3={inv.get('I3')} I4={inv.get('I4')} "
          f"succeeded={parsed.get('succeeded')}")
    vm.assert_replay_identical(run)


# --- W7: idempotency_race ---

def test_w7_idempotency_race(vm) -> None:
    run = vm.create(
        f"BANK_TRANSFERS=10 BANK_CONCURRENCY=2 BANK_PAIRED_KEYS=1 {COMMAND}",
        timeout="5m",
    )
    parsed = parse_output(run.stdout)
    assert_invariants("W7", parsed, "I1")
    print(f"  W7 idempotency: I1={parsed['invariants'].get('I1')} "
          f"succeeded={parsed.get('succeeded')}")
    vm.assert_replay_identical(run)


# --- W8: intermittent_loss ---

def test_w8_intermittent_loss(vm) -> None:
    netem = _load_fixture("w8_intermittent_loss.json")
    run = vm.create(
        f"BANK_TRANSFERS=50 BANK_CONCURRENCY=4 {COMMAND}",
        netem=netem, timeout="10m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W8 intermittent_loss: I1={inv.get('I1')} I2={inv.get('I2')} "
          f"I3={inv.get('I3')} "
          f"succeeded={parsed.get('succeeded')} failed={parsed.get('failed')}")
    vm.assert_replay_identical(run)


# --- W9: fraud_down_plus_concurrency ---

def test_w9_fraud_down(vm) -> None:
    netem = _load_fixture("w9_fraud_down.json")
    run = vm.create(
        f"BANK_TRANSFERS=50 BANK_CONCURRENCY=4 {COMMAND}",
        netem=netem, timeout="10m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W9 fraud_down: I1={inv.get('I1')} I2={inv.get('I2')} "
          f"succeeded={parsed.get('succeeded')}")
    vm.assert_replay_identical(run)


# --- W10: soak ---

def test_w10_soak(vm) -> None:
    netem = _load_fixture("w10_soak.json")
    run = vm.create(
        f"BANK_TRANSFERS=200 BANK_CONCURRENCY=4 {COMMAND}",
        netem=netem, timeout="15m",
    )
    parsed = parse_output(run.stdout)
    inv = parsed["invariants"]
    print(f"  W10 soak: I1={inv.get('I1')} I2={inv.get('I2')} "
          f"I3={inv.get('I3')} I4={inv.get('I4')} "
          f"succeeded={parsed.get('succeeded')} failed={parsed.get('failed')}")
    vm.assert_replay_identical(run)


def run(wenv_override: str | None) -> None:
    with open_vm("banking", wenv_override) as vm:
        test_w1_happy_path(vm)
        test_w2_concurrent_transfers(vm)
        test_w3_asymmetric_partition(vm)
        test_w4_full_partition(vm)
        test_w5_fraud_timeout(vm)
        test_w6_thundering_herd(vm)
        test_w7_idempotency_race(vm)
        test_w8_intermittent_loss(vm)
        test_w9_fraud_down(vm)
        test_w10_soak(vm)


if __name__ == "__main__":
    sys.exit(cli_main(run))
