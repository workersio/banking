#!/usr/bin/env python3
"""Sweep seeds to find invariant violations for each workload."""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Failure, cli_main, open_vm  # noqa: E402

COMMAND = "python3 /examples/banking/main.py"
FIXTURES = Path(__file__).parent / "fault_fixtures"

_INVARIANT_RE = re.compile(
    r"INVARIANT (?P<id>I\d) (?P<name>\S+) (?P<status>PASS|FAIL)"
)


def parse_invariants(stdout: str) -> dict[str, str]:
    result = {}
    for m in _INVARIANT_RE.finditer(stdout):
        result[m["id"]] = m["status"]
    return result


def discover(vm, workload_env: str, netem: Path | None = None,
             max_seeds: int = 50) -> str | None:
    for i in range(1, max_seeds + 1):
        seed = f"{i:016x}"
        run = vm.create(
            f"{workload_env} {COMMAND}",
            seed=seed, netem=netem, timeout="5m",
        )
        inv = parse_invariants(run.stdout)
        violations = [k for k, v in inv.items() if v == "FAIL"]
        if violations:
            print(f"FOUND: seed={seed} violated={','.join(violations)}")
            return seed
        print(f"  seed={seed} ok", end="\r")
    print(f"\nno violations found in {max_seeds} seeds")
    return None


WORKLOADS = [
    ("W2_concurrent", "BANK_TRANSFERS=50 BANK_CONCURRENCY=4", None),
    ("W3_asymmetric", "BANK_TRANSFERS=15 BANK_CONCURRENCY=1",
     FIXTURES / "w3_asymmetric_partition.json"),
    ("W6_thundering_herd", "BANK_TRANSFERS=30 BANK_CONCURRENCY=4",
     FIXTURES / "w6_thundering_herd.json"),
    ("W8_intermittent_loss", "BANK_TRANSFERS=50 BANK_CONCURRENCY=4",
     FIXTURES / "w8_intermittent_loss.json"),
    ("W9_fraud_down", "BANK_TRANSFERS=50 BANK_CONCURRENCY=4",
     FIXTURES / "w9_fraud_down.json"),
    ("W10_soak", "BANK_TRANSFERS=200 BANK_CONCURRENCY=4",
     FIXTURES / "w10_soak.json"),
]


def run(wenv_override: str | None) -> None:
    with open_vm("banking-discover", wenv_override) as vm:
        for name, env, fixture in WORKLOADS:
            print(f"\n--- {name} ---")
            seed = discover(vm, env, netem=fixture, max_seeds=50)
            if seed:
                print(f"  {name}: violation at seed={seed}")
            else:
                print(f"  {name}: clean across 50 seeds")


if __name__ == "__main__":
    sys.exit(cli_main(run))
