#!/usr/bin/env python3
"""Run a realistic banking workload and report the app's invariants."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path("/tmp/banking.db")

WORKLOAD_ENV = {
    "BANK_RUN_ID": "banking-workload",
    "BANK_TRANSFERS": "16",
    "BANK_CONCURRENCY": "1",
    "BANK_DUPLICATE_EVERY": "0",
    "BANK_SETTLE_S": "0",
    "BANK_VELOCITY_LIMIT": "1000",
}


def main() -> int:
    env = os.environ.copy()
    for key, value in WORKLOAD_ENV.items():
        env.setdefault(key, value)

    try:
        DB_PATH.unlink()
    except FileNotFoundError:
        pass

    try:
        proc = subprocess.run(
            [sys.executable, "main.py"],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
    except subprocess.TimeoutExpired as exc:
        if exc.stdout:
            print(exc.stdout, end="", flush=True)
        print("WORKLOAD status=FAIL error=timeout", flush=True)
        return 124

    print(proc.stdout, end="", flush=True)
    status = "PASS" if proc.returncode == 0 else "FAIL"
    print(f"WORKLOAD status={status} exit_code={proc.returncode}", flush=True)
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
