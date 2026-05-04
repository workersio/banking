"""Shared test harness for wenv VM-based tests.

Each test imports `VM` and writes ~30-80 lines of unique invariant
checking. The harness owns: locating wenv, setting up WENV_HOME,
launching create/replay, parsing JSON, asserting determinism + replay
bit-identity.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

DEFAULT_SEED = "deadbeefcafebabe"
WALL_BUDGET_S = 90.0


class Failure(Exception):
    pass


def find_wenv(override: str | None = None) -> Path:
    if override:
        p = Path(override)
        if not p.exists():
            raise Failure(f"--wenv {p} does not exist")
        return p
    if env := os.environ.get("WENV"):
        p = Path(env)
        if not p.exists():
            raise Failure(f"$WENV={env} does not exist")
        return p
    if found := shutil.which("wenv"):
        return Path(found)
    raise Failure(
        "wenv binary not found. Install wenv and ensure it's on PATH, "
        "set $WENV to the binary path, or pass --wenv."
    )


@dataclass
class Run:
    """A single record or replay invocation result."""
    meta: dict
    stdout: str
    elapsed_s: float

    @property
    def id(self) -> str:
        return self.meta["id"]

    @property
    def stdout_sha256(self) -> str | None:
        return self.meta.get("stdout_sha256")

    @property
    def wall_ms(self) -> int | None:
        """Host wall-clock duration in ms, from `ended_ns - started_ns` in
        meta.json. None if either timestamp is missing."""
        s = self.meta.get("started_ns")
        e = self.meta.get("ended_ns")
        if s is None or e is None:
            return None
        return (e - s) // 1_000_000

    @property
    def virt_ms(self) -> int | None:
        """Guest virtual time (icount-driven) in ms, from the
        TESTVMD-VTIME sentinel parsed by the supervisor. None if the
        guest crashed before exec or the sentinel didn't fire."""
        v = self.meta.get("virtual_ns")
        return None if v is None else int(v) // 1_000_000

    def expect_in_stdout(self, *needles: str) -> None:
        for n in needles:
            if n not in self.stdout:
                raise Failure(
                    f"expected {n!r} in stdout (run {self.id}); tail:\n"
                    f"{self.stdout[-1500:]}"
                )


class VM:
    """Wraps a `wenv` binary + temp WENV_HOME for one test session."""

    def __init__(self, label: str, *, wenv: Path, home: Path):
        self.label = label
        self.wenv = wenv
        self.home = home
        self._step = 0

    def create(
        self,
        command: str,
        *,
        seed: str = DEFAULT_SEED,
        network_mock: Path | None = None,
        netem: Path | None = None,
        schedule: Path | None = None,
        timeout: str = "5m",
    ) -> Run:
        self._step += 1
        argv = [
            str(self.wenv), "create", "--json",
            "--seed", seed,
            "--timeout", timeout,
            "--command", command,
        ]
        if network_mock:
            argv += ["--network-mock", str(network_mock)]
        if netem:
            argv += ["--netem", str(netem)]
        if schedule:
            argv += ["--schedule", str(schedule)]

        return self._invoke(argv, f"create#{self._step}")

    def replay(self, run_id: str, *, timeout: str = "5m") -> Run:
        self._step += 1
        argv = [str(self.wenv), "replay", "--json", "--timeout", timeout, run_id]
        return self._invoke(argv, f"replay#{self._step}")

    # ---- assertion helpers ----

    def assert_deterministic(self, a: Run, b: Run) -> None:
        if a.stdout_sha256 != b.stdout_sha256:
            raise Failure(
                f"{self.label} DETERMINISTIC violated:\n"
                f"  a={a.stdout_sha256}\n"
                f"  b={b.stdout_sha256}"
            )

    def assert_replay_identical(self, original: Run) -> None:
        replay = self.replay(original.id)
        meta = replay.meta
        if "bit_identical" in meta:
            ok = bool(meta["bit_identical"])
        else:
            ok = (
                original.stdout_sha256 is not None
                and meta.get("replay_run", {}).get("stdout_sha256") == original.stdout_sha256
            )
        if not ok:
            raise Failure(
                f"{self.label} REPLAY diverged for {original.id}\n"
                f"  source: {original.stdout_sha256}\n"
                f"  replay: {meta.get('replay_run', {}).get('stdout_sha256')}"
            )

    # ---- internal ----

    def _invoke(self, argv: list[str], tag: str) -> Run:
        env = os.environ.copy()
        env["WENV_HOME"] = str(self.home)
        t0 = time.monotonic()
        proc = subprocess.run(
            argv, capture_output=True, text=True,
            timeout=WALL_BUDGET_S, env=env,
        )
        elapsed = time.monotonic() - t0
        # `wenv replay` exits 1 on bit-identical (state==Succeeded passed
        # exit_for_state); but bit_identical itself is the success bit.
        # Treat any non-zero rc that produced a JSON body as recoverable.
        if proc.returncode != 0 and not proc.stdout.strip().startswith("{"):
            raise Failure(
                f"[{self.label}/{tag}] wenv exited {proc.returncode} after {elapsed:.2f}s\n"
                f"argv: {argv}\n"
                f"stdout: {proc.stdout[-2000:]}\n"
                f"stderr: {proc.stderr[-2000:]}"
            )
        try:
            meta = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise Failure(f"[{self.label}/{tag}] non-JSON stdout: {e}\n{proc.stdout[:2000]}")

        # Replay JSON nests under replay_run; flatten so callers don't care.
        if "replay_run" in meta:
            run_id = meta["replay_run"]["id"]
        else:
            run_id = meta["id"]

        log_path = self.home / "runs" / run_id / "stdout.log"
        stdout = log_path.read_text() if log_path.exists() else ""

        return Run(meta=meta, stdout=stdout, elapsed_s=elapsed)


# ---- module-level helpers --------------------------------------------------

def with_rules(rules: dict | str, dir: Path | None = None) -> Path:
    """Write JSON network-mock rules to a temp file. Returns the path.

    Accepts either a dict (json.dumps'd) or a pre-serialised string.
    """
    text = json.dumps(rules) if isinstance(rules, dict) else rules
    f = tempfile.NamedTemporaryFile(
        prefix="wenv-rules-", suffix=".json", delete=False,
        dir=str(dir) if dir else None, mode="w",
    )
    f.write(text)
    f.close()
    return Path(f.name)


def with_netem(cfg: dict | str, dir: Path | None = None) -> Path:
    """Write a JSON netem config to a temp file. Returns the path."""
    text = json.dumps(cfg) if isinstance(cfg, dict) else cfg
    f = tempfile.NamedTemporaryFile(
        prefix="wenv-netem-", suffix=".json", delete=False,
        dir=str(dir) if dir else None, mode="w",
    )
    f.write(text)
    f.close()
    return Path(f.name)


def write_schedule(timestamps_ns: list[int], path: Path | None = None) -> Path:
    """Write a `--schedule` file. Returns the path."""
    if path is None:
        f = tempfile.NamedTemporaryFile(
            prefix="wenv-sched-", suffix=".sched", delete=False, mode="w",
        )
        path = Path(f.name)
    else:
        f = path.open("w")
    with f:
        f.write("# synthesised schedule\n")
        for ts in timestamps_ns:
            f.write(f"{ts}\n")
    return path


# ---- session lifecycle -----------------------------------------------------

def open_vm(label: str, wenv_override: str | None = None) -> "_VMSession":
    """Context manager. Builds a temp WENV_HOME with artifacts symlinked
    from ~/.wenv/artifacts, yields a VM, cleans up on exit."""
    return _VMSession(label, wenv_override)


class _VMSession:
    def __init__(self, label: str, wenv_override: str | None):
        self.label = label
        self.wenv = find_wenv(wenv_override)
        self.tmp = tempfile.mkdtemp(prefix=f"wenv-test-{label}-")
        self.home = Path(self.tmp)
        self._link_artifacts()
        print(f"# {label}: WENV_HOME={self.home}  wenv={self.wenv}")

    def _link_artifacts(self) -> None:
        artifacts = self.home / "artifacts"
        if artifacts.exists():
            return
        user_home = Path(os.environ.get("HOME", "/")) / ".wenv" / "artifacts"
        if not (user_home / "rootfs.img").exists():
            raise Failure(
                "no rootfs.img found at ~/.wenv/artifacts. Run "
                "`./packages/environment/scripts/provision.sh` first."
            )
        artifacts.parent.mkdir(parents=True, exist_ok=True)
        artifacts.symlink_to(user_home)

    def __enter__(self) -> VM:
        return VM(self.label, wenv=self.wenv, home=self.home)

    def __exit__(self, *exc) -> None:
        # Tempdir preserved on failure for forensics; cleaned on success.
        if exc[0] is None:
            shutil.rmtree(self.tmp, ignore_errors=True)
        else:
            print(f"# {self.label}: tempdir preserved at {self.home}")


def cli_main(test_fn) -> int:
    """Boilerplate `if __name__ == '__main__'` body for tests."""
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--wenv", help="path to wenv binary")
    args = ap.parse_args()
    try:
        test_fn(args.wenv)
        return 0
    except Failure as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        return 1
