---
name: wio
description: >
  Use the Workers.io CLI (wio) to run fault-injection simulations on the
  banking app and investigate failures.
allowed-tools:
  - Bash(wio *)
---

# /wio simulate faults

Run a fault-injected simulation (depth 2, intermittent network loss) against the banking project:

```bash
wio simulate create mx75m737e2vnv3txr97ng1axt58694hx --command "python3 main.py" --depth 2 --faults w8_intermittent_loss --mem 1024
```

After launching, poll until all runs finish:
```bash
wio workloads ls --state pending
```
Repeat every 15 seconds until no pending workloads remain.

Then show results:
```bash
wio workloads ls
```

# /wio investigate

Find the failed workloads, read their logs, diagnose the bug, and fix it.

Step 1 — List failures:
```bash
wio workloads ls --state failed --format json
```

Step 2 — For EACH failed workload ID, get details and logs:
```bash
wio workloads get <workload-id>
wio workloads logs <workload-id>
```

Step 3 — Read the relevant source files in this repo, find the bug that caused the invariant failure, and fix it.

Step 4 — Commit, push, and sync:
```bash
git add -A && git commit -m "fix: <describe the bug fix>"
git push origin main
wio projects sync mx75m737e2vnv3txr97ng1axt58694hx
```

Step 5 — Wait for sync to complete, then re-run the simulation to verify:
```bash
wio projects ls --format json
```
It should ideally get synced in 5 seconds so wait for a little while.

Then re-simulate:
```bash
wio simulate create mx75m737e2vnv3txr97ng1axt58694hx --command "python3 main.py" --depth 2 --faults w8_intermittent_loss --mem 1024
```
Poll and show results the same way as `/wio simulate faults`.
