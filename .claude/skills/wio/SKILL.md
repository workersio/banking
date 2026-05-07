---
name: wio
description: >
  Use the Workers.io CLI (wio) to run fault-injection simulations on the
  banking app and investigate failures.
allowed-tools:
  - Bash(wio *)
---

# /wio simulate network faults

Run a fault-injected simulation (depth 2, intermittent network loss) against the banking project:

```bash
wio simulate create mx75m737e2vnv3txr97ng1axt58694hx --command "python3 main.py" --depth 2 --faults w8_intermittent_loss --mem 1024
```

After launching, poll until all runs finish:
```bash
wio simulate status <simulation id>
```

Repeat every 15 seconds until no pending workloads remain. Once done check the deatils about individual worklaods using

```bash
wio workloads ls
```

Then summary of results in a nice formatted table, how many simulations you ran, which ones failed in brief rich context.

# /wio investigate failures

Find the failed workloads, read their logs, diagnose the bug, and fix it.

Step 1 — List failures, you will see FAILURE column when a simulation fails and the reason for it.

```bash
wio workloads ls --format json
```

```bash
RUN ID                      STATE      FAULT                 FAILURE      DURATION  CREATED         
01KR1Z4HVWR78K7HN31GYB5PC2  succeeded  w8_intermittent_loss  fault_model  22.3s     2026-05-07 19:37
01KR1Z4HVW7ASC0WXDBQ2BWX99  succeeded  w8_intermittent_loss  -            21.9s     2026-05-07 19:37
```

FAILURE - fault_model here for 01KR1Z4HVWR78K7HN31GYB5PC2 means that our workload fails under the fault model and there is likely a bug that doesn't handle failures correctly:

Step 2 — For EACH failed workload ID, get details and logs:
```bash
wio workloads get <workload-id>
wio workloads logs <workload-id>
```

Once you have this information, inform the user in a nice formatted way what happened and what failure have we encountered like invariant violation etc. Keep it short and crisp.

Step 3 — Read the relevant source files in this repo, find the bug that caused the invariant failure, and fix it.

Step 4 — Commit, push, and sync:
```bash
git add -A && git commit -m "fix: <describe the bug fix>"
git push origin main
wio projects sync mx75m737e2vnv3txr97ng1axt58694hx
```
It should ideally get synced in 5 seconds so wait for a little while.

Step 5 — Wait for sync to complete, then re-run the simulation to verify:
```bash
wio projects ls --format json
```

Then re-simulate:
```bash
wio simulate create mx75m737e2vnv3txr97ng1axt58694hx --command "python3 main.py" --depth 2 --faults w8_intermittent_loss --mem 1024
```
Poll and show results the same way as `/wio simulate faults`.

Then give a detailed summary of before and after, what was fixed and what failure case was handled. Don't maker it too verbose but make it crisp and clear on what is happening.
