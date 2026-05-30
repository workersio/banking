# Banking Simulation Testing Demo

A small distributed banking application used to demonstrate simulation testing.
It models a payment transfer path with real processes, TCP service boundaries,
SQLite persistence, client retries, fraud controls, rollback, and post-run
invariants.

The system runs three services:

- `gateway.py` validates and orchestrates transfer requests.
- `accounts.py` stores balances and transaction history in SQLite.
- `fraud.py` applies velocity and amount limits before a transfer is allowed.

The app is intentionally compact but production-shaped: `main.py` starts the
services as separate processes, drives client traffic through the gateway,
prints service stats, and checks that account balances still match the ledger.

## Quick Start

```bash
python3 main.py
```

Run a larger concurrent batch:

```bash
BANK_TRANSFERS=50 BANK_CONCURRENCY=4 python3 main.py
```

Run the default Workers IO workload wrapper:

```bash
python3 .workers/workload.py
```

Run a focused workload:

```bash
python3 workloads/07_realistic_mixed_day.py
python3 workloads/03_concurrent_duplicate_retries.py
```

The account database is stored at `/tmp/banking.db`. Remove it before a run if
you want to reset account balances and transaction history:

```bash
rm -f /tmp/banking.db
python3 main.py
```

## Architecture

```text
Client driver
    |
    | TRANSFER / BALANCE / STATS
    v
Gateway service
    |
    | CHECK
    v
Fraud service

Gateway service
    |
    | DEBIT / CREDIT / ROLLBACK
    v
Account service
```

All service-to-service traffic uses length-prefixed JSON over TCP. The gateway
opens a new TCP connection for each downstream RPC.

By default, services listen on:

- Gateway: `127.0.0.1:9100`
- Fraud: `127.0.0.1:9200`
- Accounts: `127.0.0.1:9300`

## Transfer Flow

1. A client sends `TRANSFER(src, dst, amount, idempotency_key)` to the gateway.
2. The gateway rejects malformed requests before downstream side effects.
3. The gateway asks the fraud service to approve the transfer.
4. The gateway debits the source account through the account service.
5. The gateway credits the destination account through the account service.
6. If crediting fails after debit succeeds, the gateway attempts a rollback.

## Workloads

`.workers/workload.py` runs the app the way a client would use it: it starts
the services through `main.py`, composes realistic transaction phases from the
app's client primitives, submits sequential and concurrent traffic, includes
duplicate client retries, fraud-probe attempts, and client validation mistakes,
then lets the app print its own invariant checks.

Focused workloads live in `workloads/`. Each one has a narrow purpose so demo
failures are easy to explain.

| Workload | Purpose |
| --- | --- |
| `01_baseline_sequential.py` | Simple sequential transfer baseline. |
| `02_parallel_consumer_burst.py` | Concurrent customer traffic after warmup. |
| `03_concurrent_duplicate_retries.py` | Duplicate client retries racing the idempotency layer. |
| `04_hot_account_outflow.py` | Many payments from one hot source account. |
| `05_velocity_limit_pressure.py` | Fraud velocity-limit enforcement. |
| `06_amount_limit_fraud.py` | Fraud single-transaction amount cap. |
| `07_realistic_mixed_day.py` | Mixed banking day: routine traffic, retries, merchant burst, fraud probes, validation errors. |
| `08_many_small_high_concurrency.py` | High-concurrency small-transfer pressure. |
| `09_reversal_pressure.py` | Account-drain and follow-up traffic that exercises insufficient-funds handling. |
| `10_retry_after_success.py` | Client retries after successful original requests. |
| `11_seed_sweep_mobile_deadline.py` | Seeded mobile deadline profile for edge-jitter fault models. |
| `12_seed_sweep_balance_conservation.py` | Seeded settlement-tail profile for conservation and rollback pressure. |

## Fault Models

Network fault models live under `.workers/fault/net` and are intentionally
non-overlapping:

- `01_loopback_mild_loss_latency`: low baseline loss and latency on all service traffic.
- `02_accounts_rpc_slow_tail`: moderate account-service tail latency with rare loss.
- `03_fraud_rpc_bursty_loss`: burst loss isolated to fraud checks.
- `04_gateway_client_reordering`: client-to-gateway packet reordering.
- `05_accounts_partition_short_drop`: short account-service packet drop window.
- `06_low_bandwidth_all_services`: shared bandwidth pressure across the system.
- `07_mixed_service_specific_faults`: a multi-service degraded-day profile.
- `08_mobile_edge_jitter`: client-edge jitter for mobile deadline workloads.
- `09_accounts_settlement_tail`: severe account-service settlement tail for rollback pressure.

Useful pairings:

| Fault model | Workload |
| --- | --- |
| `04_gateway_client_reordering` | `03_concurrent_duplicate_retries.py` or `10_retry_after_success.py` |
| `08_mobile_edge_jitter` | `11_seed_sweep_mobile_deadline.py` |
| `09_accounts_settlement_tail` | `12_seed_sweep_balance_conservation.py` |
| `03_fraud_rpc_bursty_loss` | `05_velocity_limit_pressure.py` or `07_realistic_mixed_day.py` |
| `06_low_bandwidth_all_services` | `.workers/workload.py` |

## Invariants

The post-run checks are intentionally readable and map to different parts of the
system:

- `I1 MoneyConservation`: account totals never create or destroy money.
- `I2 BalancesNeverNegative`: account service never leaves an account negative.
- `I3 TransfersAreAtomic`: every debit reaches a terminal credit or rollback.
- `I4 LedgerMatchesBalances`: replaying the ledger reproduces live balances.
- `I5 IdempotencyKeysAreConsistent`: duplicate client retries settle once.
- `I6 LedgerTransactionShape`: each transaction has a valid terminal shape.
- `I7 LedgerBalanceAfterConsistent`: each ledger row's `balance_after` is true.

## Configuration

All settings are read from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `BANK_RUN_ID` | generated | Optional run identifier printed in logs and summary lines. |
| `BANK_TRANSFERS` | `10` | Number of generated transfers to execute in the default `main.py` smoke run. |
| `BANK_CONCURRENCY` | `1` | Number of concurrent transfer requests for the default `main.py` smoke run. |
| `BANK_SETTLE_S` | `15` | Seconds to wait after requests finish. |
| `BANK_FRAUD_TIMEOUT` | `2` | Fraud service timeout in seconds. |
| `BANK_ACCT_TIMEOUT` | `5` | Account service timeout in seconds. |
| `BANK_VELOCITY_WINDOW` | `60` | Fraud velocity window in seconds. |
| `BANK_VELOCITY_LIMIT` | `3` | Max transfers per source account per window. |
| `BANK_SINGLE_TX_LIMIT` | `5000` | Max single transfer amount in cents. |
| `BANK_ROLLBACK_RETRIES` | `3` | Max rollback attempts. |
| `BANK_ROLLBACK_BASE_S` | `1` | Initial rollback backoff in seconds. |
| `BANK_INITIAL_BALANCE` | `10000` | Starting balance per account in cents. |
| `BANK_DUPLICATE_EVERY` | `25` in `.workers/workload.py` | Add a duplicate client retry every Nth burst transfer in the workload wrapper. |

## Logging

`main.py` prints sectioned human-readable logs and compact key/value summary
lines. For large sweeps, the most useful machine-readable lines are:

- `RUN` identifies the execution.
- `CHECK` reports each invariant with structured details.
- `INVARIANT_VIOLATED` reports the id of each failed invariant.
- `RUNTIME_CHECK` reports service health and gateway stats availability.
- `SUMMARY` reports transfer counts and assertion status.
- `GATEWAY_STATS` reports service counters.
- `FINAL` reports the run result and elapsed time.

## Example Output Excerpt

```text
BANKING_VERSION: 1

========================================================================
Banking Run
========================================================================
RUN id=demo-log-check version=1 started_at=2026-05-04T00:10:13-0700
Run configuration:
  transfers         : 10
  concurrency       : 1

========================================================================
Transfer Execution
========================================================================
[   1032 ms] transfers    submitting phase name=cli_smoke attempted=10 concurrency=1
[   1053 ms] transfers    batch complete attempted=10 succeeded=10 failed=0 duration_ms=20
Transfer errors: none

========================================================================
Assertions
========================================================================
[OK  ] I1 MoneyConservation: System total stayed at 50000 cents.
CHECK id=I1 name=MoneyConservation status=PASS summary="System total stayed at 50000 cents." details={"actual_total":50000,"delta":0,"expected_total":50000}
[OK  ] I2 BalancesNeverNegative: No account ended with a negative balance.
CHECK id=I2 name=BalancesNeverNegative status=PASS summary="No account ended with a negative balance." details={"negative_accounts":{}}
[OK  ] I3 TransfersAreAtomic: Every debit was matched by a credit or rollback.
[OK  ] I4 LedgerMatchesBalances: Ledger replay matched every live account balance.
[OK  ] I6 LedgerTransactionShape: Every transaction had a valid completed, compensated, or rollback-tombstone shape.
[OK  ] I7 LedgerBalanceAfterConsistent: Every ledger row's balance_after matched per-account replay.
[OK  ] I5 IdempotencyKeysAreConsistent: Duplicate idempotency keys resolved to one consistent outcome.

========================================================================
Runtime Checks
========================================================================
[PASS] services: 0 runtime failures
RUNTIME_CHECK name=services status=PASS details=[]
[PASS] gateway_stats: gateway stats collected
RUNTIME_CHECK name=gateway_stats status=PASS details={"debit_timeout":0,"fail":0,"fraud_denied":0,"fraud_timeout":0,"invalid":0,"ok":10,"rollback_failed":0,"rollback_ok":0,"rollback_retries":0}

========================================================================
Machine Summary
========================================================================
SUMMARY run_id=demo-log-check attempted=10 succeeded=10 failed=0 transfer_duration_ms=20 db_read=ok assertions_failed=0 runtime_failures=0 gateway_stats=ok
TRANSFER_ERRORS none=true
GATEWAY_STATS debit_timeout=0 fail=0 fraud_denied=0 fraud_timeout=0 invalid=0 ok=10 rollback_failed=0 rollback_ok=0 rollback_retries=0
transfers=10 succeeded=10 failed=0
BALANCES A=10481 B=10139 C=13305 D=8082 E=7993 total=50000
RESULT: PASS
FINAL run_id=demo-log-check status=PASS elapsed_ms=1114
```

## Files

```text
.
|-- main.py        # Starts services, sends transfers, prints results.
|-- gateway.py     # Validation, idempotency, transfer orchestration, rollback.
|-- accounts.py    # SQLite-backed balances and transaction log.
|-- fraud.py       # Velocity-based transfer checks.
|-- invariants.py  # Consistency checks for balances and transactions.
|-- protocol.py    # Length-prefixed JSON wire format.
|-- config.py      # Environment variable parsing.
|-- API.md         # Wire protocol details.
|-- workloads/     # Focused workload scenarios.
`-- .workers/workload.py
    # Realistic workload entrypoint.
```
