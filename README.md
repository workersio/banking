# Banking App

A small Python banking application that models transfers between accounts.
It runs three TCP services:

- `gateway.py` orchestrates transfer requests.
- `accounts.py` stores balances and transaction history in SQLite.
- `fraud.py` applies velocity and amount limits before a transfer is allowed.

The app is intentionally simple: it starts all services from `main.py`, sends a
batch of transfers, prints service stats, and checks that account balances still
match the transaction log.

## Quick Start

```bash
python3 main.py
```

Run a larger concurrent batch:

```bash
BANK_TRANSFERS=50 BANK_CONCURRENCY=4 python3 main.py
```

Run the Workers IO workload wrapper:

```bash
python3 .workers/workload.py
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
2. The gateway asks the fraud service to approve the transfer.
3. The gateway debits the source account through the account service.
4. The gateway credits the destination account through the account service.
5. If crediting fails after debit succeeds, the gateway attempts a rollback.

## Workload

`.workers/workload.py` runs the app the way a client would use it: it starts
the services through `main.py`, composes realistic transaction phases from the
app's client primitives, submits sequential and concurrent traffic, includes
duplicate client retries and fraud-probe attempts, and lets the app print its
own invariant checks.

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
[   1032 ms] transfers    submitting batch attempted=10 concurrency=1
[   1053 ms] transfers    batch complete attempted=10 succeeded=10 failed=0 duration_ms=20
Transfer errors: none

========================================================================
Assertions
========================================================================
[OK  ] I1 conservation: total=50000 expected=50000
CHECK id=I1 name=conservation status=PASS summary="total=50000 expected=50000" details={"actual_total":50000,"delta":0,"expected_total":50000}
[OK  ] I2 non_negative: all account balances are non-negative
CHECK id=I2 name=non_negative status=PASS summary="all account balances are non-negative" details={"negative_accounts":{}}

========================================================================
Runtime Checks
========================================================================
[PASS] services: 0 runtime failures
RUNTIME_CHECK name=services status=PASS details=[]
[PASS] gateway_stats: gateway stats collected
RUNTIME_CHECK name=gateway_stats status=PASS details={"debit_timeout":0,"fail":0,"fraud_denied":0,"fraud_timeout":0,"ok":10,"rollback_failed":0,"rollback_ok":0,"rollback_retries":0}

========================================================================
Machine Summary
========================================================================
SUMMARY run_id=demo-log-check attempted=10 succeeded=10 failed=0 transfer_duration_ms=20 db_read=ok assertions_failed=0 runtime_failures=0 gateway_stats=ok
TRANSFER_ERRORS none=true
GATEWAY_STATS debit_timeout=0 fail=0 fraud_denied=0 fraud_timeout=0 ok=10 rollback_failed=0 rollback_ok=0 rollback_retries=0
transfers=10 succeeded=10 failed=0
BALANCES A=10481 B=10139 C=13305 D=8082 E=7993 total=50000
RESULT: PASS
FINAL run_id=demo-log-check status=PASS elapsed_ms=1114
```

## Files

```text
.
|-- main.py        # Starts services, sends transfers, prints results.
|-- gateway.py     # Transfer orchestration, idempotency, rollback.
|-- accounts.py    # SQLite-backed balances and transaction log.
|-- fraud.py       # Velocity-based transfer checks.
|-- invariants.py  # Consistency checks for balances and transactions.
|-- protocol.py    # Length-prefixed JSON wire format.
|-- config.py      # Environment variable parsing.
|-- API.md         # Wire protocol details.
`-- .workers/workload.py
    # Realistic workload entrypoint.
```
