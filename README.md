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

## Fault Profiles

Network fault profile JSON files are included under `.workers/fault/net/`.
They describe loopback traffic loss and delay patterns for the banking services.

## Configuration

All settings are read from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `BANK_LOCAL` | `1` | Set to `0` to bind services to distinct loopback IPs on port 9000. |
| `BANK_TRANSFERS` | `10` | Number of generated transfers to execute. |
| `BANK_CONCURRENCY` | `1` | Number of concurrent transfer requests. |
| `BANK_SEED` | `42` | Seed used to generate transfers. |
| `BANK_SETTLE_S` | `15` | Seconds to wait after requests finish. |
| `BANK_FRAUD_TIMEOUT` | `2` | Fraud service timeout in seconds. |
| `BANK_ACCT_TIMEOUT` | `5` | Account service timeout in seconds. |
| `BANK_VELOCITY_WINDOW` | `60` | Fraud velocity window in seconds. |
| `BANK_VELOCITY_LIMIT` | `3` | Max transfers per source account per window. |
| `BANK_SINGLE_TX_LIMIT` | `5000` | Max single transfer amount in cents. |
| `BANK_ROLLBACK_RETRIES` | `3` | Max rollback attempts. |
| `BANK_ROLLBACK_BASE_S` | `1` | Initial rollback backoff in seconds. |
| `BANK_INITIAL_BALANCE` | `10000` | Starting balance per account in cents. |
| `BANK_PAIRED_KEYS` | `0` | Set to `1` to reuse idempotency keys in pairs. |

## Example Output

```text
BANKING_VERSION: 1
transfers=10 succeeded=10 failed=0
fraud_denied=0 fraud_timeout=0
debit_timeout=0
rollback_ok=0 rollback_failed=0 rollback_retries=0
BALANCES A=10481 B=10139 C=13305 D=8082 E=7993 total=50000
INVARIANT I1 conservation PASS total=50000 expected=50000
INVARIANT I2 non_negative PASS
INVARIANT I3 atomicity PASS orphaned_debits=0
INVARIANT I4 tx_log_integrity PASS
elapsed_ms=1131
RESULT: PASS
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
`-- .workers/fault/net/
    `-- *.json     # Network fault profiles.
```
