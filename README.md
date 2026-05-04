# Banking — Distributed Systems Test Workload

Three microservices — **gateway**, **account service** (SQLite), **fraud service** —
process money transfers inside a [wenv](https://github.com/workersio/formal) VM.
The system finds concurrency and distributed-systems bugs by exploring different
CPU interleavings and network fault models under deterministic execution.

No manufactured bugs. Each service is written using standard engineering
practices, then wenv finds what breaks under adversarial scheduling and
network conditions.

## Architecture

```
  driver (4 threads)
      │
      │  TRANSFER / BALANCE
      ▼
┌─────────────────────────────────┐
│  Gateway           127.0.0.10   │
│  :9000                          │
│  Orchestrates fraud→debit→credit│
│  Idempotency cache (60s TTL)    │
└───┬──────────────────────┬──────┘
    │ CHECK                │ DEBIT / CREDIT / ROLLBACK
    ▼                      ▼
┌──────────────────┐  ┌──────────────────────────────┐
│ Fraud Service    │  │ Account Service               │
│ 127.0.0.20:9000  │  │ 127.0.0.30:9000               │
│                  │  │                                │
│ Velocity tracker │  │ SQLite (WAL mode):             │
│ (sliding window  │  │   accounts — id, balance       │
│ over virtual     │  │   transactions — tx log        │
│ time)            │  │                                │
└──────────────────┘  └──────────────────────────────┘
```

All inter-service traffic uses length-prefixed JSON over TCP on loopback.
Each service runs a `ThreadingTCPServer`. The gateway opens a **new TCP
connection per RPC** — no shared sockets between handler threads.

## Quick Start

### Run inside wenv (deterministic)

```bash
# Build the initramfs with banking files baked in
FORCE_INITRAMFS=1 ./scripts/provision.sh

# W1: Happy path — 10 sequential transfers, no faults
wenv create \
  --command "python3 /examples/banking/main.py" \
  --seed deadbeefcafebabe \
  --timeout 3m -f

# W2: Concurrent transfers — 50 transfers, 4 threads
wenv create \
  --command "BANK_TRANSFERS=50 BANK_CONCURRENCY=4 BANK_VELOCITY_LIMIT=100 \
             python3 /examples/banking/main.py" \
  --seed 000000000000000c \
  --timeout 5m -f
```

### Run with network faults

```bash
# Full partition between gateway and accounts
wenv create \
  --command "BANK_TRANSFERS=10 python3 /examples/banking/main.py" \
  --netem tests/fault_fixtures/w4_full_partition.json \
  --seed deadbeefcafebabe \
  --timeout 5m -f

# Fraud service unreachable (gateway proceeds fail-open)
wenv create \
  --command "BANK_TRANSFERS=20 BANK_FRAUD_TIMEOUT=2 \
             python3 /examples/banking/main.py" \
  --netem tests/fault_fixtures/w5_fraud_timeout.json \
  --seed deadbeefcafebabe \
  --timeout 5m -f
```

### Sweep seeds to find bugs

```bash
# Run the discovery script — sweeps seeds across workload configs
python3 tests/discover.py

# Or manually sweep a specific config
for seed in $(seq 1 50); do
  seed_hex=$(printf "%016x" $seed)
  wenv create \
    --command "BANK_TRANSFERS=50 BANK_CONCURRENCY=4 BANK_VELOCITY_LIMIT=100 \
               python3 /examples/banking/main.py" \
    --seed $seed_hex --timeout 5m -f 2>&1 \
  | grep "INVARIANT_VIOLATED" && echo "FOUND: seed=$seed_hex"
done
```

### Run the full test suite

```bash
python3 tests/test.py
```

## Transfer Flow

```
Client              Gateway               Fraud             Accounts
  │                    │                    │                   │
  │──TRANSFER(A→B,500)▶│                    │                   │
  │                    │──CHECK(A,B,500)───▶│                   │
  │                    │◀───APPROVED────────│                   │
  │                    │──DEBIT(A,500,tx42)─────────────────────▶│
  │                    │◀───OK(bal=9500)────────────────────────│
  │                    │──CREDIT(B,500,tx42)────────────────────▶│
  │                    │◀───OK(bal=10500)───────────────────────│
  │◀──OK(tx42)─────────│                    │                   │
```

**On failure:**
- Fraud denied → return error, no money moved
- Debit failed → return error (insufficient funds)
- Debit timeout → ambiguous, attempt rollback
- Credit failed → rollback the debit (exponential backoff, 3 retries)
- Rollback exhausted → log `ROLLBACK_FAILED:tx_id`, money in limbo

## Configuration

All settings are read from environment variables. Set them as a prefix
to the command string.

| Variable                | Default | Purpose                                  |
| ----------------------- | ------- | ---------------------------------------- |
| `BANK_TRANSFERS`        | `10`    | Number of transfers to execute           |
| `BANK_CONCURRENCY`      | `1`     | Driver threads (1 = sequential)          |
| `BANK_SETTLE_S`         | `15`    | Seconds to wait after last transfer      |
| `BANK_FRAUD_TIMEOUT`    | `2`     | Gateway's fraud-check timeout (seconds)  |
| `BANK_ACCT_TIMEOUT`     | `5`     | Gateway's account-service timeout (seconds)|
| `BANK_VELOCITY_WINDOW`  | `60`    | Fraud velocity window (seconds)          |
| `BANK_VELOCITY_LIMIT`   | `3`     | Max transfers per account per window     |
| `BANK_SINGLE_TX_LIMIT`  | `5000`  | Max single transfer amount (cents)       |
| `BANK_ROLLBACK_RETRIES` | `3`     | Max rollback attempts                    |
| `BANK_ROLLBACK_BASE_S`  | `1`     | Base delay for rollback backoff (seconds)|
| `BANK_INITIAL_BALANCE`  | `10000` | Starting balance per account (cents)     |
| `BANK_PAIRED_KEYS`      | `0`     | Set to `1` for duplicate idempotency keys|
| `BANK_LOCAL`             | `0`     | Set to `1` to use localhost ports        |

## Invariants

Checked after all transfers complete. The driver reads the SQLite database
directly (not via TCP) so invariant checking works even under network faults.

| ID | Name             | Rule                                                        |
|----|------------------|-------------------------------------------------------------|
| I1 | Conservation     | `sum(all balances) == 50,000` (initial total)               |
| I2 | Non-negative     | Every account balance ≥ 0                                   |
| I3 | Atomicity        | Every debit has a matching credit or rollback               |
| I4 | Tx log integrity | Replaying the transaction log produces the live balances    |

## Output Format

The driver prints machine-parseable output:

```
BANKING_VERSION: 1
transfers=50 succeeded=30 failed=20
fraud_denied=0 fraud_timeout=0
debit_timeout=0
rollback_ok=6 rollback_failed=1 rollback_retries=3
BALANCES A=14732 B=3798 C=13198 D=11861 E=5854 total=49443
INVARIANT I1 conservation FAIL total=49443 expected=50000
INVARIANT_VIOLATED:I1
INVARIANT I3 atomicity FAIL orphaned_debits=1 tx_ids=['tx-0001']
INVARIANT_VIOLATED:I3
virt_total_ms=29614
WORKLOAD: FAIL
```

## Files

```
banking/
├── main.py              # Entry point — starts services, fires transfers, checks invariants
├── gateway.py           # Transfer orchestration, idempotency cache, rollback
├── accounts.py          # SQLite-backed balances and transaction log
├── fraud.py             # Velocity-based rate limiting
├── invariants.py        # I1–I4 invariant checkers
├── protocol.py          # Length-prefixed JSON wire format
├── config.py            # Constants and environment variable parsing
├── API.md               # Wire protocol specification
├── README.md            # This file
└── tests/
    ├── test.py          # W1–W10 workload test suite
    ├── discover.py      # Seed sweeper — finds invariant violations
    ├── _harness.py      # Shared wenv VM test harness
    └── fault_fixtures/  # Network fault models (netem JSON)
        ├── w3_asymmetric_partition.json
        ├── w4_full_partition.json
        ├── w5_fraud_timeout.json
        ├── w6_thundering_herd.json
        ├── w8_intermittent_loss.json
        ├── w9_fraud_down.json
        └── w10_soak.json
```

## Known Bug

Seed `000000000000000c` with `BANK_TRANSFERS=50 BANK_CONCURRENCY=4
BANK_VELOCITY_LIMIT=100` deterministically produces an I1+I3 violation:
`tx-0001` debit succeeds, credit fails under concurrent load, and all 3
rollback retries are exhausted. 557 cents lost.

This reproduces bit-identically on every run with the same seed and
baked initramfs — that is the point of wenv.
