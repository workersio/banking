# Payment Operations Simulation Testing Demo

A compact distributed payment-operations application used to demonstrate
simulation testing against production-shaped behavior. It models provider-side
services, not a merchant SDK integration: collection balances, payout balances,
settlement movement, dispute reserve, operating float, idempotent provider
references, risk controls, compensation, and post-run reconciliation checks.

The system runs three services:

- `gateway.py` validates and orchestrates provider ledger movements.
- `accounts.py` stores operational balances and transaction history in SQLite.
- `fraud.py` acts as the risk-control service for velocity and amount limits.

The app is intentionally small but production-shaped: `main.py` starts services
as separate processes, drives scenario traffic through the gateway, prints
structured logs, and checks that the ledger still reconciles after realistic
network and retry behavior.

## Quick Start

```bash
python3 main.py
```

Run a larger concurrent batch:

```bash
BANK_TRANSFERS=50 BANK_CONCURRENCY=4 python3 main.py
```

Run the default workload wrapper:

```bash
python3 .workers/workload.py
```

Run focused workloads:

```bash
python3 workloads/07_realistic_mixed_day.py
python3 workloads/03_concurrent_duplicate_retries.py
```

The SQLite database is stored at `/tmp/banking.db`. Remove it before a run to
reset operational balances and transaction history:

```bash
rm -f /tmp/banking.db
python3 main.py
```

## Architecture

```text
Scenario driver
    |
    | TRANSFER / BALANCE / STATS
    v
Gateway service
    |
    | CHECK
    v
Risk service

Gateway service
    |
    | DEBIT / CREDIT / ROLLBACK
    v
Ledger service
```

All service-to-service traffic uses length-prefixed JSON over TCP. The gateway
opens a new TCP connection for each downstream RPC.

Default local ports:

- Gateway: `127.0.0.1:9100`
- Risk: `127.0.0.1:9200`
- Ledger: `127.0.0.1:9300`

## Scenario Model

The public provider docs emphasize collections, payouts, settlements, refunds,
chargebacks, transfer status outcomes, idempotency, and webhook retries. This
demo maps those concepts into a small deterministic service graph:

- Collections land in `collection_pending`.
- Payout-ready funds live in `payout_available`.
- Settled funds move to `settlement_bank`.
- Dispute and chargeback exposure can be modeled through `dispute_reserve`.
- Operational float lives in `ops_float`.

Each operation moves funds between those balances with a unique provider
reference. Duplicate references must resolve exactly once. Failed credits must
be compensated. Risk checks can deny velocity pressure and transfer amount
limit probes using provider-style failure reasons such as
`daily_transfer_limit_exceeded` and `transfer_amount_exceeds_limit`.

## Workloads

| Workload | Purpose |
| --- | --- |
| `01_baseline_sequential.py` | Sequential collection and settlement baseline. |
| `02_parallel_consumer_burst.py` | Concurrent payout burst after warmup. |
| `03_concurrent_duplicate_retries.py` | Duplicate provider references racing the idempotency layer. |
| `04_hot_account_outflow.py` | Many payouts from one hot operational balance. |
| `05_velocity_limit_pressure.py` | Daily transfer velocity-limit enforcement. |
| `06_amount_limit_fraud.py` | Single-transaction amount limit probes. |
| `07_realistic_mixed_day.py` | Mixed operations day: collections, retries, payout burst, risk probes, validation errors. |
| `08_many_small_high_concurrency.py` | High-concurrency small ledger movements. |
| `09_reversal_pressure.py` | Available-balance drain and insufficient-balance handling. |
| `10_retry_after_success.py` | Duplicate references after successful originals. |
| `11_seed_sweep_mobile_deadline.py` | Seeded webhook receiver deadline profile for edge-jitter fault models. |
| `12_seed_sweep_balance_conservation.py` | Seeded settlement-tail profile for conservation and compensation pressure. |

## Fault Models

Network fault models live under `.workers/fault/net` and are intentionally
non-overlapping:

- `01_loopback_mild_loss_latency`: low baseline loss and latency on all service traffic.
- `02_accounts_rpc_slow_tail`: moderate ledger-service tail latency with rare loss.
- `03_fraud_rpc_bursty_loss`: burst loss isolated to risk checks.
- `04_gateway_client_reordering`: scenario-driver to gateway packet reordering.
- `05_accounts_partition_short_drop`: short ledger-service packet drop window.
- `06_low_bandwidth_all_services`: shared bandwidth pressure across the system.
- `07_mixed_service_specific_faults`: a multi-service degraded-day profile.
- `08_mobile_edge_jitter`: receiver-edge jitter for webhook deadline workloads.
- `09_accounts_settlement_tail`: severe ledger-service settlement tail for compensation pressure.

Useful pairings:

| Fault model | Workload |
| --- | --- |
| `04_gateway_client_reordering` | `03_concurrent_duplicate_retries.py` or `10_retry_after_success.py` |
| `08_mobile_edge_jitter` | `11_seed_sweep_mobile_deadline.py` |
| `09_accounts_settlement_tail` | `12_seed_sweep_balance_conservation.py` |
| `03_fraud_rpc_bursty_loss` | `05_velocity_limit_pressure.py` or `07_realistic_mixed_day.py` |
| `06_low_bandwidth_all_services` | `.workers/workload.py` |

## Invariants

The post-run checks are written for operators and reconciliation teams:

- `I1 FundsConservedAcrossLedger`: ledger balances never create or lose funds.
- `I2 AvailableBalancesNeverNegative`: no operational balance is overdrawn.
- `I3 SettlementPayoutsAreAtomic`: every debit reaches a credit or compensation.
- `I4 LedgerReplayMatchesBalances`: replaying the ledger reproduces live balances.
- `I5 DuplicateReferencesResolveOnce`: duplicate provider references settle once.
- `I6 TerminalLedgerShapeIsValid`: each transaction has a valid terminal shape.
- `I7 BalanceAfterSupportsReconciliation`: each row's `balance_after` supports replay.

## Logging

`main.py` prints human-readable sections and compact key/value summary lines.
For demos and sweeps, the most useful machine-readable lines are:

- `RUN` identifies the execution.
- `CHECK` reports each invariant with structured details.
- `INVARIANT_VIOLATED` reports the id of each failed invariant.
- `RUNTIME_CHECK` reports service health and gateway stats availability.
- `SUMMARY` reports operation counts and assertion status.
- `GATEWAY_STATS` reports service counters.
- `FINAL` reports the run result and elapsed time.

Example invariant log:

```text
[OK  ] I1 FundsConservedAcrossLedger: Ledger funds stayed conserved at 50000 minor units.
CHECK id=I1 name=FundsConservedAcrossLedger status=PASS summary="Ledger funds stayed conserved at 50000 minor units." details={"actual_total":50000,"delta":0,"expected_total":50000}
[OK  ] I3 SettlementPayoutsAreAtomic: Every settlement or payout debit reached a credit or compensation.
[OK  ] I5 DuplicateReferencesResolveOnce: Duplicate provider references resolved to one consistent outcome.
```

## Configuration

Existing `BANK_*` environment names are kept so older scripts continue to run.

| Variable | Default | Purpose |
| --- | --- | --- |
| `BANK_RUN_ID` | generated | Optional run identifier printed in logs and summary lines. |
| `BANK_TRANSFERS` | `10` | Number of generated ledger movements in the default smoke run. |
| `BANK_CONCURRENCY` | `1` | Concurrent requests in the default smoke run. |
| `BANK_SETTLE_S` | `15` | Seconds to wait after requests finish. |
| `BANK_FRAUD_TIMEOUT` | `2` | Risk-service timeout in seconds. |
| `BANK_ACCT_TIMEOUT` | `5` | Ledger-service timeout in seconds. |
| `BANK_VELOCITY_WINDOW` | `60` | Risk velocity window in seconds. |
| `BANK_VELOCITY_LIMIT` | `3` | Max movements per source balance per window. |
| `BANK_SINGLE_TX_LIMIT` | `5000` | Max single movement amount in minor units. |
| `BANK_ROLLBACK_RETRIES` | `3` | Max compensation attempts. |
| `BANK_ROLLBACK_BASE_S` | `1` | Initial compensation backoff in seconds. |
| `BANK_INITIAL_BALANCE` | `10000` | Starting amount for each operational balance. |
| `BANK_DUPLICATE_EVERY` | `25` in `.workers/workload.py` | Add a duplicate provider retry every Nth burst operation. |

## Files

```text
.
|-- main.py        # Starts services, sends scenarios, prints results.
|-- gateway.py     # Validation, idempotency, orchestration, compensation.
|-- accounts.py    # SQLite-backed operational balances and transaction log.
|-- fraud.py       # Risk controls for velocity and amount limits.
|-- invariants.py  # Post-run reconciliation and safety checks.
|-- workloads/     # Focused scenarios for simulation testing.
```
