# Payment Operations API Specification

All inter-service communication uses length-prefixed JSON over TCP:

```text
[4 bytes: big-endian payload length][UTF-8 JSON payload]
```

Max payload: 1 MiB. Every request gets exactly one response on the same
connection. Connections are not reused; the gateway opens a fresh TCP socket
for each downstream RPC.

## Gateway

Default local address: `127.0.0.1:9100`.

The gateway is the only service the scenario driver talks to.

### TRANSFER

Validates and orchestrates one provider ledger movement:

`risk check -> debit source balance -> credit destination balance`

On credit failure, the gateway compensates the debit.

**Request:**

```json
{
  "op": "TRANSFER",
  "src": "collection_pending",
  "dst": "payout_available",
  "amount": 500,
  "idempotency_key": "payout-ref-0001"
}
```

**Success response:**

```json
{
  "ok": true,
  "tx_id": "tx-0001",
  "src_balance": 9500,
  "dst_balance": 10500
}
```

**Failure response:**

```json
{
  "ok": false,
  "error": "insufficient_funds",
  "tx_id": "tx-0001"
}
```

Possible `error` values include `daily_transfer_limit_exceeded`,
`transfer_amount_exceeds_limit`, `debit_timeout`, `insufficient_funds`,
`credit_failed`, `invalid_amount`, `invalid_idempotency_key`,
`same_balance_transfer`, `unknown_balance`, `idempotency_conflict`, and
`idempotency_in_progress_timeout`.

Idempotency behavior:

- The same `idempotency_key` with the same `src`, `dst`, and `amount` returns
  the first completed response, including a failure response.
- A duplicate request that arrives while the first request is still running
  waits for the first response.
- Reusing an `idempotency_key` with a different `src`, `dst`, or `amount`
  returns `idempotency_conflict`.
- Cached idempotency results are retained in memory for about 60 seconds.

Validation behavior:

- `src` and `dst` must be known operational balances.
- `src` and `dst` must differ.
- `amount` must be a positive integer minor-unit amount.
- `idempotency_key` must be a non-empty provider reference.

### BALANCE

Proxies to the ledger service.

**Request:**

```json
{ "op": "BALANCE" }
```

### STATS

Returns gateway-internal counters.

**Request:**

```json
{ "op": "STATS" }
```

**Response:**

```json
{
  "ok": 30,
  "fail": 20,
  "invalid": 1,
  "risk_denied": 2,
  "risk_timeout": 0,
  "debit_timeout": 1,
  "compensation_ok": 3,
  "compensation_failed": 1,
  "compensation_retries": 4
}
```

## Ledger Service

Default local address: `127.0.0.1:9300`.

SQLite-backed. All write operations are wrapped in transactions with a
`(tx_id, op)` primary key to prevent duplicate application.

### DEBIT

Deducts `amount` from an operational balance and logs the transaction.

**Request:**

```json
{
  "op": "DEBIT",
  "account": "payout_available",
  "amount": 500,
  "tx_id": "tx-0001"
}
```

**Success:**

```json
{ "ok": true, "balance": 9500 }
```

**Failure:**

```json
{ "ok": false, "error": "insufficient_funds" }
```

Errors: `insufficient_funds`, `duplicate_tx`, `rolled_back`, `internal`, and
`invalid_request`.

### CREDIT

Adds `amount` to an operational balance. Replaying the same `tx_id` returns the
previously recorded balance.

### ROLLBACK

Compensates a prior debit. If no debit exists yet, it records a tombstone so a
racing debit with the same transaction id cannot commit later.

### BALANCE

Returns all operational balances.

**Response:**

```json
{
  "balances": {
    "collection_pending": 9500,
    "payout_available": 10500,
    "settlement_bank": 10000,
    "dispute_reserve": 10000,
    "ops_float": 10000
  },
  "total": 50000
}
```

### TX_LOG

Returns the full transaction log, ordered by creation time.

## Risk Service

Default local address: `127.0.0.1:9200`.

Stateless across restarts. Tracks velocity in memory.

### CHECK

Evaluates whether a ledger movement should be allowed based on:

1. **Velocity limit**: max `BANK_VELOCITY_LIMIT` movements per source balance
   within a sliding `BANK_VELOCITY_WINDOW`-second window.
2. **Amount cap**: single movement must not exceed `BANK_SINGLE_TX_LIMIT`.

**Request:**

```json
{
  "op": "CHECK",
  "src": "payout_available",
  "dst": "settlement_bank",
  "amount": 500
}
```

**Approved:**

```json
{ "approved": true }
```

**Denied:**

```json
{ "approved": false, "reason": "daily_transfer_limit_exceeded" }
```

Reasons: `daily_transfer_limit_exceeded`,
`transfer_amount_exceeds_limit`.

## Error Handling

| Scenario | Gateway behavior |
| --- | --- |
| Risk service unreachable | Proceeds fail-open so downstream ledger invariants are still tested |
| Risk denied | Returns error, no funds moved |
| Debit timeout | Attempts compensation, returns error |
| Credit timeout/failure | Compensates the debit with retry/backoff |
| Compensation exhausted | Logs `SERVICE name=gateway event=compensation_failed tx_id=...` |

## SQLite Schema

```sql
CREATE TABLE accounts (
    id      TEXT PRIMARY KEY,
    balance INTEGER NOT NULL
);

CREATE TABLE transactions (
    tx_id       TEXT NOT NULL,
    op          TEXT NOT NULL,       -- 'debit', 'credit', 'rollback'
    account_id  TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    balance_after INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,    -- monotonic_ns
    PRIMARY KEY (tx_id, op)
);
```

Pragmas: `journal_mode=WAL`, `busy_timeout=10000`.
