# Banking API Specification

All inter-service communication uses **length-prefixed JSON over TCP**.
Each message is framed as:

```
[4 bytes: big-endian payload length][UTF-8 JSON payload]
```

Max payload: 1 MiB. Every request gets exactly one response on the same
connection. Connections are not reused — the gateway opens a fresh TCP
socket for each RPC.

---

## Gateway

Default local address: `127.0.0.1:9100`.

The gateway is the only service the driver talks to.

### TRANSFER

Validates and orchestrates a money transfer: fraud check → debit source →
credit destination. On credit failure, rolls back the debit.

**Request:**
```json
{
  "op": "TRANSFER",
  "src": "A",
  "dst": "B",
  "amount": 500,
  "idempotency_key": "key-0001"
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

Possible `error` values: `fraud_denied`, `velocity_exceeded`,
`amount_exceeded`, `debit_timeout`, `insufficient_funds`,
`credit_failed`, `no_account`, `invalid_amount`, `invalid_idempotency_key`,
`same_account_transfer`, `unknown_account`, `idempotency_conflict`,
`idempotency_in_progress_timeout`.

Idempotency behavior:
- The same `idempotency_key` with the same `src`, `dst`, and `amount` returns
  the first completed response, including a failure response.
- A duplicate request that arrives while the first request is still running
  waits for that first response.
- Reusing an `idempotency_key` with a different `src`, `dst`, or `amount`
  returns `idempotency_conflict`.
- Cached idempotency results are retained in memory for approximately 60 seconds.

Validation behavior:
- `src` and `dst` must be known accounts.
- `src` and `dst` must differ.
- `amount` must be a positive integer.
- `idempotency_key` must be a non-empty string.

### BALANCE

Proxies to the account service.

**Request:**
```json
{ "op": "BALANCE" }
```

**Response:** forwarded from account service (see below).

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
  "fraud_denied": 2,
  "fraud_timeout": 0,
  "debit_timeout": 1,
  "rollback_ok": 3,
  "rollback_failed": 1,
  "rollback_retries": 4
}
```

---

## Account Service

Default local address: `127.0.0.1:9300`.

SQLite-backed. All write operations are wrapped in transactions with a
`(tx_id, op)` primary key to prevent duplicate application.

### DEBIT

Deducts `amount` from `account` and logs the transaction.

**Request:**
```json
{
  "op": "DEBIT",
  "account": "A",
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

Errors: `insufficient_funds`, `no_account`, `duplicate_tx`, `internal`.
Direct service callers can also receive `invalid_request` for malformed account
or amount fields.

### CREDIT

Adds `amount` to `account`. Idempotent — replaying the same `tx_id`
returns the previously recorded balance.

**Request:**
```json
{
  "op": "CREDIT",
  "account": "B",
  "amount": 500,
  "tx_id": "tx-0001"
}
```

**Response:**
```json
{ "ok": true, "balance": 10500 }
```

Direct service callers can receive `invalid_request` for malformed account or
amount fields.

### ROLLBACK

Reverses a prior debit. Looks up the original debit by `tx_id`, then
credits the amount back. Idempotent — replaying returns the same result.

**Request:**
```json
{
  "op": "ROLLBACK",
  "tx_id": "tx-0001"
}
```

**Success:**
```json
{ "ok": true, "balance": 10000 }
```

When no debit exists yet, rollback records a tombstone so a racing debit with
the same transaction id cannot commit later.

**Failure:**
```json
{ "ok": false, "error": "internal" }
```

### BALANCE

Returns all account balances.

**Request:**
```json
{ "op": "BALANCE" }
```

**Response:**
```json
{
  "balances": { "A": 9500, "B": 10500, "C": 10000, "D": 10000, "E": 10000 },
  "total": 50000
}
```

### TX_LOG

Returns the full transaction log, ordered by creation time.

**Request:**
```json
{ "op": "TX_LOG" }
```

**Response:**
```json
{
  "entries": [
    {
      "tx_id": "tx-0001",
      "op": "debit",
      "account_id": "A",
      "amount": 500,
      "balance_after": 9500,
      "created_at": 1234567890
    }
  ]
}
```

---

## Fraud Service

Default local address: `127.0.0.1:9200`.

Stateless across restarts. Tracks velocity in memory.

### CHECK

Evaluates whether a transfer should be allowed based on:
1. **Velocity limit** — max `BANK_VELOCITY_LIMIT` transfers per source
   account within a sliding `BANK_VELOCITY_WINDOW`-second window.
2. **Amount cap** — single transfer must not exceed `BANK_SINGLE_TX_LIMIT`.

**Request:**
```json
{
  "op": "CHECK",
  "src": "A",
  "dst": "B",
  "amount": 500
}
```

**Approved:**
```json
{ "approved": true }
```

**Denied:**
```json
{ "approved": false, "reason": "velocity_exceeded" }
```

Reasons: `velocity_exceeded`, `amount_exceeded`.

---

## Error Handling

| Scenario | Gateway behavior |
|---|---|
| Fraud service unreachable | Proceeds (fail-open) |
| Fraud denied | Returns error, no money moved |
| Debit timeout | Attempts rollback, returns error |
| Credit timeout/failure | Rolls back the debit (3 retries, exponential backoff) |
| Rollback exhausted | Logs `SERVICE name=gateway event=rollback_failed tx_id=...`, money in limbo |

---

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
