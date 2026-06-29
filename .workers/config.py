"""Application configuration; all tunables read from environment variables.

By default each service binds to localhost with separate ports. Set
BANK_LOCAL=0 to bind each service to a distinct loopback IP.
"""

import os

_LOCAL = os.environ.get("BANK_LOCAL", "1") == "1"

if _LOCAL:
    GATEWAY_ADDR  = ("127.0.0.1", 9100)
    FRAUD_ADDR    = ("127.0.0.1", 9200)
    ACCOUNTS_ADDR = ("127.0.0.1", 9300)
else:
    GATEWAY_ADDR  = ("127.0.0.10", 9000)
    FRAUD_ADDR    = ("127.0.0.20", 9000)
    ACCOUNTS_ADDR = ("127.0.0.30", 9000)

ACCOUNTS      = [
    "collection_pending",
    "payout_available",
    "settlement_bank",
    "dispute_reserve",
    "ops_float",
]
INITIAL_BALANCE = int(os.environ.get("BANK_INITIAL_BALANCE", "10000"))
INITIAL_TOTAL   = INITIAL_BALANCE * len(ACCOUNTS)

TRANSFERS       = int(os.environ.get("BANK_TRANSFERS", "10"))
CONCURRENCY     = int(os.environ.get("BANK_CONCURRENCY", "1"))
SETTLE_S        = int(os.environ.get("BANK_SETTLE_S", "15"))

FRAUD_TIMEOUT   = float(os.environ.get("BANK_FRAUD_TIMEOUT", "2"))
ACCT_TIMEOUT    = float(os.environ.get("BANK_ACCT_TIMEOUT", "5"))

VELOCITY_WINDOW = int(os.environ.get("BANK_VELOCITY_WINDOW", "60"))
VELOCITY_LIMIT  = int(os.environ.get("BANK_VELOCITY_LIMIT", "3"))
SINGLE_TX_LIMIT = int(os.environ.get("BANK_SINGLE_TX_LIMIT", "5000"))

ROLLBACK_MAX_RETRIES = int(os.environ.get("BANK_ROLLBACK_RETRIES", "3"))
ROLLBACK_BASE_S      = float(os.environ.get("BANK_ROLLBACK_BASE_S", "1"))

DB_PATH = "/tmp/banking.db"

BIND_WAIT_S    = 1.0
ACCEPT_TIMEOUT = 0.05
