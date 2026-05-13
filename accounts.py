"""Account service — SQLite-backed balances and transaction log.

Exposes DEBIT, CREDIT, ROLLBACK, BALANCE, and TX_LOG operations over TCP.
Uses WAL mode with deferred transactions. The deferred BEGIN creates a
check-then-act window: two concurrent DEBITs can both read the same
balance before either commits.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import socketserver
import sqlite3
import threading
import time

import config
from protocol import recv_msg, send_msg



def _init_db() -> None:
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id      TEXT PRIMARY KEY,
            balance INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            tx_id       TEXT NOT NULL,
            op          TEXT NOT NULL,
            account_id  TEXT NOT NULL,
            amount      INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            created_at  INTEGER NOT NULL,
            PRIMARY KEY (tx_id, op)
        )
    """)
    for acct in config.ACCOUNTS:
        conn.execute(
            "INSERT OR IGNORE INTO accounts (id, balance) VALUES (?, ?)",
            (acct, config.INITIAL_BALANCE),
        )
    conn.commit()
    conn.close()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


class AccountServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int]):
        super().__init__(addr, AccountHandler, bind_and_activate=False)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_bind()
        self.server_activate()
        self.timeout = config.ACCEPT_TIMEOUT


class AccountHandler(socketserver.BaseRequestHandler):
    server: AccountServer

    def handle(self) -> None:
        try:
            req = recv_msg(self.request)
        except OSError:
            return
        op = req.get("op")
        handlers = {
            "DEBIT": self._debit,
            "CREDIT": self._credit,
            "ROLLBACK": self._rollback,
            "BALANCE": self._balance,
            "TX_LOG": self._tx_log,
        }
        handler = handlers.get(op)
        if handler:
            handler(req)
        else:
            try:
                send_msg(self.request, {"ok": False, "error": "bad op"})
            except OSError:
                pass

    def _debit(self, req: dict) -> None:
        account = req["account"]
        amount = req["amount"]
        tx_id = req["tx_id"]
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT balance FROM accounts WHERE id = ?", (account,)
            ).fetchone()
            if row is None:
                self._reply({"ok": False, "error": "no_account"})
                conn.rollback()
                return
            balance = row[0]
            if balance < amount:
                self._reply({"ok": False, "error": "insufficient_funds"})
                conn.rollback()
                return
            conn.execute(
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                (amount, account),
            )
            row2 = conn.execute(
                "SELECT balance FROM accounts WHERE id = ?", (account,)
            ).fetchone()
            new_balance = row2[0]
            now = time.monotonic_ns()
            conn.execute(
                "INSERT INTO transactions (tx_id, op, account_id, amount, balance_after, created_at) "
                "VALUES (?, 'debit', ?, ?, ?, ?)",
                (tx_id, account, amount, new_balance, now),
            )
            conn.commit()
            self._reply({"ok": True, "balance": new_balance})
        except sqlite3.IntegrityError:
            conn.rollback()
            self._reply({"ok": False, "error": "duplicate_tx"})
        except Exception:
            conn.rollback()
            self._reply({"ok": False, "error": "internal"})
        finally:
            conn.close()

    def _credit(self, req: dict) -> None:
        account = req["account"]
        amount = req["amount"]
        tx_id = req["tx_id"]
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT balance_after FROM transactions WHERE tx_id = ? AND op = 'credit'",
                (tx_id,),
            ).fetchone()
            if existing is not None:
                conn.rollback()
                self._reply({"ok": True, "balance": existing[0]})
                return
            conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                (amount, account),
            )
            row = conn.execute(
                "SELECT balance FROM accounts WHERE id = ?", (account,)
            ).fetchone()
            new_balance = row[0]
            now = time.monotonic_ns()
            conn.execute(
                "INSERT INTO transactions (tx_id, op, account_id, amount, balance_after, created_at) "
                "VALUES (?, 'credit', ?, ?, ?, ?)",
                (tx_id, account, amount, new_balance, now),
            )
            conn.commit()
            self._reply({"ok": True, "balance": new_balance})
        except Exception:
            conn.rollback()
            self._reply({"ok": False, "error": "internal"})
        finally:
            conn.close()

    def _rollback(self, req: dict) -> None:
        tx_id = req["tx_id"]
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            debit_row = conn.execute(
                "SELECT account_id, amount FROM transactions WHERE tx_id = ? AND op = 'debit'",
                (tx_id,),
            ).fetchone()
            if debit_row is None:
                conn.rollback()
                self._reply({"ok": False, "error": "no_debit_found"})
                return
            existing = conn.execute(
                "SELECT balance_after FROM transactions WHERE tx_id = ? AND op = 'rollback'",
                (tx_id,),
            ).fetchone()
            if existing is not None:
                conn.rollback()
                self._reply({"ok": True, "balance": existing[0]})
                return
            account, amount = debit_row
            conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                (amount, account),
            )
            row = conn.execute(
                "SELECT balance FROM accounts WHERE id = ?", (account,)
            ).fetchone()
            new_balance = row[0]
            now = time.monotonic_ns()
            conn.execute(
                "INSERT INTO transactions (tx_id, op, account_id, amount, balance_after, created_at) "
                "VALUES (?, 'rollback', ?, ?, ?, ?)",
                (tx_id, account, amount, new_balance, now),
            )
            conn.commit()
            self._reply({"ok": True, "balance": new_balance})
        except Exception:
            conn.rollback()
            self._reply({"ok": False, "error": "internal"})
        finally:
            conn.close()

    def _balance(self, _req: dict) -> None:
        conn = _get_conn()
        try:
            rows = conn.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall()
            balances = {r[0]: r[1] for r in rows}
            total = sum(balances.values())
            self._reply({"balances": balances, "total": total})
        finally:
            conn.close()

    def _tx_log(self, _req: dict) -> None:
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT tx_id, op, account_id, amount, balance_after, created_at "
                "FROM transactions ORDER BY created_at"
            ).fetchall()
            entries = [
                {"tx_id": r[0], "op": r[1], "account_id": r[2],
                 "amount": r[3], "balance_after": r[4], "created_at": r[5]}
                for r in rows
            ]
            self._reply({"entries": entries})
        finally:
            conn.close()

    def _reply(self, msg: dict) -> None:
        try:
            send_msg(self.request, msg)
        except OSError:
            pass


def run_accounts(stop: mp.Event) -> None:
    _init_db()
    srv = AccountServer(config.ACCOUNTS_ADDR)
    print(
        f"SERVICE name=accounts event=listening "
        f"addr={config.ACCOUNTS_ADDR[0]}:{config.ACCOUNTS_ADDR[1]} pid={os.getpid()}",
        flush=True,
    )
    while not stop.is_set():
        srv.handle_request()
    srv.server_close()
