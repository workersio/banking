"""Gateway — transfer orchestration with validation, idempotency, and rollback.

Coordinates fraud check → debit → credit for each transfer. Opens a new
TCP connection per downstream RPC (no shared sockets between handler
threads). Failed credits trigger rollback with exponential backoff.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import socketserver
import threading
import time

import config
from protocol import recv_msg, send_msg


def _validate_transfer_request(req: dict) -> str | None:
    """Return an API error code when the transfer request is invalid."""
    idem_key = req.get("idempotency_key")
    if not isinstance(idem_key, str) or not idem_key:
        return "invalid_idempotency_key"

    src = req.get("src")
    dst = req.get("dst")
    if src not in config.ACCOUNTS or dst not in config.ACCOUNTS:
        return "unknown_account"
    if src == dst:
        return "same_account_transfer"

    amount = req.get("amount")
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        return "invalid_amount"

    return None


def _transfer_timeout_budget() -> float:
    rollback_budget = sum(
        config.ROLLBACK_BASE_S * (2 ** attempt)
        for attempt in range(max(0, config.ROLLBACK_MAX_RETRIES - 1))
    )
    return config.FRAUD_TIMEOUT + (config.ACCT_TIMEOUT * 2) + rollback_budget + 1.0


def _rpc(addr: tuple[str, int], src_addr: tuple[str, int],
         msg: dict, timeout: float) -> dict | None:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.settimeout(timeout)
    try:
        s.bind((src_addr[0], 0))
        s.connect(addr)
        send_msg(s, msg)
        return recv_msg(s)
    except OSError:
        return None
    finally:
        try:
            s.close()
        except OSError:
            pass


class GatewayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int]):
        super().__init__(addr, GatewayHandler, bind_and_activate=False)
        self.next_tx = 1
        self.tx_lock = threading.Lock()
        self.idem_cache: dict[str, dict] = {}
        self.idem_lock = threading.Lock()
        self.stats = {
            "ok": 0, "fail": 0,
            "invalid": 0,
            "fraud_denied": 0, "fraud_timeout": 0,
            "debit_timeout": 0,
            "rollback_ok": 0, "rollback_failed": 0, "rollback_retries": 0,
        }
        self.stats_lock = threading.Lock()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_bind()
        self.server_activate()
        self.timeout = config.ACCEPT_TIMEOUT


class GatewayHandler(socketserver.BaseRequestHandler):
    server: GatewayServer

    def handle(self) -> None:
        try:
            req = recv_msg(self.request)
        except OSError:
            return
        op = req.get("op")
        if op == "TRANSFER":
            self._transfer(req)
        elif op == "BALANCE":
            self._balance()
        elif op == "STATS":
            self._get_stats()
        else:
            try:
                send_msg(self.request, {"ok": False, "error": "bad op"})
            except OSError:
                pass

    def _inc(self, key: str, n: int = 1) -> None:
        with self.server.stats_lock:
            self.server.stats[key] += n

    def _transfer(self, req: dict) -> None:
        validation_error = _validate_transfer_request(req)
        if validation_error is not None:
            self._inc("invalid")
            self._inc("fail")
            self._reply({"ok": False, "error": validation_error})
            return

        idem_key = req.get("idempotency_key", "")
        src, dst, amount = req["src"], req["dst"], req["amount"]
        request_fingerprint = (src, dst, amount)

        with self.server.idem_lock:
            idem_record = self.server.idem_cache.get(idem_key)
            if idem_record is None:
                idem_record = {
                    "request": request_fingerprint,
                    "event": threading.Event(),
                    "response": None,
                    "_cached_at": time.monotonic_ns(),
                }
                self.server.idem_cache[idem_key] = idem_record
                owns_request = True
            else:
                owns_request = False
                if idem_record.get("request") != request_fingerprint:
                    self._inc("invalid")
                    self._inc("fail")
                    self._reply({"ok": False, "error": "idempotency_conflict"})
                    return
                cached = idem_record.get("response")
                if cached is not None:
                    self._reply(cached)
                    return
                wait_event = idem_record["event"]

        if not owns_request:
            if wait_event.wait(timeout=_transfer_timeout_budget()):
                with self.server.idem_lock:
                    cached = self.server.idem_cache.get(idem_key, {}).get("response")
                if cached is not None:
                    self._reply(cached)
                    return
            self._inc("fail")
            self._reply({"ok": False, "error": "idempotency_in_progress_timeout"})
            return

        def finish(resp: dict) -> None:
            with self.server.idem_lock:
                idem_record["response"] = resp
                idem_record["_cached_at"] = time.monotonic_ns()
                idem_record["event"].set()
            self._reply(resp)

        with self.server.tx_lock:
            tx_id = f"tx-{self.server.next_tx:04d}"
            self.server.next_tx += 1

        fraud_resp = _rpc(
            config.FRAUD_ADDR, config.GATEWAY_ADDR,
            {"op": "CHECK", "src": src, "dst": dst, "amount": amount},
            config.FRAUD_TIMEOUT,
        )
        if fraud_resp is None:
            self._inc("fraud_timeout")
        elif not fraud_resp.get("approved"):
            self._inc("fraud_denied")
            self._inc("fail")
            finish({"ok": False, "error": fraud_resp.get("reason", "fraud_denied"), "tx_id": tx_id})
            return

        debit_resp = _rpc(
            config.ACCOUNTS_ADDR, config.GATEWAY_ADDR,
            {"op": "DEBIT", "account": src, "amount": amount, "tx_id": tx_id},
            config.ACCT_TIMEOUT,
        )

        if debit_resp is None:
            self._inc("debit_timeout")
            self._do_rollback(tx_id)
            self._inc("fail")
            finish({"ok": False, "error": "debit_timeout", "tx_id": tx_id})
            return

        if not debit_resp.get("ok"):
            self._inc("fail")
            finish({"ok": False, "error": debit_resp.get("error", "debit_failed"), "tx_id": tx_id})
            return

        credit_resp = _rpc(
            config.ACCOUNTS_ADDR, config.GATEWAY_ADDR,
            {"op": "CREDIT", "account": dst, "amount": amount, "tx_id": tx_id},
            config.ACCT_TIMEOUT,
        )

        if credit_resp is None or not credit_resp.get("ok"):
            self._do_rollback(tx_id)
            self._inc("fail")
            finish({"ok": False, "error": "credit_failed", "tx_id": tx_id})
            return

        self._inc("ok")
        finish({
            "ok": True, "tx_id": tx_id,
            "src_balance": debit_resp["balance"],
            "dst_balance": credit_resp["balance"],
        })

    def _do_rollback(self, tx_id: str) -> None:
        delay = config.ROLLBACK_BASE_S
        for attempt in range(config.ROLLBACK_MAX_RETRIES):
            if attempt > 0:
                self._inc("rollback_retries")
                time.sleep(delay)
                delay *= 2
            resp = _rpc(
                config.ACCOUNTS_ADDR, config.GATEWAY_ADDR,
                {"op": "ROLLBACK", "tx_id": tx_id},
                config.ACCT_TIMEOUT,
            )
            if resp is not None and resp.get("ok"):
                self._inc("rollback_ok")
                return
        self._inc("rollback_failed")
        print(f"SERVICE name=gateway event=rollback_failed tx_id={tx_id}", flush=True)

    def _balance(self) -> None:
        resp = _rpc(
            config.ACCOUNTS_ADDR, config.GATEWAY_ADDR,
            {"op": "BALANCE"},
            config.ACCT_TIMEOUT,
        )
        if resp is not None:
            self._reply(resp)
        else:
            self._reply({"error": "accounts_unreachable"})

    def _get_stats(self) -> None:
        with self.server.stats_lock:
            self._reply(dict(self.server.stats))

    def _reply(self, msg: dict) -> None:
        try:
            send_msg(self.request, msg)
        except OSError:
            pass


def _cache_sweeper(srv: GatewayServer, stop: mp.Event) -> None:
    while not stop.is_set():
        time.sleep(10)
        now = time.monotonic_ns()
        ttl_ns = 60 * 1_000_000_000
        with srv.idem_lock:
            expired = [
                k for k, v in srv.idem_cache.items()
                if v.get("response") is not None and now - v.get("_cached_at", 0) > ttl_ns
            ]
            for k in expired:
                del srv.idem_cache[k]


def run_gateway(stop: mp.Event) -> None:
    srv = GatewayServer(config.GATEWAY_ADDR)
    print(
        f"SERVICE name=gateway event=listening "
        f"addr={config.GATEWAY_ADDR[0]}:{config.GATEWAY_ADDR[1]} pid={os.getpid()}",
        flush=True,
    )
    t = threading.Thread(target=_cache_sweeper, args=(srv, stop), daemon=True)
    t.start()
    while not stop.is_set():
        srv.handle_request()
    srv.server_close()
