"""Gateway — transfer orchestration with idempotency and rollback.

Coordinates fraud check → debit → credit for each transfer. Opens a new
TCP connection per downstream RPC (no shared sockets between handler
threads). Failed credits trigger rollback with exponential backoff.

The idempotency cache has a TOCTOU race: the lock is released between
the cache check and the cache write, so duplicate concurrent requests
can both miss the cache.
"""

from __future__ import annotations

import multiprocessing as mp
import socket
import socketserver
import threading
import time

import config
from protocol import recv_msg, send_msg


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
        idem_key = req.get("idempotency_key", "")

        with self.server.idem_lock:
            cached = self.server.idem_cache.get(idem_key)
            if cached is not None:
                self._reply(cached)
                return

        with self.server.tx_lock:
            tx_id = f"tx-{self.server.next_tx:04d}"
            self.server.next_tx += 1

        src, dst, amount = req["src"], req["dst"], req["amount"]

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
            resp = {"ok": False, "error": fraud_resp.get("reason", "fraud_denied"), "tx_id": tx_id}
            self._reply(resp)
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
            self._reply({"ok": False, "error": "debit_timeout", "tx_id": tx_id})
            return

        if not debit_resp.get("ok"):
            self._inc("fail")
            self._reply({"ok": False, "error": debit_resp.get("error", "debit_failed"), "tx_id": tx_id})
            return

        credit_resp = _rpc(
            config.ACCOUNTS_ADDR, config.GATEWAY_ADDR,
            {"op": "CREDIT", "account": dst, "amount": amount, "tx_id": tx_id},
            config.ACCT_TIMEOUT,
        )

        if credit_resp is None or not credit_resp.get("ok"):
            self._do_rollback(tx_id)
            self._inc("fail")
            self._reply({"ok": False, "error": "credit_failed", "tx_id": tx_id})
            return

        resp = {
            "ok": True, "tx_id": tx_id,
            "src_balance": debit_resp["balance"],
            "dst_balance": credit_resp["balance"],
        }
        with self.server.idem_lock:
            cached = dict(resp)
            cached["_cached_at"] = time.monotonic_ns()
            self.server.idem_cache[idem_key] = cached
        self._inc("ok")
        self._reply(resp)

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
        print(f"ROLLBACK_FAILED:{tx_id}", flush=True)

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
            expired = [k for k, v in srv.idem_cache.items()
                       if now - v.get("_cached_at", 0) > ttl_ns]
            for k in expired:
                del srv.idem_cache[k]


def run_gateway(stop: mp.Event) -> None:
    srv = GatewayServer(config.GATEWAY_ADDR)
    print(f"[gateway] listening on {config.GATEWAY_ADDR[0]}:{config.GATEWAY_ADDR[1]}", flush=True)
    t = threading.Thread(target=_cache_sweeper, args=(srv, stop), daemon=True)
    t.start()
    while not stop.is_set():
        srv.handle_request()
    srv.server_close()
