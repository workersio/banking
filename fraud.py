"""Risk service — velocity and amount controls for payout/settlement flows.

Tracks per-balance movement frequency using in-memory sliding windows.
Denies movements that exceed velocity or single-transaction amount caps.
A background sweeper evicts stale entries.
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


class FraudServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int]):
        super().__init__(addr, FraudHandler, bind_and_activate=False)
        self.history: dict[str, list[int]] = {}
        self.lock = threading.Lock()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_bind()
        self.server_activate()
        self.timeout = config.ACCEPT_TIMEOUT


class FraudHandler(socketserver.BaseRequestHandler):
    server: FraudServer

    def handle(self) -> None:
        try:
            req = recv_msg(self.request)
        except OSError:
            return
        op = req.get("op")
        if op == "CHECK":
            self._check(req)
        else:
            try:
                send_msg(self.request, {"approved": False, "reason": "bad op"})
            except OSError:
                pass

    def _check(self, req: dict) -> None:
        src = req["src"]
        amount = req["amount"]
        now = time.monotonic_ns()
        window_ns = config.VELOCITY_WINDOW * 1_000_000_000
        cutoff = now - window_ns

        with self.server.lock:
            timestamps = self.server.history.get(src, [])
            filtered = [t for t in timestamps if t > cutoff]

            if len(filtered) >= config.VELOCITY_LIMIT:
                self.server.history[src] = filtered
                self._reply({"approved": False, "reason": "daily_transfer_limit_exceeded"})
                return

            if amount > config.SINGLE_TX_LIMIT:
                self.server.history[src] = filtered
                self._reply({"approved": False, "reason": "transfer_amount_exceeds_limit"})
                return

            filtered.append(now)
            self.server.history[src] = filtered

        self._reply({"approved": True})

    def _reply(self, msg: dict) -> None:
        try:
            send_msg(self.request, msg)
        except OSError:
            pass


def _sweeper(srv: FraudServer, stop: mp.Event) -> None:
    while not stop.is_set():
        time.sleep(10)
        now = time.monotonic_ns()
        window_ns = config.VELOCITY_WINDOW * 1_000_000_000
        cutoff = now - window_ns
        with srv.lock:
            for acct in list(srv.history):
                srv.history[acct] = [t for t in srv.history[acct] if t > cutoff]


def run_fraud(stop: mp.Event) -> None:
    srv = FraudServer(config.FRAUD_ADDR)
    print(
        f"SERVICE name=risk event=listening "
        f"addr={config.FRAUD_ADDR[0]}:{config.FRAUD_ADDR[1]} pid={os.getpid()}",
        flush=True,
    )
    t = threading.Thread(target=_sweeper, args=(srv, stop), daemon=True)
    t.start()
    while not stop.is_set():
        srv.handle_request()
    srv.server_close()
