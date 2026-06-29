"""Length-prefixed JSON wire protocol.

Every message is: [4-byte big-endian length][UTF-8 JSON payload].
Max payload size is 1 MiB. All three services share this format.
"""

import json
import socket
import struct

MAX_MSG_BYTES = 1 << 20  # 1 MiB


def send_msg(sock: socket.socket, msg: dict) -> None:
    data = json.dumps(msg, separators=(",", ":")).encode()
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_msg(sock: socket.socket) -> dict:
    hdr = _recv_exact(sock, 4)
    n = struct.unpack("!I", hdr)[0]
    if n > MAX_MSG_BYTES:
        raise OSError(f"oversize message: {n} bytes")
    return json.loads(_recv_exact(sock, n).decode())


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError("short read")
        buf += chunk
    return bytes(buf)
