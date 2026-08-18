#!/usr/bin/env python3
"""Standalone client for sending traffic through the interceptor.

Self-contained on purpose -- no imports from `testkit/` -- so you can copy this
single file straight into your own project.

    python3 send_payment.py --target 127.0.0.1:9100
    python3 send_payment.py --target 127.0.0.1:9100 --count 100
    python3 send_payment.py --target 127.0.0.1:9100 --connections 8 --count 50
    python3 send_payment.py --account-name Budi --account-number 99887766 --bank-code 5678

THE FRAMING MUST MATCH THE INTERCEPTOR'S [framing] CONFIG.
Defaults here mirror the shipped config.toml: 2-byte big-endian length prefix,
length excludes the prefix itself. If your real VP<->FMS link differs, change
the constructor arguments AND config.toml together -- if they disagree, traffic
still reaches FMS correctly but the Kafka feed will desync and stop publishing.
"""

import argparse
import socket
import struct
import sys
import threading
import time


class VPClient:
    """Length-prefixed request/response client.

    Usage:
        with VPClient("127.0.0.1", 9100) as c:
            response = c.request(b"0200|STAN=00000001|...")
    """

    def __init__(
        self,
        host: str,
        port: int,
        prefix_bytes: int = 2,
        big_endian: bool = True,
        length_includes_prefix: bool = False,
        timeout: float = 10.0,
    ) -> None:
        if prefix_bytes not in (2, 4):
            raise ValueError("prefix_bytes must be 2 or 4")
        self.host = host
        self.port = port
        self.length_includes_prefix = length_includes_prefix
        self.timeout = timeout
        self._struct = struct.Struct((">" if big_endian else "<") + ("H" if prefix_bytes == 2 else "I"))
        self._prefix_len = prefix_bytes
        self.sock: socket.socket | None = None

    # -- connection ------------------------------------------------------

    def connect(self) -> "VPClient":
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        # Without this, Nagle batches small messages and adds tens of ms.
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(self.timeout)
        return self

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None

    def __enter__(self) -> "VPClient":
        return self.connect()

    def __exit__(self, *exc) -> None:
        self.close()

    # -- framing ---------------------------------------------------------

    def _encode(self, body: bytes) -> bytes:
        declared = len(body) + (self._prefix_len if self.length_includes_prefix else 0)
        return self._struct.pack(declared) + body

    def _recv_exact(self, n: int) -> bytes:
        """recv() is free to return fewer bytes than asked; loop until satisfied."""
        chunks = []
        remaining = n
        while remaining:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise ConnectionError(f"peer closed with {remaining} of {n} bytes outstanding")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    # -- messaging -------------------------------------------------------

    def send(self, body: bytes) -> None:
        if self.sock is None:
            raise RuntimeError("not connected; call connect() first")
        self.sock.sendall(self._encode(body))

    def recv(self) -> bytes:
        (declared,) = self._struct.unpack(self._recv_exact(self._prefix_len))
        body_len = declared - self._prefix_len if self.length_includes_prefix else declared
        if body_len <= 0:
            raise ValueError(f"implausible frame length {declared}")
        return self._recv_exact(body_len)

    def request(self, body: bytes) -> bytes:
        """Send one message and wait for one response."""
        self.send(body)
        return self.recv()


def build_message(
    txn_ref: int,
    account_name: str = "Zacky",
    account_number: str = "11020134353",
    bank_code: str = "1234",
    amount_cents: int = 10000,
) -> bytes:
    """Build one request frame body.

    Comma-separated `key=value`, matching the interceptor's `[parse]` config, so
    these fields surface as real JSON keys in Kafka rather than being buried in
    the encoded payload. Replace with your actual encoder.
    """
    return (
        f"msgType=request,txnRef={txn_ref:08d}"
        f",accountName={account_name}"
        f",accountNumber={account_number}"
        f",bankCode={bank_code}"
        f",amount={amount_cents:012d},currency=360"
    ).encode()


def run_one_connection(conn_no: int, host: str, port: int, count: int, verbose: bool,
                       opts=None) -> tuple[int, int, list[float]]:
    ok = err = 0
    latencies: list[float] = []
    try:
        with VPClient(host, port) as client:
            for i in range(count):
                stan = conn_no * 100000 + i + 1
                msg = build_message(
                    stan,
                    account_name=opts.account_name if opts else "Zacky",
                    account_number=opts.account_number if opts else "11020134353",
                    bank_code=opts.bank_code if opts else "1234",
                )
                t0 = time.perf_counter()
                try:
                    resp = client.request(msg)
                    latencies.append((time.perf_counter() - t0) * 1000)
                    ok += 1
                    if verbose:
                        print(f"  [conn {conn_no}] -> {msg.decode()}")
                        print(f"  [conn {conn_no}] <- {resp.decode()}")
                except (OSError, ConnectionError, ValueError) as e:
                    err += 1
                    print(f"  [conn {conn_no}] request failed: {e}", file=sys.stderr)
                    break
    except OSError as e:
        print(f"[conn {conn_no}] connect to {host}:{port} failed: {e}", file=sys.stderr)
        err += 1
    return ok, err, latencies


def main() -> None:
    ap = argparse.ArgumentParser(description="Send ISO-8583-style traffic through the interceptor")
    ap.add_argument("--target", default="127.0.0.1:9100",
                    help="interceptor address (point at FMS directly to compare)")
    ap.add_argument("--count", type=int, default=5, help="messages per connection")
    ap.add_argument("--connections", type=int, default=1, help="parallel connections")
    ap.add_argument("--quiet", action="store_true", help="suppress per-message output")
    ap.add_argument("--account-name", default="Zacky")
    ap.add_argument("--account-number", default="11020134353")
    ap.add_argument("--bank-code", default="1234")
    opts = ap.parse_args()

    host, port = opts.target.rsplit(":", 1)
    port = int(port)
    verbose = not opts.quiet and opts.connections * opts.count <= 50

    print(f"sending {opts.count} msg x {opts.connections} conn -> {host}:{port}\n")

    results: list[tuple[int, int, list[float]]] = []
    lock = threading.Lock()

    def work(n: int) -> None:
        r = run_one_connection(n, host, port, opts.count, verbose, opts)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(opts.connections)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    ok = sum(r[0] for r in results)
    err = sum(r[1] for r in results)
    lat = sorted(x for r in results for x in r[2])

    print(f"\nok={ok} errors={err} wall={wall:.2f}s tps={ok / wall:.0f}")
    if lat:
        print(f"latency p50={lat[len(lat) // 2]:.3f}ms  "
              f"p99={lat[min(len(lat) - 1, int(len(lat) * 0.99))]:.3f}ms  "
              f"max={lat[-1]:.3f}ms")
    sys.exit(1 if err else 0)


if __name__ == "__main__":
    main()
