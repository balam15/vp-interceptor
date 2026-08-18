#!/usr/bin/env python3
"""Send real ISO 8583 traffic through the interceptor, per the EIS_FMS spec.

This is send_payment.py's sibling. That one sends a `key=value` stand-in; this
one sends messages built from EIS_FMS_ISO8583_jPOS_Packager.xml.

    python3 send_iso8583.py --target 127.0.0.1:9100
    python3 send_iso8583.py --target 127.0.0.1:9100 --count 100
    python3 send_iso8583.py --target 127.0.0.1:9100 --connections 8 --count 50
    python3 send_iso8583.py --target 127.0.0.1:9100 --template echo --dump

COPY BOTH FILES. Unlike send_payment.py, this is not self-contained: it imports
the codec from iso8583.py sitting next to it. The framing class below is still
duplicated rather than imported, so the two examples stay independent.

THE FRAMING MUST MATCH THE INTERCEPTOR'S [framing] CONFIG.
Defaults here mirror the shipped config.toml: 2-byte big-endian length prefix,
length excludes the prefix itself. If your real VP<->FMS link differs, change
the constructor arguments AND config.toml together -- if they disagree, traffic
still reaches FMS correctly but the Kafka feed will desync and stop publishing.

TWO THINGS TO EXPECT WHEN RUNNING THIS LOCALLY

1. testkit/mock_fms.py is not ISO-aware. It echoes the request body with
   `,echo=1,responseCode=00` appended, so replies come back as your 0200 plus
   trailing junk. That is the mock, not a bug here -- this client reports such
   replies as `echo` instead of failing to parse them.

2. The interceptor ships with [parse] mode = "key_value". That parser is
   deliberately literal, so it will "parse" an ISO frame into nonsense fields
   and still report parse_error: null. Set mode = "none" in config.toml before
   reading anything out of the envelope's `fields` key.

THE DATA IS SYNTHETIC BUT THE SHAPE IS NOT. DE 35 carries full track 2, exactly
as the spec says VP sends it. Track 2 is Sensitive Authentication Data under
PCI DSS 3.2 and must not be retained after authorization -- so once this
traffic reaches Kafka, go look at what is sitting in the topic.
"""

import argparse
import socket
import struct
import sys
import threading
import time

from iso8583 import FieldError, Message, TEMPLATES, transfer


class VPClient:
    """Length-prefixed request/response client.

    Usage:
        with VPClient("127.0.0.1", 9100) as c:
            response = c.request(msg.pack())
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


def describe_response(sent: bytes, raw: bytes) -> str:
    """One line describing what came back.

    A real FMS answers with a 0210 this codec can read. The local mock echoes
    instead, so distinguish the two rather than treating an echo as an error --
    otherwise every local run looks like a total failure.
    """
    if raw.startswith(sent):
        extra = raw[len(sent):]
        return f"echo (mock FMS, not ISO-aware) +{len(extra)} bytes {extra[:40]!r}"
    try:
        msg = Message.unpack(raw)
    except (FieldError, UnicodeDecodeError) as e:
        return f"unparsed ({e})"
    rc = msg.fields.get(39, "--")
    auth = msg.fields.get(38, "")
    return f"MTI {msg.mti} DE39={rc}" + (f" DE38={auth}" if auth else "")


def run_one_connection(conn_no: int, host: str, port: int, count: int, verbose: bool,
                       opts=None) -> tuple[int, int, list[float]]:
    ok = err = 0
    latencies: list[float] = []
    build = TEMPLATES[opts.template] if opts else transfer
    try:
        with VPClient(host, port) as client:
            for i in range(count):
                stan = (conn_no * 100000 + i + 1) % 1000000
                try:
                    body = build(stan).pack()
                except FieldError as e:
                    print(f"  [conn {conn_no}] could not build message: {e}", file=sys.stderr)
                    err += 1
                    break
                t0 = time.perf_counter()
                try:
                    resp = client.request(body)
                    latencies.append((time.perf_counter() - t0) * 1000)
                    ok += 1
                    if verbose:
                        print(f"  [conn {conn_no}] -> {len(body)} bytes  {body[:60].decode('ascii')}...")
                        print(f"  [conn {conn_no}] <- {describe_response(body, resp)}")
                except (OSError, ConnectionError, ValueError) as e:
                    err += 1
                    print(f"  [conn {conn_no}] request failed: {e}", file=sys.stderr)
                    break
    except OSError as e:
        print(f"[conn {conn_no}] connect to {host}:{port} failed: {e}", file=sys.stderr)
        err += 1
    return ok, err, latencies


def main() -> None:
    ap = argparse.ArgumentParser(description="Send EIS_FMS ISO 8583 traffic through the interceptor")
    ap.add_argument("--target", default="127.0.0.1:9100",
                    help="interceptor address (point at FMS directly to compare)")
    ap.add_argument("--count", type=int, default=5, help="messages per connection")
    ap.add_argument("--connections", type=int, default=1, help="parallel connections")
    ap.add_argument("--template", choices=sorted(TEMPLATES), default="transfer")
    ap.add_argument("--quiet", action="store_true", help="suppress per-message output")
    ap.add_argument("--dump", action="store_true",
                    help="print the field breakdown of one message and exit without sending")
    opts = ap.parse_args()

    if opts.dump:
        TEMPLATES[opts.template](1).dump()
        return

    host, port = opts.target.rsplit(":", 1)
    port = int(port)
    verbose = not opts.quiet and opts.connections * opts.count <= 50

    sample = TEMPLATES[opts.template](1).pack()
    print(f"sending {opts.count} x {opts.template} ({len(sample)} bytes) "
          f"x {opts.connections} conn -> {host}:{port}\n")

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
