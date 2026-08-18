#!/usr/bin/env python3
"""Reference passthrough proxy -- VP <-> FMS with no Kafka.

This is NOT the deliverable. It exists so the harness can be validated before
the real interceptor builds, and so there is a floor to compare against: any
proxy pays a kernel-hop cost, and this measures that cost alone. The Rust
interceptor should land close to this line, and the gap between them is the
true price of the Kafka tee.

    python3 null_proxy.py --listen 127.0.0.1:9100 --upstream 127.0.0.1:8583

--tee-framing additionally reassembles frames off the hot path (counting only,
no publish), mirroring the real interceptor's structure so the framing config
can be validated against real VP traffic before Kafka is in the picture.
"""

import argparse
import asyncio
import struct
import sys
import time

PREFIX = struct.Struct(">H")

STATS = {"conns": 0, "active": 0, "b_v2f": 0, "b_f2v": 0, "frames": 0, "desyncs": 0}


class Framer:
    """Same contract as the Rust Framer: fed off the hot path, never blocking."""

    def __init__(self, max_frame: int) -> None:
        self.buf = bytearray()
        self.max_frame = max_frame
        self.desynced = False

    def push(self, chunk: bytes) -> int:
        if self.desynced:
            return 0
        self.buf += chunk
        n = 0
        while len(self.buf) >= PREFIX.size:
            (length,) = PREFIX.unpack_from(self.buf, 0)
            if length == 0 or length > self.max_frame:
                self.desynced = True
                STATS["desyncs"] += 1
                print(f"[proxy] framing desync: length={length}", file=sys.stderr, flush=True)
                return n
            if len(self.buf) < PREFIX.size + length:
                break
            del self.buf[: PREFIX.size + length]
            n += 1
        return n


async def pump(reader, writer, counter: str, framer, bufsize: int) -> None:
    try:
        while True:
            chunk = await reader.read(bufsize)
            if not chunk:
                break
            # Forward first. Always.
            writer.write(chunk)
            await writer.drain()
            STATS[counter] += len(chunk)
            # Then tee.
            if framer is not None:
                STATS["frames"] += framer.push(chunk)
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.write_eof()
        except (OSError, RuntimeError):
            pass


async def handle(client_r, client_w, opts) -> None:
    STATS["conns"] += 1
    STATS["active"] += 1
    up_host, up_port = opts.upstream.rsplit(":", 1)
    try:
        up_r, up_w = await asyncio.wait_for(
            asyncio.open_connection(up_host, int(up_port)), timeout=opts.connect_timeout
        )
    except (OSError, asyncio.TimeoutError) as e:
        print(f"[proxy] upstream connect failed: {e}", file=sys.stderr, flush=True)
        client_w.close()
        STATS["active"] -= 1
        return

    for sock in (client_w.get_extra_info("socket"), up_w.get_extra_info("socket")):
        if sock is not None:
            import socket as _s

            sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_NODELAY, 1)

    f_v2f = Framer(opts.max_frame) if opts.tee_framing else None
    f_f2v = Framer(opts.max_frame) if opts.tee_framing else None

    await asyncio.gather(
        pump(client_r, up_w, "b_v2f", f_v2f, opts.bufsize),
        pump(up_r, client_w, "b_f2v", f_f2v, opts.bufsize),
    )

    for w in (client_w, up_w):
        try:
            w.close()
        except OSError:
            pass
    STATS["active"] -= 1


async def report(interval: float) -> None:
    last = dict(STATS)
    while True:
        await asyncio.sleep(interval)
        now = dict(STATS)
        print(
            f"[proxy] conns={now['conns']} active={now['active']} "
            f"v2f={now['b_v2f']}B f2v={now['b_f2v']}B "
            f"frames={now['frames']} (+{now['frames'] - last['frames']}) "
            f"desyncs={now['desyncs']}",
            flush=True,
        )
        last = now


async def main() -> None:
    ap = argparse.ArgumentParser(description="Reference passthrough proxy")
    ap.add_argument("--listen", default="127.0.0.1:9100")
    ap.add_argument("--upstream", default="127.0.0.1:8583")
    ap.add_argument("--bufsize", type=int, default=16384)
    ap.add_argument("--connect-timeout", type=float, default=3.0)
    ap.add_argument("--tee-framing", action="store_true", help="reassemble frames off the hot path")
    ap.add_argument("--max-frame", type=int, default=65536)
    ap.add_argument("--report-interval", type=float, default=5.0)
    opts = ap.parse_args()

    host, port = opts.listen.rsplit(":", 1)
    server = await asyncio.start_server(lambda r, w: handle(r, w, opts), host, int(port))
    print(
        f"[proxy] listening on {opts.listen} -> {opts.upstream} "
        f"tee_framing={opts.tee_framing}",
        flush=True,
    )
    asyncio.create_task(report(opts.report_interval))
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[proxy] shutting down", flush=True)
