#!/usr/bin/env python3
"""Mock Fraud Management System.

Accepts length-prefixed frames and replies with a response frame. Supports
injected latency and failure modes so the interceptor can be exercised against
an FMS that is slow, flaky, or rude about closing connections.

    python3 mock_fms.py --port 8583
    python3 mock_fms.py --port 8583 --delay-ms 5 --jitter-ms 3
    python3 mock_fms.py --port 8583 --drop-rate 0.01 --reset-rate 0.001
"""

import argparse
import random
import socket
import socketserver
import threading
import time

from protocol import encode, read_frame, to_response

STATE = threading.local()
COUNTERS = {"conns": 0, "frames": 0, "dropped": 0, "reset": 0}
COUNTERS_LOCK = threading.Lock()


def bump(key: str, n: int = 1) -> None:
    with COUNTERS_LOCK:
        COUNTERS[key] += n


class Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        opts = self.server.opts
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        bump("conns")
        rng = random.Random(threading.get_ident())

        try:
            while True:
                try:
                    body = read_frame(self.request)
                except (ConnectionError, OSError, ValueError):
                    return

                bump("frames")

                if opts.reset_rate and rng.random() < opts.reset_rate:
                    # Abortive close (RST) -- the nastiest thing a real FMS does.
                    bump("reset")
                    self.request.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct_linger()
                    )
                    return

                if opts.drop_rate and rng.random() < opts.drop_rate:
                    bump("dropped")
                    continue  # swallow the request, never answer

                if opts.delay_ms or opts.jitter_ms:
                    delay = opts.delay_ms + (rng.random() * opts.jitter_ms)
                    time.sleep(delay / 1000.0)

                self.request.sendall(encode(to_response(body)))
        finally:
            try:
                self.request.close()
            except OSError:
                pass


def struct_linger() -> bytes:
    import struct

    return struct.pack("ii", 1, 0)  # l_onoff=1, l_linger=0 -> RST on close


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    # socketserver defaults this to 5. VP opens its connection pool all at once,
    # so a small backlog gets the surplus SYNs reset and shows up as "broken
    # pipe on first send" -- which looks like a proxy bug and is not one.
    request_queue_size = 512


def report_loop(interval: float) -> None:
    last = dict(COUNTERS)
    while True:
        time.sleep(interval)
        with COUNTERS_LOCK:
            now = dict(COUNTERS)
        rate = (now["frames"] - last["frames"]) / interval
        print(
            f"[fms] conns={now['conns']} frames={now['frames']} "
            f"({rate:.0f}/s) dropped={now['dropped']} reset={now['reset']}",
            flush=True,
        )
        last = now


def main() -> None:
    ap = argparse.ArgumentParser(description="Mock FMS endpoint")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8583)
    ap.add_argument("--delay-ms", type=float, default=0.0, help="fixed processing delay")
    ap.add_argument("--jitter-ms", type=float, default=0.0, help="extra uniform random delay")
    ap.add_argument("--drop-rate", type=float, default=0.0, help="fraction of requests silently ignored")
    ap.add_argument("--reset-rate", type=float, default=0.0, help="fraction of requests answered with a TCP RST")
    ap.add_argument("--report-interval", type=float, default=5.0)
    opts = ap.parse_args()

    server = Server((opts.host, opts.port), Handler)
    server.opts = opts

    threading.Thread(target=report_loop, args=(opts.report_interval,), daemon=True).start()

    print(
        f"[fms] listening on {opts.host}:{opts.port} "
        f"delay={opts.delay_ms}ms jitter={opts.jitter_ms}ms "
        f"drop={opts.drop_rate} reset={opts.reset_rate}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[fms] shutting down", flush=True)
        server.shutdown()


if __name__ == "__main__":
    main()
