#!/usr/bin/env python3
"""Mock Vynamic Payments client / load generator.

Opens N parallel TCP connections, drives request/response traffic, and reports
latency percentiles. Every response is compared byte-for-byte against what was
sent, so stream corruption or reordering by the interceptor shows up as a
`mismatch`, not as a silently-passing benchmark.

    # baseline: straight at FMS
    python3 vp_client.py --target 127.0.0.1:8583 --connections 16 --duration 10 --label direct

    # through the interceptor
    python3 vp_client.py --target 127.0.0.1:9100 --connections 16 --duration 10 --label intercepted

    # several frames per TCP segment, exercising the reassembler
    python3 vp_client.py --target 127.0.0.1:9100 --pipeline 8
"""

import argparse
import json
import socket
import sys
import threading
import time

from protocol import build_request, encode, parse_stan, read_frame, to_response


class Result:
    def __init__(self) -> None:
        self.latencies_ns: list[int] = []
        self.errors = 0
        self.mismatches = 0
        self.connect_errors = 0


def percentile(sorted_vals: list[int], q: float) -> float:
    if not sorted_vals:
        return float("nan")
    # Nearest-rank; with >=1000 samples this is accurate enough for p99.9 and
    # avoids interpolating between two real measurements.
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * len(sorted_vals) + 0.5)) - 1))
    return sorted_vals[idx]


def worker(conn_id: int, opts, clock: dict, result: Result, start_gate: threading.Event) -> None:
    host, port = opts.target.rsplit(":", 1)
    try:
        sock = socket.create_connection((host, int(port)), timeout=opts.timeout)
    except OSError as e:
        result.connect_errors += 1
        print(f"[vp] conn {conn_id} connect failed: {e}", file=sys.stderr)
        return

    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(opts.timeout)

    stan = conn_id * 1_000_000
    sent = 0
    interval = 1.0 / opts.rate if opts.rate else 0.0
    next_send = time.perf_counter()

    # Connections are all established before the gate opens, so connect cost
    # never lands in the latency histogram.
    start_gate.wait()

    try:
        while time.time() < clock["deadline"] and (not opts.count or sent < opts.count):
            batch = []
            for _ in range(opts.pipeline):
                stan += 1
                body = build_request(stan, conn_id, opts.pad)
                batch.append(body)

            if interval:
                now = time.perf_counter()
                if next_send > now:
                    time.sleep(next_send - now)
                next_send += interval * opts.pipeline

            # One sendall for the whole batch: with pipeline > 1 this puts
            # multiple frames into a single segment, which is exactly the case
            # that breaks naive framing code.
            t0 = time.perf_counter_ns()
            sock.sendall(b"".join(encode(b) for b in batch))

            for body in batch:
                resp = read_frame(sock)
                t1 = time.perf_counter_ns()
                if resp != to_response(body):
                    result.mismatches += 1
                    if result.mismatches <= 3:
                        print(
                            f"[vp] MISMATCH conn={conn_id} stan={parse_stan(body)}\n"
                            f"     sent: {body!r}\n     recv: {resp!r}",
                            file=sys.stderr,
                        )
                else:
                    result.latencies_ns.append(t1 - t0)
                sent += 1
    except (OSError, ConnectionError, ValueError) as e:
        result.errors += 1
        if result.errors <= 3:
            print(f"[vp] conn {conn_id} error after {sent} msgs: {e}", file=sys.stderr)
    finally:
        try:
            sock.close()
        except OSError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="VP load generator")
    ap.add_argument("--target", default="127.0.0.1:9100", help="host:port of interceptor or FMS")
    ap.add_argument("--connections", type=int, default=8, help="parallel TCP connections")
    ap.add_argument("--duration", type=float, default=10.0, help="seconds")
    ap.add_argument("--count", type=int, default=0, help="messages per connection (overrides duration)")
    ap.add_argument("--rate", type=float, default=0.0, help="msgs/sec per connection; 0 = unthrottled")
    ap.add_argument("--pipeline", type=int, default=1, help="frames per send() batch")
    ap.add_argument("--pad", type=int, default=0, help="pad request body to this many bytes")
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--label", default="run")
    ap.add_argument("--json", action="store_true", help="emit machine-readable summary")
    opts = ap.parse_args()

    results = [Result() for _ in range(opts.connections)]
    gate = threading.Event()
    clock = {"deadline": float("inf")}

    threads = [
        threading.Thread(target=worker, args=(i, opts, clock, results[i], gate), daemon=True)
        for i in range(opts.connections)
    ]
    for t in threads:
        t.start()

    time.sleep(0.2)  # let every connection establish before the clock starts
    wall_start = time.perf_counter()
    clock["deadline"] = time.time() + opts.duration
    gate.set()

    for t in threads:
        t.join(timeout=opts.duration + opts.timeout + 5)
    wall = time.perf_counter() - wall_start

    lat = sorted(x for r in results for x in r.latencies_ns)
    errors = sum(r.errors for r in results)
    mismatches = sum(r.mismatches for r in results)
    connect_errors = sum(r.connect_errors for r in results)

    def ms(ns: float) -> float:
        return round(ns / 1e6, 3)

    summary = {
        "label": opts.label,
        "target": opts.target,
        "connections": opts.connections,
        "pipeline": opts.pipeline,
        "ok": len(lat),
        "errors": errors,
        "mismatches": mismatches,
        "connect_errors": connect_errors,
        "wall_s": round(wall, 2),
        "tps": round(len(lat) / wall, 1) if wall > 0 else 0,
        "p50_ms": ms(percentile(lat, 0.50)),
        "p90_ms": ms(percentile(lat, 0.90)),
        "p99_ms": ms(percentile(lat, 0.99)),
        "p999_ms": ms(percentile(lat, 0.999)),
        "max_ms": ms(lat[-1]) if lat else float("nan"),
    }

    if opts.json:
        print(json.dumps(summary))
    else:
        print(f"\n=== {opts.label} -> {opts.target} ===")
        print(f"  connections {opts.connections}   pipeline {opts.pipeline}   wall {summary['wall_s']}s")
        print(f"  ok {summary['ok']}   tps {summary['tps']}")
        print(f"  errors {errors}   mismatches {mismatches}   connect_errors {connect_errors}")
        print(f"  p50   {summary['p50_ms']} ms")
        print(f"  p90   {summary['p90_ms']} ms")
        print(f"  p99   {summary['p99_ms']} ms")
        print(f"  p99.9 {summary['p999_ms']} ms")
        print(f"  max   {summary['max_ms']} ms")

    if mismatches:
        print("\nFAIL: payload corruption detected", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
