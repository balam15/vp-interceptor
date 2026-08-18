#!/usr/bin/env python3
"""A/B latency benchmark: direct-to-FMS versus through the interceptor.

The only number that matters for this project is the DELTA between the two.
Absolute latency here is dominated by the mock FMS and the Python client; the
difference between the two runs is the interceptor's actual cost.

    python3 bench.py --fms 127.0.0.1:8583 --interceptor 127.0.0.1:9100

Run rate-limited (the default). At saturation you measure the load generator,
not the interceptor.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
FIELDS = [
    ("p50_ms", "p50"),
    ("p90_ms", "p90"),
    ("p99_ms", "p99"),
    ("p999_ms", "p99.9"),
    ("max_ms", "max"),
]


def run(target: str, label: str, opts) -> dict:
    cmd = [
        sys.executable, str(HERE / "vp_client.py"),
        "--target", target,
        "--connections", str(opts.connections),
        "--duration", str(opts.duration),
        "--pipeline", str(opts.pipeline),
        "--label", label,
        "--json",
    ]
    if opts.rate:
        cmd += ["--rate", str(opts.rate)]
    if opts.pad:
        cmd += ["--pad", str(opts.pad)]

    print(f"  running {label} -> {target} ...", flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 2:
        print(proc.stderr, file=sys.stderr)
        sys.exit("FAIL: payload corruption detected -- interceptor is altering the stream")
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        sys.exit(f"load generator failed for {label}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main() -> None:
    ap = argparse.ArgumentParser(description="A/B interceptor latency benchmark")
    ap.add_argument("--fms", default="127.0.0.1:8583", help="direct FMS address (baseline)")
    ap.add_argument("--interceptor", default="127.0.0.1:9100", help="interceptor address")
    ap.add_argument("--connections", type=int, default=8)
    ap.add_argument("--rate", type=float, default=500.0, help="msgs/sec per connection; 0 = unthrottled")
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--pipeline", type=int, default=1)
    ap.add_argument("--pad", type=int, default=0)
    ap.add_argument("--warmup", type=float, default=2.0)
    ap.add_argument("--budget-ms", type=float, default=1.0,
                    help="max acceptable p99 increase vs direct; exceeding it fails the run")
    opts = ap.parse_args()

    if opts.warmup:
        print(f"warmup {opts.warmup}s ...", flush=True)
        w = argparse.Namespace(**vars(opts))
        w.duration = opts.warmup
        run(opts.interceptor, "warmup", w)

    print(f"\nbenchmark: {opts.connections} conns, "
          f"rate={opts.rate or 'unthrottled'}/conn, {opts.duration}s, pipeline={opts.pipeline}\n")

    a = run(opts.fms, "direct", opts)
    b = run(opts.interceptor, "intercepted", opts)

    print()
    print(f"{'':>8}  {'direct':>10}  {'intercepted':>12}  {'delta':>10}")
    print("  " + "-" * 46)
    for key, name in FIELDS:
        d = b[key] - a[key]
        print(f"{name:>8}  {a[key]:>10.3f}  {b[key]:>12.3f}  {d:>+10.3f} ms")
    print("  " + "-" * 46)
    print(f"{'tps':>8}  {a['tps']:>10.1f}  {b['tps']:>12.1f}")
    print(f"{'ok':>8}  {a['ok']:>10}  {b['ok']:>12}")
    print(f"{'errors':>8}  {a['errors']:>10}  {b['errors']:>12}")
    print(f"{'corrupt':>8}  {a['mismatches']:>10}  {b['mismatches']:>12}")

    problems = []
    if b["mismatches"]:
        problems.append("stream corruption through the interceptor")
    if b["errors"] > a["errors"]:
        problems.append("interceptor introduced connection errors")
    if b["p99_ms"] - a["p99_ms"] > opts.budget_ms:
        problems.append(f"p99 delta {b['p99_ms'] - a['p99_ms']:.3f}ms exceeds budget {opts.budget_ms}ms")

    print()
    if problems:
        for p in problems:
            print(f"  FAIL: {p}")
        sys.exit(1)
    print("  PASS")


if __name__ == "__main__":
    main()
