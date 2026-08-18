#!/usr/bin/env python3
"""Rust vs Java interceptor: latency, memory, and concurrency.

Both implementations read the SAME config.toml and are driven by the SAME load
generator against the SAME mock FMS, so the only variable is the runtime.

    python3 benchmark/compare.py
    python3 benchmark/compare.py --duration 15 --conns 8,64,256

Methodology notes that materially affect the numbers:

  * Both builds get an identical warm-up before any measurement. Without it the
    JVM's JIT makes the first seconds meaningless (observed: 353 ms on the first
    frame through the tee, versus sub-millisecond once warm).
  * Latency is reported as a DELTA against talking to FMS directly, measured in
    the same run. Absolute numbers are dominated by the Python mock.
  * Runs are rate-limited. Unthrottled runs measure the Python load generator,
    not the interceptor.
  * RSS is the honest memory number for "how much does this cost to run".
    JVM heap usage is not comparable to Rust's, but RSS is.
"""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTKIT = ROOT / "testkit"

RUST_BIN = ROOT / "rust-interceptor/target/release/vp-fms-interceptor"
JAVA_JAR = ROOT / "java-interceptor/target/vp-fms-interceptor.jar"

FMS_PORT = 8583
LISTEN_PORT = 9100
ADMIN_PORT = 9101


def wait_port(port: int, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def wait_healthz(timeout: float = 40.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            out = subprocess.run(
                ["curl", "-s", "--max-time", "1", f"http://127.0.0.1:{ADMIN_PORT}/healthz"],
                capture_output=True, text=True, timeout=3)
            if out.stdout.strip() == "ok":
                return True
        except Exception:
            pass
        time.sleep(0.05)
    return False


def rss_mb(pid: int) -> float:
    """Resident set size. Includes JVM overhead, which is the point."""
    try:
        out = subprocess.run(["ps", "-p", str(pid), "-o", "rss="],
                             capture_output=True, text=True).stdout.strip()
        return int(out) / 1024.0
    except Exception:
        return float("nan")


def cpu_seconds(pid: int) -> float:
    """Cumulative CPU time across all threads."""
    try:
        out = subprocess.run(["ps", "-p", str(pid), "-o", "time="],
                             capture_output=True, text=True).stdout.strip()
        m = re.match(r"(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$", out)
        if not m:
            return float("nan")
        days, hours, mins, secs = m.groups()
        return ((int(days or 0) * 86400) + (int(hours or 0) * 3600)
                + int(mins) * 60 + float(secs))
    except Exception:
        return float("nan")


def load(target_port: int, conns: int, duration: float, rate: float, label: str) -> dict:
    cmd = [sys.executable, str(TESTKIT / "vp_client.py"),
           "--target", f"127.0.0.1:{target_port}",
           "--connections", str(conns),
           "--duration", str(duration),
           "--label", label, "--json"]
    if rate:
        cmd += ["--rate", str(rate)]
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=str(TESTKIT))
    line = (p.stdout or "").strip().splitlines()
    if not line:
        raise RuntimeError(f"load generator produced nothing: {p.stderr[:400]}")
    return json.loads(line[-1])


class Impl:
    def __init__(self, name, cmd, env=None):
        self.name = name
        self.cmd = cmd
        self.env = env
        self.proc = None
        self.startup_ms = None

    def start(self, config_path: str):
        env = dict(os.environ)
        if self.env:
            env.update(self.env)
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(
            self.cmd + [config_path],
            stdout=open(f"/tmp/bench_{self.name}.log", "w"),
            stderr=subprocess.STDOUT, env=env, cwd=str(ROOT))
        if not wait_healthz():
            raise RuntimeError(f"{self.name} never became healthy; see /tmp/bench_{self.name}.log")
        self.startup_ms = (time.perf_counter() - t0) * 1000

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        # the port must be free before the next implementation binds it
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", LISTEN_PORT), timeout=0.2):
                    time.sleep(0.1)
            except OSError:
                return
        time.sleep(1)


def fmt(v, nd=3):
    return "n/a" if v is None or v != v else f"{v:.{nd}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=12.0)
    ap.add_argument("--rate", type=float, default=400.0, help="per-connection msg/s for the latency test")
    ap.add_argument("--conns", default="8,64,256")
    ap.add_argument("--warmup", type=float, default=15.0)
    ap.add_argument("--out", default=str(ROOT / "benchmark/results.json"))
    opts = ap.parse_args()

    conn_levels = [int(c) for c in opts.conns.split(",")]

    if not RUST_BIN.exists():
        sys.exit(f"missing {RUST_BIN} -- run: cargo build --release -p vp-fms-interceptor\n                 (cd rust-interceptor && cargo build --release)")
    if not JAVA_JAR.exists():
        sys.exit(f"missing {JAVA_JAR} -- run: mvn -f java-interceptor/pom.xml package")

    # config with a per-implementation topic, everything else identical
    base_cfg = (ROOT / "config.toml").read_text()

    impls = [
        Impl("rust", [str(RUST_BIN)]),
        Impl("java", ["java", "-jar", str(JAVA_JAR)]),
    ]

    if not wait_port(FMS_PORT, timeout=2):
        sys.exit(f"mock FMS is not listening on {FMS_PORT}. Start it:\n"
                 f"  cd testkit && python3 mock_fms.py --port {FMS_PORT} &")

    results = {"config": vars(opts), "direct": {}, "impls": {}}

    print("=" * 74)
    print("BASELINE: direct to FMS (no interceptor)")
    print("=" * 74)
    for c in conn_levels:
        r = load(FMS_PORT, c, opts.duration, opts.rate, f"direct-{c}")
        results["direct"][c] = r
        print(f"  {c:>4} conns   tps {r['tps']:>8.0f}   p50 {r['p50_ms']:>7.3f}   "
              f"p99 {r['p99_ms']:>7.3f}   errors {r['errors']}")

    for impl in impls:
        cfg_path = f"/tmp/bench_{impl.name}.toml"
        Path(cfg_path).write_text(
            base_cfg.replace('topic = "vp.fms.iso8583"', f'topic = "vp.fms.bench.{impl.name}"'))

        print()
        print("=" * 74)
        print(f"IMPLEMENTATION: {impl.name}")
        print("=" * 74)

        impl.start(cfg_path)
        pid = impl.proc.pid
        time.sleep(2)
        idle_rss = rss_mb(pid)
        print(f"  startup to healthy : {impl.startup_ms:.0f} ms")
        print(f"  RSS idle           : {idle_rss:.1f} MB")

        print(f"  warming up {opts.warmup:.0f}s ...", flush=True)
        load(LISTEN_PORT, 8, opts.warmup, 0, "warmup")
        warm_rss = rss_mb(pid)
        cpu_before = cpu_seconds(pid)
        print(f"  RSS after warmup   : {warm_rss:.1f} MB")

        entry = {"startup_ms": impl.startup_ms, "rss_idle_mb": idle_rss,
                 "rss_warm_mb": warm_rss, "levels": {}, "pid": pid}

        msgs_total = 0
        for c in conn_levels:
            r = load(LISTEN_PORT, c, opts.duration, opts.rate, f"{impl.name}-{c}")
            msgs_total += r["ok"]
            d = results["direct"][c]
            entry["levels"][c] = {
                "tps": r["tps"], "ok": r["ok"], "errors": r["errors"],
                "mismatches": r["mismatches"],
                "p50_ms": r["p50_ms"], "p99_ms": r["p99_ms"], "p999_ms": r["p999_ms"],
                "d_p50": r["p50_ms"] - d["p50_ms"],
                "d_p99": r["p99_ms"] - d["p99_ms"],
                "rss_mb": rss_mb(pid),
            }
            e = entry["levels"][c]
            print(f"  {c:>4} conns   tps {r['tps']:>8.0f}   p50 {r['p50_ms']:>7.3f} "
                  f"(d {e['d_p50']:+.3f})   p99 {r['p99_ms']:>7.3f} (d {e['d_p99']:+.3f})   "
                  f"RSS {e['rss_mb']:>6.1f}MB   err {r['errors']} mism {r['mismatches']}")

        cpu_after = cpu_seconds(pid)
        entry["rss_peak_mb"] = max(v["rss_mb"] for v in entry["levels"].values())
        entry["cpu_seconds"] = cpu_after - cpu_before
        entry["messages"] = msgs_total
        entry["cpu_ms_per_1k_msgs"] = (
            (cpu_after - cpu_before) * 1000 / (msgs_total / 1000) if msgs_total else float("nan"))

        # counters straight from the interceptor
        try:
            m = subprocess.run(["curl", "-s", f"http://127.0.0.1:{ADMIN_PORT}/metrics"],
                               capture_output=True, text=True, timeout=5).stdout
            entry["metrics"] = {
                k: int(v) for k, v in re.findall(r"^vp_interceptor_(\w+) (\d+)$", m, re.M)}
        except Exception:
            entry["metrics"] = {}

        print(f"  CPU for run        : {entry['cpu_seconds']:.2f} s "
              f"({entry['cpu_ms_per_1k_msgs']:.1f} ms per 1k msgs)")
        print(f"  RSS peak           : {entry['rss_peak_mb']:.1f} MB")

        results["impls"][impl.name] = entry
        impl.stop()
        time.sleep(2)

    Path(opts.out).write_text(json.dumps(results, indent=2))
    summary(results, conn_levels)
    print(f"\nraw results -> {opts.out}")


def summary(results, conn_levels):
    r = results["impls"].get("rust", {})
    j = results["impls"].get("java", {})
    if not r or not j:
        return

    print()
    print("=" * 74)
    print("SUMMARY  (lower is better everywhere)")
    print("=" * 74)
    print(f"{'metric':<34}{'Rust':>13}{'Java':>13}{'ratio':>12}")
    print("-" * 74)

    def row(label, rv, jv, nd=1, unit=""):
        if rv is None or jv is None or rv != rv or jv != jv:
            return
        ratio = (jv / rv) if rv else float("inf")
        print(f"{label:<34}{rv:>13.{nd}f}{jv:>13.{nd}f}{ratio:>11.1f}x")

    row("startup to healthy (ms)", r.get("startup_ms"), j.get("startup_ms"), 0)
    row("RSS idle (MB)", r.get("rss_idle_mb"), j.get("rss_idle_mb"))
    row("RSS after warmup (MB)", r.get("rss_warm_mb"), j.get("rss_warm_mb"))
    row("RSS peak under load (MB)", r.get("rss_peak_mb"), j.get("rss_peak_mb"))
    row("CPU ms per 1k messages", r.get("cpu_ms_per_1k_msgs"), j.get("cpu_ms_per_1k_msgs"), 1)

    print("-" * 74)
    for c in conn_levels:
        rl, jl = r["levels"].get(c), j["levels"].get(c)
        if not rl or not jl:
            continue
        print(f"{('p50 delta @ ' + str(c) + ' conns (ms)'):<34}"
              f"{rl['d_p50']:>13.3f}{jl['d_p50']:>13.3f}")
        print(f"{('p99 delta @ ' + str(c) + ' conns (ms)'):<34}"
              f"{rl['d_p99']:>13.3f}{jl['d_p99']:>13.3f}")
        print(f"{('throughput @ ' + str(c) + ' conns (tps)'):<34}"
              f"{rl['tps']:>13.0f}{jl['tps']:>13.0f}")
        print("-" * 74)


if __name__ == "__main__":
    main()
