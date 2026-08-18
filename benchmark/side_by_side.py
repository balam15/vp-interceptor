#!/usr/bin/env python3
"""Rust vs Java under load AT THE SAME TIME: memory, CPU time, throughput.

`compare.py` runs each implementation in turn. This script runs BOTH AT ONCE, on
different ports, against the same mock FMS, driven by two identical load
generators started together.

    python3 benchmark/side_by_side.py
    python3 benchmark/side_by_side.py --duration 20 --conns 8,64

WHY BOTH AT ONCE -- and what it costs you
-----------------------------------------
Sequential runs have one confound this removes: conditions drift between run A
and run B. Thermal state, background processes, page cache, broker load and CPU
frequency all change over the four minutes `compare.py` takes. Running the two
implementations concurrently means every sample of each is taken under the
*identical* ambient conditions, so the RATIO between them is trustworthy.

The cost is that they now compete for cores, and so do the two load generators.
Therefore:

  * ABSOLUTE numbers here are NOT comparable to compare.py's. Throughput will be
    lower and CPU-per-message slightly higher for both. Do not mix the two
    scripts' figures in one table.
  * RATIOS (Rust vs Java memory, CPU, throughput) are the output that means
    something, and they are more trustworthy than sequential ratios.
  * LATENCY is reported for completeness but is contended and should not be
    quoted. Use compare.py for latency, and read ../benchmark/README.md sec 6 on
    what is signal and what is noise on this rig.

If the machine has fewer cores than (rust threads + jvm threads + 2 python load
generators) needs, you are measuring the scheduler. The script prints the core
count and a warning when the level's connection count looks too high for it.

PORTS
-----
    Rust   listen 9100   admin 9101
    Java   listen 9200   admin 9201
    mock FMS 8583 (shared upstream -- must already be running)
"""

import argparse
import atexit
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTKIT = ROOT / "testkit"

RUST_BIN = ROOT / "rust-interceptor/target/release/vp-fms-interceptor"
JAVA_JAR = ROOT / "java-interceptor/target/vp-fms-interceptor.jar"

FMS_PORT = 8583

# Each implementation gets its own listen/admin pair so both can run at once.
PORTS = {
    "rust": {"listen": 9100, "admin": 9101},
    "java": {"listen": 9200, "admin": 9201},
}

_PROCS: list[subprocess.Popen] = []


# ---------------------------------------------------------------- process I/O

def wait_port(port: int, timeout: float = 2.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def port_free(port: int) -> bool:
    return not wait_port(port, timeout=0.3)


def wait_healthz(admin_port: int, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            out = subprocess.run(
                ["curl", "-s", "--max-time", "1",
                 f"http://127.0.0.1:{admin_port}/healthz"],
                capture_output=True, text=True, timeout=3)
            if out.stdout.strip() == "ok":
                return True
        except Exception:
            pass
        time.sleep(0.05)
    return False


_TIME_RE = re.compile(r"(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$")


def _parse_cpu(field: str) -> float:
    m = _TIME_RE.match(field)
    if not m:
        return float("nan")
    days, hours, mins, secs = m.groups()
    return ((int(days or 0) * 86400) + (int(hours or 0) * 3600)
            + int(mins) * 60 + float(secs))


def sample(pids: dict[str, int]) -> dict[str, dict]:
    """One `ps` call for every pid, so both samples are taken at the same instant.

    Two separate calls would be tens of milliseconds apart, which is enough to
    matter when the whole point is comparing them fairly.
    """
    live = {name: pid for name, pid in pids.items() if pid}
    if not live:
        return {}
    arg = ",".join(str(p) for p in live.values())
    try:
        out = subprocess.run(["ps", "-p", arg, "-o", "pid=,rss=,time="],
                             capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return {}
    by_pid = {}
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        by_pid[int(parts[0])] = {"rss_mb": int(parts[1]) / 1024.0,
                                 "cpu_s": _parse_cpu(parts[2])}
    return {name: by_pid[pid] for name, pid in live.items() if pid in by_pid}


def metrics(admin_port: int) -> dict:
    try:
        out = subprocess.run(["curl", "-s", "--max-time", "3",
                              f"http://127.0.0.1:{admin_port}/metrics"],
                             capture_output=True, text=True, timeout=5).stdout
        return {k: int(v) for k, v in
                re.findall(r"^vp_interceptor_(\w+) (\d+)$", out, re.M)}
    except Exception:
        return {}


# ------------------------------------------------------------------- sampling

class Sampler(threading.Thread):
    """Records (t, rss, cpu) for every process on a fixed interval.

    Runs for the whole session; per-level figures are sliced out of the series
    afterwards by timestamp, so a level's peak RSS is a real observed peak rather
    than a single reading taken at the end.
    """

    def __init__(self, pids: dict[str, int], interval: float):
        super().__init__(daemon=True)
        self.pids = pids
        self.interval = interval
        self.series: list[tuple[float, dict]] = []
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            s = sample(self.pids)
            if s:
                self.series.append((time.time(), s))
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()

    def slice(self, t0: float, t1: float, name: str) -> dict:
        rss = [s[name]["rss_mb"] for t, s in self.series
               if t0 <= t <= t1 and name in s]
        cpu = [s[name]["cpu_s"] for t, s in self.series
               if t0 <= t <= t1 and name in s]
        cpu = [c for c in cpu if c == c]
        if not rss:
            return {"rss_mean_mb": float("nan"), "rss_peak_mb": float("nan"),
                    "cpu_s": float("nan"), "samples": 0}
        return {
            "rss_mean_mb": sum(rss) / len(rss),
            "rss_peak_mb": max(rss),
            # CPU time is cumulative, so the window's cost is last minus first.
            "cpu_s": (cpu[-1] - cpu[0]) if len(cpu) >= 2 else float("nan"),
            "samples": len(rss),
        }


# ------------------------------------------------------------ implementations

class Impl:
    def __init__(self, name: str, cmd: list[str]):
        self.name = name
        self.cmd = cmd
        self.listen = PORTS[name]["listen"]
        self.admin = PORTS[name]["admin"]
        self.proc = None
        self.startup_ms = float("nan")
        self.log = f"/tmp/sbs_{name}.log"

    def start(self, config_path: str):
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(
            self.cmd + [config_path],
            stdout=open(self.log, "w"), stderr=subprocess.STDOUT,
            cwd=str(ROOT))
        _PROCS.append(self.proc)
        if not wait_healthz(self.admin):
            raise RuntimeError(
                f"{self.name} never became healthy on admin {self.admin}; see {self.log}")
        self.startup_ms = (time.perf_counter() - t0) * 1000

    @property
    def pid(self):
        return self.proc.pid if self.proc else None

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def cleanup():
    for p in _PROCS:
        if p.poll() is None:
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=10)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass


atexit.register(cleanup)


# ------------------------------------------------------------ load generation

def spawn_load(target_port: int, conns: int, duration: float, rate: float,
               pipeline: int, label: str) -> subprocess.Popen:
    """Start a load generator WITHOUT waiting, so both can be launched together."""
    cmd = [sys.executable, str(TESTKIT / "vp_client.py"),
           "--target", f"127.0.0.1:{target_port}",
           "--connections", str(conns),
           "--duration", str(duration),
           "--pipeline", str(pipeline),
           "--label", label, "--json"]
    if rate:
        cmd += ["--rate", str(rate)]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, cwd=str(TESTKIT))


def reap_load(p: subprocess.Popen, timeout: float) -> dict:
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
    lines = (out or "").strip().splitlines()
    if not lines:
        raise RuntimeError(f"load generator produced nothing: {(err or '')[:400]}")
    return json.loads(lines[-1])


# ------------------------------------------------------------------ reporting

def fmt(v, nd=1):
    return "n/a" if v is None or v != v else f"{v:.{nd}f}"


def ratio(rust_v, java_v, higher_is_better=False):
    """Always expressed as 'how many times better is Rust'."""
    if rust_v is None or java_v is None or rust_v != rust_v or java_v != java_v:
        return float("nan")
    if higher_is_better:
        return rust_v / java_v if java_v else float("inf")
    return java_v / rust_v if rust_v else float("inf")


def main():
    ap = argparse.ArgumentParser(
        description="Load-test the Rust and Java interceptors simultaneously.")
    ap.add_argument("--duration", type=float, default=15.0,
                    help="measured seconds per concurrency level")
    ap.add_argument("--conns", default="8,64,256",
                    help="comma-separated concurrency levels, applied to EACH impl")
    ap.add_argument("--rate", type=float, default=400.0,
                    help="msg/s per connection; 0 = unthrottled (measures the client)")
    ap.add_argument("--pipeline", type=int, default=1,
                    help="frames per send(); >1 exercises the reassembler")
    ap.add_argument("--warmup", type=float, default=15.0,
                    help="seconds of warm-up, run against both at once")
    ap.add_argument("--sample-interval", type=float, default=0.25,
                    help="seconds between RSS/CPU samples")
    ap.add_argument("--settle", type=float, default=3.0,
                    help="seconds between levels, to let both drain")
    ap.add_argument("--out", default=str(ROOT / "benchmark/results_side_by_side.json"))
    opts = ap.parse_args()

    levels = [int(c) for c in opts.conns.split(",")]
    cores = os.cpu_count() or 0

    # -------------------------------------------------------------- preflight
    if not RUST_BIN.exists():
        sys.exit(f"missing {RUST_BIN}\n  (cd rust-interceptor && cargo build --release)")
    if not JAVA_JAR.exists():
        sys.exit(f"missing {JAVA_JAR}\n  mvn -f java-interceptor/pom.xml package")
    if not wait_port(FMS_PORT):
        sys.exit(f"mock FMS is not listening on {FMS_PORT}. Start it:\n"
                 f"  cd testkit && python3 mock_fms.py --port {FMS_PORT} &")
    busy = [p for impl in PORTS.values() for p in impl.values() if not port_free(p)]
    if busy:
        sys.exit(f"ports already in use: {busy}\n"
                 f"  something is running there -- try: pkill -f vp-fms-interceptor")

    # Both read the SAME config; only the ports and the Kafka topic differ, so a
    # settings difference can never explain a difference in results.
    base_cfg = (ROOT / "config.toml").read_text()
    impls = [Impl("rust", [str(RUST_BIN)]),
             Impl("java", ["java", "-jar", str(JAVA_JAR)])]

    print("=" * 78)
    print("SIDE-BY-SIDE LOAD TEST  --  both implementations running concurrently")
    print("=" * 78)
    print(f"  cores available    : {cores}")
    print(f"  levels             : {levels} connections, to EACH implementation")
    print(f"  rate               : {opts.rate or 'unthrottled'} msg/s per connection")
    print(f"  duration           : {opts.duration:.0f}s measured per level"
          f"  (+{opts.warmup:.0f}s warm-up)")
    print(f"  upstream           : mock FMS on {FMS_PORT} (shared)")
    for i in impls:
        print(f"  {i.name:<4}               : listen {i.listen}  admin {i.admin}")
    print()

    results = {
        "mode": "side_by_side_concurrent",
        "config": vars(opts),
        "cores": cores,
        "impls": {i.name: {"levels": {}} for i in impls},
    }

    # ---------------------------------------------------------------- startup
    # Started one at a time so each startup figure is measured on an idle box.
    for impl in impls:
        cfg = base_cfg
        cfg = re.sub(r'(?m)^addr = "0\.0\.0\.0:9100"',
                     f'addr = "0.0.0.0:{impl.listen}"', cfg)
        cfg = re.sub(r'(?m)^addr = "127\.0\.0\.1:9101"',
                     f'addr = "127.0.0.1:{impl.admin}"', cfg)
        cfg = cfg.replace('topic = "vp.fms.iso8583"',
                          f'topic = "vp.fms.sbs.{impl.name}"')
        cfg_path = f"/tmp/sbs_{impl.name}.toml"
        Path(cfg_path).write_text(cfg)

        impl.start(cfg_path)
        results["impls"][impl.name]["startup_ms"] = impl.startup_ms
        print(f"  started {impl.name:<4} pid {impl.pid:<7} "
              f"startup to healthy {impl.startup_ms:>7.0f} ms")

    pids = {i.name: i.pid for i in impls}
    time.sleep(2)
    idle = sample(pids)
    for i in impls:
        results["impls"][i.name]["rss_idle_mb"] = idle.get(i.name, {}).get("rss_mb")
    print(f"  RSS idle           : "
          + "   ".join(f"{n} {fmt(idle.get(n, {}).get('rss_mb'))} MB" for n in pids))

    sampler = Sampler(pids, opts.sample_interval)
    sampler.start()

    # ---------------------------------------------------------------- warm-up
    # Both warmed at once and for the same duration, so neither is favoured and
    # both experience the contention they will see during measurement.
    if opts.warmup > 0:
        print(f"\n  warming up both for {opts.warmup:.0f}s "
              f"(the JVM needs this; without it you measure the JIT) ...", flush=True)
        procs = {i.name: spawn_load(i.listen, 8, opts.warmup, 0, opts.pipeline,
                                    f"warmup-{i.name}") for i in impls}
        for name, p in procs.items():
            try:
                reap_load(p, opts.warmup + 60)
            except Exception as e:
                print(f"    warm-up for {name} failed: {e}")
        warm = sample(pids)
        for i in impls:
            results["impls"][i.name]["rss_warm_mb"] = warm.get(i.name, {}).get("rss_mb")
        print(f"  RSS after warm-up  : "
              + "   ".join(f"{n} {fmt(warm.get(n, {}).get('rss_mb'))} MB" for n in pids))

    # ----------------------------------------------------------------- levels
    for c in levels:
        if cores and c * 2 > cores * 64:
            print(f"\n  ! {c} conns x2 implementations on {cores} cores -- "
                  f"you may be measuring the scheduler, not the interceptors")

        print(f"\n{'-' * 78}")
        print(f"LEVEL: {c} connections to EACH implementation, {opts.duration:.0f}s")
        print(f"{'-' * 78}")

        # Launch both generators before waiting on either: the two runs must
        # overlap, otherwise this is just compare.py with extra steps.
        t0 = time.time()
        procs = {i.name: spawn_load(i.listen, c, opts.duration, opts.rate,
                                    opts.pipeline, f"sbs-{i.name}-{c}")
                 for i in impls}
        loads = {}
        for name, p in procs.items():
            loads[name] = reap_load(p, opts.duration + 120)
        t1 = time.time()

        for i in impls:
            lo = loads[i.name]
            res = sampler.slice(t0, t1, i.name)
            msgs = lo["ok"]
            cpu_s = res["cpu_s"]
            entry = {
                "tps": lo["tps"],
                "ok": msgs,
                "errors": lo["errors"],
                "mismatches": lo["mismatches"],
                "connect_errors": lo["connect_errors"],
                "p50_ms": lo["p50_ms"],
                "p99_ms": lo["p99_ms"],
                "rss_mean_mb": res["rss_mean_mb"],
                "rss_peak_mb": res["rss_peak_mb"],
                "cpu_s": cpu_s,
                "cpu_ms_per_1k": (cpu_s * 1000 / (msgs / 1000)) if msgs and cpu_s == cpu_s else float("nan"),
                "cpu_cores_used": (cpu_s / (t1 - t0)) if cpu_s == cpu_s else float("nan"),
                "samples": res["samples"],
            }
            results["impls"][i.name]["levels"][c] = entry

            print(f"  {i.name:<5} tps {entry['tps']:>8.0f}   "
                  f"RSS mean {fmt(entry['rss_mean_mb']):>7} peak {fmt(entry['rss_peak_mb']):>7} MB   "
                  f"CPU {fmt(entry['cpu_s'], 2):>6}s ({fmt(entry['cpu_ms_per_1k'], 1)} ms/1k)   "
                  f"err {entry['errors']} mism {entry['mismatches']}")

        time.sleep(opts.settle)

    # ---------------------------------------------------------------- wrap-up
    sampler.stop()
    sampler.join(timeout=5)

    for i in impls:
        e = results["impls"][i.name]
        e["metrics"] = metrics(i.admin)
        lv = e["levels"].values()
        e["rss_peak_mb"] = max((v["rss_peak_mb"] for v in lv if v["rss_peak_mb"] == v["rss_peak_mb"]), default=float("nan"))
        e["messages_total"] = sum(v["ok"] for v in lv)
        cpu_total = sum(v["cpu_s"] for v in lv if v["cpu_s"] == v["cpu_s"])
        e["cpu_seconds_total"] = cpu_total
        e["cpu_ms_per_1k_msgs"] = (cpu_total * 1000 / (e["messages_total"] / 1000)
                                   if e["messages_total"] else float("nan"))

    results["series"] = [
        {"t": round(t - sampler.series[0][0], 3),
         **{n: {"rss_mb": round(v["rss_mb"], 2), "cpu_s": v["cpu_s"]}
            for n, v in s.items()}}
        for t, s in sampler.series
    ] if sampler.series else []

    Path(opts.out).write_text(json.dumps(results, indent=2, default=str))
    summary(results, levels)

    for impl in impls:
        impl.stop()

    print(f"\nraw results (including the full RSS/CPU time series) -> {opts.out}")

    bad = sum(v["mismatches"] + v["connect_errors"]
              for e in results["impls"].values() for v in e["levels"].values())
    if bad:
        print("\nFAIL: mismatches or connect errors occurred -- do not quote these timings.")
        sys.exit(2)


def summary(results, levels):
    r = results["impls"].get("rust", {})
    j = results["impls"].get("java", {})
    if not r or not j:
        return

    print()
    print("=" * 78)
    print("SUMMARY  --  both under load simultaneously; ratios are the point")
    print("=" * 78)
    print(f"{'metric':<32}{'Rust':>13}{'Java':>13}{'Rust advantage':>18}")
    print("-" * 78)

    def row(label, rv, jv, nd=1, higher_better=False):
        if rv is None or jv is None or rv != rv or jv != jv:
            print(f"{label:<32}{'n/a':>13}{'n/a':>13}{'':>18}")
            return
        rt = ratio(rv, jv, higher_better)
        print(f"{label:<32}{rv:>13.{nd}f}{jv:>13.{nd}f}{rt:>17.1f}x")

    row("startup to healthy (ms)", r.get("startup_ms"), j.get("startup_ms"), 0)
    row("RSS idle (MB)", r.get("rss_idle_mb"), j.get("rss_idle_mb"))
    row("RSS after warm-up (MB)", r.get("rss_warm_mb"), j.get("rss_warm_mb"))
    row("RSS peak under load (MB)", r.get("rss_peak_mb"), j.get("rss_peak_mb"))
    row("CPU seconds, all levels", r.get("cpu_seconds_total"), j.get("cpu_seconds_total"), 2)
    row("CPU ms per 1k messages", r.get("cpu_ms_per_1k_msgs"), j.get("cpu_ms_per_1k_msgs"), 1)

    print("-" * 78)
    for c in levels:
        rl, jl = r["levels"].get(c), j["levels"].get(c)
        if not rl or not jl:
            continue
        print(f"  @ {c} connections")
        row("    throughput (msg/s)", rl["tps"], jl["tps"], 0, higher_better=True)
        row("    RSS mean (MB)", rl["rss_mean_mb"], jl["rss_mean_mb"])
        row("    CPU cores consumed", rl["cpu_cores_used"], jl["cpu_cores_used"], 2)
        print(f"{'    p50 / p99 (ms, CONTENDED)':<32}"
              f"{rl['p50_ms']:>6.2f} /{rl['p99_ms']:>6.2f}"
              f"{jl['p50_ms']:>7.2f} /{jl['p99_ms']:>6.2f}"
              f"{'not comparable':>18}")
    print("-" * 78)

    rk = r.get("metrics", {})
    jk = j.get("metrics", {})
    if rk or jk:
        print(f"{'interceptor counters':<32}{'Rust':>13}{'Java':>13}")
        for key in ("tee_accepted", "tee_dropped", "frames_emitted",
                    "framer_desyncs", "kafka_enqueued", "kafka_delivered",
                    "kafka_delivery_failed"):
            print(f"  {key:<30}{rk.get(key, 0):>13}{jk.get(key, 0):>13}")
        if rk.get("kafka_delivered", 0) == 0 and rk.get("kafka_enqueued", 0) > 0:
            print("\n  ! kafka_delivered is 0 -- the broker was unreachable, so both")
            print("    implementations shed on the publish path. That is a LIGHTER")
            print("    workload; these numbers are not comparable to a Kafka-up run.")

    print("\nReminder: absolute figures here are depressed by the two implementations")
    print("competing for cores. Compare them against each other, not against")
    print("compare.py. See benchmark/README.md for what is signal and what is noise.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted -- shutting down both implementations")
        cleanup()
        sys.exit(130)
