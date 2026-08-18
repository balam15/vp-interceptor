# benchmark — running the Rust vs Java comparison

Two scripts, answering different questions:

| script | how it runs | use it for |
|---|---|---|
| `compare.py` | each implementation **in turn** | latency deltas vs a direct-to-FMS baseline; the numbers quoted in `../BENCHMARK.md` |
| `side_by_side.py` | both **at the same time** | memory / CPU / throughput ratios under identical instantaneous conditions (§8) |

`compare.py` starts each implementation in turn, drives identical load against
both, and prints a summary. Results from the last full run are in
`../BENCHMARK.md`; raw JSON is in `results.json` / `results_run2.json`.

Sections 1 and 5–7 below (prerequisites, Kafka, noise, troubleshooting) apply to
both scripts.

---

## 1. Prerequisites

**Both builds must exist.** `compare.py` refuses to start otherwise.

```bash
# Rust  (rustup.rs is blocked on this network -- use Homebrew)
brew install rust cmake openssl@3
(cd rust-interceptor && cargo build --release)

# Java  (mvn is a shell alias; non-interactive shells need sdkman sourced first)
source ~/.sdkman/bin/sdkman-init.sh
mvn -f java-interceptor/pom.xml package
```

Produces:

| artifact | path |
|---|---|
| Rust binary | `rust-interceptor/target/release/vp-fms-interceptor` |
| Java fat jar | `java-interceptor/target/vp-fms-interceptor.jar` |

**The mock FMS must be running.** `compare.py` does *not* start it — it is the
upstream both implementations proxy to, and it must outlive both.

```bash
cd testkit && python3 mock_fms.py --port 8583 &
```

**Kafka should be reachable** at whatever `[kafka] brokers` in `config.toml`
points at. See §5 — running without it silently changes what you are measuring.

```bash
docker start kafka          # if you already have a broker container
# or:  cd testkit && docker compose up -d
```

Ports used: `8583` mock FMS, `9100` interceptor, `9101` admin. All three must be
free — `compare.py` binds `9100`/`9101` for each implementation in turn.

---

## 2. Run it

From the **project root**:

```bash
source ~/.sdkman/bin/sdkman-init.sh          # so `java` is on PATH
python3 benchmark/compare.py
```

That is the default run: 12s per level, 400 msg/s per connection, at 8 / 64 / 256
connections, with a 15s warm-up per implementation. Takes roughly 4 minutes.

`compare.py` handles starting and stopping each interceptor itself. Do not have
one already running on `9100`.

### Flags

| flag | default | meaning |
|---|---|---|
| `--duration` | `12` | seconds per concurrency level |
| `--rate` | `400` | msg/s **per connection**; `0` = unthrottled |
| `--conns` | `8,64,256` | concurrency levels to sweep |
| `--warmup` | `15` | seconds of warm-up before measuring |
| `--out` | `benchmark/results.json` | raw output path |

```bash
# quick smoke run
python3 benchmark/compare.py --duration 5 --warmup 5 --conns 8

# concurrency-focused
python3 benchmark/compare.py --conns 8,32,128,512 --duration 20

# second run to a different file, to check reproducibility
python3 benchmark/compare.py --out benchmark/results_run2.json
```

---

## 3. Reading the output

```
metric                                     Rust         Java       ratio
--------------------------------------------------------------------------
startup to healthy (ms)                      30         1185       40.1x
RSS idle (MB)                               4.0         89.2       22.1x
RSS peak under load (MB)                   43.1        197.6        4.6x
CPU ms per 1k messages                     28.4         45.1        1.6x
--------------------------------------------------------------------------
p50 delta @ 8 conns (ms)                  0.013        0.071
p99 delta @ 8 conns (ms)                  0.292        1.545
throughput @ 8 conns (tps)                 3254         3242
```

- **delta** = latency through the interceptor *minus* latency straight to FMS,
  measured in the same run. This is the only latency number that means anything;
  absolutes are dominated by the Python mock.
- **RSS** is resident memory of the interceptor process, sampled at each level.
- **CPU ms per 1k messages** is cumulative process CPU divided by messages
  handled — the most reproducible performance number this rig produces.
- **`errors` and `mismatches` must both be 0.** `mismatches` means the
  interceptor corrupted the stream; treat any non-zero value as a failed run and
  do not report the timings.

---

## 4. Methodology — why the script does what it does

These are not incidental; removing any of them invalidates the numbers.

**Warm-up is mandatory.** Without it you measure the JVM's JIT, not the design.
The very first frame through the Java tee took **353 ms** versus sub-millisecond
once warm. Both builds get the identical warm-up so neither is favoured.

**Rate-limiting is mandatory for latency.** The Python load generator saturates
around 40–70k msg/s and is GIL-bound. At `--rate 0` you measure the client. If
throughput at two concurrency levels comes out nearly identical between
implementations, that is the tell — the generator was the bottleneck, not either
interceptor.

**Both read the same `config.toml`.** `compare.py` copies it per implementation
and rewrites only the Kafka topic, so a difference in settings can never explain
a difference in results.

**Run it twice.** This rig is noisy enough that single runs mislead — see §6.

---

## 5. Kafka up or down changes what you measure

With the broker **reachable**, each frame is framed, parsed, JSON-encoded,
base64-encoded and published. That is the real workload.

With the broker **down**, both builds shed on the publish path. They keep serving
correctly — that is the design — but you are now measuring a much lighter
workload, and the results are **not comparable** to numbers taken with Kafka up.

Check afterwards:

```bash
curl -s localhost:9101/metrics | grep -E 'kafka_delivered|kafka_delivery_failed'
```

A large `kafka_delivery_failed` with `kafka_delivered` near zero means the broker
was unavailable for that run. `compare.py` records the counters into the JSON so
you can check this after the fact.

---

## 6. What is signal and what is noise

Established by running the full comparison twice on this hardware.

**Reproducible — safe to quote:** startup time, idle/peak RSS, CPU per message
(stable to within 3% across runs), and p50 delta at low concurrency.

**Noise — do not quote:** p99 and p99.9 above 8 connections. The sign *flips*
between runs. At 256 connections Rust's p99 delta was +7.1 ms then +10.1 ms while
Java went +10.8 then +4.1 — same code both times. The tail here is dominated by
CPython GC in the mock, the JVM broker, and Docker.

Also: **negative deltas at high concurrency** mean the direct-to-FMS baseline was
saturated too. They do not mean the interceptor made anything faster.

For trustworthy tail numbers, re-run across two real hosts with a non-Python load
generator. Close other heavy applications first — this measurement is sensitive
to whatever else is competing for cores.

---

## 7. Troubleshooting

| symptom | cause |
|---|---|
| `mock FMS is not listening on 8583` | start it: `cd testkit && python3 mock_fms.py --port 8583 &` |
| `missing .../vp-fms-interceptor` | build that implementation — see §1 |
| `java: command not found` | `source ~/.sdkman/bin/sdkman-init.sh` |
| `never became healthy` | read `/tmp/bench_rust.log` or `/tmp/bench_java.log` |
| `Address already in use` | something is on `9100`/`9101`: `pkill -f vp-fms-interceptor` |
| `mismatches` non-zero | stream corruption — a real bug; do not report the timings |
| Java much slower than expected | warm-up too short, or the broker is down (§5) |

Clean up after an interrupted run:

```bash
pkill -f vp-fms-interceptor
pkill -f mock_fms.py
```

---

## 8. `side_by_side.py` — both under load at once

`compare.py` measures Rust, then measures Java. Four minutes pass in between, and
in that time thermal state, CPU frequency, background processes, page cache and
broker load all drift. That drift is invisible in the output and lands entirely
on whichever implementation ran second.

`side_by_side.py` removes that confound by running **both implementations
concurrently** on separate ports, driven by two load generators started together,
sampling RSS and CPU from both processes with a single `ps` call so the two
readings are taken at the same instant.

```bash
cd testkit && python3 mock_fms.py --port 8583 &      # shared upstream
python3 benchmark/side_by_side.py
```

Ports: Rust `9100`/`9101`, Java `9200`/`9201`, mock FMS `8583`. All four
interceptor ports must be free — the script refuses to start otherwise.

### Flags

| flag | default | meaning |
|---|---|---|
| `--duration` | `15` | measured seconds per level |
| `--conns` | `8,64,256` | levels, applied to **each** implementation |
| `--rate` | `400` | msg/s per connection; `0` = unthrottled |
| `--pipeline` | `1` | frames per `send()`; `>1` exercises the reassembler |
| `--warmup` | `15` | warm-up seconds, run against both at once |
| `--sample-interval` | `0.25` | seconds between RSS/CPU samples |
| `--out` | `benchmark/results_side_by_side.json` | raw output |

### The trade you are making

**What gets better:** every sample of each implementation is taken under
identical ambient conditions, so the *ratios* are more trustworthy than
sequential ones. Warm-up is also concurrent, so neither build is favoured and
both experience the contention they will see while being measured.

**What gets worse:** the two implementations now compete for cores, as do the two
Python load generators. So:

- **Absolute numbers are depressed and are NOT comparable to `compare.py`'s.**
  Never mix figures from the two scripts in one table.
- **Latency is contended.** It is printed for completeness and labelled
  `not comparable`. Use `compare.py` for latency, and read §6 first.
- On a machine with few cores, high `--conns` measures the scheduler. The script
  prints the core count and warns.

### Reading the output

```
metric                                   Rust         Java    Rust advantage
------------------------------------------------------------------------------
RSS peak under load (MB)                 19.1        198.3             10.4x
CPU ms per 1k messages                   40.0         87.7              2.2x
  @ 64 connections
    throughput (msg/s)                  13330        13310              1.0x
    CPU cores consumed                   0.47         0.87              1.8x
```

- **`Rust advantage`** is always "how many times better is Rust", whichever
  direction the metric runs — lower CPU is better, higher throughput is better.
- **`CPU cores consumed`** is process CPU seconds divided by wall seconds: `0.47`
  means it kept about half a core busy. Easier to reason about than raw seconds
  when comparing against your core count.
- **Identical throughput on both means the rate limiter was the bottleneck**, not
  either interceptor — that is the expected result at any `--rate` they can both
  sustain. To find where each one actually breaks, use `--rate 0`, and treat the
  result with §4's warning about the Python generator in mind.
- **`tee_dropped` is the interesting counter here.** It is the publish path
  shedding because a shard queue filled. A large value on one implementation and
  zero on the other, at the same offered load, says that one could not keep up on
  the Kafka side — while still forwarding correctly, which is the design working.
- The JSON output includes the **full RSS/CPU time series** for both processes
  (`series`), so you can plot memory growth over the run rather than trusting
  three summary numbers.

`mismatches` or `connect_errors` anywhere means stream corruption or refused
connections; the script exits non-zero and those timings must not be quoted.
