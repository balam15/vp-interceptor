# Rust vs Java — measured comparison

Two complete implementations of the same interceptor, benchmarked against each
other. Reproduce with:

```bash
cd testkit && python3 mock_fms.py --port 8583 &
cd .. && python3 benchmark/compare.py --duration 12 --rate 400 --conns 8,64,256
```

**Full instructions, prerequisites and troubleshooting: `benchmark/README.md`.**

## Method

Both builds **read the same `config.toml`**, are driven by the **same load
generator** against the **same mock FMS**, and are measured in the same session.
The only variable is the runtime.

- Both get an identical 15s warm-up before any measurement. Without it the JVM's
  JIT makes early numbers meaningless — the very first frame through the Java
  tee took **353 ms**, versus sub-millisecond once warm.
- Latency is a **delta against talking to FMS directly**, measured in the same
  run. Absolute values are dominated by the Python mock.
- Runs are rate-limited. Unthrottled runs measure the load generator.
- Ran twice end-to-end; both runs are reported so you can see what is stable.

**Hardware:** single laptop, loopback, Docker Kafka and a Python mock competing
for the same cores. See "Confidence" below — some of these numbers are solid and
some are noise, and the difference matters.

## Results

| metric | Rust | Java | ratio | run 2 |
|---|---:|---:|---:|---|
| Startup to healthy (ms) | **30** | 1185 | **40x** | 31 / 1062 |
| RSS idle (MB) | **4.0** | 89.2 | **22x** | 2.9 / 97.9 |
| RSS after warmup (MB) | **20.9** | 151.7 | **7.3x** | 20.5 / 110.8 |
| RSS peak under load (MB) | **43.1** | 197.6 | **4.6x** | 59.0 / 170.4 |
| CPU ms per 1k messages | **28.4** | 45.1 | **1.6x** | 27.5 / 44.6 |
| Binary/artifact size (MB) | **3.4** | 19 | 5.6x | — |

### Latency delta vs direct-to-FMS (ms, lower is better)

| connections | Rust p50 | Java p50 | Rust p99 | Java p99 |
|---|---:|---:|---:|---:|
| 8 | **+0.013** | +0.071 | **+0.292** | +1.545 |
| 64 | +0.318 | **+0.265** | **+1.129** | +1.667 |
| 256 | −2.173 | −0.913 | +7.101 | +10.771 |

### Throughput (tps)

| connections | Rust | Java |
|---|---:|---:|
| 8 | 3,254 | 3,242 |
| 64 | 26,032 | 26,036 |
| 256 | **35,156** | 28,735 |

At 8 and 64 connections both are **load-generator bound**, not interceptor
bound — identical throughput means the Python client was the limit, not either
implementation. Only the 256-connection figure reflects the interceptor, and it
varied run to run (Rust +22% in run 1, +4% in run 2).

## Confidence — read before quoting any of this

**Solid, reproduced across both runs:**

- **Startup time: Rust ~30 ms, Java ~1.1 s (34–40x).** Matters for restart
  windows and for how fast a failed node rejoins.
- **Idle memory: 4 MB vs ~90 MB (22–33x).** Matters for density.
- **Peak memory under load: 3–5x.**
- **CPU per message: consistently 1.6x** (28 vs 45 ms/1k, stable to within 3%
  across runs). This is the most reproducible performance number here.
- **p50 delta at low concurrency: Rust ~3x lower** (0.013–0.024 vs 0.071–0.080).

**Noise — do not quote:**

- **p99 and p99.9 at 64+ connections.** The sign flips between runs: at 256
  connections Rust's p99 delta was +7.1 ms in run 1 and +10.1 ms in run 2, while
  Java went +10.8 then +4.1. On this rig the tail is dominated by CPython GC in
  the mock, the JVM broker, and Docker — not by the interceptor.
- **p50 delta at 64 connections.** Run 1 favoured Java, run 2 favoured Rust by
  5x. Same code both times.
- **Negative deltas at 256 connections** mean the *direct* baseline was also
  saturated. They do not mean the interceptor made things faster.

To get trustworthy tail numbers, re-run across two real hosts with a non-Python
load generator.

## The bug this benchmark found

The first run showed **Rust peaking at 409 MB against Java's 167 MB** — backwards
from every expectation, which is exactly why it was worth chasing rather than
publishing.

Cause: `BytesMut::split().freeze()` is zero-copy, but the resulting `Bytes`
keeps the **entire** read allocation alive. A 122-byte payments message sitting
in the tee queue pinned its whole 16 KiB read buffer. With
`8192 × 4 shards = 32,768` queue slots, worst case is ~512 MB.

Confirmed by experiment before fixing — running with an 8x smaller read buffer
dropped RSS from 224 MB to 96 MB, isolating ~146 MB as pinned buffers.

The fix (`rust-interceptor/src/proxy.rs`): when a read fills less than a quarter of the buffer,
copy it into an exact-size `Bytes` instead of slicing. That is a ~100-byte
memcpy at payments message sizes, and it lets the buffer be reused immediately.

**Result: peak RSS 409 MB → 43 MB, with no latency cost** (p50 delta actually
improved). 19/19 tests still pass; 353,388 messages verified with 0 mismatches
and 0 framer desyncs after the change.

The irony worth noting: Java was never exposed to this, because it has no
refcounted-slice type and *must* copy into a right-sized array. The copy I
described in the Java code as "the one unavoidable extra cost of the JVM
implementation" was the thing protecting it.

## Semantic differences between the two builds

These are not tuning choices — they are places where the client libraries differ
and the code had to compensate.

**`KafkaProducer.send()` can block.** librdkafka's `send` never blocks; Java's
blocks for up to `max.block.ms` when the buffer is full *or metadata is missing*.
Left at the default (60 s) that would put Kafka directly into the payment path —
the exact failure this whole design exists to prevent. The Java build sets
`max.block.ms=0`, making `send()` throw instantly instead, which is caught and
counted.

**That creates a second problem:** with `max.block.ms=0`, sends issued before
topic metadata is known fail immediately, so Java would silently drop the first
messages after every startup. librdkafka buffers them instead. The Java build
compensates with a background metadata warm-up thread — off-thread, so a dead
broker still cannot delay serving.

**Java must copy on the tee path.** No refcounted slice type. As above, this
turned out to be an advantage.

**Virtual threads (Java 21) were used deliberately** — one per connection
direction, the same shape as the Rust build's task-per-direction. A thread-pool
implementation would have been a strawman comparison.

## Bugs found in the Java build during bring-up

1. **`delivery.timeout.ms` must be ≥ `linger.ms + request.timeout.ms`.** Setting
   both to 5000 throws `ConfigException` at producer construction. Because the
   code correctly degrades to proxy-only on producer failure, the symptom was
   *traffic flowing perfectly with `tee_accepted = 0`* — a silent loss of the
   entire Kafka feed. The graceful-degradation path worked exactly as designed,
   which is precisely what made the bug quiet.
2. **SLF4J version clash.** `kafka-clients` pulls slf4j-api 1.7.x; slf4j-simple
   2.x then binds to nothing and logging silently becomes a NOP — hiding the
   error above. Fixed by pinning slf4j-api 2.x.

Both are worth remembering: they are the kind of failure that looks like nothing
is wrong.

## Which to run

**Rust, for this workload.** Not because of throughput — they are comparable —
but because:

- 22–33x less idle memory and 3–5x less under load
- 1.6x less CPU per message, the most reproducible number in this comparison
- 34–40x faster startup
- No GC to tune, and no tail-latency risk from one

**Java is a reasonable choice if** the team maintains it better than they would
maintain Rust. It kept up on throughput and its p50 delta is still well under a
millisecond. An unmaintainable Rust service is worse than a well-run Java one.
But it needs `max.block.ms=0`, the metadata warm-up, and GC tuning that this
benchmark did not attempt — all defaults here.

**What this benchmark does not tell you:** how either behaves across a real
network, against real VP and real FMS, over hours rather than seconds, or under
GC pressure from a tuned heap. The Java build was measured entirely on JVM
defaults.
