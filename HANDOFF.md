# Handoff — everything learned building this

Written to survive a context reset. `README.md` explains the design, `RUNBOOK.md`
explains operation. **This file holds what neither can: the reasoning behind the
decisions, the numbers actually measured, what is proven versus assumed, and the
environment traps that cost time.**

Status as of the last session: **working, built, tested end-to-end against a real
Kafka broker. Never run against real VP or real FMS.**

---

## 1. What this is

A transparent TCP interceptor between Vynamic Payments (VP) and a Fraud
Management System (FMS). It forwards the connection untouched and publishes a
copy of every reassembled message to Kafka.

```
VP ═══TCP═══> [ interceptor :9100 ] ═══TCP═══> FMS :8583
                      │
                      └─ try_send (never blocks, drops when full)
                           │
                      [4 shard workers] → framer → parser → librdkafka → Kafka
```

VP must be repointed at the interceptor. It is a man-in-the-middle by design.

**Two complete implementations exist:**

- **Rust + Tokio** (`rust-interceptor/`) — the primary. ~1,900 lines, 19 unit tests, 3.4 MB
  binary, no runtime dependencies.
- **Java 21 + virtual threads** (`java-interceptor/`) — a full port, built to
  make the language choice an evidence-based decision rather than an assertion.
  19 MB fat jar.

Both read the same `config.toml` and emit a byte-compatible Kafka envelope and
identical metric names, so the same consumers and dashboards work against either.
**Rust is the recommendation — see §4b for the measured reasons.**

---

## 2. The governing rule

> **Kafka must never be able to affect VP↔FMS.**

Every design decision below follows from it. If you change one thing in this
codebase, check it against that sentence first.

Four mechanisms enforce it:

1. **Forward first, tee second** (`rust-interceptor/src/proxy.rs`). The pump writes to the far
   socket, *then* offers a copy to the publish path. Kafka work is never in the
   critical section.
2. **A bounded queue is the isolation boundary** (`rust-interceptor/src/tee.rs`). `try_send` into
   a fixed channel; when full, frames are counted and dropped. There is no
   unbounded buffer anywhere, so no path by which a slow broker becomes
   backpressure on the socket.
3. **Framing never gates forwarding** (`rust-interceptor/src/framing.rs`). Bytes are proxied raw
   as they arrive; a *separate* accumulator reassembles frames for Kafka. A
   framing bug can corrupt the Kafka feed but can never corrupt or stall FMS
   traffic.
4. **Kafka cannot fail startup** (`rust-interceptor/src/kafka.rs`). librdkafka connects lazily. If
   producer *construction* fails, the service logs and runs proxy-only rather
   than exiting.

---

## 3. Decisions and why

### Rust + Tokio, not Go or Java

Originally chosen for **tail latency, not throughput or parallelism**. Inline on
an authorization path, the thing that hurts is the p99.9 outlier that trips an
FMS timeout — which is exactly what a GC produces.

**This was later tested rather than assumed** (§4b). The verdict held, but the
*reasons* shifted: the tail-latency argument turned out to be unmeasurable on
this rig (p99 noise swamps it), while memory, startup, and CPU-per-message showed
large, reproducible differences. If you are re-arguing the language choice, argue
it on those numbers, not on the GC theory — the GC theory is sound but this
project has no evidence for it.

Go remains a defensible option if the team knows it better.

Parallelism was never the hard part — connections are independent, so
`tokio::spawn` per connection with two pump tasks each scales flat across cores.

### Publishing fans IN, not out

All connections funnel into a small fixed number of shard workers (default 4,
`conn_id % N`) sharing one producer. Per-connection producers would destroy
batching and multiply broker sockets. Sharding by `conn_id` also means each
worker owns its framer and timing state **without locking**, and keeps
per-connection frame ordering intact.

Both directions of a connection hash to the same shard — this is what makes
request/response timing pairing possible without shared state.

### `ThreadedProducer`, not `FutureProducer`

`FutureProducer` would mean one future per message to observe delivery. The
`ThreadedProducer` + `ProducerContext::delivery` callback counts outcomes on
librdkafka's own thread with zero per-message futures. It is also genuinely
non-blocking on `send()`.

### Kafka key = `conn_id`

Keeps one connection's messages ordered within one partition. There is **no
global ordering** across connections. Changing the key changes partitioning and
breaks that guarantee.

### Payload is encoded, not parsed (by default)

Real ISO 8583 is binary with bitmaps; decoding into named fields needs the
message spec for the specific installation. The envelope keeps `payload` as the
byte-identical frame so the topic stays a faithful record of what crossed the
link, not an interpretation of it.

A `key=value` parser was added later on request (§5). `fields` and `payload`
coexist deliberately — for a fraud/audit feed, keeping the original bytes matters
when someone disputes what was actually sent.

### `panic = "abort"` deliberately NOT set

A panic in one connection task must kill that connection, not the process and
every other in-flight authorization.

---

## 4. Measured performance

All on loopback, single laptop, mock FMS. **The delta between direct-to-FMS and
intercepted is the only meaningful number** — absolute latency is dominated by
the Python mock and load generator.

| metric | value | confidence |
|---|---|---|
| p50 overhead | **+0.02 to +0.06 ms** | high — stable across ~10 runs |
| p99 overhead | **+0.18 to +0.65 ms** | moderate — varies run to run |
| p99.9 overhead | **−3.0 to +8.2 ms** | **none — this is pure noise** |
| Throughput | 36k–52k msg/s (load-gen bound, not interceptor bound) | — |
| Binary size | 3.4 MB | — |

### The p99.9 trap — read this before benchmarking

Across **identical** runs the p99.9 delta swung from −3.03 ms to +8.21 ms. This
was checked deliberately, three times, after an early run showed +4.4 ms and
looked like a regression. It is not signal. Causes: CPython GC in the mock and
client, the JVM broker competing for CPU, and Docker.

**Do not draw tail-latency conclusions from this rig.** Re-measure across two
real hosts before trusting anything at p99+ for production sizing. p50 is the
only number here that is stable enough to reason about.

### Load generator caveats

- Python `vp_client.py` saturates ~40–70k msg/s and is GIL-bound. **Always use
  `--rate`** for latency work; unthrottled runs measure the client.
- Baseline p99.9 is several ms *even direct to FMS* — CPython GC.
- `mock_fms.py` is threaded, one thread per connection; above ~200 connections it
  becomes the bottleneck, not the interceptor.
- Loopback has no NIC and near-zero jitter. It flatters any proxy.

---

## 4b. Rust vs Java — the head-to-head

A full Java 21 implementation exists in `java-interceptor/` (virtual threads, one
per connection direction — the same shape as the Rust task-per-direction, not a
thread-pool strawman). Both read the **same `config.toml`**, are driven by the
**same load generator** against the **same mock FMS**, in the same session.
`BENCHMARK.md` has the detail; this is the summary that matters.

Reproduce: `python3 benchmark/compare.py --duration 12 --rate 400 --conns 8,64,256`

| metric | Rust | Java | ratio | run 2 |
|---|---:|---:|---:|---|
| Startup to healthy (ms) | **30** | 1185 | **40x** | 31 / 1062 |
| RSS idle (MB) | **4.0** | 89.2 | **22x** | 2.9 / 97.9 |
| RSS after warmup (MB) | **20.9** | 151.7 | 7.3x | 20.5 / 110.8 |
| RSS peak under load (MB) | **43.1** | 197.6 | 4.6x | 59.0 / 170.4 |
| CPU ms per 1k messages | **28.4** | 45.1 | **1.6x** | 27.5 / 44.6 |
| Artifact size (MB) | **3.4** | 19 | 5.6x | — |
| p50 delta @ 8 conns (ms) | **+0.013** | +0.071 | 5x | 0.024 / 0.080 |
| Throughput @ 256 conns | **35,156** | 28,735 | 1.2x | 33,061 / 31,826 |

**Ran the whole comparison twice.** What survived both runs:

- Startup, idle memory, peak memory, and **CPU per message** — the last
  reproduced to within 3% (28.4/45.1 then 27.5/44.6), the single most reliable
  performance number in this whole project.
- p50 delta at low concurrency: Rust ~3–5x lower.

**What did NOT survive and must not be quoted:**

- p99/p99.9 above 8 connections. The sign *flips* between runs — at 256
  connections Rust went +7.1 then +10.1 ms while Java went +10.8 then +4.1 ms.
  Same code both times.
- p50 delta at 64 connections: run 1 favoured Java, run 2 favoured Rust by 5x.
- Negative deltas at 256 connections mean the *direct baseline* was saturated
  too. They do not mean the interceptor made anything faster.
- Throughput at 8 and 64 connections is **identical** between implementations
  because the Python load generator was the bottleneck, not either interceptor.
  Only the 256-connection figure says anything, and it varied 4–22%.

### The memory bug this benchmark found (now fixed)

First run showed **Rust peaking at 409 MB against Java's 167 MB** — backwards
from every expectation, which is why it was worth chasing rather than publishing.

`BytesMut::split().freeze()` is zero-copy but the resulting `Bytes` keeps the
**whole** read allocation alive. A 122-byte message sitting in the tee queue
pinned its entire 16 KiB read buffer. With `8192 x 4 shards = 32,768` slots,
worst case ~512 MB.

Confirmed by experiment *before* fixing: an 8x smaller read buffer dropped RSS
224 MB → 96 MB, isolating ~146 MB as pinned buffers.

Fix in `rust-interceptor/src/proxy.rs`: when a read fills less than a quarter of the buffer, copy
into an exact-size `Bytes` rather than slicing. ~100-byte memcpy at payments
message sizes, and the buffer is reusable immediately.
**Peak RSS 409 MB → 43 MB at no latency cost.** 19/19 tests still pass; 353,388
messages verified afterwards with 0 mismatches and 0 desyncs.

The irony worth remembering: Java was structurally immune, because it has no
refcounted slice type and *must* copy into a right-sized array. The copy
described in the Java source as "the one unavoidable extra cost of the JVM
implementation" was the thing protecting it.

### Java-specific traps (all silent failures)

1. **`KafkaProducer.send()` BLOCKS** for up to `max.block.ms` (60 s default) when
   the buffer is full or metadata is missing. That puts Kafka directly into the
   payment path — the exact failure this design exists to prevent. librdkafka
   never blocks. The Java build sets `max.block.ms=0`. **This is mandatory, not
   tuning.**
2. **That creates a second problem:** with `max.block.ms=0`, sends issued before
   topic metadata is known fail instantly, so Java silently drops the first
   messages after every startup where librdkafka would buffer them. Compensated
   with a background metadata warm-up thread (off-thread, so a dead broker still
   cannot delay serving).
3. **`delivery.timeout.ms` must be >= `linger.ms + request.timeout.ms`** or the
   producer throws `ConfigException` at construction. Because the code correctly
   degrades to proxy-only on producer failure, the symptom was *traffic flowing
   perfectly with `tee_accepted = 0`* — total silent loss of the Kafka feed. The
   graceful-degradation path working as designed is what made it quiet.
4. **Pin `slf4j-api` to 2.x.** `kafka-clients` drags in 1.7.x; the mismatch
   downgrades logging to a NOP binding and hid trap #3 completely.
5. **JIT warm-up is not a rounding error.** The first frame through the Java tee
   took **353 ms** versus sub-millisecond once warm. Any benchmark without a
   warm-up phase is measuring the JIT.

### Which to run

**Rust for this workload** — not for throughput, which is comparable, but for
22–33x less idle memory, 1.6x less CPU per message, 40x faster startup, and no GC
to tune inline on an authorization path.

**Java is defensible if the team will maintain it better than they would
maintain Rust.** An unmaintainable Rust service is worse than a well-run Java
one, and Java's p50 delta is still well under a millisecond. But it needs
`max.block.ms=0`, the metadata warm-up, and GC tuning that was **not attempted**
— the Java build was measured entirely on JVM defaults.

**What the benchmark does not tell you:** behaviour across a real network,
against real VP and real FMS, over hours rather than seconds, or under a tuned
heap.

---

## 5. What is PROVEN vs ASSUMED

### Proven by execution

| claim | evidence |
|---|---|
| Starts with Kafka down | Broker refused on IPv4+IPv6; interceptor listened and served |
| Broker death mid-flight is invisible | 96,617 msgs, 0 errors, p50 `0.303 ms` — unchanged |
| Recovers without restart | +9,928 delivered after broker restart, +0 failures |
| Drop path works | 39,747 dropped of 726,576 offers (5.5%); 686,829 accepted — **broker count matched exactly** |
| Stream integrity | **0 mismatches** across >1.5M byte-compared transactions |
| Framing correctness | >1.33M frames, **0 desyncs**, incl. 8-frame pipelining and 4 KB frames spanning segments |
| Parse failure never suppresses publish | Binary frame → `parse_error` set, payload bytes `\xff\xfe\x00\x80\x01` intact, still delivered |
| RTT measurement is accurate | Client measured p50 `33.647 ms`; interceptor independently measured `rtt_ms` `33.554 ms` — agree within 0.1 ms |
| Graceful shutdown | `no longer accepting` → `flushing kafka producer` → `stopped` |
| Health check catches dead FMS | With FMS killed, process/port/healthz all reported UP; only the end-to-end probe caught it |
| Java port is functionally equivalent | Same envelope, same metric names, 2.36M msgs delivered, 0 mismatches, 0 desyncs |
| Rust memory fix is safe | 353,388 msgs after the change: 0 mismatches, 0 desyncs, 706,776 frames = exactly 2x |

### Assumed, NOT verified — the risk list

1. **The real VP↔FMS wire format is unknown.** `[framing]` assumes a 2-byte
   big-endian length prefix excluding itself. This is a common Vynamic/ISO 8583
   convention but was **never checked against a real capture**. A wrong guess
   does not break payments — it silently kills the Kafka feed. This is the
   single biggest open risk.
2. **`rtt_ms` assumes responses return in request order** on a connection. True
   for a strict request/response link. If VP pipelines and FMS answers out of
   order, `rtt_ms` is silently wrong. Mitigation: `pair_request_response = false`,
   then use `gap_ms` which needs no pairing assumption.
3. **Never tested across a network.** Loopback only.
4. **Never tested against real VP or real FMS.** Everything upstream/downstream
   was a Python mock.
5. **Kafka leg is PLAINTEXT-only.** The build has no `ssl`/`gssapi` features. If
   brokers require SASL/TLS, rebuild with those features on the `rdkafka`
   dependency and set `security.protocol` under `[kafka.properties]`.
6. **The `key_value` parser is deliberately literal.** It splits on delimiters
   without validating field names, so `this is not key=value` "parses" into
   `{"this is not key": "value"}`. `parse_error: null` does **not** mean the data
   was valid business data.
7. **The Java build was measured on JVM defaults.** No heap sizing, no GC
   selection, no tuning. Its memory numbers would improve with a capped heap;
   its CPU-per-message probably would not change much. Do not read §4b as "Java
   tuned versus Rust tuned" — it is "both out of the box".
8. **The Java build has had far less soak time than the Rust one.** It passed the
   same functional checks, but the chaos tests (broker killed mid-flight, drop
   path forced, recovery without restart) were only ever run against Rust.

---

## 6. Environment traps (cost real time — read before debugging)

### rustup is blocked on this network

`sh.rustup.rs` and `static.rust-lang.org` return **403** — even outside the
sandbox. rustup cannot be installed here.

```bash
brew install rust cmake openssl@3     # this is the only working path
```

Cargo itself works fine: `crates.io` (the website) is 403, but `index.crates.io`
(sparse index) and `static.crates.io` (downloads) both return 200, which is all
cargo needs.

### Piping through `tail` masks exit codes

`brew install rust 2>&1 | tail -20` reported **exit 0 while brew had actually
failed** (llvm download hit `Could not resolve host: ghcr.io`). The pipeline
returns `tail`'s status. Use `set -o pipefail` or redirect to a file. This burned
a full cycle of believing an install had succeeded.

### ghcr.io DNS is intermittent

Homebrew bottle downloads failed once with DNS resolution errors and succeeded on
a plain retry. If `brew install` fails on a download, just retry.

### Kafka CLI changed in 3.8+

- `kafka-run-class.sh kafka.tools.GetOffsetShell` → use `kafka-get-offsets.sh`
- `--property print.headers=true` is deprecated → `--formatter-property`

### `grep 'metric_name '` matches the `# TYPE` comment too

Prometheus output includes `# TYPE vp_interceptor_x counter` before each value.
Naive `grep | awk '{print $2}'` returns `TYPE`. Use
`awk '$1=="vp_interceptor_x"{print $2}'`.

### Python `socketserver` default backlog is 5

This silently RST'd 10 of 16 simultaneous connections in the first benchmark and
would have skewed every measurement. Fixed with `request_queue_size = 512` in
`testkit/mock_fms.py`. If you write another mock, set it.

### Java toolchain

- JDK 21 (Temurin) via SDKMAN; **Maven needs `source ~/.sdkman/bin/sdkman-init.sh`
  first** — `mvn` is a shell alias, not on `PATH` for non-interactive shells.
- Maven Central (`repo1.maven.org`) is reachable; only the Rust domains are
  blocked.
- Build: `mvn -f java-interceptor/pom.xml package` → `target/vp-fms-interceptor.jar`

### Local environment

- Kafka: `apache/kafka:latest`, container `kafka`, `localhost:9092`, on OrbStack
- Redpanda Console UI on `:8080`
- The Kafka container appears to have been recreated at some point — topics from
  earlier sessions vanished. Do not assume topic persistence.
- Java is installed; Go is not; Node and Python 3.14 are.

---

## 7. Rust API notes worth remembering

- `ClientConfig::set` is generic over `Into<String>` — a generic bound gets **no
  deref coercion** from `&String`. Use `.as_str()`.
- `rdkafka::message::Header { key, value: Some(&v) }` with `OwnedHeaders::insert`;
  chained temporaries live to end of statement, so inline `.to_string()` is fine.
- `BaseRecord<'_, str, [u8], ()>` is the annotation that works with
  `ThreadedProducer` whose `DeliveryOpaque = ()`.
- Disjoint field borrows work: `self.state.entry(..)` (mut) alongside
  `self.stats` / `self.publisher` (shared) compiles fine in one method body.
- `BytesMut::split().freeze()` gives a refcounted `Bytes` — but it keeps the
  WHOLE read allocation alive. A 122-byte message pinned its entire 16 KiB
  buffer while queued, peaking at 409 MB RSS at 256 connections. `rust-interceptor/src/proxy.rs`
  now copies into an exact-size `Bytes` when a read fills <1/4 of the buffer:
  peak RSS 409 MB -> 43 MB at no latency cost. See `BENCHMARK.md`.

---

## 8. Current message format

Key = `conn_id`. Headers duplicate the metadata so consumers can filter without
deserializing. Value:

```json
{
  "conn_id": 1,
  "direction": "vp_to_fms",
  "seq": 1,
  "peer": "127.0.0.1:49687",
  "ts_ms": 1786618986985,
  "length": 122,
  "conn_age_ms": 31.128,
  "gap_ms": 0.127,
  "rtt_ms": 33.554,
  "fields": {
    "msgType": "request",
    "txnRef": "00000001",
    "accountName": "Zacky",
    "accountNumber": "11020134353",
    "bankCode": "1234",
    "amount": "000000010000",
    "currency": "360"
  },
  "encoding": "base64",
  "payload": "bXNnVHlwZT1yZXF1ZXN0..."
}
```

- `rtt_ms` — `fms_to_vp` frames only; how long FMS took to answer
- `gap_ms` — since previous frame on this connection, either direction
- `conn_age_ms` — since the connection was accepted
- `parse_error` appears **instead of** `fields` when parsing fails; payload always survives

Switches: `[kafka] value_format = json|raw`, `payload_encoding = base64|hex|utf8`,
`[parse] mode = key_value|none`, `[timing] enabled`, `pair_request_response`.

---

## 9. Open items

**Before production**

1. **Verify the real framing against a live capture.** Run
   `testkit/null_proxy.py --tee-framing` inline on real VP↔FMS traffic and
   confirm `desyncs=0`. Nothing else matters until this is done.
2. **PCI scope.** The link is plaintext, so the interceptor sees full PANs /
   account numbers, and they currently reach Kafka in cleartext (visible in
   `payload` and `fields.accountNumber`). The topic inherits full CDE scope:
   encryption at rest, access control, retention limits. **Masking in the tee
   worker before publish is the cheapest fix and is far cheaper now than after
   the first consumer exists.**
3. **Decide on Kafka transport.** Current build is PLAINTEXT-only.
4. **Confirm `pair_request_response`** matches how VP actually behaves.
5. **Admin endpoint has no auth or TLS.** Bound to `127.0.0.1` by default. Do not
   expose it.

**Nice to have**

- Field-level ISO 8583 parsing (RUNBOOK §10 describes where and the two rules:
  keep it on the shard workers; never let a parse failure suppress the publish)
- `rtt_ms` correlation by business key (e.g. `txnRef`) instead of FIFO order —
  would remove assumption #2 entirely. Requires moving parsing from
  `rust-interceptor/src/kafka.rs` into `rust-interceptor/src/tee.rs` so the correlation key is available there.
- Alert on **rate of increase** of `upstream_connect_failed`, not absolute value —
  counters are cumulative, so one historical blip pages forever.

**Housekeeping**

- Topics left in the broker from testing: `vp.fms.iso8583`, `vp.fms.timing`.
- `testkit/config.droptest.toml` is a deliberately broken config (1 shard, 1-slot
  queue) used to force the drop path. Never use it for anything else.

---

## 10. File map

| file | role |
|---|---|
| `rust-interceptor/src/main.rs` | accept loop, connection limit, graceful drain |
| `rust-interceptor/src/proxy.rs` | **the hot path** — two pumps per connection |
| `rust-interceptor/src/tee.rs` | bounded queue, shard workers, per-connection timing |
| `rust-interceptor/src/framing.rs` | length-prefix reassembler |
| `rust-interceptor/src/parse.rs` | `key=value` field extraction |
| `rust-interceptor/src/kafka.rs` | fire-and-forget producer, JSON envelope, encodings |
| `rust-interceptor/src/stats.rs` | atomic counters |
| `rust-interceptor/src/admin.rs` | `/metrics`, `/healthz` (hand-rolled, no web framework) |
| `config.toml` | all tuning |
| `scripts/status.sh` | health check **with end-to-end probe** — use its exit code |
| `examples/send_payment.py` | standalone client to copy into your project |
| `examples/consume_json.py` | decodes envelopes, `--field k=v` filter |
| `testkit/` | load, chaos, and A/B benchmark rig (pure stdlib) |
| `java-interceptor/` | Java 21 port — see its README for the three real differences |
| `BENCHMARK.md` | **Rust vs Java: measured, with confidence levels** |
| `benchmark/compare.py` | runs the comparison end to end |
| `benchmark/README.md` | **how to run it** — prerequisites, flags, noise caveats |
| `benchmark/results*.json` | raw output of both runs |

**Metrics that matter:** `framer_desyncs` (framing config wrong → Kafka feed
dead), `tee_dropped` (Kafka degraded → payments fine), `upstream_connect_failed`
(**FMS unreachable → payments incident**), `rtt_unmatched` (FMS dropping
requests, or the ordering assumption is wrong).

The on-call distinction that matters: `tee_dropped`/`kafka_*` climbing while
`bytes_vp_to_fms` keeps rising is a **Kafka** incident with payments healthy.
`upstream_connect_failed` climbing is a **payments** incident. Page differently.

---

## 11. Resuming work

```bash
cd "/Users/zacky/Documents/vynamic listener"
export PATH="/opt/homebrew/bin:$PATH"     # cargo is Homebrew-installed

(cd rust-interceptor && cargo test && cargo build --release)   # 19 tests

cd testkit && python3 mock_fms.py --port 8583 &
cd .. && ./rust-interceptor/target/release/vp-fms-interceptor config.toml &
./scripts/status.sh                        # exit 0 = healthy

python3 examples/send_payment.py --target 127.0.0.1:9100 --count 3
cd testkit && python3 bench.py             # A/B latency delta, exits non-zero on regression

pkill -f vp-fms-interceptor; pkill -f mock_fms.py
```

`bench.py` fails the run on payload corruption or a p99 delta beyond
`--budget-ms`. It is the closest thing to a regression gate.

### Building and benchmarking the Java port

```bash
source ~/.sdkman/bin/sdkman-init.sh          # mvn is an alias, not on PATH
mvn -f java-interceptor/pom.xml package      # -> java-interceptor/target/vp-fms-interceptor.jar
java -jar java-interceptor/target/vp-fms-interceptor.jar config.toml

# head-to-head (needs mock_fms on 8583; starts and stops each build itself)
python3 benchmark/compare.py --duration 12 --rate 400 --conns 8,64,256
```

`compare.py` warms both builds identically before measuring — without that, the
JVM's JIT dominates and the numbers are meaningless (§4b).
