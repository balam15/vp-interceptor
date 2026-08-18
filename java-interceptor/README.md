# java-interceptor

Java 21 implementation of the same VP→FMS interceptor. Structurally mirrors the
Rust build class-for-module so the two can be compared meaningfully — see
`../BENCHMARK.md` for measured results.

**Reads the same `../config.toml`.** Deliberately: a benchmark between two
implementations is only meaningful if neither can quietly run different settings.

## Build and run

```bash
mvn package                                    # -> target/vp-fms-interceptor.jar (19 MB)
java -jar target/vp-fms-interceptor.jar ../config.toml
```

Requires JDK 21+ for virtual threads.

## Structure

| class | mirrors | role |
|---|---|---|
| `Main` | `rust-interceptor/src/main.rs` | accept loop, connection limit, shutdown hook |
| `Proxy` | `proxy.rs` | the hot path — one virtual thread per direction |
| `Tee` | `tee.rs` | bounded queue, shard workers, per-connection timing |
| `Framer` | `framing.rs` | length-prefix reassembler |
| `KvParser` | `parse.rs` | `key=value` field extraction |
| `Publisher` | `kafka.rs` | fire-and-forget producer, JSON envelope |
| `Stats` | `stats.rs` | counters (same metric names) |
| `Admin` | `admin.rs` | `/metrics`, `/healthz` |

Metric names and the Kafka envelope are byte-for-byte compatible with the Rust
build, so the same dashboards and consumers work against either.

## Three things that differ from the Rust build

These are not style choices — the client libraries genuinely differ.

**1. `max.block.ms=0` is mandatory, not tuning.**
`KafkaProducer.send()` blocks for up to `max.block.ms` when the buffer is full or
metadata is missing. At the 60s default, Kafka would sit directly in the payment
path — the exact failure this design exists to prevent. librdkafka never blocks.

**2. A metadata warm-up thread is required because of (1).**
With `max.block.ms=0`, sends issued before topic metadata is known fail
instantly, so without warm-up the first messages after every startup are silently
dropped. librdkafka buffers them. `Publisher.warmUpMetadataInBackground()`
compensates — off-thread, so a dead broker still cannot delay serving.

**3. The tee must copy.**
Java has no equivalent of Rust's refcounted `Bytes`, so `Tee.offer` copies into a
right-sized array. This looked like a disadvantage and turned out to be the
opposite: the Rust build originally pinned whole 16 KiB read buffers behind small
messages and peaked at 409 MB. Java cannot make that mistake. See
`../BENCHMARK.md`.

## Virtual threads, not a thread pool

One virtual thread per connection direction — the same shape as the Rust build's
task-per-direction. Verified to 256 concurrent connections.

The tee shard workers are **platform** threads, not virtual: they are long-lived
and CPU-bound, so virtual threads would add scheduling overhead per frame and buy
nothing.

## Gotchas hit during bring-up

- **`delivery.timeout.ms` must be ≥ `linger.ms + request.timeout.ms`**, or the
  producer throws `ConfigException` at construction. Because the code degrades to
  proxy-only on producer failure, the symptom is traffic flowing perfectly with
  `tee_accepted = 0` — a silent loss of the entire Kafka feed.
- **Pin `slf4j-api` to 2.x.** `kafka-clients` drags in 1.7.x; the mismatch
  silently downgrades logging to a NOP binding and hides startup errors exactly
  when you need them.
