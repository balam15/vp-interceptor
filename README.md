# vp-fms-interceptor

Transparent TCP interceptor between **Vynamic Payments (VP)** and a **Fraud
Management System (FMS)**. It forwards the connection unchanged and, as a side
effect, publishes a copy of the message stream to Kafka.

```
VP ═══TCP═══> [ interceptor ] ═══TCP═══> FMS     ← hot path: blocking, ordered, untouched
                    │
                    └─ try_send (never blocks, drops when full)
                         │
                    [shard workers] → framer → librdkafka → Kafka
```

## The contract

The interceptor sits inline on an authorization path. Everything below follows
from one rule: **Kafka must never be able to affect VP↔FMS.**

1. **Forward first, tee second.** The pump writes to the far socket, *then*
   offers a copy to the publish path. Kafka work is never in the critical
   section. (`rust-interceptor/src/proxy.rs`)
2. **A bounded queue is the isolation boundary.** `try_send` into a fixed-size
   channel; when full, frames are counted and dropped. No unbounded buffering, so
   no path by which a slow broker becomes backpressure on the socket.
   (`rust-interceptor/src/tee.rs`)
3. **Framing never gates forwarding.** Bytes are proxied raw as they arrive; a
   *separate* accumulator reassembles length-prefixed frames for Kafka. A framing
   bug can corrupt the Kafka feed but can never corrupt or stall FMS traffic.
   (`rust-interceptor/src/framing.rs`)
4. **Kafka cannot fail startup.** librdkafka connects lazily, so a down or
   unreachable broker is a non-event. If producer *construction* fails, the
   service logs and runs proxy-only rather than exiting. (`rust-interceptor/src/kafka.rs`)

## Why Rust

Not for throughput — for **tail latency**. Inline on an authorization path, the
thing that hurts is the p99.9 outlier that trips an FMS timeout, and that is
exactly what a GC produces. No GC, no runtime pauses, no stop-the-world.

Parallelism is the easy part: `tokio::spawn` per connection, two pump tasks each,
spread across cores by a work-stealing scheduler. Connections are independent, so
this scales flat. Publishing deliberately fans *in*, not out — all connections
share one producer via `shards` workers, because per-connection producers would
destroy batching and multiply broker sockets.

Go is a defensible second choice if the team knows it better. Java inline on this
path is not, without serious GC work.

## Layout

| file | role |
|---|---|
| `rust-interceptor/src/main.rs` | accept loop, connection limit, graceful drain |
| `rust-interceptor/src/proxy.rs` | the hot path — two pumps per connection |
| `rust-interceptor/src/tee.rs` | bounded queue, sharded framing/publish workers |
| `rust-interceptor/src/framing.rs` | length-prefix reassembler (unit-tested) |
| `rust-interceptor/src/kafka.rs` | fire-and-forget producer, delivery-report counters |
| `rust-interceptor/src/admin.rs` | `/metrics`, `/healthz` |
| `config.toml` | all tuning |
| `HANDOFF.md` | **decisions, measured numbers, proven-vs-assumed, environment traps** |
| `RUNBOOK.md` | how to run it and send traffic to it |
| `BENCHMARK.md` | Rust vs Java comparison — memory, CPU, latency, startup |
| `benchmark/README.md` | how to run that comparison yourself |
| `java-interceptor/` | Java 21 implementation of the same design |
| `scripts/status.sh` | health check with an end-to-end probe |
| `examples/send_payment.py` | standalone Python client to copy into your own project |
| `examples/consume_json.py` | decodes the JSON envelopes off the topic |
| `testkit/` | local verification rig — see `testkit/README.md` |

## Build

Requires a Rust toolchain plus `cmake` and OpenSSL (librdkafka builds from
source):

```bash
# rustup.rs is blocked on this network (403) -- install Rust via Homebrew
brew install rust cmake openssl@3

cd rust-interceptor
cargo test              # 19 unit tests
cargo build --release
./target/release/vp-fms-interceptor ../config.toml
```

## Verify

The `testkit/` rig is pure-stdlib Python — no install — and checks payload
integrity, not just latency:

```bash
cd testkit
python3 mock_fms.py --port 8583 &
../rust-interceptor/target/release/vp-fms-interceptor ../config.toml &
python3 bench.py --fms 127.0.0.1:8583 --interceptor 127.0.0.1:9100
```

`bench.py` reports the p50/p99/p99.9 **delta** between direct-to-FMS and
intercepted, and exits non-zero on corruption or a p99 regression beyond budget.

## Metrics that matter

On `:9101/metrics`:

| counter | meaning |
|---|---|
| `tee_dropped` | frames discarded because a shard queue was full — **alert on this** |
| `kafka_enqueue_failed` | librdkafka's queue was full |
| `kafka_delivery_failed` | broker rejected or timed out |
| `framer_desyncs` | framing config does not match the real wire format |
| `bytes_vp_to_fms` | payment path throughput |

`tee_dropped` climbing while `bytes_vp_to_fms` advances at an unchanged rate is
the isolation boundary doing its job. That is a Kafka incident, not a payments
incident, and the two should page differently.

## Before production

- **Confirm the real framing.** `[framing]` defaults to a 2-byte big-endian
  length prefix excluding itself. Verify against an actual VP↔FMS capture; run
  with `testkit/null_proxy.py --tee-framing` against real traffic and confirm
  `desyncs=0` before trusting the Kafka feed.
- **TLS — confirmed not in use.** VP↔FMS is plain TCP, so the interceptor reads
  frames directly and no TLS termination is needed. Revisit this if the link is
  ever encrypted: at that point the Kafka feed becomes ciphertext, and making it
  readable again would mean terminating TLS here, which is a PCI scope decision
  rather than a technical one.
- **PCI DSS.** Raw ISO 8583 contains PANs, and because the link is plaintext the
  interceptor sees them in the clear. The Kafka topic therefore inherits full CDE
  scope: encryption at rest, access control, retention limits. Strongly consider
  masking or tokenizing PAN in the framer before publish — the cheapest time to
  do it is before the first consumer exists.
- **Kafka transport.** The link being plaintext says nothing about the Kafka leg.
  If the brokers require SASL/TLS, rebuild with the `ssl` and `gssapi` features on
  the `rdkafka` dependency and set `security.protocol` under
  `[kafka.properties]`; the current build is PLAINTEXT-only.
- **Ordering.** Messages are keyed by `conn_id`, so per-connection order is
  preserved within a partition. There is no global ordering across connections.
- **Drops are silent by design.** If the business needs a guaranteed audit trail,
  this architecture is the wrong one — that requires durable spooling, which
  reintroduces the latency risk this design exists to avoid.
