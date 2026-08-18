# testkit — local verification rig

Pure-stdlib Python. No install step, no dependencies. Everything here targets an
interceptor over TCP, so it works against the Rust build, against the reference
Python proxy, or against nothing at all.

```
vp_client.py ──TCP──> [ interceptor ] ──TCP──> mock_fms.py
   (load gen)               (SUT)                (mock FMS)
                              │
                              └──> Kafka (docker-compose.yml)
```

| file | role |
|---|---|
| `protocol.py` | wire format: 2-byte BE length prefix + pipe-delimited ISO-8583-ish body |
| `mock_fms.py` | mock FMS: replies, and can inject delay, jitter, drops, and RSTs |
| `vp_client.py` | mock VP: N parallel connections, latency percentiles, **integrity checks** |
| `null_proxy.py` | reference passthrough proxy — validates the rig, and is the latency floor |
| `bench.py` | A/B driver: direct vs intercepted, prints the delta, exits non-zero on regression |
| `docker-compose.yml` | single-node Kafka (KRaft) |

## The point of `vp_client.py`

It is not just a load generator. Every response is compared **byte-for-byte**
against what was sent (`mock_fms` echoes the body back with the MTI flipped), so
any corruption, truncation, or reordering by the interceptor surfaces as
`mismatches > 0` and a non-zero exit — rather than as a benchmark that passes
while quietly mangling payment traffic.

## Quick start

```bash
cd testkit

# terminal 1
python3 mock_fms.py --port 8583

# terminal 2 — the real interceptor (either implementation)
../rust-interceptor/target/release/vp-fms-interceptor ../config.toml
# or: java -jar ../java-interceptor/target/vp-fms-interceptor.jar ../config.toml
# or the dependency-free reference proxy, to sanity-check the rig itself:
#     python3 null_proxy.py --listen 127.0.0.1:9100 --upstream 127.0.0.1:8583 --tee-framing

# terminal 3
python3 bench.py --fms 127.0.0.1:8583 --interceptor 127.0.0.1:9100
```

`bench.py` runs both legs and prints the delta. **The delta is the deliverable
number.** Absolute latency is dominated by the Python mock and the load
generator; only the difference between the two legs is the interceptor's cost.

## Test matrix

```bash
# parallel connections — VP opens its pool all at once
python3 vp_client.py --target 127.0.0.1:9100 --connections 64 --duration 20

# several frames per TCP segment (breaks naive framing code)
python3 vp_client.py --target 127.0.0.1:9100 --pipeline 8 --duration 10

# frames larger than the read buffer, forcing reassembly across segments
python3 vp_client.py --target 127.0.0.1:9100 --pad 8000 --duration 10

# realistic FMS think-time, so latency is not pure loopback
python3 mock_fms.py --port 8583 --delay-ms 5 --jitter-ms 3

# hostile FMS: silent drops and abortive resets
python3 mock_fms.py --port 8583 --drop-rate 0.01 --reset-rate 0.001
```

## The acceptance tests that matter

These are the ones tied directly to the stated requirements.

**1. Kafka down at startup.** Do not start Kafka. Start the interceptor. It must
listen and serve normally; `bench.py` must PASS. This proves a failed broker
handshake cannot gate the payment path.

```bash
docker compose down
# start interceptor, then:
python3 bench.py
```

**2. Kafka dies under load.** Start Kafka, begin a long run, kill the broker
mid-flight. Latency must not move.

```bash
docker compose up -d
python3 vp_client.py --target 127.0.0.1:9100 --connections 32 --duration 60 &
sleep 20 && docker compose stop kafka
```

Then check the interceptor's counters on `:9101/metrics`: `tee_dropped` and
`kafka_enqueue_failed` should climb while `bytes_vp_to_fms` keeps advancing at
the same rate. **Those counters climbing while latency stays flat is the proof
that the isolation boundary works** — it is the single most important
observation in this whole rig.

**3. Kafka recovers.** `docker compose start kafka`. Publishing resumes. No
restart of the interceptor, no impact on VP.

**4. Slow broker (worse than a dead one).** A broker that accepts connections but
stalls is the nastiest case, because a naive implementation blocks on it. Add
latency to the broker port and confirm `tee_dropped` rises instead of p99.

## Reading the Kafka side

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vp.fms.iso8583 \
  --from-beginning --property print.headers=true
```

Headers carry `conn_id`, `direction`, `seq`, `peer`, `ts_ms`. The value is a JSON
envelope whose `payload` field holds the reassembled frame, encoded per
`[kafka] payload_encoding`. Pipe it through the decoder to read it:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vp.fms.iso8583 --from-beginning \
  | python3 ../examples/consume_json.py
```

## Known measurement caveats

- The Python load generator saturates around 40–70k msg/s on loopback and is
  GIL-bound. **Always use `--rate`** for latency work; unthrottled runs measure
  the client.
- Baseline `p99.9` is several milliseconds *even direct to FMS*, caused by
  CPython GC in the mock and the client. Do not read absolute tail numbers as the
  interceptor's; read the delta.
- Loopback has no NIC, no real RTT, and near-zero jitter. It will flatter any
  proxy. Re-run across two hosts before trusting the numbers for production
  sizing.
- `mock_fms.py` is threaded, one thread per connection. Above ~200 connections it
  becomes the bottleneck, not the interceptor.
