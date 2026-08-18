# Runbook — running the interceptor and sending traffic to it

Everything in this file has been executed against the real binary. Commands are
copy-pasteable from the project root.

---

## 1. What you are starting

The interceptor sits **between** VP and FMS. It is a man-in-the-middle by design:
VP must be pointed at the interceptor instead of at FMS.

```
VP (your script)  ──TCP──>  interceptor :9100  ──TCP──>  FMS :8583
                                  │
                                  └──> Kafka :9092   topic vp.fms.iso8583
```

| port | who listens | purpose |
|---|---|---|
| `9100` | interceptor | **point VP here** |
| `8583` | FMS | real upstream, configured as `[upstream] addr` |
| `9101` | interceptor | admin: `/metrics`, `/healthz` |
| `9092` | Kafka | broker |

---

## 2. Prerequisites

```bash
# Rust toolchain (rustup.rs is blocked on this network; use Homebrew)
brew install rust cmake openssl@3

cd rust-interceptor && cargo build --release   # ~40s; librdkafka builds from source
```

Produces `rust-interceptor/target/release/vp-fms-interceptor` (~3.4 MB, no runtime deps).

Kafka must be reachable but **does not need to be running** — see §6.

---

## 3. Configure

Everything lives in `config.toml`. The five settings you will actually change:

```toml
[listen]
addr = "0.0.0.0:9100"          # where VP connects

[upstream]
addr = "127.0.0.1:8583"        # where the real FMS is

[kafka]
brokers = "localhost:9092"
topic = "vp.fms.iso8583"

[framing]
prefix_bytes = 2               # MUST match the real VP<->FMS wire format
```

> **`[framing]` is the one setting that can silently go wrong.** If it does not
> match reality, VP↔FMS traffic still flows correctly — but the framer desyncs
> and the Kafka feed silently stops. Verify with `framer_desyncs == 0` (§7).

Run with a different file to use a different profile:

```bash
./rust-interceptor/target/release/vp-fms-interceptor /path/to/other-config.toml
```

---

## 4. Start it

```bash
./rust-interceptor/target/release/vp-fms-interceptor config.toml
```

Healthy startup looks like this:

```
INFO kafka: kafka producer created (lazy connect) brokers=localhost:9092 topic=vp.fms.iso8583
INFO tee: tee workers started shards=4 queue_capacity=8192
INFO interceptor ready listen=0.0.0.0:9100 upstream=127.0.0.1:8583 max_connections=4096
INFO admin: admin listening (/metrics, /healthz) addr=127.0.0.1:9101
```

`interceptor ready` is the line that matters. Increase detail with
`RUST_LOG=debug`; quieten librdkafka noise with `RUST_LOG=info,rdkafka=warn`.

Stop with `Ctrl-C` or `SIGTERM` — it stops accepting, drains in-flight
connections (30s cap), flushes Kafka, then exits.

### Need an FMS to test against?

```bash
cd testkit && python3 mock_fms.py --port 8583
```

---

## 5. Send traffic from your own Python script

Copy `examples/send_payment.py` into your project — it is standalone, stdlib
only, and imports nothing from this repo.

### Sending real ISO 8583

`send_payment.py` sends a `key=value` stand-in body. For traffic that matches
the spec the VP team supplied, use `examples/send_iso8583.py` instead — same
CLI, but the body is built from `EIS_FMS_ISO8583_jPOS_Packager.xml`:

```bash
python3 examples/send_iso8583.py --target 127.0.0.1:9100 --count 50
python3 examples/send_iso8583.py --dump --template transfer   # breakdown, sends nothing
```

Templates are `transfer` (0200, 263 bytes), `reversal` (0420), and `echo`
(0800). The codec lives beside it in `examples/iso8583.py` — copy both files,
not just the client. `python3 examples/iso8583.py --selftest` round-trips every
template.

Two things to expect locally: `testkit/mock_fms.py` is not ISO-aware and echoes
rather than replying with a real 0210, and the shipped `[parse] mode =
"key_value"` will "parse" an ISO frame into nonsense while still reporting
`parse_error: null`. Set `mode = "none"` before trusting `fields`.

### Minimal version

```python
import socket, struct

HOST, PORT = "127.0.0.1", 9100          # the INTERCEPTOR, not FMS
PREFIX = struct.Struct(">H")            # 2-byte big-endian, excludes itself

def recv_exact(sock, n):
    """recv() may return fewer bytes than requested. Always loop."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf

sock = socket.create_connection((HOST, PORT))
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # do not skip this

body = b"msgType=request,txnRef=00000001,accountName=Zacky,accountNumber=11020134353,bankCode=1234"
sock.sendall(PREFIX.pack(len(body)) + body)                  # send

(length,) = PREFIX.unpack(recv_exact(sock, 2))               # receive
response = recv_exact(sock, length)
print(response.decode())

sock.close()
```

### Reusable client

```python
from send_payment import VPClient, build_message

with VPClient("127.0.0.1", 9100) as client:
    response = client.request(build_message(1, account_name="Zacky",
                                            account_number="11020134353",
                                            bank_code="1234"))
    print(response.decode())
```

The body is comma-separated `key=value`, matching `[parse]` in `config.toml`, so
these become real JSON keys in Kafka rather than opaque bytes:

```
msgType=request,txnRef=00000001,accountName=Zacky,accountNumber=11020134353,bankCode=1234,amount=000000010000,currency=360
```

Constructor arguments must mirror `config.toml`'s `[framing]`:

```python
VPClient(
    host="127.0.0.1",
    port=9100,
    prefix_bytes=2,               # framing.prefix_bytes
    big_endian=True,              # framing.big_endian
    length_includes_prefix=False, # framing.length_includes_prefix
)
```

### Parallel connections

The interceptor handles connections independently, one task per direction. Plain
threads are fine on the client side:

```python
import threading

def worker(n):
    with VPClient("127.0.0.1", 9100) as c:
        for i in range(100):
            c.request(build_message(n * 100000 + i))

threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
[t.start() for t in threads]
[t.join() for t in threads]
```

### From the CLI

```bash
python3 examples/send_payment.py --target 127.0.0.1:9100 --count 3
python3 examples/send_payment.py --target 127.0.0.1:9100 --connections 8 --count 20 --quiet
```

### Three rules for any client you write

1. **Set `TCP_NODELAY`.** Nagle batches small messages and adds tens of ms.
2. **Never trust one `recv()`.** TCP is a byte stream; a "message" can arrive
   split across reads or several messages can arrive in one. Always loop to the
   declared length.
3. **Match the framing to `config.toml`.** A mismatch does not break payments —
   it silently breaks your Kafka feed, which is much harder to notice.

---

## 6. Kafka behaviour you can rely on

The interceptor never waits on Kafka. Verified:

| situation | what happens |
|---|---|
| Broker down at startup | Starts and serves normally; logs the refusal |
| Broker dies under load | No latency change; 96,617 msgs, 0 errors |
| Broker comes back | Publishing resumes; **no restart needed** |
| Publish queue saturated | Frames dropped and counted; payment path unaffected |
| Bad Kafka config | Logs the error, runs proxy-only rather than exiting |

Measured overhead versus talking to FMS directly: **+0.047 ms p50, +0.419 ms
p99** with Kafka actively publishing.

---

## 7. Verify it is healthy

### Quickest answer

```bash
./scripts/status.sh          # exit 0 = healthy, 1 = not
```

Runs five checks and, critically, pushes a real frame end-to-end through to FMS
and back. Use its exit code for monitoring.

The probe frame matches `[parse]`, so it lands in Kafka as clean fields:

```
msgType=healthcheck,source=status.sh,txnRef=00000000
```

Override it for a different wire format:

```bash
PROBE_BODY='0800|STAN=0|NETMGMT=ECHO' ./scripts/status.sh
```

> **Each health check publishes two messages** (request and response) to the
> topic — it is real traffic through the real path, which is exactly why it
> catches what a port check misses. Filter them out downstream on
> `fields.msgType == "healthcheck"`. At a 30s monitoring interval that is ~5,760
> messages/day; if that is unwelcome, point `PROBE_BODY` at a value your
> consumers already ignore, or probe less often.

### Checking by hand

```bash
curl -s localhost:9101/healthz          # -> ok
pgrep -fl vp-fms-interceptor            # process alive?
lsof -nP -iTCP:9100 -sTCP:LISTEN        # listening?
curl -s localhost:9101/metrics
```

> **Liveness is not health.** All four checks above pass happily while FMS is
> unreachable — the interceptor is listening, it just cannot complete anything.
> Only the end-to-end probe in `status.sh` catches that, which is why it exists.
> If you wire one thing into monitoring, wire that.

| counter | healthy | means trouble when |
|---|---|---|
| `framer_desyncs` | **0** | non-zero → `[framing]` is wrong, Kafka feed is dead |
| `tee_dropped` | 0 | climbing → Kafka degraded (payments still fine) |
| `kafka_delivery_failed` | 0 | climbing → broker rejecting or unreachable |
| `upstream_connect_failed` | 0 | climbing → **FMS is unreachable — real incident** |
| `conns_rejected` | 0 | non-zero → raise `listen.max_connections` |
| `bytes_vp_to_fms` | rising | flat under load → payments are stalled |

Drop rate = `tee_dropped / (tee_accepted + tee_dropped)`.

**The distinction that matters at 3am:** `tee_dropped` and `kafka_*` climbing
while `bytes_vp_to_fms` keeps rising is a *Kafka* incident — payments are
healthy. `upstream_connect_failed` climbing is a *payments* incident. Page
differently on these.

### Confirm data is reaching Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic vp.fms.iso8583

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vp.fms.iso8583 \
  --from-beginning --max-messages 5 \
  --formatter-property print.headers=true --formatter-property print.key=true
```

Each transaction produces **two** messages (request and response), so the offset
count is 2× your transaction count.

### Message format

- **key** — `conn_id` (keeps one connection's messages ordered in one partition)
- **headers** — `conn_id`, `direction`, `seq`, `peer`, `ts_ms` (duplicated from
  the body so you can filter without deserializing)
- **value** — a JSON envelope:

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
  "payload": "bXNnVHlwZT1yZXF1ZXN0LHR4blJlZj0wMDAw..."
}
```

### Per-connection timing

| field | meaning | on |
|---|---|---|
| `conn_age_ms` | ms since this connection was accepted | every frame |
| `gap_ms` | ms since the previous frame on this connection, either direction | every frame after the first |
| `rtt_ms` | request → response time: **how long FMS took to answer** | `fms_to_vp` frames only |

Measured on the tee workers from reassembly timestamps, so it costs the
forwarding path nothing (verified: p50 delta unchanged at +0.03–0.05 ms).

Aggregates on `:9101/metrics` — exposed as sum/count/max rather than an average,
so a scraper can compute rates over an interval:

```
rtt_count  rtt_sum_us  rtt_max_us  rtt_unmatched
conn_duration_count  conn_duration_sum_ms  conn_duration_max_ms
```

```bash
# average FMS response time since start
curl -s localhost:9101/metrics | awk '
  $1=="vp_interceptor_rtt_count"{c=$2} $1=="vp_interceptor_rtt_sum_us"{s=$2}
  END{if(c)printf "FMS avg %.3f ms over %d samples\n",(s/c)/1000,c}'
```

> **`rtt_ms` assumes responses return in request order** on a given connection.
> That holds for a strict request/response link. If VP pipelines *and* FMS can
> answer out of order, the pairing mismatches and `rtt_ms` becomes wrong —
> silently. Set `pair_request_response = false` and use `gap_ms` instead, which
> needs no pairing assumption.
>
> `rtt_unmatched` counts requests that closed without a response. A few at
> shutdown is normal (in-flight when the connection closed). A steadily climbing
> value means FMS is dropping requests, or the ordering assumption is wrong.

`fields` comes from `[parse]` splitting the frame on `,` and `=`. `payload` is
always the reassembled frame, byte-identical to the wire — it is kept even when
parsing succeeds, so the topic remains a faithful record of what crossed the
link, not just an interpretation of it.

Controlled by `[parse]` and `[kafka]`:

```toml
[parse]
mode = "key_value"           # none | key_value
pair_delimiter = ","
kv_delimiter = "="
trim = true
max_fields = 64

[kafka]
value_format = "json"        # json | raw
payload_encoding = "base64"  # base64 | hex | utf8
```

### When a frame does not parse

Parsing **never** suppresses publishing. A frame that cannot be parsed is emitted
with `parse_error` instead of `fields`, payload intact:

```json
{"conn_id":2,"direction":"vp_to_fms","seq":2,"ts_ms":1786619015476,"length":5,
 "parse_error":"not valid utf-8: invalid utf-8 sequence of 1 bytes from index 0",
 "encoding":"base64","payload":"//4AgAE="}
```

A message you cannot parse is still evidence; dropping it would be the worse
outcome. Failures are logged rate-limited (1st, then every 1000th).

> **`key_value` is deliberately literal.** It splits on the delimiters and does
> not validate field names, so `this is not key=value` parses "successfully"
> into `{"this is not key": "value"}`. It reports failure only on genuinely
> undecodable input — non-UTF-8, a missing `=`, or more than `max_fields`. If
> your real traffic is binary ISO 8583, set `mode = "none"` and see §10.

| encoding | when |
|---|---|
| `base64` | **default.** Safe for binary ISO 8583, ~1.33× size |
| `hex` | traditional ISO 8583 dump format, easy to read against a spec, 2× size |
| `utf8` | text protocols only — **lossy on binary**, never for evidence |

`value_format = "raw"` restores the previous behaviour: bare frame bytes as the
value, metadata in headers only. Lower overhead, but not self-describing.

### Decoding it

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vp.fms.iso8583 --from-beginning \
  | python3 examples/consume_json.py
```

```
conn=1 seq=1 -> vp_to_fms ts=1786618986985 len=122
   accountName      = Zacky
   accountNumber    = 11020134353
   amount           = 000000010000
   bankCode         = 1234
   currency         = 360
   msgType          = request
   txnRef           = 00000001
```

Filter on a parsed field:

```bash
... | python3 examples/consume_json.py --field accountNumber=11020134353
```

In your own consumer, the only part you need is `decode_envelope`:

```python
import base64

def decode_envelope(envelope: dict) -> bytes:
    enc = envelope.get("encoding", "base64")
    if enc == "base64":
        return base64.b64decode(envelope["payload"])
    if enc == "hex":
        return bytes.fromhex(envelope["payload"])
    return envelope["payload"].encode()
```

Read `encoding` from the message rather than hardcoding it — that way the
consumer keeps working if the interceptor is reconfigured.

---

## 10. Extending: parsing ISO 8583 into JSON fields

The envelope keeps `payload` opaque on purpose. Decoding ISO 8583 into named
fields (`pan`, `stan`, `mti`, `amount`) needs the message spec for your specific
installation — field 48 and the private fields differ per deployment.

**We now have that spec.** `EIS_FMS_ISO8583_jPOS_Packager.xml` is the VP team's
packager, and `examples/iso8583.py` implements it: the field table, the four
composite fields (DE 3, 22, 43, 61), and the bitmap rules. Port that table
rather than re-deriving it. Two things it settles that were previously assumed:

- The payload is **ASCII, not binary** — every field is an `IFA_*` class. That
  makes `payload_encoding = "hex"` genuinely readable against the spec.
- The bitmap is **32 hex characters**, not the 16 the XML's own comment claims;
  DE 100/102/103/123/125/127 are defined, so a secondary bitmap is required.
  Confirm this with the VP team, since the two readings are incompatible.

If you want field-level JSON, the place to add it is `Framer`'s output in
`rust-interceptor/src/tee.rs`, where whole frames already exist. Two cautions:

1. **Keep it off the hot path.** It already is — frames are parsed on the shard
   workers, never on the forwarding path. Adding parsing there cannot affect
   VP↔FMS latency. Measured: JSON + base64 encoding added no detectable p50/p99
   cost versus raw.
2. **Never let a parse failure stop publishing.** Emit the envelope with the
   opaque payload and a `parse_error` field instead. A message you cannot parse
   is still evidence; dropping it is a worse outcome than storing it unparsed.

Since the link is plaintext, this is also the natural place to mask or tokenize
PAN before anything reaches Kafka.

---

## 8. Troubleshooting

| symptom | cause | fix |
|---|---|---|
| `connect failed: Connection refused` from your script | interceptor not running | check `interceptor ready` in its log |
| Client hangs with no response | `[upstream] addr` wrong, or FMS down | check `upstream_connect_failed` |
| `binding listen address: Address already in use` | port taken | `lsof -nP -iTCP:9100 -sTCP:LISTEN` |
| `framer_desyncs` climbing | `[framing]` wrong | see §3; payments are unaffected |
| Kafka empty but payments fine | broker unreachable, or framing wrong | check `kafka_delivery_failed`, `framer_desyncs` |
| `implausible frame length` in your client | client framing ≠ config framing | align both |
| Log flooded by `FAIL|rdkafka` | broker unreachable — cosmetic only | `RUST_LOG=info,rdkafka=warn` |

---

## 9. Full local loop, start to finish

### One command

```bash
./scripts/test_iso8583.sh                              # full run, with Kafka
./scripts/test_iso8583.sh --count 200 --connections 16 # heavier
./scripts/test_iso8583.sh --skip-kafka                 # no Docker available
KEEP_UP=1 ./scripts/test_iso8583.sh                    # leave the rig running
```

Brings up Kafka, a mock FMS, and the interceptor; pushes spec-accurate ISO 8583
through all three; drains the topic and re-parses every frame with the same
codec that built it; then tears down whatever it started. Exit code is 0 only
if every check passes.

It kills only the processes it started, so an interceptor you are running on
purpose survives. If Kafka was already up, it reuses it and leaves it up;
if it started Kafka, it removes the container **and its volume** — by that
point the topic holds full track 2.

What a pass proves: the codec round-trips, the reassembler handles real frame
sizes under concurrency, and the Kafka copy is byte-identical to what crossed
the link. What it does **not** prove: that `[framing]` is right. The script's
client and the interceptor read the same setting, so their agreement is
circular. Only a real VP↔FMS capture settles that — see §8 and the open items
in `HANDOFF.md`.

The PCI and `key_value` warnings print on every passing run. `PASS` means the
pipeline works, not that the configuration is safe to ship.

### By hand

```bash
# 1. FMS mock
cd testkit && python3 mock_fms.py --port 8583 &

# 2. interceptor
cd .. && ./rust-interceptor/target/release/vp-fms-interceptor config.toml &

# 3. send traffic
python3 examples/send_payment.py --target 127.0.0.1:9100 --connections 8 --count 20

# 4. confirm
curl -s localhost:9101/metrics | grep -E 'tee_|framer_desyncs|kafka_delivered'

# 5. measure the cost versus talking to FMS directly
cd testkit && python3 bench.py --fms 127.0.0.1:8583 --interceptor 127.0.0.1:9100

# 6. tear down
pkill -f vp-fms-interceptor; pkill -f mock_fms.py
```

See `testkit/README.md` for load, chaos, and failure-injection testing.
