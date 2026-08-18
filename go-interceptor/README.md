# go-interceptor

Go implementation of the VP→FMS interceptor. Structurally mirrors the Rust build
package-for-module so the three can be compared meaningfully — see
`../BENCHMARK.md` for the Rust/Java measurements.

**Reads the same `../config.toml`.** Deliberately: a comparison between
implementations is only meaningful if none of them can quietly run different
settings.

## Build and run

```bash
go test ./...                                   # 51 tests
go build -o bin/vp-fms-interceptor ./cmd/interceptor
./bin/vp-fms-interceptor ../config.toml
```

Dependencies are **vendored** (`./vendor`), so the build never touches the
network. That is not tidiness — the corporate proxy on this network returns 403
for some module zips, exactly like the rustup 403 noted in
`../rust-interceptor/README.md`. If you ever need to re-resolve them:

```bash
GOPROXY=direct GOFLAGS=-mod=mod go mod tidy && go mod vendor
```

`GOPROXY=direct` fetches over git, which the proxy does allow.

Produces an ~11 MB static binary (~8 MB with `-ldflags="-s -w"`) with no runtime
dependencies — `CGO_ENABLED=0` by default here, since nothing in the tree needs
cgo.

## Docker

```bash
# from the PROJECT ROOT, not from here -- config.toml is baked in as the default
docker build -f go-interceptor/Dockerfile -t vp-fms-interceptor:go .
docker run --rm -p 9100:9100 -p 9101:9101 \
  -v "$PWD/config.toml:/etc/vp-fms/config.toml:ro" vp-fms-interceptor:go
```

The runtime stage is `FROM scratch`: the image is the binary, a CA bundle and the
config. Two config settings must change for a container — see the comment block
at the top of the `Dockerfile`.

## Admin endpoints

Identical to the other two builds, on the address in `[admin] addr` (default
`127.0.0.1:9101`).

| URL | Content-Type | Body |
|---|---|---|
| <http://127.0.0.1:9101/healthz> | `text/plain` | `ok` — liveness |
| <http://127.0.0.1:9101/> | `application/json` | every counter as one JSON object |
| <http://127.0.0.1:9101/metrics> | `text/plain; version=0.0.4` | Prometheus text format |

Metric names, the Kafka envelope and its field order are byte-for-byte compatible
with the Rust and Java builds, so the same dashboards and consumers work against
any of them.

Counters worth reading first: `tee_dropped` (Kafka is degraded — and it proves
the hot path was not), `framer_desyncs` (wire framing does not match
`[framing]`), and `upstream_connect_failed`.

## Structure

| file | mirrors | role |
|---|---|---|
| `cmd/interceptor/main.go` | `main.rs` | accept loop, connection limit, graceful drain |
| `internal/proxy/proxy.go` | `proxy.rs` | **the hot path** — two goroutines per connection |
| `internal/tee/tee.go` | `tee.rs` | bounded queue, shard workers, per-connection timing |
| `internal/framing/framing.go` | `framing.rs` | length-prefix reassembler (unit-tested) |
| `internal/parse/parse.go` | `parse.rs` | `key=value` field extraction (unit-tested) |
| `internal/kafka/kafka.go` | `kafka.rs` | fire-and-forget producer, JSON envelope, encodings |
| `internal/stats/stats.go` | `stats.rs` | atomic counters |
| `internal/admin/admin.go` | `admin.rs` | `/metrics`, `/healthz` |

## The rule this code serves

> **Kafka must never be able to affect VP↔FMS.**

Check any change against that sentence first. The mechanism that is easiest to
break by accident is in `internal/proxy/proxy.go` — bytes must reach the far
socket **before** anything is handed to the tee, and `tee.Offer` must never
block.

Measured on this machine against `testkit/mock_fms.py`, 16 connections:

| Kafka | tps | p50 | p99 | errors | mismatches |
|---|---|---|---|---|---|
| reachable | 36,271 | 0.377 ms | 1.421 ms | 0 | 0 |
| **unreachable** | **38,598** | **0.281 ms** | **2.496 ms** | **0** | **0** |

With the broker gone, all 617,832 publishes were shed and counted in
`kafka_enqueue_failed`, and forwarding was, if anything, slightly faster. That
table is the whole design in one row.

## Four things that differ from the Rust build

These are not style choices — the language and the client library genuinely
differ.

**1. The tee must copy.**
Go has no equivalent of Rust's refcounted `Bytes`, so `Tee.Offer` copies into a
right-sized slice, as the Java build does. This looked like a disadvantage and
turned out to be the opposite: the Rust build originally pinned whole 16 KiB read
buffers behind small messages and peaked at 409 MB RSS. Go cannot make that
mistake. See `../BENCHMARK.md`.

**2. `Publish` is bounded by a context, not by a "never blocks" guarantee.**
kafka-go's `WriteMessages` is non-blocking with `Async: true`, but it resolves
topic metadata first, and that is a network call. `enqueueTimeoutCap` (1s) caps
it, so a cold cache or a dead broker turns into "shed and count" rather than
"wait". This is the Go equivalent of the Java build's `max.block.ms=0`, and it
needs the same background metadata warm-up for the same reason. librdkafka needs
neither.

**3. `queue_buffering_max_messages` / `_kbytes` are ignored.**
They are librdkafka accumulator bounds with no kafka-go equivalent. The process
says so once at startup rather than leaving them a silent no-op. In this build
the isolation bound is `tee.queue_capacity`, which is where it belongs anyway.

**4. `[kafka.properties]` is translated, not passed through.**
Only the security keys have kafka-go equivalents: `security.protocol`,
`sasl.mechanisms` (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), `sasl.username`,
`sasl.password`, `ssl.ca.location`,
`ssl.endpoint.identification.algorithm=none`. Anything else is logged as ignored
— an operator who sets a property and gets neither a behaviour change nor a
message will assume it took effect, and on a payments link that assumption is
expensive.

## Confinement is a design rule here, not a proof

The tee worker owns two maps and mutates them on every message with no locks,
because sharding by connection ID guarantees one goroutine reaches them. Rust's
compiler *proves* that; Go does not. `go test -race` checks it empirically, which
is why the suite is run under `-race` in CI and why the shard assignment in
`Tee.shard` is load-bearing rather than an optimisation.

## Testing

```bash
go test ./...                     # 51 unit + integration tests
go test -race ./...               # the confinement check above
go vet ./...

# end-to-end -- run these from the PROJECT ROOT, not from here.
# See ../RUNBOOK.md and ../testkit/README.md.
(cd testkit && python3 mock_fms.py --port 8583 &)
./go-interceptor/bin/vp-fms-interceptor config.toml &
./scripts/status.sh                           # exit 0 = healthy
```

`internal/proxy/proxy_test.go` runs the real hot path over real TCP sockets:
forwarding both directions, upstream-connect failure, and half-close
propagation. `internal/tee/tee_test.go` asserts the isolation property directly
— `Offer` against a full queue with no worker draining it must drop and count
rather than block.
