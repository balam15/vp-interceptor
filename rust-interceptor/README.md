# rust-interceptor

Rust + Tokio implementation of the VP→FMS interceptor. **This is the primary
build** — see `../BENCHMARK.md` for the measured reasons.

Reads `../config.toml`, the same file the Java build uses.

## Build and run

```bash
# rustup.rs returns 403 on this network; Homebrew is the working path
brew install rust cmake openssl@3

cargo test                # 19 unit tests
cargo build --release     # ~40s, librdkafka builds from source via cmake
./target/release/vp-fms-interceptor ../config.toml
```

Produces a 3.4 MB static-ish binary with no runtime dependencies.

## Admin endpoints

Served by `src/admin.rs` on the address in `[admin] addr` (default
`127.0.0.1:9101`). Open any of these in a browser while the process is running:

| URL | Content-Type | Body |
|---|---|---|
| <http://127.0.0.1:9101/healthz> | `text/plain` | `ok` — liveness |
| <http://127.0.0.1:9101/> | `application/json` | every counter as one JSON object |
| <http://127.0.0.1:9101/metrics> | `text/plain; version=0.0.4` | Prometheus text format |

```bash
curl -s localhost:9101/healthz                        # ok
curl -s localhost:9101/ | python3 -m json.tool        # counters, pretty-printed
```

Three things to know:

- **Loopback-only by default, and there is no auth.** `127.0.0.1` means a browser
  on this host can reach it and another machine cannot. If you need it scraped
  remotely, change `config.toml` rather than the code, and put something in front
  of it — these endpoints expose operational internals next to a payments path.
- **A bind failure here does not stop the proxy.** `serve` logs and returns; you
  lose metrics, not availability. That is deliberate, and the function returns
  `()` so it *cannot* report failure upward.
- **Your browser will also request `/favicon.ico`.** Unknown paths fall through
  to the catch-all arm, so it gets served the JSON body. Harmless, but it is why
  you may see two hits per page load.

Counters worth reading first: `tee_dropped` (Kafka is degraded — and it proves
the hot path was not), `framer_desyncs` (wire framing does not match
`[framing]`), and `upstream_connect_failed`.

## Structure

| file | role |
|---|---|
| `src/main.rs` | accept loop, connection limit, graceful drain |
| `src/proxy.rs` | **the hot path** — two pump tasks per connection |
| `src/tee.rs` | bounded queue, shard workers, per-connection timing |
| `src/framing.rs` | length-prefix reassembler (unit-tested) |
| `src/parse.rs` | `key=value` field extraction (unit-tested) |
| `src/kafka.rs` | fire-and-forget producer, JSON envelope, encodings |
| `src/stats.rs` | atomic counters |
| `src/admin.rs` | `/metrics`, `/healthz` — hand-rolled, no web framework |

No web framework on purpose: this process sits inline on an authorization path,
and every dependency is one more thing that can allocate, block, or spawn threads
next to the hot path.

Every source file is annotated with `// LEARN:` (Rust semantics) and `// JAVA:`
(the equivalent, and the trade-off) comments. `RUST_EXPLAINED.md` in this
directory is the same material as prose. To read the code without them:
`grep -v "LEARN:\|JAVA:" src/tee.rs`

## The rule this code serves

> **Kafka must never be able to affect VP↔FMS.**

Check any change against that sentence first. The four mechanisms enforcing it
are documented in `../README.md`; the one that is easiest to break by accident is
in `src/proxy.rs` — bytes must reach the far socket **before** anything is handed
to the tee, and the tee call must never `.await`.

## One trap in `src/proxy.rs` worth knowing

`BytesMut::split().freeze()` is zero-copy, but the resulting `Bytes` keeps the
**whole** read allocation alive. A 122-byte message queued for Kafka pinned its
entire 16 KiB read buffer — 409 MB RSS at 256 connections before this was found.

Small reads are now copied into an exact-size `Bytes` instead (peak RSS 409 MB →
43 MB, no latency cost). If you touch that code, do not "optimise" the copy away
without re-measuring RSS under concurrency. See `../BENCHMARK.md`.

## Testing

```bash
cargo test                                    # unit tests only

# end-to-end -- run these from the PROJECT ROOT, not from here.
# See ../RUNBOOK.md and ../testkit/README.md.
(cd testkit && python3 mock_fms.py --port 8583 &)
./rust-interceptor/target/release/vp-fms-interceptor config.toml &
./scripts/status.sh                           # exit 0 = healthy
```
