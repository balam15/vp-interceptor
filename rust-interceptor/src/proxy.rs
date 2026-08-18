// ============================================================================
// proxy.rs -- one VP connection: dial FMS, then copy bytes both ways.
//
// THIS IS THE HOT PATH. It is also where the most valuable lesson in the
// codebase lives: lines 151-172 document a real 10x memory regression that
// compiled perfectly, was 100% memory-safe, and passed every test.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 3.
// ============================================================================

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

// LEARN: Bytes / BytesMut, from the `bytes` crate. These are the centre of the
//   memory story:
//     BytesMut -- a UNIQUE, mutable, growable buffer. Owns its allocation.
//     Bytes    -- an IMMUTABLE, REFERENCE-COUNTED, shareable view of a byte
//                 region. Cloning it is a refcount bump, not a copy. Two Bytes
//                 can point at different, overlapping regions OF THE SAME
//                 ALLOCATION, which survives until the last one drops.
// JAVA: Netty's ByteBuf is the closest equivalent (and has the identical
//   pinning hazard, plus manual retain()/release() and the leak detector that
//   exists to catch mistakes). Plain ByteBuffer cannot do refcounted sharing
//   safely at all, and Arrays.copyOfRange always copies.
use bytes::{Bytes, BytesMut};

// LEARN: AsyncReadExt / AsyncWriteExt are EXTENSION TRAITS -- an idiom with no
//   Java equivalent worth knowing. They add convenience methods (read_buf,
//   write_all, shutdown, ...) to ANYTHING implementing the base AsyncRead /
//   AsyncWrite trait. YOU MUST IMPORT THE TRAIT TO CALL ITS METHODS.
//   If you ever see "method `read_buf` not found", the cause is almost always a
//   missing trait import, not a missing method.
// JAVA: closest is a static utility class (Files.readAllBytes(x)), but that
//   reads backwards. Kotlin extension functions are the real match.
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// A read filling less than 1/Nth of the buffer is copied rather than
/// sliced, so it cannot pin the whole allocation in the tee queue.
// LEARN: remember this constant -- the payoff is at the bottom of `pump`.
const SMALL_READ_RATIO: usize = 4;

use crate::config::Config;
use crate::stats::Stats;
use crate::tee::{Direction, Tee};

/// Serves one VP connection: dials FMS, then runs the two directions as
/// independent tasks so the runtime can place them on different cores.
// LEARN: EVERY PARAMETER IS TAKEN BY VALUE -- this function takes ownership of
//   all six. That is deliberate: the task owns everything it needs, so nothing
//   it touches can be freed by anyone else, and the compiler can prove the task
//   may safely outlive the accept loop that spawned it. The Arcs make "owning"
//   cheap (a refcount bump each).
// LEARN: `pub` means visible outside this module. Rust's DEFAULT VISIBILITY IS
//   PRIVATE TO THE MODULE, which is stricter than Java's package-private
//   default. Other levels: pub(crate) (whole binary), pub(super) (parent module).
pub async fn handle(
    client: TcpStream,
    peer: Arc<str>,
    conn_id: u64,
    cfg: Arc<Config>,
    tee: Tee,
    stats: Arc<Stats>,
) {
    let connect = TcpStream::connect(&cfg.upstream.addr);
    // LEARN: NESTED PATTERN MATCHING, one of Rust's genuinely nice features.
    //   timeout() returns Result<Result<TcpStream, io::Error>, Elapsed>:
    //     outer = "did we time out?"   inner = "did the connect succeed?"
    //   One match destructures BOTH layers and gives three distinct arms, and
    //   the compiler guarantees you covered all three.
    // JAVA: this is a try/catch(IOException) nested inside a
    //   try/catch(TimeoutException) around Future.get(timeout) -- and the two
    //   failures end up in structurally different places in the source.
    let upstream = match timeout(
        Duration::from_millis(cfg.upstream.connect_timeout_ms),
        connect,
    )
    .await
    {
        Ok(Ok(s)) => s,       // connected
        Ok(Err(e)) => {       // connect refused / failed
            Stats::inc(&stats.upstream_connect_failed);
            tracing::warn!(conn_id, %peer, upstream = %cfg.upstream.addr, error = %e, "upstream connect failed");
            // LEARN: bare `return` in a function returning `()`.
            return;
        }
        Err(_) => {           // timed out
            Stats::inc(&stats.upstream_connect_failed);
            tracing::warn!(conn_id, %peer, upstream = %cfg.upstream.addr, "upstream connect timed out");
            return;
        }
    };

    if cfg.proxy.nodelay {
        // Nagle would coalesce small ISO 8583 messages and add tens of
        // milliseconds -- far more than everything else here combined.
        //
        // JAVA: Socket.setTcpNoDelay(true).
        // LEARN: `let _ =` again -- if setting a socket option fails we do not
        //   care enough to abort a payment.
        let _ = client.set_nodelay(true);
        let _ = upstream.set_nodelay(true);
    }

    // Start the connection clock only once FMS is actually connected, so
    // conn_age_ms measures serving time rather than dial time.
    tee.open(conn_id);

    // LEARN: `into_split()` is PURE OWNERSHIP THINKING and is worth sitting with.
    //   A TCP socket is full-duplex -- you can read and write simultaneously.
    //   But `&mut TcpStream` is EXCLUSIVE: the borrow checker allows only one at
    //   a time. So how do you run reads and writes concurrently?
    //   into_split() CONSUMES the TcpStream (note the `into_` prefix) and returns
    //   two separate OWNED values, OwnedReadHalf and OwnedWriteHalf, each of
    //   which can be moved into a DIFFERENT TASK. The underlying fd is shared via
    //   an internal Arc; the type system guarantees only the read half can read
    //   and only the write half can write.
    // JAVA: socket.getInputStream() / getOutputStream() gives you two streams
    //   over the same socket and TRUSTS YOU not to do something incoherent. Rust
    //   makes the split explicit and the exclusivity provable.
    // LEARN: naming conventions, consistent across all Rust code:
    //     into_x -- consumes self, converts. Ownership transferred.
    //     to_x   -- borrows self, produces an owned copy. Allocates.
    //     as_x   -- borrows self, produces a borrowed view. Free.
    let (client_rd, client_wr) = client.into_split();
    let (up_rd, up_wr) = upstream.into_split();

    // LEARN: one task per direction, so a connection is THREE tasks: `handle`
    //   plus one `pump` each way. The doc comment above says why: separate tasks
    //   can be scheduled on separate cores by tokio's work-stealing scheduler,
    //   so VP->FMS and FMS->VP genuinely run in parallel.
    // LEARN: `pump(...)` is CALLED here only to BUILD the future. Nothing runs
    //   until spawn drives it.
    // LEARN: eight arguments, four Arc::clone refcount bumps, a bool pair and an
    //   enum byte. That is the entire cost of setting up a direction.
    let to_fms = tokio::spawn(pump(
        client_rd,
        up_wr,
        Direction::VpToFms,
        conn_id,
        Arc::clone(&peer),
        Arc::clone(&cfg),
        tee.clone(),
        Arc::clone(&stats),
    ));
    let to_vp = tokio::spawn(pump(
        up_rd,
        client_wr,
        Direction::FmsToVp,
        conn_id,
        Arc::clone(&peer),
        Arc::clone(&cfg),
        tee.clone(),
        Arc::clone(&stats),
    ));

    // LEARN: an ARRAY LITERAL OF TUPLES, iterated with destructuring in the
    //   `for` pattern. `.await` on a JoinHandle is Java's Future.get(), but
    //   non-blocking.
    // LEARN (subtle): the array elements are evaluated when the ARRAY IS BUILT,
    //   so BOTH .awaits happen before the loop body runs at all, in order:
    //   to_fms first, then to_vp. Since we want both to finish regardless, that
    //   is correct -- but it is not obvious from the shape, and it would bite
    //   you if the arms had side effects whose order mattered.
    for (dir, joined) in [
        (Direction::VpToFms.as_str(), to_fms.await),
        (Direction::FmsToVp.as_str(), to_vp.await),
    ] {
        // LEARN: nested match again, with three specific meanings:
        match joined {
            // LEARN: task finished, pump returned Ok(()). Clean close. Note you
            //   can pattern-match the unit value `()` itself.
            Ok(Ok(())) => {}
            // LEARN: task finished, pump returned an IO error. `debug` level,
            //   because a connection reset is completely routine.
            Ok(Err(e)) => tracing::debug!(conn_id, direction = dir, error = %e, "stream ended"),
            // LEARN: THE TASK PANICKED. JoinHandle's error type is JoinError.
            //   `error` level, because a panic is a bug.
            // LEARN: this is where the Cargo.toml decision cashes out -- because
            //   panics UNWIND rather than abort, a bug in one connection surfaces
            //   here as a log line while every other connection keeps running.
            Err(e) => tracing::error!(conn_id, direction = dir, error = %e, "pump task panicked"),
        }
    }

    // LEARN: after both directions end, so the tee worker can free its
    //   per-connection framer and timing state.
    tee.close(conn_id);
}

/// Copies one direction of the stream.
///
/// The ordering here IS the design: bytes reach the far socket before the tee is
/// offered anything, and the tee call cannot await. Kafka may be down, slow, or
/// absent without adding a microsecond to this loop.
// LEARN: `mut rd` is a MUTABLE PARAMETER BINDING. Reading from a socket requires
//   &mut self, so the local binding must be declared mut. Java parameters are
//   effectively mutable by default and nobody thinks about it; here it is stated.
// LEARN: io::Result<()> is an alias for Result<(), io::Error>.
async fn pump(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    dir: Direction,
    conn_id: u64,
    peer: Arc<str>,
    cfg: Arc<Config>,
    tee: Tee,
    stats: Arc<Stats>,
) -> io::Result<()> {
    let cap = cfg.proxy.read_buffer_bytes;
    // LEARN: `match` on an integer WITH A BINDING ARM. `0 => None` matches the
    //   literal; `ms => ...` is a catch-all that BINDS the value to `ms`. Java's
    //   switch cannot bind the matched value like this.
    // LEARN: this converts the config's "0 means disabled" sentinel into a TYPE
    //   once, at the top, so the hot loop below never re-checks a magic number --
    //   it matches on Option instead. Very Rust habit: PUSH THE MESSY
    //   REPRESENTATION TO THE BOUNDARY, CARRY A PRECISE TYPE INSIDE.
    let idle = match cfg.proxy.idle_timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
    // LEARN: resolve which counter to use ONCE, outside the loop, and hold a
    //   borrow of it. The explicit `: &AtomicU64` is a borrow of a field of a
    //   struct behind an Arc, held ACROSS EVERY .await in the loop below. The
    //   borrow checker verifies that `stats` outlives this reference -- and it
    //   does, because pump owns it for the whole function.
    // JAVA: you would hold a field reference and rely on the GC. Here the
    //   compiler PROVES the target outlives the reference, statically, at zero
    //   runtime cost, with no possibility of a dangling pointer.
    let byte_counter: &AtomicU64 = match dir {
        Direction::VpToFms => &stats.bytes_vp_to_fms,
        Direction::FmsToVp => &stats.bytes_fms_to_vp,
    };

    let mut buf = BytesMut::with_capacity(cap);

    loop {
        // LEARN: `reserve(cap)` ensures at least `cap` bytes of SPARE capacity.
        //   Critically, IF THE BUFFER ALREADY HAS ROOM IT DOES NOTHING -- no
        //   allocation. That is what makes the buffer reusable across iterations.
        buf.reserve(cap);

        let n = match idle {
            Some(d) => match timeout(d, rd.read_buf(&mut buf)).await {
                // LEARN: outer Ok = "did not time out"; `r` is the inner
                //   io::Result<usize>; `?` propagates an IO error out of pump,
                //   where handle() logs it.
                Ok(r) => r?,
                Err(_) => {
                    // LEARN: `dir.as_str()` returns &'static str -- a pointer to
                    //   a string literal baked into the binary's read-only data
                    //   section. ZERO ALLOCATION.
                    // JAVA: enum.name() returns a cached String object; also
                    //   cheap after the first call, but still an object with a
                    //   header rather than a raw pointer into .rodata.
                    tracing::debug!(conn_id, direction = dir.as_str(), "idle timeout");
                    let _ = wr.shutdown().await;
                    return Ok(());
                }
            },
            // LEARN: `rd.read_buf(&mut buf)` takes an EXCLUSIVE borrow of buf for
            //   the duration of the call. While that borrow is outstanding you
            //   cannot touch `buf` at all -- the compiler forbids it. A
            //   concurrent-modification bug is a COMPILE ERROR here, not a
            //   runtime surprise.
            None => rd.read_buf(&mut buf).await?,
        };

        if n == 0 {
            // Half-close: propagate FIN so the peer can finish its own direction
            // rather than having an in-flight response torn down.
            //
            // LEARN: read() returning 0 means EOF. Rather than tearing the whole
            //   connection down, we propagate the FIN so the other direction can
            //   finish -- if VP half-closes after sending a request, FMS's
            //   response still gets delivered. Correct TCP proxy behaviour, and
            //   frequently got wrong.
            let _ = wr.shutdown().await;
            return Ok(());
        }

        // LEARN: THIS IS THE LINE THE WHOLE PROGRAM EXISTS TO PROTECT. Bytes go
        //   to the far socket FIRST, before any tee work happens.
        // LEARN: `&buf[..n]` is a SLICE. `[..n]` is range syntax for "from 0 up
        //   to n". A slice is a BORROWED VIEW: a pointer and a length, 16 bytes
        //   on the stack, NO COPY AND NO ALLOCATION.
        // JAVA: the nearest equivalents are ByteBuffer.slice() (an allocated
        //   object) or Arrays.copyOfRange (a full copy). Note String.substring
        //   WAS a zero-copy view until Java 7u6, when it was changed to always
        //   copy -- precisely because a small substring could pin a huge backing
        //   array. THAT IS EXACTLY THE BUG DOCUMENTED 30 LINES BELOW. Java solved
        //   it by always copying; Rust lets you choose, and this code chooses per
        //   read based on size.
        // LEARN: `write_all` loops until every byte is written. Java's
        //   OutputStream.write already guarantees this for blocking streams, but
        //   NIO channels do NOT, and forgetting the loop is a classic NIO bug.
        wr.write_all(&buf[..n]).await?;
        // LEARN: `n as u64` -- usize to u64. Explicit, always.
        Stats::add(byte_counter, n as u64);

        // LEARN: cheap gate. If Kafka is off, or this direction is not published,
        //   we skip all the buffer work below. should_offer is #[inline] and
        //   reads two bools, so after LTO this is a single predictable branch.
        if tee.should_offer(dir) {
            // `split().freeze()` is zero-copy, but the resulting `Bytes` keeps
            // the WHOLE read allocation alive -- so a 120-byte payments message
            // pins its entire 16 KiB buffer for as long as it sits in the tee
            // queue. Measured: 409 MB RSS at 256 connections, ~146 MB of it
            // pinned buffers.
            //
            // For a small read, copying into an exact-size `Bytes` is a ~100
            // byte memcpy and lets the buffer be reused immediately. Only take
            // the zero-copy path when the read actually filled the buffer,
            // where the copy would be the expensive option.
            //
            // ================== THE MEMORY LESSON ==================
            // LEARN: trace the ORIGINAL (buggy) version concretely:
            //   1. BytesMut::with_capacity(16384) -> one 16 KiB heap allocation.
            //   2. A 120-byte payments message arrives. n = 120.
            //   3. buf.split().freeze() -> a Bytes describing bytes [0..120],
            //      but THE REFCOUNT IS ON THE WHOLE 16 KiB ALLOCATION.
            //   4. That Bytes is pushed into the tee queue (8192 x 4 shards).
            //   5. While it sits there, 16 KiB IS RETAINED TO HOLD 120 BYTES --
            //      a 136:1 waste ratio.
            //   6. buf is now empty, so the next reserve(cap) allocates a FRESH
            //      16 KiB.
            //   At 256 connections with a backed-up queue: 409 MB RSS, ~146 MB
            //   of it pinned buffers -- WORSE THAN THE JAVA BUILD, which is what
            //   made the team look.
            //
            // LEARN: THE POINT. The borrow checker guaranteed no use-after-free
            //   and no data race. IT SAID NOTHING ABOUT RETENTION. Holding a
            //   small view of a large allocation is perfectly memory-SAFE and
            //   badly memory-INEFFICIENT. Only a benchmark found it.
            //   MEMORY SAFETY IS NOT MEMORY EFFICIENCY.
            //
            // LEARN: `saturating_mul` rather than `*` -- if n were enormous,
            //   `n * 4` could overflow usize and wrap to a small number, making
            //   this condition wrongly true. saturating_mul clamps at usize::MAX
            //   so the condition is correctly false. Costs one cmov, eliminates
            //   a whole class of bug.
            let chunk = if n.saturating_mul(SMALL_READ_RATIO) < cap {
                // LEARN: allocates EXACTLY 120 bytes and copies. A ~120-byte
                //   memcpy is tens of nanoseconds -- far below the noise floor
                //   of a network hop.
                let exact = Bytes::copy_from_slice(&buf[..n]);
                buf.clear(); // keeps the allocation for the next read
                // LEARN: clear() sets the length to 0 but KEEPS THE CAPACITY, so
                //   the next reserve(cap) is a no-op and THE SAME 16 KiB
                //   ALLOCATION SERVES THIS CONNECTION FOR ITS ENTIRE LIFETIME.
                // JAVA: ByteBuffer.clear() does exactly this too.
                exact
            } else {
                // LEARN: a genuinely large read that filled most of the buffer --
                //   here the copy would be the expensive option, so take the
                //   zero-copy path. split() removes the written bytes and
                //   freeze() converts BytesMut -> Bytes. Neither copies.
                buf.split().freeze()
            };
            // RESULT OF THE FIX: peak RSS 409 MB -> 43 MB, with NO latency cost.
            // =======================================================
            tee.offer(conn_id, dir, &peer, chunk);
        } else {
            // LEARN: not publishing -- reuse the buffer, allocate nothing, ever.
            //   Steady-state allocation for a non-publishing connection is ZERO
            //   BYTES PER MESSAGE.
            buf.clear();
        }
    }
}
