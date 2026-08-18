// ============================================================================
// stats.rs -- process-wide counters.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 8.
// ============================================================================

use std::sync::atomic::{AtomicU64, Ordering};

/// All counters are plain relaxed atomics. They are incremented on the hot path,
/// so ordering guarantees are deliberately the weakest available -- we only ever
/// read them for reporting, never to make a decision.
// LEARN: `#[derive(Default)]` generates a `default()` that zeroes every field.
//   main.rs calls Stats::default().
// JAVA: Lombok's @Builder.Default territory, except built into the language and
//   with no reflection.
//
// ============================ MEMORY LAYOUT =================================
// LEARN: this struct is ONE CONTIGUOUS ALLOCATION. 21 AtomicU64 fields = 168
//   bytes, inline, in a single Arc box. Field access is a FIXED OFFSET from a
//   base pointer -- no indirection whatsoever.
//
// JAVA: 21 SEPARATE AtomicLong OBJECTS, each with a 12-16 byte header plus an
//   8-byte volatile long plus padding (~24 bytes each, ~504 bytes total), PLUS
//   21 reference fields in the containing object, PLUS a pointer dereference on
//   every single access, PLUS they are scattered wherever the allocator put them
//   so they sit on 21 different cache lines.
//   Roughly 3x the memory and 21 pointer chases. Java CANNOT embed one object
//   inside another -- every non-primitive field is a reference to a separately
//   allocated object with its own header. (Project Valhalla will eventually give
//   Java value types; it is not here yet.)
//
// LEARN (the caveat that cuts the other way): because these counters are packed
//   together, several share a 64-byte cache line, so concurrent increments from
//   different cores cause FALSE SHARING. Irrelevant at this volume, but at
//   extreme rates you would add #[repr(align(64))] padding. Java's scattered
//   layout accidentally avoids this, and LongAdder addresses it explicitly with
//   per-thread cells.
// =============================================================================
#[derive(Default)]
pub struct Stats {
    pub conns_accepted: AtomicU64,
    pub conns_active: AtomicU64,
    pub conns_rejected: AtomicU64,
    pub upstream_connect_failed: AtomicU64,

    pub bytes_vp_to_fms: AtomicU64,
    pub bytes_fms_to_vp: AtomicU64,

    /// Chunks accepted into a shard queue.
    ///
    /// Named `accepted`, not `offered`, because it counts only the successes:
    /// total offers are `accepted + dropped`, which is what a drop-RATE alert
    /// needs as its denominator.
    pub tee_accepted: AtomicU64,
    /// Chunks discarded because a shard queue was full. This is the number to
    /// alert on: it means Kafka is degraded, and it proves the hot path was not.
    pub tee_dropped: AtomicU64,

    pub frames_emitted: AtomicU64,
    pub framer_desyncs: AtomicU64,

    pub kafka_enqueued: AtomicU64,
    /// librdkafka's own queue was full -- shed here rather than block.
    pub kafka_enqueue_failed: AtomicU64,
    pub kafka_delivered: AtomicU64,
    pub kafka_delivery_failed: AtomicU64,

    // -- timing ----------------------------------------------------------
    // Exposed as sum/count/max rather than an average, so a scraper can
    // compute rates over an interval. An average since process start is
    // almost useless -- it never recovers from one bad hour.
    //
    // LEARN: this is the standard Prometheus pattern -- export monotonic
    //   counters and let the scraper compute rate(sum)/rate(count) over any
    //   window it likes. Storing a precomputed average throws away that ability.
    /// FMS round-trip: request forwarded -> matching response seen.
    pub rtt_count: AtomicU64,
    pub rtt_sum_us: AtomicU64,
    pub rtt_max_us: AtomicU64,
    /// Requests that closed without a matching response.
    pub rtt_unmatched: AtomicU64,
    /// Connection lifetime, accept -> close.
    pub conn_duration_count: AtomicU64,
    pub conn_duration_sum_ms: AtomicU64,
    pub conn_duration_max_ms: AtomicU64,
}

impl Stats {
    // LEARN: `#[inline]` is a hint that this should be inlined even ACROSS CRATE
    //   BOUNDARIES (within a crate, LLVM decides on its own). Combined with
    //   `lto = "fat"` in Cargo.toml, this whole function disappears -- the call
    //   compiles down to a single atomic instruction.
    // JAVA: the JIT inlines based on RUNTIME PROFILING and can in theory be more
    //   aggressive than any static compiler -- but only once the method is hot,
    //   and it can deoptimise later. Rust decides at build time, forever.
    //
    // ======================= Ordering::Relaxed ==============================
    // LEARN: Rust exposes the full C++11 MEMORY MODEL. You choose per operation:
    //     Relaxed           -- atomicity only, NO ordering vs other memory ops.
    //                          Cheapest possible atomic.
    //     Acquire / Release -- one-way barriers; pair to establish happens-before
    //     AcqRel            -- both
    //     SeqCst            -- total global order across all threads. Most costly.
    //
    // JAVA: AtomicLong.incrementAndGet() IS ALWAYS SEQUENTIALLY CONSISTENT. You
    //   cannot ask for less. (Java 9's VarHandle added getAndAddRelease and
    //   getAndAddOpaque, roughly Relaxed, but almost nobody uses them.)
    //
    // LEARN: does it actually matter? DEPENDS ON THE ARCHITECTURE:
    //     x86-64 -- fetch_add compiles to `lock xadd` regardless of ordering.
    //               No difference for read-modify-write.
    //     ARM64  -- GENUINELY DIFFERENT (your Apple Silicon Mac, and Graviton).
    //               Relaxed -> ldxr/stxr loop with no barriers.
    //               SeqCst  -> ldaxr/stlxr, plus possibly a dmb.
    //               With several counters per message on a hot path, that is real.
    //
    // LEARN: the doc comment on the struct justifies the choice precisely --
    //   "we only ever read them for reporting, never to make a decision".
    //   Relaxed is CORRECT because no code branches on a counter. If conns_active
    //   gated admission, you would need stronger ordering. STATING THE INVARIANT
    //   IS WHAT MAKES THIS REVIEWABLE.
    //
    //   PRO Rust: pick the cheapest correct ordering, documented at the call site.
    //   CON Rust: you MUST choose, on every single atomic operation, and choosing
    //     wrong gives you a bug that reproduces only on weakly-ordered hardware,
    //     under load, rarely. The C++11 memory model is genuinely hard. Java's
    //     "always sequentially consistent" is slower but essentially
    //     unfootgunnable.
    // =========================================================================
    #[inline]
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    // LEARN: `load` reads the current value.
    // JAVA: a volatile read, but again Java gives you no ordering choice.
    #[inline]
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    // LEARN: `fetch_max` is an atomic "set to max(current, n)" -- one instruction
    //   on platforms that support it, a CAS loop otherwise.
    // JAVA: THERE IS NO AtomicLong.getAndUpdateMax. You write the CAS loop by
    //   hand and hope you got it right:
    //       long prev;
    //       do { prev = max.get(); if (n <= prev) break; }
    //       while (!max.compareAndSet(prev, n));
    //   Five lines of easily-mis-written code versus one method call.
    #[inline]
    pub fn max(counter: &AtomicU64, n: u64) {
        counter.fetch_max(n, Ordering::Relaxed);
    }

    /// Record one observation into a sum/count/max triple.
    // LEARN: three SEPARATE atomics, deliberately NOT atomic as a group. A
    //   scraper could observe count incremented but sum not yet. That is fine
    //   for metrics, and making it atomic as a group would need a lock.
    // LEARN: `Self::inc(...)` -- calling an associated function of the current
    //   type. `Self` here is `Stats`.
    #[inline]
    pub fn observe(count: &AtomicU64, sum: &AtomicU64, max: &AtomicU64, value: u64) {
        Self::inc(count);
        Self::add(sum, value);
        Self::max(max, value);
    }

    // LEARN: returns a Vec of (&'static str, u64) TUPLES. The names are pointers
    //   into .rodata -- NO STRING ALLOCATION AT ALL. One Vec allocation holds 21
    //   tuples of 24 bytes each.
    // JAVA: Map<String, Long> -- a HashMap allocation, 21 Long boxes, 21
    //   Map.Entry objects. Roughly 20x the allocation for the same data.
    //   (Called once per /metrics scrape, so it does not matter here. It is
    //   included as an illustration of the constant factor.)
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("conns_accepted", Self::get(&self.conns_accepted)),
            ("conns_active", Self::get(&self.conns_active)),
            ("conns_rejected", Self::get(&self.conns_rejected)),
            ("upstream_connect_failed", Self::get(&self.upstream_connect_failed)),
            ("bytes_vp_to_fms", Self::get(&self.bytes_vp_to_fms)),
            ("bytes_fms_to_vp", Self::get(&self.bytes_fms_to_vp)),
            ("tee_accepted", Self::get(&self.tee_accepted)),
            ("tee_dropped", Self::get(&self.tee_dropped)),
            ("frames_emitted", Self::get(&self.frames_emitted)),
            ("framer_desyncs", Self::get(&self.framer_desyncs)),
            ("kafka_enqueued", Self::get(&self.kafka_enqueued)),
            ("kafka_enqueue_failed", Self::get(&self.kafka_enqueue_failed)),
            ("kafka_delivered", Self::get(&self.kafka_delivered)),
            ("kafka_delivery_failed", Self::get(&self.kafka_delivery_failed)),
            ("rtt_count", Self::get(&self.rtt_count)),
            ("rtt_sum_us", Self::get(&self.rtt_sum_us)),
            ("rtt_max_us", Self::get(&self.rtt_max_us)),
            ("rtt_unmatched", Self::get(&self.rtt_unmatched)),
            ("conn_duration_count", Self::get(&self.conn_duration_count)),
            ("conn_duration_sum_ms", Self::get(&self.conn_duration_sum_ms)),
            ("conn_duration_max_ms", Self::get(&self.conn_duration_max_ms)),
        ]
    }

}
