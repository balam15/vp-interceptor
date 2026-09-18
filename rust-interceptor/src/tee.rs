// ============================================================================
// tee.rs -- the publish side: sharded workers that reassemble frames and hand
// them to Kafka, entirely off the forwarding path.
//
// THIS IS WHERE RUST'S CONCURRENCY MODEL PAYS OFF MOST VISIBLY: the Worker
// below owns two HashMaps and mutates them on every message with ZERO LOCKS and
// ZERO CONCURRENT COLLECTIONS, because the compiler proved only one task can
// reach them.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 5.
// ============================================================================

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::framing::{Framer, Step};
use crate::kafka::{self, Publisher};
use crate::payload_log;
use crate::stats::Stats;

// LEARN: six traits derived in one line. What each buys you:
//   Clone      -- explicit .clone()
//   Copy       -- THE TYPE IS A PLAIN BIT PATTERN, so assignment COPIES instead
//                 of moving. This is why `dir` can be passed around freely
//                 without & or .clone() and stay usable. Only valid for types
//                 with no ownership (no heap, no Drop).
//   PartialEq,
//   Eq         -- equals(). Needed for == and to be a HashMap key.
//   Hash       -- hashCode(). Needed to be a HashMap key.
//   Debug      -- a developer-facing toString(), used by {:?} and tracing's ?.
// JAVA: a Java enum gives you ALL of these automatically with no way to opt out.
//   Rust makes you list them -- more verbose, but a type only gets capabilities
//   you asked for, so you cannot accidentally put something in a HashSet whose
//   equality is meaningless.
// LEARN: SIZE. This enum is ONE BYTE. A Java enum constant is a heap object
//   (16-byte header + fields) referenced by a 4-or-8-byte pointer. Inside
//   Event::Data below, this field costs 1 byte and zero indirection.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction {
    VpToFms,
    FmsToVp,
}

impl Direction {
    // LEARN: `self` BY VALUE (not &self) because Direction is Copy -- passing it
    //   is one byte in a register, cheaper than passing a pointer to it.
    // LEARN: returns &'static str -- a pointer into the binary's .rodata section.
    //   NO ALLOCATION, EVER.
    // JAVA: enum.name() returns a cached String; the object already exists so it
    //   is cheap, but it is still an object with a header rather than a raw
    //   pointer to read-only data. A custom toString() usually builds a new one.
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::VpToFms => "vp_to_fms",
            Direction::FmsToVp => "fms_to_vp",
        }
    }
}

// LEARN: STRUCT VARIANTS -- named fields inside an enum variant, rather than
//   positional. Note the absence of `pub`: Event is PRIVATE TO THIS MODULE, so
//   the entire event-passing protocol is an implementation detail invisible to
//   the rest of the crate. Rust's module privacy makes that trivial to enforce.
//
// LEARN: SIZE ANALYSIS, because it decides the queue's memory:
//   Data is the largest variant:
//     u64 (8) + Direction (1, padded) + Arc<str> (16, fat ptr) + Bytes (32)
//     ~= 64 bytes, plus a discriminant tag.
//   The mpsc channel with capacity 8192 PREALLOCATES ~8192 x 64 B ~= 512 KiB per
//   shard; four shards ~= 2 MiB. FLAT, CONTIGUOUS, NO PER-MESSAGE ALLOCATION.
//
// JAVA: BlockingQueue<Event> with record instances allocates A NEW OBJECT PER
//   EVENT, PER MESSAGE, FOREVER, and every one becomes garbage a millisecond
//   later. That is continuous young-gen pressure directly proportional to
//   payment volume. (ArrayBlockingQueue preallocates the array of REFERENCES,
//   but the Event objects themselves are still allocated per message.)
enum Event {
    /// Sent when the connection is accepted, so `conn_age_ms` is measured from
    /// the real accept time rather than from the first frame.
    Open {
        conn_id: u64,
        at: Instant,
    },
    Data {
        conn_id: u64,
        dir: Direction,
        peer: Arc<str>,
        chunk: Bytes,
    },
    Close {
        conn_id: u64,
        at: Instant,
    },
}

/// Cloneable handle held by every connection pump.
// LEARN: THE CLONEABLE HANDLE PATTERN, ubiquitous in Rust. Tee is a lightweight
//   handle; cloning it bumps two refcounts and copies three bools. Every pump
//   task gets its own.
#[derive(Clone)]
pub struct Tee {
    // LEARN: one shared, IMMUTABLE vector of channel senders. Because it is
    //   never mutated after construction, NO LOCK IS NEEDED TO READ IT --
    //   shared + immutable is always safe, and the compiler knows that from the
    //   absence of any &mut.
    shards: Arc<Vec<mpsc::Sender<Event>>>,
    stats: Arc<Stats>,
    publish_vp_to_fms: bool,
    publish_fms_to_vp: bool,
    /// False when Kafka is disabled or unbuildable: `offer` then costs one
    /// predictable branch and nothing else.
    active: bool,
}

impl Tee {
    pub fn spawn(cfg: &Config, publisher: Option<Arc<Publisher>>, stats: Arc<Stats>) -> Tee {
        // LEARN: `match` on a &str RETURNING A TUPLE, destructured into two
        //   bindings in one statement. Java would need a two-field record or two
        //   separate switches. Tuples are free -- no type declaration, no
        //   allocation, returned in registers.
        let (pub_v2f, pub_f2v) = match cfg.tee.publish_directions.as_str() {
            "vp_to_fms" => (true, false),
            "fms_to_vp" => (false, true),
            _ => (true, true),
        };

        // LEARN: unwrap the Option, or early-return an inert Tee. AFTER THIS
        //   LINE, `publisher` IS SHADOWED AS A PLAIN Arc<Publisher> -- the
        //   Option is GONE FROM THE TYPE, so the rest of this function cannot
        //   possibly forget to handle None. The compiler tracks it.
        // LEARN: this is the "parse, don't validate" idiom -- convert
        //   uncertainty into a certain type at the boundary, and everything
        //   downstream is both simpler AND provably correct.
        let publisher = match publisher {
            Some(p) => p,
            None => {
                return Tee {
                    // LEARN: an empty Vec does not allocate; the Arc is one small
                    //   allocation. Negligible.
                    shards: Arc::new(Vec::new()),
                    stats,
                    publish_vp_to_fms: false,
                    publish_fms_to_vp: false,
                    active: false,
                };
            }
        };

        // LEARN: `Vec::with_capacity(n)` pre-sizes to avoid regrowth.
        // JAVA: new ArrayList<>(n)
        let mut senders = Vec::with_capacity(cfg.tee.shards);
        // LEARN: `0..n` is an EXCLUSIVE RANGE, which is an Iterator. Compiles to
        //   a plain counted loop.
        for shard in 0..cfg.tee.shards {
            // LEARN: a BOUNDED multi-producer SINGLE-consumer channel, returning
            //   a (Sender, Receiver) pair destructured in one line.
            // LEARN: THE "SINGLE CONSUMER" PART IS ENFORCED BY THE TYPE SYSTEM --
            //   Receiver is not Clone. There is exactly one, and it gets moved
            //   into exactly one worker.
            // JAVA: BlockingQueue lets ANY number of threads call take(), and
            //   single-consumer discipline is a comment you hope people read.
            let (tx, rx) = mpsc::channel(cfg.tee.queue_capacity);
            senders.push(tx);
            let worker = Worker {
                shard,
                rx,
                publisher: Arc::clone(&publisher),
                // LEARN: each worker gets its OWN COPY of the framing/timing
                //   config, avoiding shared state entirely. A handful of small
                //   Strings cloned four times at startup. Buying independence
                //   with a trivial one-time cost is very idiomatic Rust.
                framing: cfg.framing.clone(),
                timing_cfg: cfg.timing.clone(),
                debug_payload: cfg.debug_payload.clone(),
                stats: Arc::clone(&stats),
                state: HashMap::new(),
                timing: HashMap::new(),
            };
            // LEARN: LOOK AT THE SIGNATURE OF run(): `async fn run(mut self)` --
            //   `mut self` BY VALUE. The method CONSUMES the Worker. The struct,
            //   its receiver, its two HashMaps and its config copies are all
            //   MOVED INTO THE FUTURE, which is moved into the task.
            //   The consequences are exactly what makes this design work:
            //     1. The Worker and everything it owns lives inside ONE task.
            //     2. NOTHING ELSE IN THE PROGRAM HAS A REFERENCE TO IT -- the
            //        compiler proved that by taking ownership.
            //     3. Therefore self.state and self.timing are touched by exactly
            //        one task, and the &mut self methods below need NO LOCK, NO
            //        ConcurrentHashMap, AND NO ATOMICS.
            // JAVA: the equivalent is thread confinement BY CONVENTION -- and you
            //   would probably reach for ConcurrentHashMap anyway because you
            //   cannot PROVE the confinement. That is a per-operation CAS and a
            //   more complex data structure, bought for a guarantee Rust gives
            //   you free at compile time.
            tokio::spawn(worker.run());
        }

        tracing::info!(
            shards = cfg.tee.shards,
            queue_capacity = cfg.tee.queue_capacity,
            "tee workers started"
        );

        Tee {
            shards: Arc::new(senders),
            stats,
            publish_vp_to_fms: pub_v2f,
            publish_fms_to_vp: pub_f2v,
            active: true,
        }
    }

    // LEARN: `dir: Direction` by value -- 1 byte, Copy.
    #[inline]
    fn wants(&self, dir: Direction) -> bool {
        match dir {
            Direction::VpToFms => self.publish_vp_to_fms,
            Direction::FmsToVp => self.publish_fms_to_vp,
        }
    }

    /// Whether a frame in this direction would actually be queued. Lets the
    /// pump skip preparing a chunk it would only throw away.
    #[inline]
    pub fn should_offer(&self, dir: Direction) -> bool {
        self.active && self.wants(dir)
    }

    /// Hand a copy of the wire bytes to the publish path.
    ///
    /// This is called from the forwarding hot path and MUST NOT block, await, or
    /// fail upward. `chunk` is a `Bytes`, so the clone is a refcount bump rather
    /// than a copy of the payment message.
    //
    // ================= THE MOST SAFETY-CRITICAL SIGNATURE HERE =================
    // LEARN: read the signature as the architecture, because that is what it is:
    //   `&self`        -- a SHARED borrow. offer() is called concurrently from
    //                     every pump task with NO SYNCHRONISATION, because it
    //                     only READS self. Compiler-verified.
    //   `chunk: Bytes` -- BY VALUE, taking ownership. The caller gives up its
    //                     handle. Since Bytes is refcounted this is a pointer
    //                     move, not a copy of the payment message.
    //   NO `async`     -- IT CANNOT SUSPEND. It runs to completion on the calling
    //                     thread, always.
    //   returns `()`   -- IT CANNOT FAIL UPWARD. The caller has no error to
    //                     handle, so it cannot be tempted to retry or propagate.
    //
    // LEARN: THE COMPILER ENFORCES BOTH. Someone who later tries to add `.await`
    //   here must change the signature, which breaks pump(), which forces them to
    //   confront the design. THE RULE "KAFKA MUST NEVER AFFECT VP<->FMS" IS
    //   ENCODED IN A FUNCTION SIGNATURE.
    // JAVA: no signature can express "cannot block" or "cannot fail". You would
    //   write it in Javadoc and rely on review.
    // ===========================================================================
    #[inline]
    pub fn offer(&self, conn_id: u64, dir: Direction, peer: &Arc<str>, chunk: Bytes) {
        if !self.active || !self.wants(dir) {
            return;
        }
        // LEARN: shard by connection ID. THIS IS WHAT GUARANTEES both directions
        //   of one connection land on the SAME worker -- which is why ConnTiming
        //   below needs no locking at all.
        let shard = &self.shards[(conn_id as usize) % self.shards.len()];
        // LEARN: field init shorthand for three of the four fields.
        //   Arc::clone(peer) is one atomic increment. The whole event is built on
        //   the stack and MOVED into the queue slot. ZERO ALLOCATIONS.
        let ev = Event::Data {
            conn_id,
            dir,
            peer: Arc::clone(peer),
            chunk,
        };
        // LEARN: `try_send` is non-blocking -- returns Err immediately if full.
        // JAVA: BlockingQueue.offer() (as opposed to put()).
        match shard.try_send(ev) {
            Ok(()) => Stats::inc(&self.stats.tee_accepted),
            // Queue full: Kafka is behind. Drop and keep forwarding. This is the
            // whole point of the design -- the payment path does not care.
            //
            // LEARN: THE LOAD-SHEDDING DECISION, IN ONE LINE. Kafka is behind ->
            //   drop the audit copy, keep forwarding the payment. The correct
            //   trade, and it is observable via the counter.
            Err(_) => Stats::inc(&self.stats.tee_dropped),
        }
    }

    #[inline]
    pub fn open(&self, conn_id: u64) {
        if !self.active {
            return;
        }
        let shard = &self.shards[(conn_id as usize) % self.shards.len()];
        // Best-effort. If dropped, the worker falls back to first-frame time.
        //
        // LEARN: `Instant::now()` is a MONOTONIC clock reading -- System.nanoTime().
        //   It cannot go backwards and is unaffected by NTP or clock changes.
        // LEARN: Rust makes this A DIFFERENT TYPE from SystemTime (wall clock),
        //   so YOU CANNOT ACCIDENTALLY SUBTRACT A WALL-CLOCK TIME FROM A
        //   MONOTONIC ONE -- that is a type error, not a production incident.
        // JAVA: gives you two longs and hopes.
        let _ = shard.try_send(Event::Open {
            conn_id,
            at: Instant::now(),
        });
    }

    #[inline]
    pub fn close(&self, conn_id: u64) {
        if !self.active {
            return;
        }
        let shard = &self.shards[(conn_id as usize) % self.shards.len()];
        // Best-effort. A dropped Close only means the framer state lives until
        // the worker's idle sweep reclaims it.
        let _ = shard.try_send(Event::Close {
            conn_id,
            at: Instant::now(),
        });
    }
}

struct StreamState {
    framer: Framer,
    seq: u64,
    last_touched: Instant,
}

/// Per-connection timing. Both directions of a connection hash to the same
/// shard, so this needs no locking.
struct ConnTiming {
    opened: Instant,
    /// Previous frame on this connection, either direction.
    last_frame: Option<Instant>,
    /// Forwarded requests awaiting a response, oldest first.
    // LEARN: VecDeque is a RING BUFFER, so pop_front is O(1) with no shifting.
    // JAVA: ArrayDeque.
    pending: VecDeque<Instant>,
    last_touched: Instant,
}

impl ConnTiming {
    fn new(now: Instant) -> Self {
        Self {
            opened: now,
            last_frame: None,
            pending: VecDeque::new(),
            last_touched: now,
        }
    }
}

struct Worker {
    shard: usize,
    rx: mpsc::Receiver<Event>,
    publisher: Arc<Publisher>,
    framing: crate::config::Framing,
    timing_cfg: crate::config::Timing,
    debug_payload: crate::config::DebugPayload,
    stats: Arc<Stats>,
    // LEARN: A TUPLE AS A COMPOSITE KEY. This works because (u64, Direction)
    //   gets Hash and Eq automatically from its components (both derived them).
    // JAVA: you would need `record ConnDir(long id, Direction dir)` -- a class
    //   declaration plus A HEAP ALLOCATION PER LOOKUP, since the key must be
    //   boxed. Rust's tuple key is 16 bytes ON THE STACK, hashed in place, NO
    //   ALLOCATION PER LOOKUP. On a per-message path that is the difference
    //   between steady garbage and none.
    state: HashMap<(u64, Direction), StreamState>,
    timing: HashMap<u64, ConnTiming>,
}

const IDLE_SWEEP: Duration = Duration::from_secs(60);
const IDLE_MAX: Duration = Duration::from_secs(600);

impl Worker {
    // LEARN: `mut self` BY VALUE -- see the long note at tokio::spawn above.
    //   This is the single most important signature in the file.
    async fn run(mut self) {
        // LEARN: a periodic timer.
        // JAVA: ScheduledExecutorService.scheduleAtFixedRate.
        let mut sweep = tokio::time::interval(IDLE_SWEEP);
        // LEARN: if ticks are missed (the worker was busy), DO NOT fire them all
        //   back-to-back; skip to the next scheduled slot.
        // JAVA: scheduleAtFixedRate DOES fire them back-to-back, which is a
        //   classic production surprise -- a paused service resumes and instantly
        //   runs the task 50 times. Rust forces you to choose the policy.
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // LEARN: recv() returns Option<Event>: Some(ev) for a message,
                //   and `None` WHEN EVERY Sender HAS BEEN DROPPED. That is the
                //   shutdown signal -- no poison pill, no sentinel value, no
                //   `AtomicBoolean running`. When main exits and drops the Tee,
                //   the senders drop, the refcount hits zero, recv() returns
                //   None, the worker breaks and logs. SHUTDOWN FALLS OUT OF THE
                //   OWNERSHIP MODEL FOR FREE.
                // LEARN (cancellation safety): select! may cancel this recv()
                //   when the sweep branch wins. tokio's mpsc::Receiver::recv is
                //   DOCUMENTED as cancellation-safe, so no message is lost. That
                //   is a property you must check PER API, and it is the sharpest
                //   edge in async Rust.
                ev = self.rx.recv() => match ev {
                    Some(ev) => self.handle(ev),
                    None => break,
                },
                _ = sweep.tick() => {
                    let now = Instant::now();
                    let before = self.state.len() + self.timing.len();
                    // LEARN: `retain(|k, v| bool)` keeps entries where the closure
                    //   returns true.
                    // JAVA: map.entrySet().removeIf(...) -- inverted.
                    // LEARN: this is a MANUAL LEAK GUARD. If a Close event were
                    //   dropped (queue full), that connection's state would live
                    //   forever. The sweep bounds it at 10 minutes.
                    //   NOTE: Rust's ownership model DOES NOT SAVE YOU HERE. This
                    //   is a LOGICAL leak, not a memory-safety issue, and Java
                    //   would need exactly the same sweep. Internalise it: Rust
                    //   prevents use-after-free, NOT forgot-to-remove-from-the-map.
                    self.state.retain(|_, s| now.duration_since(s.last_touched) < IDLE_MAX);
                    self.timing.retain(|_, t| now.duration_since(t.last_touched) < IDLE_MAX);
                    let reclaimed = before - (self.state.len() + self.timing.len());
                    if reclaimed > 0 {
                        tracing::debug!(shard = self.shard, reclaimed, "swept idle connection state");
                    }
                }
            }
        }
        tracing::info!(shard = self.shard, "tee worker stopped");
    }

    // LEARN: `&mut self` -- exclusive access, needed because it mutates the maps.
    //   No lock, because of the ownership argument at spawn().
    fn handle(&mut self, ev: Event) {
        match ev {
            Event::Open { conn_id, at } => {
                if self.timing_cfg.enabled {
                    self.timing.insert(conn_id, ConnTiming::new(at));
                }
            }
            Event::Close { conn_id, at } => {
                self.state.remove(&(conn_id, Direction::VpToFms));
                self.state.remove(&(conn_id, Direction::FmsToVp));
                // LEARN: `if let Some(t) = map.remove(&k)` -- remove returns
                //   Option, and this handles "was present" in one construct.
                if let Some(t) = self.timing.remove(&conn_id) {
                    // LEARN: `saturating_duration_since` CLAMPS TO ZERO rather
                    //   than panicking if `opened` were somehow later than `at`.
                    //   Duration is UNSIGNED, and the plain `-` operator on
                    //   Instant panics on a negative result. Explicit overflow
                    //   discipline again.
                    let dur = at.saturating_duration_since(t.opened);
                    Stats::observe(
                        &self.stats.conn_duration_count,
                        &self.stats.conn_duration_sum_ms,
                        &self.stats.conn_duration_max_ms,
                        dur.as_millis() as u64,
                    );
                    if !t.pending.is_empty() {
                        Stats::add(&self.stats.rtt_unmatched, t.pending.len() as u64);
                    }
                    tracing::debug!(
                        conn_id,
                        duration_ms = ms(dur),
                        unmatched_requests = t.pending.len(),
                        "connection closed"
                    );
                }
            }
            Event::Data {
                conn_id,
                dir,
                peer,
                chunk,
            } => {
                // LEARN: THIS CLONE EXISTS FOR A BORROW-CHECKER REASON, and it is
                //   the most common friction a Java developer hits in Rust:
                //     - self.state.entry(...) takes &mut self.state.
                //     - the closure below would need &self.framing.
                //     - the compiler's borrow analysis cannot always prove
                //       self.state and self.framing are disjoint when both are
                //       reached through `self`, so it rejects the overlap.
                //     - cloning into a LOCAL first breaks the dependency: the
                //       closure now captures a local, not self.
                //   HONEST ASSESSMENT:
                //     CON: you sometimes restructure code, or pay a small clone,
                //          to satisfy a checker stricter than strictly necessary.
                //     PRO: the same checker NEVER lets you ship a data race or a
                //          use-after-free, and that same discipline is what makes
                //          the lock-free worker design above sound.
                let framing = self.framing.clone();
                // LEARN: `entry(key).or_insert_with(closure)` returns
                //   &mut StreamState -- a mutable reference INTO the map.
                // JAVA: map.computeIfAbsent(k, f)
                let entry = self
                    .state
                    .entry((conn_id, dir))
                    .or_insert_with(|| StreamState {
                        framer: Framer::new(framing),
                        seq: 0,
                        last_touched: Instant::now(),
                    });
                entry.last_touched = Instant::now();

                // LEARN: `push(&chunk)` passes a SHARED BORROW -- Bytes derefs to
                //   &[u8] automatically (deref coercion again).
                // LEARN: exhaustive match over all three Step variants; add a
                //   fourth variant to the enum and this fails to compile.
                match entry.framer.push(&chunk) {
                    Step::Ignored => {}
                    // LEARN: binds the &'static str out of the variant.
                    Step::Desynced(reason) => {
                        Stats::inc(&self.stats.framer_desyncs);
                        tracing::warn!(
                            conn_id,
                            direction = dir.as_str(),
                            reason,
                            "framing desync; publishing suspended for this stream (proxy unaffected)"
                        );
                    }
                    Step::Frames(frames) => {
                        let ts = now_ms();
                        // LEARN: ONE String allocation per CHUNK, not per frame,
                        //   deliberately hoisted out of the loop. It is the Kafka
                        //   partition key, so both directions of a connection land
                        //   on the same partition and stay ordered.
                        let key = conn_id.to_string();
                        // LEARN: `for frame in frames` CONSUMES the Vec (an
                        //   IntoIterator by value), yielding OWNED Bytes. After
                        //   the loop, `frames` is gone. To keep it you would
                        //   write `for frame in &frames`.
                        // JAVA: for-each always borrows. Rust makes the
                        //   distinction visible, and consuming here avoids a
                        //   refcount bump per frame.
                        for frame in frames {
                            entry.seq += 1;
                            Stats::inc(&self.stats.frames_emitted);

                            // Timing is measured per frame, not per chunk, so a
                            // pipelined read yields one measurement per message.
                            let now = Instant::now();
                            let (conn_age_ms, gap_ms, rtt_ms) = if self.timing_cfg.enabled {
                                Self::measure(
                                    &mut self.timing,
                                    &self.timing_cfg,
                                    &self.stats,
                                    conn_id,
                                    dir,
                                    now,
                                )
                            } else {
                                (None, None, None)
                            };

                            // LEARN: Meta IS A BORROWING STRUCT -- see its <'a>
                            //   lifetime in kafka.rs. `peer: &peer` stores a
                            //   REFERENCE, not a clone: no refcount bump, no
                            //   allocation. Meta is built on the stack, passed by
                            //   reference, and dies at the end of this iteration.
                            //   IT COSTS ZERO HEAP BYTES.
                            // JAVA: cannot express this. Every field referencing
                            //   another object is a GC-tracked reference, the
                            //   holder is a heap allocation with a header, and it
                            //   becomes garbage. Escape analysis CAN sometimes
                            //   stack-allocate it, but only after profiling, only
                            //   in compiled code, and it silently stops working
                            //   when the code shape changes.
                            let meta = kafka::Meta {
                                conn_id,
                                direction: dir.as_str(),
                                seq: entry.seq,
                                peer: &peer,
                                ts_ms: ts,
                                conn_age_ms,
                                gap_ms,
                                rtt_ms,
                            };
                            payload_log::log_bytes(
                                &self.debug_payload,
                                conn_id,
                                dir.as_str(),
                                "kafka_frame_ready",
                                &frame,
                            );
                            self.publisher.publish(&key, &meta, &frame);
                        }
                    }
                }
            }
        }
    }
}

// LEARN: a SECOND impl block for the same type. Perfectly legal -- you can split
//   an impl across blocks (and across files, within the same crate) to group
//   related methods. Java requires everything in one class body.
impl Worker {
    /// Returns `(conn_age_ms, gap_ms, rtt_ms)` for one frame.
    ///
    /// `rtt_ms` is only produced for `fms_to_vp` frames, by pairing with the
    /// oldest unanswered request on the connection.
    // LEARN: NOTE THIS IS AN ASSOCIATED FUNCTION TAKING `&mut HashMap`
    //   EXPLICITLY, not a method taking `&mut self`. Another borrow-checker
    //   accommodation: the caller is already holding `entry` (a &mut into
    //   self.state), so it cannot ALSO hand out &mut self. Passing the specific
    //   fields it needs lets the compiler see they are disjoint -- which it can
    //   do for DIRECT FIELD ACCESS, just not through a whole-self borrow.
    // LEARN: THE IDIOM TO REMEMBER -- when the borrow checker complains about
    //   `&mut self`, pass individual fields instead. Mechanical fix once you
    //   recognise the shape.
    // LEARN: returns a 3-TUPLE of Option<f64> -- three optional measurements, no
    //   wrapper class, no allocation, returned in registers.
    fn measure(
        timing: &mut HashMap<u64, ConnTiming>,
        cfg: &crate::config::Timing,
        stats: &Stats,
        conn_id: u64,
        dir: Direction,
        now: Instant,
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        // Falls back to first-frame time if the Open event was dropped.
        let t = timing
            .entry(conn_id)
            .or_insert_with(|| ConnTiming::new(now));
        t.last_touched = now;

        let conn_age_ms = ms(now.saturating_duration_since(t.opened));
        // LEARN: `Option::map` -- if Some, apply the closure; if None, stay None.
        // JAVA: Optional.map. Produces Option<f64> in one expression, with no
        //   hand-written branch and no allocation.
        let gap_ms = t
            .last_frame
            .map(|prev| ms(now.saturating_duration_since(prev)));
        t.last_frame = Some(now);

        let mut rtt_ms = None;
        if cfg.pair_request_response {
            match dir {
                Direction::VpToFms => {
                    if t.pending.len() >= cfg.max_pending {
                        // Responses have stopped arriving. Drop the oldest rather
                        // than grow without bound.
                        //
                        // LEARN: the bounded-memory guard. If FMS stops answering,
                        //   `pending` cannot grow past max_pending.
                        t.pending.pop_front();
                        Stats::inc(&stats.rtt_unmatched);
                    }
                    t.pending.push_back(now);
                }
                Direction::FmsToVp => {
                    // LEARN: pop_front() returns Option<Instant> -- None if empty,
                    //   so an UNSOLICITED response cannot corrupt anything. The
                    //   `if let Some(sent)` handles both cases in one construct.
                    // LEARN: the FIFO pairing ASSUMES responses come back in
                    //   request order. That assumption is documented honestly in
                    //   config.rs, at the config field an operator will read, with
                    //   a flag to disable it.
                    if let Some(sent) = t.pending.pop_front() {
                        let d = now.saturating_duration_since(sent);
                        rtt_ms = Some(ms(d));
                        Stats::observe(
                            &stats.rtt_count,
                            &stats.rtt_sum_us,
                            &stats.rtt_max_us,
                            d.as_micros() as u64,
                        );
                    }
                }
            }
        }

        (Some(conn_age_ms), gap_ms, rtt_ms)
    }
}

/// Duration as milliseconds with microsecond resolution.
// LEARN: MODULE-LEVEL PRIVATE FUNCTIONS. No class needed -- Rust has free
//   functions, and this is idiomatic rather than a code smell.
// JAVA: you would need a static method on some utility class.
// LEARN: converts via microseconds so you get 0.001 ms resolution rather than
//   integer milliseconds.
fn ms(d: Duration) -> f64 {
    (d.as_micros() as f64) / 1000.0
}

// LEARN: SystemTime is the WALL CLOCK -- System.currentTimeMillis(). It returns
//   a Result because SystemTime::now() can theoretically be BEFORE the Unix
//   epoch if the system clock is absurd. `.map(...).unwrap_or(0)` handles it
//   without a hand-written branch.
// LEARN: note the TWO CLOCK TYPES in use in this file -- Instant (monotonic) for
//   durations, SystemTime (wall) for timestamps published to Kafka. The right
//   tool for each, and the type system will not let you mix them up.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
