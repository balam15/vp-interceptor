// ============================================================================
// kafka.rs -- builds the producer and publishes frames. Never blocks, never
// fails upward, never connects eagerly.
//
// This file has the most Rust-specific type machinery in the crate: lifetimes
// ('a), associated types, auto-traits (Send/Sync), turbofish, and compile-time
// serialisation. Take it slowly.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 6.
// ============================================================================

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// LEARN: `as _` IMPORTS THE TRAIT FOR ITS METHODS BUT DOES NOT BIND ITS NAME.
//   We need Engine's methods to be callable, but we never write `Engine` in this
//   file, so binding the name would trigger an unused-import warning. This says
//   "bring the trait into scope anonymously".
// JAVA: no analogue -- Java has no extension-method resolution at all.
use base64::Engine as _;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use serde::Serialize;

use crate::config::{Kafka as KafkaCfg, KafkaFilter, KafkaFilterAllOf, Parse as ParseCfg};
use crate::parse::KvParser;
use crate::stats::Stats;

/// Delivery reports arrive on librdkafka's own background thread. We only count
/// them -- there is deliberately no retry or recovery logic here, because the
/// contract with the proxy is that Kafka outcomes are never acted upon.
pub struct CountingContext {
    stats: Arc<Stats>,
    log_every: u64,
}

// LEARN: AN EMPTY IMPL BLOCK. ClientContext has DEFAULT IMPLEMENTATIONS for
//   every method, so implementing it requires no code at all. It is effectively
//   a marker: "this type is eligible to be a client context".
// JAVA: an interface with all-default methods (Java 8+). Same idea.
impl ClientContext for CountingContext {}

impl ProducerContext for CountingContext {
    // LEARN: `type DeliveryOpaque = ();` is an ASSOCIATED TYPE. The trait
    //   declares "implementors must specify a type named DeliveryOpaque"; we
    //   specify `()` -- we attach no per-message user data.
    // JAVA: closest is a generic type parameter on the interface
    //   (interface ProducerContext<D>). The difference is that with an
    //   ASSOCIATED type, a type can implement ProducerContext ONLY ONCE, with
    //   one choice of DeliveryOpaque. Use an associated type when there is one
    //   natural choice per implementor; use a generic parameter when there are
    //   many. (Java cannot implement the same interface twice with different
    //   type arguments anyway, due to erasure -- its own limitation.)
    type DeliveryOpaque = ();

    // LEARN: `DeliveryResult<'_>` -- the `'_` is an ANONYMOUS LIFETIME. It says
    //   "this type has a lifetime parameter; infer it". Without it you would
    //   write DeliveryResult<'a> and have to declare 'a. It means "borrowed from
    //   somewhere, and I do not need to name the source".
    // LEARN: `_: ()` is a parameter matched and discarded -- the unit-typed
    //   opaque we do not use.
    //
    // ==================== Send / Sync -- READ THIS ============================
    // LEARN: THIS CALLBACK RUNS ON LIBRDKAFKA'S OWN BACKGROUND C THREAD. `&self`
    //   means it only reads CountingContext, and the compiler enforces that. The
    //   Arc<Stats> is shared across the thread boundary safely because Stats
    //   contains only atomics, which makes it `Sync`.
    //
    //   Send = "this type can be MOVED to another thread."
    //   Sync = "&T can be SHARED across threads", i.e. T is safe for concurrent
    //          access.
    //
    //   These are AUTO-TRAITS: the compiler derives them STRUCTURALLY. A struct
    //   is Sync if all its fields are. AtomicU64 is Sync; Cell<u64> is not.
    //   Arc<T> is Send+Sync only if T: Send+Sync. Rc<T> (the NON-atomic refcount)
    //   is NEITHER, so YOU CANNOT COMPILE A PROGRAM THAT SHARES AN Rc ACROSS
    //   THREADS. tokio::spawn requires its future to be Send, so if you
    //   accidentally hold a non-thread-safe value across an .await, THE PROGRAM
    //   DOES NOT COMPILE.
    //
    // JAVA: HAS NO EQUIVALENT WHATSOEVER. @ThreadSafe is a comment. Sharing a
    //   HashMap across threads compiles perfectly and fails in production at 3am
    //   under load, intermittently.
    //   This is arguably the single strongest argument for Rust in a concurrent
    //   system -- stronger than any of the memory numbers.
    // =========================================================================
    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        match result {
            Ok(_) => Stats::inc(&self.stats.kafka_delivered),
            // LEARN: DESTRUCTURING A TUPLE INSIDE THE Err VARIANT -- two levels of
            //   pattern in one expression. `_msg` is bound but unused; the leading
            //   underscore silences the unused-variable warning WHILE DOCUMENTING
            //   what the field is. (A bare `_` would discard it without naming it.)
            Err((err, _msg)) => {
                // LEARN: fetch_add returns the value BEFORE the addition, hence
                //   the `+ 1`.
                // JAVA: getAndIncrement().
                let n = self.stats.kafka_delivery_failed.fetch_add(1, Ordering::Relaxed) + 1;
                // Rate-limit: a broker outage would otherwise produce one log line
                // per transaction, and log I/O is the one thing that could still
                // starve the runtime.
                //
                // LEARN: that comment names a real production failure mode --
                //   MONITORING CODE TAKING DOWN THE THING IT MONITORS.
                if n == 1 || n % self.log_every == 0 {
                    tracing::warn!(failures = n, error = %err, "kafka delivery failing");
                }
            }
        }
    }
}

/// How the raw frame is represented inside the JSON envelope.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PayloadEncoding {
    /// Safe for binary ISO 8583. The default.
    Base64,
    /// Lowercase hex -- verbose, but the traditional ISO 8583 dump format and
    /// far easier to eyeball against a spec.
    Hex,
    /// Only for text-ish protocols. Invalid sequences become U+FFFD, so this is
    /// lossy and must not be used where the bytes are evidence.
    Utf8,
}

impl PayloadEncoding {
    // LEARN: String -> enum ONCE, AT STARTUP. Everything downstream matches on a
    //   1-byte enum instead of comparing strings. Compare framing.rs, which
    //   compares `mode == "raw"` per call -- this is the better pattern, and it
    //   is on the hotter path where it matters more.
    fn parse(s: &str) -> Self {
        match s {
            "hex" => PayloadEncoding::Hex,
            "utf8" => PayloadEncoding::Utf8,
            _ => PayloadEncoding::Base64,
        }
    }

    fn name(self) -> &'static str {
        match self {
            PayloadEncoding::Base64 => "base64",
            PayloadEncoding::Hex => "hex",
            PayloadEncoding::Utf8 => "utf8",
        }
    }

    fn encode(self, bytes: &[u8]) -> String {
        match self {
            PayloadEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
            PayloadEncoding::Hex => {
                // LEARN: EXACTLY ONE ALLOCATION for the whole hex string,
                //   correctly sized up front.
                // JAVA: StringBuilder grows by DOUBLING from 16, meaning ~7
                //   reallocations and copies for a 1 KiB frame.
                let mut s = String::with_capacity(bytes.len() * 2);
                // LEARN: iterating &[u8] yields &u8; `b >> 4` auto-dereferences.
                for b in bytes {
                    // Avoids a `format!` per byte, which is surprisingly costly
                    // at payments volume.
                    //
                    // LEARN: the comment is correct and measurable. format! builds
                    //   a formatter, parses the format spec at runtime, and
                    //   allocates a String PER CALL. Per byte, at payments volume,
                    //   that is real. (Java's String.format("%02x", b) is worse.)
                    // LEARN: char::from_digit(v, 16) returns Option<char> -- None
                    //   if v >= 16. Here `b >> 4` on a u8 is always <= 15, so the
                    //   .unwrap() is PROVABLY safe. That is the correct use of
                    //   unwrap: when you can state the proof.
                    s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
                    s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
                }
                s
            }
            // LEARN: from_utf8_lossy returns a Cow<str> (borrow-or-own);
            //   .into_owned() forces it to an owned String. See admin.rs for the
            //   full Cow explanation.
            PayloadEncoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ValueFormat {
    /// JSON envelope: metadata plus the encoded payload.
    Json,
    /// The reassembled frame, byte-for-byte, with metadata only in headers.
    Raw,
}

/// Per-message metadata. Borrowed rather than owned so publishing a frame
/// allocates only the JSON body.
//
// ======================= LIFETIMES -- THE HARD PART ==========================
// LEARN: `<'a>` is a LIFETIME PARAMETER -- a generic parameter over lifetimes,
//   and the concept with NO JAVA ANALOGUE AT ALL.
//   Meta<'a> holds two &'a str references, and the declaration means:
//     "A Meta<'a> MAY NOT OUTLIVE 'a."
//   The compiler checks this at every use site. Concretely, in tee.rs:
//       let meta = kafka::Meta { ..., peer: &peer, ... };
//       self.publisher.publish(&key, &meta, &frame);
//   `peer` lives in the enclosing scope; `meta` borrows it; the compiler
//   verifies meta dies BEFORE peer does. If you tried to stash meta in a field
//   that outlives the loop, IT WOULD NOT COMPILE.
//
// LEARN: WHY IT MATTERS HERE (see the doc comment above):
//   - Meta OWNS NOTHING. It is STACK-ALLOCATED, ~72 bytes, freed by moving the
//     stack pointer.
//   - The alternative -- String fields -- would mean TWO HEAP ALLOCATIONS AND
//     TWO MEMCPYS PER FRAME.
//   - At payments volume that is the difference between zero and millions of
//     short-lived allocations.
//
// JAVA: CANNOT EXPRESS THIS. Any Java object holding a String field holds a
//   GC-tracked reference; the object itself is heap-allocated with a header; and
//   it becomes garbage. Escape analysis CAN sometimes stack-allocate such an
//   object, but only if the JIT proves non-escape after profiling, only in
//   compiled code, and it silently stops working when the code shape changes.
//
//   PRO Rust: zero-allocation borrowed views are GUARANTEED, statically, always,
//     from the first instruction.
//   CON Rust: lifetimes are the hardest part of the language. They infect
//     signatures (Meta<'a>, Envelope<'a>, DeliveryResult<'_>), and "fighting the
//     borrow checker" over lifetimes is the classic beginner experience.
// =============================================================================
pub struct Meta<'a> {
    pub conn_id: u64,
    pub direction: &'a str,
    pub seq: u64,
    pub peer: &'a str,
    pub ts_ms: u64,
    /// Milliseconds since this connection was accepted.
    pub conn_age_ms: Option<f64>,
    /// Milliseconds since the previous frame on this connection, either
    /// direction. On a strict request/response link this is the FMS think-time.
    pub gap_ms: Option<f64>,
    /// Request-to-response time, present on `fms_to_vp` frames only.
    pub rtt_ms: Option<f64>,
}

/// The Kafka message value when `value_format = "json"`.
///
/// Field order here is the field order on the wire, so keep the identifying
/// fields first -- it makes `kafka-console-consumer` output readable without
/// piping through `jq`.
//
// LEARN: `#[derive(Serialize)]` GENERATES THE JSON WRITER AT COMPILE TIME. This
//   is the sharpest Rust-vs-Java contrast in the crate:
//     Jackson works by RUNTIME REFLECTION -- it inspects fields and annotations
//       at runtime, builds a serialiser, caches it, and walks it per object.
//     Serde works by COMPILE-TIME CODE GENERATION -- this attribute expands into
//       a hand-written-quality serialize() method BEFORE your code is compiled.
//       At runtime there is no reflection, no field lookup, no cache warm-up, no
//       first-call penalty.
//   PRO: faster, no warm-up cliff, no reflection config for native images, no
//     setAccessible breakage, and the JSON shape is checked at compile time.
//   CON: you cannot serialise a type you cannot annotate without writing an impl
//     by hand, and it inflates compile time and binary size.
//   This is directly visible in BENCHMARK.md: Java takes 1185 ms to reach
//   healthy, Rust takes 30 ms. A large chunk of Java's is classloading and
//   reflective setup that Rust did at compile time.
//
// LEARN: SERDE EMITS FIELDS IN DECLARATION ORDER, which is why the doc comment
//   above treats field order as a wire-format decision. Jackson's order is
//   unspecified unless you add @JsonPropertyOrder.
#[derive(Serialize)]
struct Envelope<'a> {
    conn_id: u64,
    direction: &'a str,
    seq: u64,
    peer: &'a str,
    ts_ms: u64,
    length: usize,
    // LEARN: omit the field entirely when None.
    // JAVA: @JsonInclude(JsonInclude.Include.NON_NULL)
    // LEARN: note the FUNCTION PATH GIVEN AS A STRING LITERAL. It is resolved AT
    //   COMPILE TIME by the macro, so a typo is a COMPILE error. Contrast
    //   Jackson, where an annotation naming a bad class fails at runtime.
    #[serde(skip_serializing_if = "Option::is_none")]
    conn_age_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gap_ms: Option<f64>,
    /// Present on `fms_to_vp` frames: how long FMS took to answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    rtt_ms: Option<f64>,
    /// Parsed `key=value` fields, when `[parse]` is enabled and the frame parses.
    // LEARN: an optional BORROWED map -- not cloned. And it is 8 bytes, thanks to
    //   the null-pointer niche optimisation on the reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<&'a BTreeMap<String, String>>,
    /// Why parsing failed. Present *instead of* `fields`; the payload is still
    /// published either way.
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_error: Option<&'a str>,
    encoding: &'a str,
    // LEARN: EVERY STRING FIELD HERE IS A BORROW. Serialising this envelope
    //   allocates EXACTLY ONE THING: the output Vec<u8>. Nothing else.
    payload: &'a str,
}

pub struct Publisher {
    producer: ThreadedProducer<CountingContext>,
    topic: String,
    value_format: ValueFormat,
    payload_encoding: PayloadEncoding,
    // LEARN: Option<KvParser> stored INLINE (KvParser is 24 bytes with no heap),
    //   not behind a pointer. Java would need a reference to a heap object.
    parser: Option<KvParser>,
    filter: KafkaFilter,
    stats: Arc<Stats>,
    enqueue_fail_log: AtomicU64,
    parse_fail_log: AtomicU64,
}

impl Publisher {
    /// Builds the producer. This does NOT connect to any broker: librdkafka
    /// resolves and connects lazily on its background threads. A broker that is
    /// down, unreachable, or rejecting the handshake cannot fail this call and
    /// therefore cannot delay or prevent the proxy from serving VP.
    ///
    /// Returns `Ok(None)` when Kafka is disabled, and logs-and-degrades rather
    /// than propagating an error when the *configuration* is unusable -- losing
    /// the audit feed is strictly better than dropping the payment path.
    // LEARN: THE RETURN TYPE IS Option<...>, NOT Result<...>. That is deliberate:
    //   a caller CANNOT propagate a Kafka failure into the startup path, because
    //   there is no error value to propagate. The architectural rule is enforced
    //   by the signature, not by review.
    pub fn build(
        cfg: &KafkaCfg,
        parse_cfg: &ParseCfg,
        stats: Arc<Stats>,
    ) -> Option<Arc<Publisher>> {
        if !cfg.enabled {
            tracing::info!("kafka disabled by config; running as plain proxy");
            return None;
        }

        let mut cc = ClientConfig::new();
        // `.as_str()` throughout: ClientConfig::set is generic over Into<String>,
        // and a generic bound gets no deref coercion from &String.
        //
        // LEARN: THIS COMMENT DOCUMENTS A GENUINELY SUBTLE RULE THAT WILL BITE YOU.
        //   Normally &String COERCES to &str automatically (deref coercion), so
        //   you can pass a &String anywhere a &str is expected. But coercion
        //   happens during TYPE CHECKING AGAINST A CONCRETE EXPECTED TYPE. When
        //   the parameter is generic -- fn set<K: Into<String>>(k: K, ...) -- the
        //   compiler must first INFER K. It infers K = &String, then asks "does
        //   &String implement Into<String>?" IT DOES NOT. Error.
        //   `.as_str()` performs the conversion explicitly, so K = &str, which
        //   does implement Into<String>.
        //   RULE OF THUMB: DEREF COERCION WORKS FOR CONCRETE PARAMETER TYPES, NOT
        //   FOR GENERIC BOUNDS.
        // JAVA: nothing comparable, because Java has no user-definable implicit
        //   conversions at all -- arguably simpler, definitely less powerful.
        cc.set("bootstrap.servers", cfg.brokers.as_str())
            .set("acks", cfg.acks.as_str())
            .set("compression.type", cfg.compression.as_str())
            .set("linger.ms", cfg.linger_ms.to_string())
            .set("message.timeout.ms", cfg.message_timeout_ms.to_string())
            .set("queue.buffering.max.messages", cfg.queue_buffering_max_messages.to_string())
            .set("queue.buffering.max.kbytes", cfg.queue_buffering_max_kbytes.to_string())
            // Never let a full internal queue block the caller. Combined with our
            // own bounded tee queue this makes the whole publish path total.
            .set("queue.buffering.max.ms", cfg.linger_ms.to_string())
            .set("enable.idempotence", "false");

        // LEARN: `for (k, v) in &cfg.properties` iterates a BTreeMap BY
        //   REFERENCE, yielding (&String, &String). Iterating `cfg.properties`
        //   without the & would try to CONSUME the map, which we cannot do
        //   because we only have a &KafkaCfg borrow.
        for (k, v) in &cfg.properties {
            cc.set(k.as_str(), v.as_str());
        }

        let value_format = match cfg.value_format.as_str() {
            "raw" => ValueFormat::Raw,
            _ => ValueFormat::Json,
        };
        let payload_encoding = PayloadEncoding::parse(&cfg.payload_encoding);
        let parser = KvParser::new(parse_cfg);
        let filter = cfg.filter.clone();

        let ctx = CountingContext { stats: Arc::clone(&stats), log_every: 1000 };
        // LEARN: `::<...>` IS THE "TURBOFISH". It supplies generic type arguments
        //   explicitly when inference cannot determine them.
        // JAVA: writes `Foo.<String>bar()`. Rust needs the extra `::` because
        //   `Foo<String>` would be ambiguous with the less-than operator in
        //   expression position -- hence the fish.
        // LEARN: needed here because create_with_context is generic over BOTH the
        //   context type and the producer type, and only the return type would
        //   pin down the latter.
        // LEARN: ThreadedProducer runs its OWN BACKGROUND POLLING THREAD. So this
        //   process has REAL OS THREADS beyond tokio's: tokio's workers plus
        //   librdkafka's internals. The delivery callback fires on one of
        //   librdkafka's, which is exactly why Send/Sync matter above.
        match cc.create_with_context::<CountingContext, ThreadedProducer<CountingContext>>(ctx) {
            Ok(producer) => {
                tracing::info!(
                    brokers = %cfg.brokers,
                    topic = %cfg.topic,
                    // LEARN: `?` sigil = record using the Debug impl (from
                    //   #[derive(Debug)]). Compare `%` = Display.
                    value_format = ?value_format,
                    payload_encoding = payload_encoding.name(),
                    field_parsing = parser.is_some(),
                    "kafka producer created (lazy connect)"
                );
                Some(Arc::new(Publisher {
                    producer,
                    topic: cfg.topic.clone(),
                    value_format,
                    payload_encoding,
                    parser,
                    filter,
                    stats,
                    enqueue_fail_log: AtomicU64::new(0),
                    parse_fail_log: AtomicU64::new(0),
                }))
            }
            Err(e) => {
                // LEARN: PRODUCER CREATION FAILURE RETURNS None, NOT AN ERROR.
                //   The type system makes the architectural rule unforgettable.
                tracing::error!(error = %e, "kafka producer creation failed; continuing WITHOUT publishing");
                None
            }
        }
    }

    /// Non-blocking enqueue. Returns immediately in every case, including when
    /// librdkafka's queue is full.
    ///
    /// Runs on a tee shard worker, never on the forwarding path, so the JSON and
    /// base64 allocations here cannot affect VP<->FMS latency.
    // LEARN: `&Meta<'_>` -- anonymous lifetime again: "a Meta borrowed from
    //   somewhere, I do not need to relate its lifetime to anything else here".
    pub fn publish(&self, key: &str, meta: &Meta<'_>, frame: &[u8]) {
        if should_drop_frame(&self.filter, frame) {
            return;
        }

        let headers = self.headers(meta);

        match self.value_format {
            ValueFormat::Raw => self.send(key, frame, headers),
            ValueFormat::Json => {
                let payload = self.payload_encoding.encode(frame);

                // Parsing never gates publishing: a frame we cannot read is still
                // published, with the reason attached.
                //
                // LEARN: `.as_ref()` converts &Option<T> -> Option<&T>. You get an
                //   Option OF A BORROW, without moving the parser out of self
                //   (which &self forbids anyway). Extremely common idiom.
                // LEARN: `.map(|p| p.parse(frame))` then produces
                //   Option<Result<BTreeMap, String>>.
                // LEARN: THIS BINDING IS LOAD-BEARING. `parsed` must be bound to a
                //   variable BEFORE the match, because the BTreeMap inside it has
                //   to OUTLIVE the Envelope that borrows it. If you inlined the
                //   expression into the match, the temporary would be dropped at
                //   the end of the statement and the borrow would dangle -- WHICH
                //   THE COMPILER WOULD REJECT.
                // JAVA: this would work by accident, because the GC keeps the map
                //   alive as long as anything references it. Here you must think
                //   about it -- and in exchange you know exactly when it dies.
                let parsed = self.parser.as_ref().map(|p| p.parse(frame));
                // LEARN: `match &parsed` matches ON A REFERENCE, so the arms bind
                //   references (map: &BTreeMap, reason: &String) rather than
                //   moving out. That is how you inspect a value without consuming
                //   it.
                // LEARN: the three arms cover parser-enabled-and-succeeded /
                //   enabled-and-failed / not-enabled -- exhaustively, by the
                //   compiler. The TUPLE RETURN assigns two variables from one
                //   match; in Java this would be two mutable locals initialised to
                //   null and assigned in branches.
                let (fields, parse_error) = match &parsed {
                    Some(Ok(map)) => (Some(map), None),
                    Some(Err(reason)) => {
                        let n = self.parse_fail_log.fetch_add(1, Ordering::Relaxed) + 1;
                        if n == 1 || n % 1000 == 0 {
                            tracing::warn!(failures = n, reason = %reason, "frame did not parse; publishing unparsed");
                        }
                        (None, Some(reason.as_str()))
                    }
                    None => (None, None),
                };

                let envelope = Envelope {
                    conn_id: meta.conn_id,
                    direction: meta.direction,
                    seq: meta.seq,
                    peer: meta.peer,
                    ts_ms: meta.ts_ms,
                    length: frame.len(),
                    conn_age_ms: meta.conn_age_ms,
                    gap_ms: meta.gap_ms,
                    rtt_ms: meta.rtt_ms,
                    fields,
                    parse_error,
                    encoding: self.payload_encoding.name(),
                    payload: &payload,
                };
                match serde_json::to_vec(&envelope) {
                    Ok(body) => self.send(key, &body, headers),
                    Err(e) => {
                        // Should be unreachable: every field is a plain scalar or
                        // an already-encoded string.
                        Stats::inc(&self.stats.kafka_enqueue_failed);
                        tracing::error!(error = %e, "json encode failed; dropping frame");
                    }
                }
            }
        }
    }

    fn send(&self, key: &str, value: &[u8], headers: OwnedHeaders) {
        // LEARN: FOUR GENERIC PARAMETERS -- a lifetime, the key type, the payload
        //   type, and the delivery-opaque type.
        // LEARN: note `str` and `[u8]` are UNSIZED TYPES used DIRECTLY as type
        //   parameters. That works because BaseRecord declares them as `?Sized`
        //   (opting out of the default "must have a known compile-time size"
        //   bound). Java cannot express this; every type parameter is a reference
        //   to a sized object.
        // LEARN: the explicit annotation is needed because inference cannot
        //   determine the opaque type `()` from usage alone.
        let record: BaseRecord<'_, str, [u8], ()> =
            // LEARN: BUILDER CHAINING where each method CONSUMES self and returns
            //   Self. Java builders return `this` and mutate; Rust's move-based
            //   builders mean the intermediate value cannot be reused by accident.
            BaseRecord::to(&self.topic).key(key).payload(value).headers(headers);

        match self.producer.send(record) {
            Ok(()) => Stats::inc(&self.stats.kafka_enqueued),
            Err((err, _rejected)) => {
                Stats::inc(&self.stats.kafka_enqueue_failed);
                let n = self.enqueue_fail_log.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 || n % 1000 == 0 {
                    tracing::warn!(failures = n, error = %err, "kafka enqueue rejected (queue full?); dropping");
                }
            }
        }
    }

    /// Headers duplicate the envelope's metadata on purpose: a consumer can route
    /// or filter on them without deserializing the body, and they still work when
    /// value_format = "raw".
    fn headers(&self, meta: &Meta<'_>) -> OwnedHeaders {
        OwnedHeaders::new()
            // LEARN: `Some(&meta.conn_id.to_string())` is an Option<&String> where
            //   the String is a TEMPORARY. It lives until the end of the enclosing
            //   statement, which is long enough for insert() to copy the bytes into
            //   the headers. The compiler VERIFIES this -- if insert tried to
            //   RETAIN the reference, this would not compile.
            // LEARN: this does allocate three Strings per message (conn_id, seq,
            //   ts_ms), a minor cost the design accepts. It runs on the tee
            //   worker, never on the forwarding path.
            .insert(Header { key: "conn_id", value: Some(&meta.conn_id.to_string()) })
            .insert(Header { key: "direction", value: Some(meta.direction) })
            .insert(Header { key: "seq", value: Some(&meta.seq.to_string()) })
            .insert(Header { key: "peer", value: Some(meta.peer) })
            .insert(Header { key: "ts_ms", value: Some(&meta.ts_ms.to_string()) })
    }

    // LEARN: `if let Err(e) = ...` is the Err-side counterpart to
    //   `if let Some(x)`. Reads as "if this returned an error, bind it and log".
    //   The success case is implicitly ignored. Concise and complete.
    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            tracing::warn!(error = %e, "kafka flush incomplete on shutdown");
        }
    }
}

fn should_drop_frame(filter: &KafkaFilter, frame: &[u8]) -> bool {
    if filter.any_of.is_empty() {
        return false;
    }
    let Some((mti, de70)) = extract_mti_and_de70(frame) else {
        return false;
    };

    filter
        .any_of
        .iter()
        .any(|any_of| any_of.all_of.iter().all(|all_of| matches_rule(all_of, mti, de70)))
}

fn matches_rule(rule: &KafkaFilterAllOf, mti: &str, de70: &str) -> bool {
    (rule.mti.is_empty() || rule.mti.iter().any(|v| v == mti))
        && (rule.de70.is_empty() || rule.de70.iter().any(|v| v == de70))
}

fn extract_mti_and_de70(frame: &[u8]) -> Option<(&str, &str)> {
    let (bits, mut pos) = bitmap_fields(frame)?;
    let mti = ascii_digits(frame, 0, 4)?;

    let mut de70 = None;
    for field in 2u8..=70u8 {
        if !bits[field as usize] {
            continue;
        }
        match field {
            70 => {
                de70 = Some(ascii_digits(frame, pos, 3)?);
                break;
            }
            _ => pos = skip_iso_field(field, frame, pos)?,
        }
    }

    de70.map(|de70| (mti, de70))
}

fn bitmap_fields(frame: &[u8]) -> Option<([bool; 129], usize)> {
    if frame.len() < 20 {
        return None;
    }

    let mut bits = [false; 129];
    let primary = parse_hex_u64(&frame[4..20])?;
    for i in 0..64 {
        if primary & (1u64 << (63 - i)) != 0 {
            bits[i + 1] = true;
        }
    }

    let mut pos = 20;
    if bits[1] {
        if frame.len() < 36 {
            return None;
        }
        let secondary = parse_hex_u64(&frame[20..36])?;
        for i in 0..64 {
            if secondary & (1u64 << (63 - i)) != 0 {
                bits[i + 65] = true;
            }
        }
        pos = 36;
    }

    Some((bits, pos))
}

fn parse_hex_u64(bytes: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(bytes).ok()?;
    u64::from_str_radix(s, 16).ok()
}

fn ascii_digits(frame: &[u8], pos: usize, len: usize) -> Option<&str> {
    let end = pos.checked_add(len)?;
    let bytes = frame.get(pos..end)?;
    let s = std::str::from_utf8(bytes).ok()?;
    if s.chars().all(|c| c.is_ascii_digit()) {
        Some(s)
    } else {
        None
    }
}

fn parse_len(frame: &[u8], pos: usize, digits: usize, max: usize) -> Option<(usize, usize)> {
    let s = ascii_digits(frame, pos, digits)?;
    let n = s.parse::<usize>().ok()?;
    if n > max {
        return None;
    }
    Some((pos + digits, n))
}

fn skip_iso_field(field: u8, frame: &[u8], pos: usize) -> Option<usize> {
    match field {
        2 => skip_ll_field(frame, pos, 2, 19),
        3 => skip_fixed(frame, pos, 6),
        4 | 6 => skip_fixed(frame, pos, 12),
        7 => skip_fixed(frame, pos, 10),
        11 => skip_fixed(frame, pos, 6),
        12 => skip_fixed(frame, pos, 6),
        13 | 15 => skip_fixed(frame, pos, 4),
        18 => skip_fixed(frame, pos, 4),
        22 => skip_fixed(frame, pos, 3),
        28 => skip_fixed(frame, pos, 9),
        32 => skip_ll_field(frame, pos, 2, 11),
        35 => skip_ll_field(frame, pos, 2, 37),
        37 => skip_fixed(frame, pos, 12),
        38 => skip_fixed(frame, pos, 6),
        39 => skip_fixed(frame, pos, 2),
        41 => skip_fixed(frame, pos, 16),
        42 => skip_fixed(frame, pos, 15),
        43 => skip_fixed(frame, pos, 41),
        48 => skip_lll_field(frame, pos, 999),
        49 | 50 | 51 => skip_fixed(frame, pos, 3),
        61 => skip_lll_field(frame, pos, 999),
        63 => skip_lll_field(frame, pos, 8),
        66 => skip_fixed(frame, pos, 3),
        _ => None,
    }
}

fn skip_fixed(frame: &[u8], pos: usize, len: usize) -> Option<usize> {
    pos.checked_add(len).filter(|end| *end <= frame.len())
}

fn skip_ll_field(frame: &[u8], pos: usize, digits: usize, max: usize) -> Option<usize> {
    let (start, len) = parse_len(frame, pos, digits, max)?;
    start.checked_add(len).filter(|end| *end <= frame.len())
}

fn skip_lll_field(frame: &[u8], pos: usize, max: usize) -> Option<usize> {
    skip_ll_field(frame, pos, 3, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaFilter, KafkaFilterAllOf, KafkaFilterAnyOf};

    fn filter() -> KafkaFilter {
        KafkaFilter {
            any_of: vec![KafkaFilterAnyOf {
                all_of: vec![KafkaFilterAllOf {
                    mti: vec!["0800".into(), "0810".into()],
                    de70: vec!["301".into()],
                }],
            }],
        }
    }

    #[test]
    fn echo_frames_matching_filter_are_dropped() {
        let req = b"0800822000000000000040000000000000000910075643000048301";
        let res = b"081082200000020000000400000000000000091007564300004800301";
        assert!(should_drop_frame(&filter(), req));
        assert!(should_drop_frame(&filter(), res));
    }

    #[test]
    fn non_matching_or_malformed_frames_are_kept() {
        let mismatch = b"0800822000000000000040000000000000000910075643000048302";
        assert!(!should_drop_frame(&filter(), mismatch));
        assert!(!should_drop_frame(&filter(), b"0810"));
    }

    #[test]
    fn hex_encodes_lowercase_two_chars_per_byte() {
        assert_eq!(PayloadEncoding::Hex.encode(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }

    #[test]
    fn base64_roundtrips() {
        let raw = b"0200|STAN=00000001";
        let enc = PayloadEncoding::Base64.encode(raw);
        let dec = base64::engine::general_purpose::STANDARD.decode(&enc).unwrap();
        assert_eq!(dec, raw);
    }

    #[test]
    fn base64_handles_non_utf8_binary() {
        // A real ISO 8583 bitmap is not valid UTF-8; this must not panic or lose
        // bytes the way the utf8 encoding would.
        let raw = &[0xff, 0xfe, 0x00, 0x80, 0x7f];
        let enc = PayloadEncoding::Base64.encode(raw);
        let dec = base64::engine::general_purpose::STANDARD.decode(&enc).unwrap();
        assert_eq!(dec, raw);
    }

    #[test]
    fn envelope_serialises_with_expected_keys() {
        let payload = PayloadEncoding::Base64.encode(b"hi");
        let env = Envelope {
            conn_id: 7,
            direction: "vp_to_fms",
            seq: 3,
            peer: "127.0.0.1:5000",
            ts_ms: 1700000000000,
            length: 2,
            conn_age_ms: None,
            gap_ms: None,
            rtt_ms: None,
            fields: None,
            parse_error: None,
            encoding: "base64",
            payload: &payload,
        };
        let json = serde_json::to_string(&env).unwrap();
        // LEARN: `r#"..."#` is a RAW STRING LITERAL -- no escape processing, so
        //   the embedded quotes are literal. The `#` count can grow (r##"..."##)
        //   if the content itself contains "#.
        // JAVA: Java 15+ text blocks (""" ... """) are the closest thing.
        // LEARN: this asserts EXACT FIELD ORDER, which is why the Envelope doc
        //   comment treats declaration order as a wire-format decision.
        assert_eq!(
            json,
            r#"{"conn_id":7,"direction":"vp_to_fms","seq":3,"peer":"127.0.0.1:5000","ts_ms":1700000000000,"length":2,"encoding":"base64","payload":"aGk="}"#
        );
    }

    #[test]
    fn envelope_includes_parsed_fields() {
        let raw = b"accountName=Zacky,accountNumber=11020134353,bankCode=1234";
        let payload = PayloadEncoding::Utf8.encode(raw);
        let parser = KvParser::new(&crate::config::Parse::default()).unwrap();
        let map = parser.parse(raw).unwrap();
        let env = Envelope {
            conn_id: 1,
            direction: "vp_to_fms",
            seq: 1,
            peer: "127.0.0.1:5000",
            ts_ms: 1700000000000,
            length: raw.len(),
            conn_age_ms: None,
            gap_ms: None,
            rtt_ms: None,
            // LEARN: `Some(&map)` -- the Envelope BORROWS the map, which is why
            //   `map` must stay alive on the line above. Lifetimes in practice.
            fields: Some(&map),
            parse_error: None,
            encoding: "utf8",
            payload: &payload,
        };
        // LEARN: serde_json::Value is an untyped JSON tree, for when you want to
        //   poke at fields dynamically.
        // JAVA: Jackson's JsonNode.
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&env).unwrap()).unwrap();
        assert_eq!(v["fields"]["accountName"], "Zacky");
        assert_eq!(v["fields"]["accountNumber"], "11020134353");
        assert_eq!(v["fields"]["bankCode"], "1234");
        assert!(v.get("parse_error").is_none(), "parse_error must be omitted on success");
    }

    #[test]
    fn unparseable_frame_still_carries_its_payload() {
        let raw = b"\xff\xfe not parseable";
        let payload = PayloadEncoding::Base64.encode(raw);
        let env = Envelope {
            conn_id: 1,
            direction: "vp_to_fms",
            seq: 1,
            peer: "p",
            ts_ms: 0,
            length: raw.len(),
            conn_age_ms: None,
            gap_ms: None,
            rtt_ms: None,
            fields: None,
            parse_error: Some("not valid utf-8"),
            encoding: "base64",
            payload: &payload,
        };
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&env).unwrap()).unwrap();
        assert!(v.get("fields").is_none());
        assert_eq!(v["parse_error"], "not valid utf-8");
        // The point: the bytes survive even when parsing does not.
        let back = base64::engine::general_purpose::STANDARD
            .decode(v["payload"].as_str().unwrap())
            .unwrap();
        assert_eq!(back, raw);
    }

    #[test]
    fn json_escaping_survives_quotes_and_backslashes() {
        // serde_json owns escaping; this guards against anyone "optimising" the
        // envelope into hand-rolled string concatenation later.
        //
        // LEARN: contrast admin.rs, which DOES hand-roll JSON -- safe there only
        //   because every key is a fixed identifier and every value is a u64.
        //   Here the data is attacker-influenced, so this test exists to stop a
        //   future "optimisation".
        let payload = PayloadEncoding::Utf8.encode(br#"a"b\c"#);
        let env = Envelope {
            conn_id: 1,
            direction: "vp_to_fms",
            seq: 1,
            peer: "p",
            ts_ms: 0,
            length: 5,
            conn_age_ms: None,
            gap_ms: None,
            rtt_ms: None,
            fields: None,
            parse_error: None,
            encoding: "utf8",
            payload: &payload,
        };
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains(r#""payload":"a\"b\\c""#), "got {json}");
        let back: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back["payload"], r#"a"b\c"#);
    }
}
