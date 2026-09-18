// ============================================================================
// main.rs -- process entry point, accept loop, graceful shutdown.
//
// ANNOTATED FOR LEARNING. Two comment tags are used throughout this crate:
//   // LEARN:  what a Rust construct means / how it works
//   // JAVA:   the Java equivalent, and the trade-off between them
// Every other comment is original design rationale -- read those first, they
// explain WHY the system is shaped this way. Full prose version of the LEARN
// notes is in ../RUST_EXPLAINED.md. To read the code without the tutorial:
//   grep -v "LEARN:\|JAVA:" src/main.rs
// ============================================================================

// LEARN: `mod X;` is a MODULE DECLARATION, not an import. It tells the compiler
//   "there is a module named X, go find X.rs and compile it into this crate".
//   A .rs file that no `mod` statement points at is NOT COMPILED AT ALL.
// JAVA: Java infers package structure from directories -- any .java file in the
//   folder is in the build automatically. Rust makes the module tree explicit
//   and greppable, so dead files cannot silently drift into the build. The cost
//   is that you must remember to add the line.
// LEARN: These 8 lines also DEFINE the module tree, which is why other files
//   write `use crate::config::Config` -- `crate` is the root of this tree.
mod admin;
mod config;
mod framing;
mod kafka;
mod parse;
mod payload_log;
mod proxy;
mod stats;
mod tee;

// LEARN: `use` is purely a naming convenience -- zero runtime effect.
// JAVA: `import`. But unlike Java there is no classloading implication at all;
//   `use` cannot fail at runtime because there is no runtime linking.
// LEARN: Convention (not enforced, but universal): std first, then external
//   crates, then local modules, separated by blank lines.
use std::sync::atomic::Ordering;

// LEARN: Arc<T> = Atomically Reference Counted pointer. A heap allocation
//   holding [strong count | weak count | your T]. `Arc::clone(&x)` bumps the
//   strong count (one atomic increment) and hands you another owning handle.
//   When the LAST handle drops, the count hits zero and T is freed right there.
// JAVA: This is the closest thing Rust has to an ordinary object reference.
//   Java uses TRACING GC (a collector periodically proves what is unreachable);
//   Arc uses REFERENCE COUNTING (maintained eagerly on every clone/drop).
//   PRO: deterministic free at a known instruction, no GC threads, no heap
//        headroom, no pauses, memory returns to the allocator immediately.
//   CON: every clone/drop is an atomic RMW (cheap, not free), and REFERENCE
//        CYCLES LEAK -- Rust does not collect them, which is why Weak<T> exists.
//        Java's tracing GC handles cycles for free.
//   There are no cycles in this codebase, so refcounting is a pure win here.
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use config::Config;
use stats::Stats;
use tee::Tee;

// LEARN: `const` is INLINED at every use site (there is no single storage
//   location for it) and is evaluated AT COMPILE TIME -- `Duration::from_secs`
//   runs inside the compiler, not at startup.
// JAVA: `static final Duration` is computed in the static initialiser at class
//   load time, and has one storage location.
// LEARN: SCREAMING_SNAKE_CASE is enforced by a compiler LINT, not just style.
//   Same for snake_case functions and PascalCase types -- rustc warns you.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const KAFKA_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

// LEARN: `#[tokio::main]` is a PROCEDURAL MACRO -- code that runs at compile
//   time and rewrites your source. It transforms this async fn into:
//     fn main() -> anyhow::Result<()> {
//         tokio::runtime::Builder::new_multi_thread()
//             .enable_all().build().unwrap()
//             .block_on(async { /* body below */ })
//     }
//   You can see the expansion with `cargo expand`.
// JAVA: Annotation processors (Lombok) can do this, but they are a bolt-on.
//   Rust macros are a first-class language feature.
//
// LEARN: `async fn` returns a Future; it DOES NOT execute immediately. The
//   compiler rewrites the body into a state-machine struct holding exactly the
//   locals that are alive across each `.await` point, plus an integer state.
// JAVA: Closest is a CompletableFuture-returning method, but the mechanics
//   differ fundamentally: Rust futures are POLL-BASED AND INERT -- a Rust future
//   does nothing until something polls it. A CompletableFuture is ALREADY
//   RUNNING when you receive it. The consequence shows up in `select!` below:
//   a Rust future that loses a race is simply never polled again, so cancelling
//   it is free.
//
// LEARN: `anyhow::Result<()>` is shorthand for `Result<(), anyhow::Error>`.
//   `()` is the UNIT TYPE -- the type with exactly one value, written `()`.
// JAVA: `()` is `void`, except it is a real type you can put in generics.
//   `Result<(), E>` means "succeeds with no value, or fails". Java cannot say
//   this; `Void` is a hack that can only ever hold null.
// LEARN: Returning Result from main is special-cased: on Err, Rust prints the
//   error to stderr and exits with code 1.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // LEARN: Builder chaining works exactly as in Java.
    // JAVA: This is programmatic Logback configuration. `tracing` is SLF4J (the
    //   facade); `tracing_subscriber` is Logback (the backend).
    tracing_subscriber::fmt()
        .with_env_filter(
            // LEARN: reads the RUST_LOG env var; returns Result because the var
            //   may be missing or malformed.
            tracing_subscriber::EnvFilter::try_from_default_env()
                // LEARN: `unwrap_or_else` is a Result COMBINATOR: "give me the
                //   Ok value, or call this closure to produce a fallback".
                // JAVA: Optional.orElseGet(...)
                // LEARN: `|_| expr` is a CLOSURE. Pipes instead of Java's arrow;
                //   `_` binds and discards the error argument.
                // LEARN: `.into()` converts &'static str -> EnvFilter via the
                //   Into trait. THE TARGET TYPE IS INFERRED FROM CONTEXT -- the
                //   compiler knows unwrap_or_else must return an EnvFilter, so
                //   it selects that impl. Java has no return-type-directed
                //   inference like this.
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // LEARN: `let` declares an IMMUTABLE binding. Immutable-by-default is a deep
    //   design choice; the Java equivalent would be every local being `final`
    //   unless you wrote otherwise. You opt into mutation with `let mut`.
    // LEARN: `std::env::args()` returns a lazy ITERATOR over the args.
    // JAVA: like a Stream. `.nth(1)` is the second item (0 is the program name)
    //   and returns Option<String> because there might not be one --
    //   `args[1]` in Java would throw ArrayIndexOutOfBoundsException. Rust turns
    //   absence into a value you must handle.
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".into());

    // LEARN: `::` is the path separator for associated functions and types;
    //   `.` is for methods on a value. So `Config::load` is a "static method".
    // LEARN: `&path` passes a SHARED BORROW of the String -- the callee can read
    //   it but does not take ownership, so `path` is still usable afterwards.
    // LEARN: `?` is the PROPAGATION OPERATOR: if this is Err, return that Err
    //   from main immediately; otherwise unwrap the Ok and carry on. It is an
    //   early return, NOT a throw -- there is no stack unwinding.
    // JAVA: `Config.load(path)` either throws or does not, and you cannot tell
    //   by looking at the call site.
    //   PRO of `?`: every fallible call is visibly marked; no invisible control
    //     flow; no stack-trace capture cost (building a Java exception walks the
    //     stack, which matters on a payments hot path).
    //   CON of `?`: verbose -- error handling is in your face on every line.
    // LEARN: `Arc::new(...)` moves the Config onto the heap inside a refcounted
    //   box, because it is about to be shared with every connection task. Rust
    //   makes you SAY how a value is shared; Java shares everything by default
    //   and you never state intent.
    let cfg = Arc::new(Config::load(&path)?);
    // LEARN: `Default` is a trait meaning "sensible zero value". stats.rs uses
    //   #[derive(Default)], which fills every AtomicU64 with 0.
    let stats = Arc::new(Stats::default());

    // Kafka first, but note this cannot fail the startup: a broker that is down,
    // unreachable, or rejecting the handshake yields a producer that connects
    // lazily in the background, and a bad config yields None (proxy-only mode).
    //
    // LEARN: `&cfg.kafka` borrows a FIELD through the Arc. Arc<T> implements
    //   Deref<Target=T>, so `cfg.kafka` auto-dereferences. This is DEREF
    //   COERCION and it is why Arc feels like a plain reference. (There is a
    //   documented case where this mechanism does NOT kick in -- see kafka.rs,
    //   the `.as_str()` comment.)
    // LEARN: The return type is Option<Arc<Publisher>>, which occupies exactly
    //   8 BYTES -- the same as Arc<Publisher>. The compiler knows a valid Arc
    //   pointer can never be 0, so it uses 0 as the `None` tag. This is called
    //   NICHE OPTIMIZATION.
    // JAVA: Optional<T> is a whole extra heap object wrapping a reference.
    // LEARN: The Option here ENCODES THE ARCHITECTURAL RULE in the type system:
    //   downstream code is FORCED BY THE COMPILER to handle the no-Kafka case.
    let publisher = kafka::Publisher::build(
        &cfg.kafka,
        &cfg.parse,
        &cfg.debug_payload,
        Arc::clone(&stats),
    );
    // LEARN: `.clone()` on Option<Arc<_>> clones the Option, which clones the
    //   inner Arc if present -- one refcount bump, or nothing at all. We clone
    //   rather than move because `publisher` is needed again at shutdown below.
    let tee = Tee::spawn(&cfg, publisher.clone(), Arc::clone(&stats));

    // LEARN: `tokio::spawn` takes a future and schedules it on the runtime's
    //   thread pool, returning a JoinHandle (itself a Future) which we ignore.
    // JAVA: executor.submit(...) -- but note the crucial difference:
    //   `admin::serve(...)` DOES NOT RUN at this point. It only CONSTRUCTS the
    //   future. tokio::spawn is what starts driving it. In Java the method body
    //   would have executed on the calling thread. Internalise this: CALLING AN
    //   ASYNC FN DOES NOTHING.
    // LEARN: `.clone()` on the String is a genuine deep copy -- a real
    //   allocation, done once at startup, completely irrelevant here. Rust makes
    //   you WRITE `.clone()` where Java would silently share a reference; the
    //   discipline is that copies are visible in the source.
    tokio::spawn(admin::serve(cfg.admin.addr.clone(), Arc::clone(&stats)));

    // LEARN: `.await` suspends this task until the future completes.
    //   Mechanically: the state machine returns Pending to the scheduler, which
    //   runs other tasks; when the OS reports the socket ready, the task is
    //   polled again and resumes exactly here. THE THREAD IS NOT BLOCKED.
    // JAVA: Java 21 virtual threads achieve the same effect with the same
    //   programming model and NO `.await` keyword -- the JVM unmounts the
    //   carrier thread for you.
    //   PRO Java: no "function colouring"; any method is callable from anywhere.
    //   PRO Rust: the task is a compiler-generated struct sized to its live
    //     locals (~64 bytes), not a growable stack (~200-800 bytes + JVM
    //     bookkeeping). That density is most of the RSS difference.
    // LEARN: `.map_err(|e| ...)` transforms the error and leaves Ok alone.
    // JAVA: catch (IOException e) { throw new RuntimeException("binding "+addr, e); }
    //   but without the throw.
    // LEARN: `anyhow::anyhow!(...)` -- the `!` is how you spot a MACRO call.
    // LEARN: Format strings: `{}` takes the next positional arg; `{e}` is INLINE
    //   CAPTURE of the variable named e. They are CHECKED AT COMPILE TIME, so a
    //   mismatched placeholder is a compile error -- unlike String.format,
    //   which blows up at runtime.
    let listener = TcpListener::bind(&cfg.listen.addr)
        .await
        .map_err(|e| anyhow::anyhow!("binding listen address {}: {e}", cfg.listen.addr))?;
    // LEARN: STRUCTURED LOGGING -- these are key/value fields, not string
    //   concatenation. The `%` sigil means "record this using its Display impl"
    //   (i.e. toString()). There is also `?` for Debug formatting (see kafka.rs).
    //   A bare `name` is shorthand for `name = name`. The message comes LAST.
    // JAVA: SLF4J with MDC, or logstash-encoder's structured arguments.
    tracing::info!(
        listen = %cfg.listen.addr,
        upstream = %cfg.upstream.addr,
        max_connections = cfg.listen.max_connections,
        "interceptor ready"
    );

    // LEARN: Same concept as java.util.concurrent.Semaphore, but ASYNC-AWARE:
    //   acquire() yields the task instead of blocking the OS thread.
    let permits = Arc::new(Semaphore::new(cfg.listen.max_connections));
    // LEARN: `let mut` -- the first MUTABLE binding in this file. Without `mut`,
    //   the `conn_id = ...` assignment below would not compile.
    // LEARN: `u64` is UNSIGNED 64-bit. Java has no unsigned types at all; you
    //   fake them with Long.parseUnsignedLong and friends. Rust has u8/u16/u32/
    //   u64/u128/usize and i8..i128/isize.
    // LEARN: `usize` (used for max_connections) is POINTER-SIZED and is the type
    //   for indices and lengths. It is NOT interchangeable with u64 without a
    //   cast -- Rust has NO IMPLICIT NUMERIC WIDENING AT ALL, which is why you
    //   see `as usize` / `as u32` below. Java silently promotes int to long;
    //   Rust refuses. Verbose, but no accidental precision loss.
    let mut conn_id: u64 = 0;

    // LEARN: `loop` is `while (true)`.
    loop {
        // LEARN: `tokio::select!` polls several futures concurrently ON THIS
        //   TASK and runs the branch of whichever completes first. THE LOSING
        //   FUTURES ARE THEN DROPPED -- i.e. cancelled. Because Rust futures are
        //   inert state machines, cancellation is just "stop polling and free
        //   the struct": no interrupt flag, no InterruptedException, no
        //   cooperative-cancellation protocol.
        // JAVA: closest is StructuredTaskScope.ShutdownOnSuccess (Java 21), but
        //   heavier -- real threads get interrupted, and interruption in Java is
        //   ADVISORY: the target has to cooperate.
        // LEARN (the sharp edge): CANCELLATION SAFETY. If a future is cancelled
        //   mid-.await, any work it had partially done is lost. For accept()
        //   that is fine -- you either got a connection or you did not. For a
        //   partially-consumed read it can lose data. This is exactly why the
        //   tee uses try_send rather than send().await: no await point means
        //   nothing to cancel.
        tokio::select! {
            // LEARN: select branch syntax is `binding = future => { block }`.
            accepted = listener.accept() => {
                // LEARN: `let (sock, addr) = ...` DESTRUCTURES A TUPLE.
                //   accept() yields io::Result<(TcpStream, SocketAddr)>.
                // JAVA: Java 21 record patterns are the nearest thing, but Rust
                //   tuples are anonymous and free -- no class declaration, no
                //   allocation, returned in registers.
                // LEARN: `match` is PATTERN MATCHING and is EXHAUSTIVE BY
                //   COMPILER ENFORCEMENT -- you must cover Ok and Err or it will
                //   not build. It is also an EXPRESSION, so it evaluates to a
                //   value that is assigned to the pattern on the left.
                // JAVA: switch expressions (Java 14+) are the analogue.
                let (sock, addr) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        // EMFILE and friends: do not spin the CPU retrying.
                        //
                        // LEARN: without the sleep, a bare `continue` here would
                        //   spin a core at 100%, because on EMFILE (out of file
                        //   descriptors) accept fails IMMEDIATELY and forever.
                        //   20 ms of backoff turns a CPU meltdown into a log line.
                        // LEARN: the Err arm ends in `continue`, which never
                        //   produces a value. Rust types that as `!` ("never"),
                        //   which coerces to any type, so both arms typecheck.
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };

                // LEARN: `try_acquire_owned()` is a NON-BLOCKING acquire --
                //   returns Err immediately if no permit is free.
                // JAVA: Semaphore.tryAcquire().
                // LEARN: we Arc::clone first because `_owned` means the returned
                //   OwnedSemaphorePermit carries its OWN Arc to the semaphore.
                //   That is what lets it be moved into the spawned task, which
                //   may outlive this loop iteration -- and the compiler enforces
                //   that lifetime relationship.
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    // LEARN: `permit` is an RAII GUARD. There is no release()
                    //   call anywhere in this file: the permit returns to the
                    //   semaphore when the VALUE IS DROPPED.
                    // JAVA: semaphore.acquire(); try { ... } finally { release(); }
                    //   Forget the finally and you leak a permit forever. In Rust
                    //   you CANNOT forget, because release is tied to the value's
                    //   destruction -- INCLUDING WHEN THE TASK PANICS, since a
                    //   panic unwinds and runs destructors. That last property is
                    //   exactly why Cargo.toml rejected `panic = "abort"`.
                    Ok(p) => p,
                    Err(_) => {
                        // Close immediately rather than queue. VP gets a fast,
                        // unambiguous failure instead of a mystery timeout.
                        Stats::inc(&stats.conns_rejected);
                        tracing::warn!(%addr, "connection limit reached; rejecting");
                        // LEARN: `drop` is just a function that TAKES OWNERSHIP
                        //   and does nothing, so the value dies at the end of its
                        //   body. Dropping a TcpStream closes the fd. It would
                        //   happen anyway at the end of this block; writing it
                        //   makes "close it RIGHT NOW, do not queue" unmissable.
                        drop(sock);
                        continue;
                    }
                };

                // LEARN: EXPLICIT OVERFLOW SEMANTICS. Rust integer arithmetic is
                //   CHECKED in debug builds -- `conn_id + 1` at u64::MAX would
                //   PANIC. In release it wraps silently. Since that is a
                //   behaviour difference between profiles, Rust makes you
                //   declare intent per call site:
                //     wrapping_add    -> wrap around (what Java always does)
                //     checked_add     -> Option, None on overflow (see framing.rs)
                //     saturating_add  -> clamp at the max      (see proxy.rs)
                //     overflowing_add -> value + a bool flag
                // JAVA: exactly one behaviour, silently, always. Integer overflow
                //   bugs are famously invisible.
                // LEARN: wrapping is correct here -- connection IDs are labels,
                //   and wrapping after 1.8e19 connections is fine.
                conn_id = conn_id.wrapping_add(1);
                // LEARN: this is a COPY, not a move. u64 implements the `Copy`
                //   trait, meaning it is a plain bit pattern with no ownership
                //   semantics. All primitives are Copy. Non-Copy types (String,
                //   Vec, Bytes) MOVE instead.
                let id = conn_id;
                Stats::inc(&stats.conns_accepted);
                // LEARN: Ordering::Relaxed = the cheapest possible atomic; see
                //   the long treatment in stats.rs.
                stats.conns_active.fetch_add(1, Ordering::Relaxed);

                // LEARN: Arc<str> IS NOT Arc<String>, and the difference is a
                //   deliberate memory optimisation:
                //     Arc<String>: [Arc: strong|weak|String{ptr,len,cap}]
                //                     --> [heap bytes "127.0.0.1:5000"]
                //                  TWO allocations, TWO pointer hops per read.
                //     Arc<str>:    [Arc: strong|weak| "127.0.0.1:5000" ]
                //                  ONE allocation, ONE pointer hop.
                //   `str` is an UNSIZED type -- raw UTF-8 bytes with no capacity
                //   field. Arc<str> is a FAT POINTER (address + length, 16 bytes
                //   on the stack) and the bytes live INLINE in the Arc allocation.
                // LEARN: why it matters here -- `peer` is cloned into EVERY
                //   Event::Data that goes through the tee. With Arc<str> that
                //   clone is one atomic increment. With String it would be a
                //   fresh allocation + memcpy PER MESSAGE.
                // JAVA: no equivalent choice exists. A Java String is always an
                //   object header + a reference to a byte[] (itself a header +
                //   length + data) -- permanently the Arc<String> shape, two
                //   allocations, with no way to flatten it.
                // LEARN: Arc::from(&str) copies the bytes into the Arc once, at
                //   connect time. Pay once per connection, save per message.
                let peer: Arc<str> = Arc::from(addr.to_string().as_str());
                // LEARN: SHADOWING -- this rebinds the name `cfg` to a new value
                //   for the rest of the scope. Idiomatic Rust; Java forbids
                //   shadowing a local outright.
                let cfg = Arc::clone(&cfg);
                // LEARN: Tee derives Clone and holds Arcs internally, so this is
                //   a few refcount bumps plus three bytes. Cheap by construction.
                let tee = tee.clone();
                // LEARN: named stats2 (not shadowed) only because the outer
                //   `stats` is still needed after the loop, for drain().
                let stats2 = Arc::clone(&stats);

                // LEARN: `async move { ... }` is an ASYNC BLOCK -- an inline
                //   future, like a lambda body that can .await.
                // LEARN: `move` forces the closure to capture BY VALUE (take
                //   ownership) rather than by reference. It is MANDATORY here:
                //   the task outlives this loop iteration, so it cannot borrow
                //   anything from it -- and the compiler PROVES that and rejects
                //   the non-move version.
                // JAVA: a lambda captures effectively-final locals by value
                //   automatically and you never think about it. Java also cannot
                //   have a dangling reference, because of the GC. `move` is
                //   where you SEE the borrow checker preventing a use-after-free
                //   at compile time.
                // LEARN: this is the fan-out point -- ONE TOKIO TASK PER
                //   CONNECTION. Same shape as one virtual thread per connection
                //   in Java 21, but the task is a struct sized to the live
                //   locals of proxy::handle, not a growable stack.
                tokio::spawn(async move {
                    // LEARN: the order of these three lines is exact. Serve the
                    //   connection, decrement the active gauge, THEN release the
                    //   permit. If the permit were released first, drain() could
                    //   observe every permit free while conns_active was still
                    //   non-zero.
                    proxy::handle(sock, peer, id, cfg, tee, Arc::clone(&stats2)).await;
                    stats2.conns_active.fetch_sub(1, Ordering::Relaxed);
                    drop(permit);
                });
            }

            // LEARN: `_ = fut => {}` discards the future's output (it is `()`).
            // LEARN: `break` exits the loop, which DROPS the listener -- no new
            //   connections are accepted from that instant, with no flag to set.
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received; no longer accepting");
                break;
            }
        }
    }

    drain(&stats, &permits, cfg.listen.max_connections).await;

    // LEARN: `if let Some(p) = publisher` is PATTERN MATCHING IN AN IF. Read it
    //   as "if publisher matches the pattern Some(p), bind the inner value to p
    //   and run the block". It is the idiomatic single-case match.
    // JAVA: `if (publisher != null) { ... }` or publisher.ifPresent(p -> ...).
    //   The Rust version is CHECKED -- you cannot reach `p` on the None path,
    //   because `p` does not exist there.
    // LEARN: this MOVES `publisher` (consumes the Option). Fine -- last use.
    if let Some(p) = publisher {
        tracing::info!("flushing kafka producer");
        p.flush(KAFKA_FLUSH_TIMEOUT);
    }

    tracing::info!("stopped");
    // LEARN: `Ok(())` constructs a successful Result carrying unit. THERE IS NO
    //   `return` KEYWORD HERE -- the last expression of a block IS its value.
    //   `return` exists but is only for early exit.
    // LEARN: note the MISSING SEMICOLON. A trailing `;` would turn this into a
    //   statement evaluating to `()`, and you would get a type error. That rule
    //   catches every Rust beginner at least once.
    Ok(())
}

// LEARN: `///` is a DOC COMMENT (Javadoc). It is Markdown, and `cargo doc`
//   renders it. Crucially, CODE BLOCKS INSIDE DOC COMMENTS ARE COMPILED AND RUN
//   AS TESTS by `cargo test`. Javadoc snippets rot silently; Rust doc examples
//   cannot.
/// Waits for in-flight connections to finish, bounded by DRAIN_TIMEOUT.
///
/// Acquiring every permit means every connection task has completed.
// LEARN: `&Arc<Stats>` is a BORROW OF an Arc handle -- we are not keeping it, so
//   we do not clone it. The habit: clone only when you need to own.
async fn drain(stats: &Arc<Stats>, permits: &Arc<Semaphore>, total: usize) {
    let active = Stats::get(&stats.conns_active);
    if active == 0 {
        return;
    }
    // LEARN: `active` alone is shorthand for `active = active` -- the field name
    //   is inferred from the variable name.
    tracing::info!(active, "draining in-flight connections");

    // LEARN: the clever bit. ACQUIRING ALL N PERMITS IS EQUIVALENT TO WAITING
    //   FOR ALL TASKS TO FINISH, because each running task holds exactly one.
    //   No task registry, no CountDownLatch, no JoinSet -- the semaphore already
    //   encodes the answer.
    // LEARN: `total as u32` is an EXPLICIT cast from usize. Required; there is
    //   no implicit narrowing in Rust.
    // LEARN: this builds the future but DOES NOT await it -- it is stored in
    //   `all` and handed to timeout() to be driven. This is the inertness of
    //   Rust futures being used deliberately.
    // JAVA: calling an async method starts the work immediately, and you would
    //   need `.orTimeout(...)` on the resulting CompletableFuture.
    let all = permits.acquire_many(total as u32);
    // LEARN: `timeout(d, fut)` wraps a future and returns Result<T, Elapsed>.
    //   On timeout the inner future is DROPPED, i.e. cancelled, for free.
    match tokio::time::timeout(DRAIN_TIMEOUT, all).await {
        // LEARN: both arms evaluate to `()`, so the match typechecks as a
        //   statement.
        Ok(_) => tracing::info!("all connections drained"),
        Err(_) => tracing::warn!(
            remaining = Stats::get(&stats.conns_active),
            "drain timed out; closing anyway"
        ),
    }
}

// LEARN: `#[cfg(unix)]` is CONDITIONAL COMPILATION. On a non-Unix target this
//   function does not exist -- not "compiled then dead-stripped", but NEVER
//   PARSED INTO THE BUILD. That is what lets it call platform-specific APIs
//   that would not even link elsewhere (tokio::signal::unix has no Windows
//   counterpart).
// JAVA: the equivalent is a runtime `if (System.getProperty("os.name")...)`,
//   with BOTH branches always present in the bytecode and both required to
//   compile against available APIs.
#[cfg(unix)]
async fn shutdown_signal() {
    // LEARN: a `use` INSIDE a function body, scoped to that function. Perfectly
    //   legal. Java imports are file-level only.
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot install SIGTERM handler");
            // LEARN: `std::future::pending()` is a future that NEVER COMPLETES.
            //   If we cannot install the SIGTERM handler, this select branch
            //   simply never fires, so the accept loop runs forever and only
            //   ctrl_c can stop it. Degraded but alive -- matching the project's
            //   philosophy everywhere else.
            return std::future::pending().await;
        }
    };
    // LEARN: wait for SIGINT or SIGTERM, whichever lands first. Both arms are
    //   empty blocks because we only care THAT it happened.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    // LEARN: `let _ = ...` EXPLICITLY DISCARDS the Result. Why bother? Because
    //   Result is marked #[must_use] -- ignoring one produces a compiler
    //   warning. `let _ =` is how you say "I have considered this error and I am
    //   choosing to ignore it".
    // JAVA: no such mechanism; ignored return values are completely silent.
    // LEARN: you will see `let _ =` many times in this crate, always where
    //   failure genuinely does not matter (best-effort sends, socket shutdowns).
    let _ = tokio::signal::ctrl_c().await;
}
