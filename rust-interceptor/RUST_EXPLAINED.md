# Rust, explained line by line — for a Java engineer

This document explains **every Rust construct in this codebase**, using your Java
mental model as the anchor. It assumes you are a strong engineer who has never
written Rust, so it explains syntax first and theory second, but it does not
soften the theory.

Read order:

- **Part 0** — the seven ideas that make all Rust syntax readable. Read this first.
- **Parts 1–10** — every source file, line by line.
- **Part 11** — where exactly the memory savings come from *in this code*.
- **Part 12** — Java↔Rust cheat sheet.

---

# Part 0 — The seven ideas

You can read almost all Rust once you hold these seven things. Everything in
Parts 1–10 is an application of one of them.

## 0.1 Ownership: every value has exactly one owner

In Java, an object lives on the heap and any number of variables can point at
it. The object dies when the GC proves nobody points at it anymore. You never
think about this.

```java
byte[] a = new byte[16];
byte[] b = a;          // two references, one array. Both usable.
```

In Rust, every value has exactly **one owner variable**. Assigning it to another
variable *moves* ownership. The original variable becomes unusable — not at
runtime, but at **compile time**; the compiler refuses to build the program.

```rust
let a = vec![0u8; 16];
let b = a;             // ownership MOVED into b
// println!("{:?}", a); // COMPILE ERROR: borrow of moved value: `a`
```

When the owner goes out of scope, the value is freed. Immediately.
Deterministically. No GC, no finalizer, no pause.

```rust
{
    let v = vec![0u8; 16];   // heap allocation happens here
}                            // `v` goes out of scope -> free() happens HERE, exactly here
```

This is the single biggest difference from Java. It means:

- **Memory is freed at a known instruction**, not "eventually".
- **There is no GC**, so no GC threads, no GC heap headroom, no pause.
- **The compiler knows the lifetime of everything**, so it can stack-allocate
  aggressively and inline aggressively.

> **The Java analogy that gets closest:** try-with-resources. `AutoCloseable`
> gives you deterministic cleanup at end of scope. Rust applies that same rule
> to *literally every value*, including memory itself, and the compiler enforces
> that you cannot use the thing after it closes. Rust's name for this is **RAII**
> (Resource Acquisition Is Initialization) — you will see it used deliberately in
> `main.rs` with the semaphore permit.

## 0.2 Borrowing: `&T` and `&mut T`

Moving everything would be unusable, so you can lend a value out. That is a
**borrow**, written `&`.

| Rust | Meaning | Java's closest thing |
|---|---|---|
| `T` | I own it. I am responsible for freeing it. | — (Java has no concept of owning) |
| `&T` | Shared borrow. Read-only. Many at once. | A reference you promise not to mutate |
| `&mut T` | Exclusive borrow. Read-write. **Exactly one at a time.** | A reference under an exclusive lock |

The rule the compiler enforces, called the **borrow checker**:

> At any given moment, for any given value, you may have
> **either** any number of `&T`
> **or** exactly one `&mut T`.
> Never both.

This is a *compile-time* mutual-exclusion proof. Read that again, because it is
the whole reason Rust can be both fast and safe:

**If only one piece of code can hold a `&mut T` at a time, and the compiler
proves it, then a data race on `T` is impossible without any lock at runtime.**

In Java you achieve exclusive access with `synchronized`, a `ReentrantLock`, or
by convention ("this object is confined to one thread, please don't touch it").
The convention route is free but unverified — it is exactly how production
concurrency bugs happen. Rust gives you the free route *with* the verification.

You will see this pay off in `tee.rs`: the `Worker` struct owns two `HashMap`s
and mutates them on every message, with **zero locks and zero concurrent
collections**, because the compiler proved only one task can reach them.

## 0.3 There is no `null`. There is `Option<T>`

```rust
enum Option<T> {
    None,
    Some(T),
}
```

`Option<T>` is either nothing or a `T`. To get at the `T` you *must* handle both
cases — the compiler will not let you skip one. This is `java.util.Optional<T>`,
except:

- It is not optional (pun intended). There is no `null` to fall back to. A
  `String` in Rust is *always* a valid string.
- **It is free.** `Optional<T>` in Java is a heap object wrapping a reference:
  16 bytes of header plus a pointer, plus an allocation, plus a dereference.
  `Option<T>` in Rust is laid out inline, and for pointer-like `T` it costs
  **zero extra bytes** (see §0.7).

You will see `Option<Arc<Publisher>>` in `main.rs` — "there may or may not be a
Kafka publisher" — expressed in the type, checkable by the compiler, costing
nothing.

## 0.4 There are no exceptions. There is `Result<T, E>`

```rust
enum Result<T, E> {
    Ok(T),
    Err(E),
}
```

A fallible function returns `Result`. There is **no stack unwinding, no
`throws`, no catch**. An error is an ordinary value you return.

Java:

```java
Config cfg = Config.load(path);   // may throw; you can't tell from here
```

Rust:

```rust
let cfg = Config::load(&path)?;   // returns Result; the `?` is visible
```

The `?` operator means: *if this is `Err`, return that `Err` from the current
function immediately; otherwise unwrap the `Ok` and carry on.* It is early
return, not a throw.

**Pros vs Java exceptions:**

- Every fallible call site is visibly marked. You can *see* the error paths by
  reading. No invisible control flow.
- No stack unwinding cost, no stack trace capture. Constructing a Java exception
  walks the stack — expensive, and on a payments hot path it matters.
- The compiler forces you to handle or explicitly propagate. Rust has a
  `#[must_use]` lint on `Result`: ignoring one is a warning.

**Cons vs Java exceptions:**

- Verbose. Error handling is in your face on every line.
- No "handle it 12 frames up" convenience without threading the type through
  (which is why this project uses the `anyhow` crate — see Part 1).

## 0.5 `enum` is a sum type, not a constant list

Java's `enum` is a fixed set of singleton objects. Rust's `enum` is a **tagged
union**: each variant can carry different data.

```rust
enum Event {
    Open  { conn_id: u64, at: Instant },
    Data  { conn_id: u64, dir: Direction, peer: Arc<str>, chunk: Bytes },
    Close { conn_id: u64, at: Instant },
}
```

The Java 21 equivalent is a sealed interface with records:

```java
sealed interface Event {
    record Open(long connId, long at) implements Event {}
    record Data(long connId, Direction dir, String peer, byte[] chunk) implements Event {}
    record Close(long connId, long at) implements Event {}
}
```

**Difference that matters:** the Java version is three separate heap objects
behind an interface pointer — allocation, header, pointer chase, GC pressure.
The Rust version is **one flat value**, sized as `max(variant sizes) + tag`,
that can live on the stack or be moved into a queue slot with a `memcpy`. No
allocation, no indirection, no polymorphic dispatch.

You destructure it with `match`, which the compiler checks for exhaustiveness —
add a fourth variant and every `match` that doesn't handle it fails to compile.

## 0.6 Traits are interfaces, resolved at compile time

```rust
trait ClientContext { /* methods with defaults */ }

impl ClientContext for CountingContext {}   // "CountingContext implements ClientContext"
```

Two differences from Java interfaces:

1. **You can implement a trait for a type you didn't define** (subject to the
   orphan rule). Java cannot do this at all — it's why you write adapter classes.
2. **Dispatch is static by default.** `fn f<T: Trait>(x: T)` monomorphises: the
   compiler generates a separate specialised copy of `f` for each concrete `T`,
   with every call inlined. There is no vtable and no virtual call. Java's
   generics erase to `Object` and dispatch through an interface vtable (the JIT
   often devirtualises, but only after profiling, and only if the call site is
   monomorphic in practice).

   If you *want* runtime polymorphism you opt in with `dyn Trait`, which is a fat
   pointer (data pointer + vtable pointer) — that is the Java default, made
   explicit.

`#[derive(...)]` is the compiler generating a trait impl for you at compile time:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction { VpToFms, FmsToVp }
```

That one line generates `clone()`, makes it copyable, generates `equals`,
`hashCode`, and `toString` equivalents. Java's Lombok, except built into the
language and with zero reflection.

## 0.7 Zero-cost abstraction is a literal claim about machine code

This is not marketing. Concretely:

- `Option<Arc<T>>` occupies **8 bytes** — the same as `Arc<T>`. The compiler knows
  a valid `Arc` pointer can never be `0`, so it uses `0` as the `None` tag. This
  is called **niche optimization**. Java's `Optional<T>` costs a whole extra
  object.
- `Direction` occupies **1 byte**. A Java enum reference is 4–8 bytes *pointing
  at* a heap object with a 16-byte header.
- Generic functions inline completely. `Stats::inc(&counter)` compiles to a
  single `lock xadd` instruction — the function call disappears.
- Iterator chains (`.map().filter().collect()`) compile to the same loop you
  would write by hand.

The trade is compile time (Rust builds are slow) and the borrow checker fighting
you until the ownership model clicks.

## 0.8 One more: async/await vs virtual threads

This project uses `tokio`, an async runtime. Mentally:

| | Java 21 virtual threads | Rust + tokio |
|---|---|---|
| Unit of work | `Thread.ofVirtual().start(...)` | `tokio::spawn(async { ... })` |
| Blocking call | Looks blocking, JVM unmounts the carrier thread | Must be `.await`, compiler rewrites the function into a state machine |
| Cost per task | ~200–800 bytes + JVM bookkeeping | ~64 bytes + the size of your future's locals |
| Who schedules | JVM `ForkJoinPool` | tokio work-stealing scheduler over N OS threads |
| Colour | No colouring — any method can be called from a virtual thread | **Coloured** — `async fn` can only be `.await`ed from another `async fn` |

The mechanical difference: an `async fn` in Rust is **compiled into a struct**
that holds exactly the local variables that are alive across each `.await`
point, plus an integer state field. `.await` is a resumption point. There is no
stack to park — the "stack" of the task *is* that struct, and it is exactly as
big as it needs to be. That is why a tokio task is ~64 bytes while a virtual
thread needs a growable stack.

Rust's cost: **function colouring** is genuinely worse ergonomics than Java 21.
You cannot call an async function from a sync one without a runtime. Java's
virtual threads are strictly nicer to use. Rust's win is memory density and no
JVM underneath.

---

Now the code.

---

# Part 1 — `Cargo.toml`

Cargo is `mvn` + `pom.xml` + `mvnw`, in one tool that ships with the language.

```toml
[package]
name = "vp-fms-interceptor"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
```

- `name` / `version` — `artifactId` / `version`.
- `edition = "2021"` — **this has no Java equivalent and is worth understanding.**
  An edition is an opt-in set of breaking language changes (new keywords,
  changed defaults). Crates of different editions link together fine. It lets
  Rust evolve the *language* without a Python-2-to-3 event. Think "source level"
  in `maven-compiler-plugin`, except it can change syntax, not just features.
- `rust-version = "1.75"` — minimum supported compiler. Like `<release>17</release>`.

```toml
[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "net", "io-util", "macros", "sync", "time", "signal"] }
```

- `version = "1"` is **caret semantics by default**: means `>=1.0.0, <2.0.0`.
  Maven's default is "exactly this version" and you need a range syntax to do
  otherwise. Rust's default is the semver-compatible range, with `Cargo.lock`
  pinning the exact resolved version for reproducibility. `Cargo.lock` is
  committed here — correct for a binary.
- **`features` is the big one with no Java equivalent.** A crate ships optional
  chunks of itself that are *compiled out* if you don't ask for them. We asked
  for tokio's multi-threaded runtime, networking, IO utilities, macros,
  synchronisation primitives, timers, and signal handling — and got none of its
  process spawning, filesystem, or other subsystems.

  In Java you get the whole jar and the whole class graph whether you use 2
  classes or 200; the JVM lazily loads classes but the artifact and the
  dependency surface are fixed. In Rust the unused code **does not exist in your
  binary**. This is a real part of why the binary is 3.4 MB and idle RSS is 4 MB.

```toml
bytes = "1"
```

The `Bytes` / `BytesMut` types. Reference-counted byte buffers with cheap slicing.
This crate is the centre of gravity for the memory story — see Part 11.

```toml
rdkafka = { version = "0.36", features = ["cmake-build", "libz"] }
```

Bindings to **librdkafka**, the C Kafka client. `cmake-build` means Cargo will
compile the C library from source during build (hence the long first build, and
hence needing cmake installed). Java's `kafka-clients` is pure Java; here you
are linking a C library into your binary statically. That's why there's no
separate runtime dependency to deploy — the binary is self-contained.

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`serde` is **SER**ialise / **DE**serialise. The `derive` feature enables
`#[derive(Serialize)]` / `#[derive(Deserialize)]`.

> **This is the single sharpest Rust-vs-Java contrast in the whole file.**
> Jackson works by **runtime reflection**: it inspects your class's fields and
> annotations at runtime, builds a serialiser, caches it, and walks it per
> object. Serde works by **compile-time code generation**: `#[derive(Serialize)]`
> expands into a hand-written-quality `serialize()` method *before* your code is
> compiled. At runtime there is no reflection, no field lookup, no cache
> warm-up, no first-call penalty.
>
> **Pros:** faster, no warm-up cliff, no reflection config for native images, no
> `setAccessible` breakage, and the JSON shape is checked at compile time.
> **Cons:** you cannot serialise a type you cannot annotate without writing an
> impl manually, and it inflates compile time and binary size.
>
> This is also directly visible in your benchmark: **Java takes 1185 ms to reach
> healthy, Rust takes 30 ms.** A large chunk of Java's is classloading and
> reflective setup that Rust did at compile time.

```toml
base64 = "0.22"
toml = "0.8"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
```

- `tracing` / `tracing-subscriber` — SLF4J (the facade) and Logback (the backend).
  `tracing` adds *structured* fields and async-aware spans natively.
- `anyhow` — a type-erased error type. `anyhow::Error` is roughly Java's
  `Exception`: "some error, with a message and a cause chain, I don't need the
  caller to switch on the type." Used in `main` and `config` where you only want
  to print and exit. Library-quality code uses precise error enums instead;
  binaries use `anyhow`. Version `"0.1"` on tracing means `>=0.1.0, <0.2.0` —
  for `0.x` versions, caret semantics treat the *minor* as breaking.

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
```

Release build tuning. There is no Java equivalent because the JIT does this at
runtime instead.

- `opt-level = 3` — maximum optimisation.
- `lto = "fat"` — **Link-Time Optimization across every crate in the dependency
  graph**, including tokio and serde. The optimiser sees your code and library
  code as one unit and inlines across the boundary. This is why `Stats::inc`
  really does become one instruction.
- `codegen-units = 1` — compile the crate as a single unit rather than splitting
  it for parallelism. Slower build, better optimisation, because the optimiser
  sees everything at once.

The JIT's advantage is that it optimises against *observed* runtime behaviour
(actual branch frequencies, actual receiver types) and can re-optimise. Its
disadvantage is that it must first interpret, then profile, then compile, then
possibly deoptimise — which is the 1185 ms startup and the "first frame through
Java is meaningless" note in your `BENCHMARK.md`. Rust pays the whole cost at
build time and is at full speed on instruction one.

```toml
# Deliberately NOT panic = "abort". A panic in one connection task must kill that
# connection, not take down the process and every other in-flight authorization.
```

A **panic** is Rust's `RuntimeException` — an unrecoverable bug (index out of
bounds, `unwrap()` on `None`). By default it unwinds the stack like a Java
exception, running destructors, and tokio catches it at the task boundary.

`panic = "abort"` would instead `SIGABRT` the whole process — smaller, faster
binary, since no unwind tables. That would be catastrophic here: one malformed
connection would kill every in-flight payment authorization. The comment records
that the default was chosen on purpose. Good comment.

---

# Part 2 — `main.rs`, line by line

```rust
mod admin;
mod config;
mod framing;
mod kafka;
mod parse;
mod proxy;
mod stats;
mod tee;
```

**Module declarations.** This is not an import. `mod admin;` tells the compiler
*"there is a module named `admin`; go find `admin.rs` and compile it as part of
this crate."*

Java infers the package structure from directory layout — any `.java` file in
the folder is part of the build. **Rust requires you to declare it.** A `.rs`
file that no `mod` statement points at is *not compiled at all*.

Why this is arguably better: the module tree is explicit and greppable, and
dead files can't silently drift into the build. Why it's annoying: you have to
remember to add the line.

These 8 lines also define the crate's module tree, which is why other files say
`use crate::config::Config` — `crate` is the root, like an absolute package path.

```rust
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use config::Config;
use stats::Stats;
use tee::Tee;
```

`use` is `import`. Purely a naming convenience — zero runtime effect, and unlike
Java there's no classloading implication whatsoever.

Note the grouping convention: `std` first, then external crates, then local
modules. Not enforced, but universal in Rust code.

`std::sync::Arc` — **A**tomically **R**eference **C**ounted pointer. This is the
type you will see most in this codebase, so:

> **`Arc<T>` is the closest thing Rust has to an ordinary Java object reference.**
> It is a heap allocation containing a strong count, a weak count, and your `T`.
> `Arc::clone(&x)` bumps the strong count by one (an atomic increment) and gives
> you another handle. When the last handle drops, the count hits zero and `T` is
> freed right there.
>
> **vs Java:** Java uses *tracing* GC — a background collector periodically
> proves which objects are unreachable. Rust's `Arc` uses *reference counting* —
> the count is maintained eagerly on every clone/drop.
>
> **Pros of `Arc`:** deterministic free at a known point, no GC threads, no heap
> headroom, no pauses, memory returns to the allocator immediately.
> **Cons of `Arc`:** every clone and drop is an atomic RMW instruction (cheap,
> but not free); and **reference cycles leak** — Rust does not collect cycles,
> which is why `Weak<T>` exists. Java's tracing GC handles cycles automatically.
>
> In this codebase there are no cycles, so refcounting is a pure win.

```rust
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const KAFKA_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
```

`const` = `static final`, with a difference: a Rust `const` is **inlined at every
use site** (there is no single storage location for it), and it is evaluated at
**compile time**. `Duration::from_secs(30)` runs in the compiler, not at
startup. Java's `static final Duration` would be computed in the static
initialiser at class load.

`SCREAMING_SNAKE_CASE` for constants is enforced by a compiler lint, not just
convention — `rustc` will warn you if you name it otherwise. Same for
`snake_case` functions and `PascalCase` types.

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
```

Three things here.

**`#[tokio::main]`** is a **procedural macro** — code that runs at compile time
and rewrites your source. It transforms this async `main` into:

```rust
fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { /* your body */ })
}
```

Java has annotation processors (Lombok) that can do this, but they are a
bolt-on. Rust macros are a first-class language feature. You can see the
expansion with `cargo expand`.

**`async fn`** — this function returns a `Future`, it does not execute
immediately. The compiler rewrites the body into a state machine struct. The
closest Java analogy is a `CompletableFuture`-returning method, but the
implementation is fundamentally different: Rust futures are **poll-based and
inert**. A Rust future does nothing until something polls it. A
`CompletableFuture` is already running when you receive it. This has a real
consequence you'll see in `select!`: a Rust future that loses a race is simply
never polled again, so it's cancelled *for free*.

**`anyhow::Result<()>`** — shorthand for `Result<(), anyhow::Error>`.

`()` is the **unit type** — the type with exactly one value, written `()`. It is
`void`, except it's a real type you can put in generics. `Result<(), E>` means
"succeeds with no value, or fails". Java can't express this; `Void` is a hack
that can only ever be `null`.

Returning `Result` from `main` is special-cased: if it's `Err`, Rust prints the
error to stderr and exits with code 1.

```rust
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
```

Logging setup. Equivalent to programmatic Logback configuration.

- `tracing_subscriber::fmt()` — builder, returns a `Builder`. Method chaining
  works exactly as in Java.
- `EnvFilter::try_from_default_env()` — reads the `RUST_LOG` env var. Returns
  `Result`, because the var may be missing or malformed.
- `.unwrap_or_else(|_| "info".into())` — **`Result` combinator.** `unwrap_or_else`
  means "give me the `Ok` value, or call this closure to produce a fallback."
  Identical in spirit to `Optional.orElseGet`.
  - `|_| "info".into()` is a **closure** — Java's `_ -> ...`. Pipes instead of
    an arrow. The `_` binds and discards the error argument.
  - `.into()` calls the `Into` trait to convert `&'static str` → `EnvFilter`.
    **The target type is inferred from context** — the compiler knows
    `unwrap_or_else` must return an `EnvFilter`, so it picks that impl. This is
    return-type-directed inference; Java has nothing like it.
- `.init()` installs it as the global subscriber. Runs once, at startup.

```rust
    let path = std::env::args().nth(1).unwrap_or_else(|| "config.toml".into());
```

- `std::env::args()` returns an **iterator** over command-line args. Lazy, like
  a Java `Stream`.
- `.nth(1)` — the second item (0 is the program name). Returns `Option<String>`,
  because there might not be one. Java's `args[1]` would throw
  `ArrayIndexOutOfBoundsException`; Rust makes absence a value.
- `.unwrap_or_else(...)` — the same combinator, now on `Option`. Note `||` with
  no parameter: a zero-argument closure, Java's `() -> ...`.

`let` declares an immutable binding. **Immutable by default** is a deep design
choice — the Java equivalent would be if every local were `final` unless you
wrote otherwise. You opt into mutation with `let mut`.

```rust
    let cfg = Arc::new(Config::load(&path)?);
    let stats = Arc::new(Stats::default());
```

- `Config::load(&path)` — `::` is the path separator for associated
  functions/types; `.` is for methods on a value. `Config::load` is a static
  method. `&path` passes a *shared borrow* of the `String` — the function can
  read it but doesn't take ownership, so `path` is still usable afterwards.
- `?` — the propagation operator. `Config::load` returns
  `anyhow::Result<Config>`. If `Err`, `main` returns that error immediately. If
  `Ok`, we get the `Config`.

  Compare to Java: `Config.load(path)` either throws or doesn't, and you can't
  tell by looking. The `?` makes the error edge **visible in the source text**.
  That is the core trade of the whole error model.

- `Arc::new(...)` — moves the `Config` onto the heap inside a refcounted box.
  Why? Because it's about to be shared with every connection task, and Rust
  requires you to *say* how a value is shared. Java shares everything by default
  and you never state your intent.
- `Stats::default()` — `Default` is a trait meaning "sensible zero value". Recall
  `stats.rs` says `#[derive(Default)]`, which fills every `AtomicU64` with 0.

```rust
    let publisher = kafka::Publisher::build(&cfg.kafka, &cfg.parse, Arc::clone(&stats));
```

- `&cfg.kafka` — a shared borrow of a *field*. Note you can borrow through the
  `Arc` transparently: `Arc<T>` implements `Deref<Target = T>`, so `cfg.kafka`
  auto-dereferences. This is **deref coercion**, and it's why `Arc` feels like a
  plain reference. (There's a subtle failure of this mechanism documented in
  `kafka.rs` — see Part 6.)
- `Arc::clone(&stats)` — bumps the refcount, produces a second owning handle.

  **Style note worth internalising:** this could be written `stats.clone()`, but
  the codebase consistently uses `Arc::clone(&stats)`. The reason is
  readability: `stats.clone()` looks like it might deep-copy the `Stats` struct,
  whereas `Arc::clone` unambiguously says "refcount bump, same object". This is
  the near-universal convention in production Rust.

Returns `Option<Arc<Publisher>>` — remember, this is 8 bytes, same as
`Arc<Publisher>`, thanks to the null-pointer niche.

The comment above it is the key architectural claim: building the producer
cannot fail startup. Broker down → still starts. Bad config → `None` →
proxy-only mode. This encodes the governing rule "Kafka must never affect
VP↔FMS" **in the type system**: downstream code holds an `Option` and is
*forced by the compiler* to handle the no-Kafka case.

```rust
    let tee = Tee::spawn(&cfg, publisher.clone(), Arc::clone(&stats));
```

`publisher.clone()` on an `Option<Arc<_>>` clones the `Option`, which clones the
inner `Arc` if present — one refcount bump, or nothing. Note we `clone()` rather
than move, because `publisher` is needed again at shutdown on line 109.

```rust
    tokio::spawn(admin::serve(cfg.admin.addr.clone(), Arc::clone(&stats)));
```

`tokio::spawn` = `executor.submit(...)`. It takes a future and schedules it on
the runtime's thread pool. Returns a `JoinHandle` (a `Future`), which we ignore
here — fire and forget.

`admin::serve(...)` **does not run** at this point. It constructs the future.
`tokio::spawn` starts driving it. In Java, `admin.serve(...)` would have
executed the body on the calling thread. Get used to this: **calling an async fn
does nothing.**

`.clone()` on the `String` here is a genuine deep copy of the address string —
a real allocation, done once at startup, and completely irrelevant. Rust makes
you write `.clone()` where Java would silently share; the discipline is that
copies are visible.

```rust
    let listener = TcpListener::bind(&cfg.listen.addr).await.map_err(|e| {
        anyhow::anyhow!("binding listen address {}: {e}", cfg.listen.addr)
    })?;
```

- `.await` — suspend this task until the future completes. Mechanically: the
  state machine returns `Pending` to the scheduler, which goes and runs other
  tasks; when the OS says the socket is ready, the task is polled again and
  resumes here.

  **The thread is not blocked.** Java 21 virtual threads achieve the same effect
  with the same programming model but no `.await` keyword. Java is nicer here;
  Rust's `.await` is a syntactic tax paid for not needing a runtime that can
  unmount stacks.
- `.map_err(|e| ...)` — transform the error, leave `Ok` alone. Adds context so a
  bind failure says *which address*. Analogous to
  `catch (IOException e) { throw new RuntimeException("binding " + addr, e); }`
  but without the throw.
- `anyhow::anyhow!(...)` — a macro (the `!` is how you spot a macro call)
  constructing an error from a format string.
- `"binding listen address {}: {e}"` — format syntax. `{}` takes the next
  positional argument; `{e}` is **inline capture** of the variable named `e`
  (Rust 2021, similar to Java 21 string templates). Format strings are checked at
  **compile time** — a mismatched placeholder is a compile error, unlike
  `String.format` blowing up at runtime.

```rust
    tracing::info!(
        listen = %cfg.listen.addr,
        upstream = %cfg.upstream.addr,
        max_connections = cfg.listen.max_connections,
        "interceptor ready"
    );
```

**Structured logging.** These are key–value fields, not string concatenation.
Equivalent to SLF4J with MDC, or logstash-encoder's structured arguments.

The `%` sigil means "record this using its `Display` impl" (i.e. `toString()`).
Without a sigil, the value is recorded with its native type. There's also `?`
for `Debug` formatting — you'll see `value_format = ?value_format` in `kafka.rs`.

The message comes last. That's the `tracing` convention.

```rust
    let permits = Arc::new(Semaphore::new(cfg.listen.max_connections));
    let mut conn_id: u64 = 0;
```

`Semaphore` — same concept as `java.util.concurrent.Semaphore`, but async-aware:
`acquire()` yields the task instead of blocking the thread.

`let mut` — the first mutable binding in the file. Without `mut`, `conn_id += 1`
would not compile.

`u64` — unsigned 64-bit. **Java has no unsigned types.** Java's `long` is signed;
you'd use `Long.parseUnsignedLong` and friends to fake it. Rust has
`u8/u16/u32/u64/u128/usize` and `i8/.../i128/isize`.

`usize` (used for `max_connections`) is pointer-sized — 64-bit here. It's the
type for indices and lengths, and it is *not* interchangeable with `u64` without
a cast, which is why you see `as usize` and `as u32` conversions later. Rust has
**no implicit numeric widening at all**. Java silently promotes `int` to `long`;
Rust refuses and makes you write `as`. Verbose, but it means no accidental
precision loss.

```rust
    loop {
        tokio::select! {
```

`loop` = `while (true)`.

**`tokio::select!`** is the most important construct in this file and has no
clean Java equivalent.

It polls several futures concurrently *on the current task* and runs the branch
of whichever completes first. **The losing futures are then dropped —
cancelled.** Because Rust futures are inert state machines, cancellation is just
"stop polling and free the struct". No interrupt flag, no `InterruptedException`,
no cooperative-cancellation protocol.

The Java 21 analogue is `StructuredTaskScope.ShutdownOnSuccess`, which is close
but heavier: real threads get interrupted, and interruption in Java is
advisory — the target has to cooperate.

> **The cancellation-safety caveat**, since you'll hit it: if a future is
> cancelled mid-`.await`, any work it had partially done is lost. For
> `listener.accept()` that's fine — either you got a connection or you didn't.
> For something like a partially-consumed read, it can lose data. This is why
> the tee uses `try_send` rather than `send().await` — no await point, nothing
> to cancel.

```rust
            accepted = listener.accept() => {
                let (sock, addr) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };
```

- `accepted = listener.accept() => { ... }` — select branch syntax: bind the
  future's output to `accepted`, then run the block.
- `let (sock, addr) = ...` — **destructuring a tuple**. `accept()` yields
  `io::Result<(TcpStream, SocketAddr)>`. Java 21's record patterns are the
  nearest thing, but Rust tuples are anonymous and free — no class needed.
- `match` — pattern matching. Like `switch` in Java 21, but **exhaustive by
  compiler enforcement**: you must cover `Ok` and `Err` or it won't compile.
- **`match` is an expression**, so it evaluates to a value that's assigned to the
  destructuring pattern. Java's switch expressions (`yield`) got here in 14.
- In the `Err` arm we `continue`, which never produces a value — Rust types this
  as `!` ("never"), which coerces to any type, so the arms typecheck.
- `tokio::time::sleep(...).await` — non-blocking sleep. The comment explains why
  it exists: on `EMFILE` (out of file descriptors), `accept` fails *immediately*
  and forever, so a bare `continue` would spin a core at 100%. 20 ms of backoff
  converts a CPU meltdown into a log line. This is exactly the kind of bug that
  is invisible in review and obvious in production.

```rust
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        Stats::inc(&stats.conns_rejected);
                        tracing::warn!(%addr, "connection limit reached; rejecting");
                        drop(sock);
                        continue;
                    }
                };
```

- `try_acquire_owned()` — non-blocking acquire. Returns immediately with `Err`
  if no permit is free, rather than waiting. `Semaphore.tryAcquire()` in Java.
- `Arc::clone(&permits)` first, because `_owned` means the returned
  `OwnedSemaphorePermit` holds its own `Arc` to the semaphore — that's what lets
  it be moved into the spawned task, which may outlive this loop iteration. The
  lifetime relationship is enforced by the compiler.
- **`permit` is an RAII guard.** There is no `release()` call anywhere in this
  file. The permit is returned to the semaphore when the value is dropped. Look
  at line 96 — `drop(permit)` — which is *explicit documentation*, not
  necessity; it would be released at the end of the task block anyway.

  Java equivalent, and note how much harder it is to get right:
  ```java
  semaphore.acquire();
  try { ... } finally { semaphore.release(); }
  ```
  Forget the `finally` and you leak a permit forever. In Rust you cannot forget,
  because release is tied to the value's destruction, and destruction is
  guaranteed — **including when the task panics**, since the panic unwinds and
  runs destructors. That last part is why `panic = "abort"` was rejected in
  `Cargo.toml`.
- `%addr` again — `Display` formatting of the peer address.
- `drop(sock)` — explicit destruction. `drop` is just a function that takes
  ownership and does nothing, so the value dies at end of its body. Dropping a
  `TcpStream` closes the fd. Again, it would happen anyway at the end of the
  block; writing it makes the intent ("close it right now, immediately, don't
  queue") unmissable.

```rust
                conn_id = conn_id.wrapping_add(1);
```

**Explicit overflow semantics.** Rust integer arithmetic is checked in debug
builds — `conn_id + 1` on `u64::MAX` would *panic*. In release it wraps
silently, which is a difference in behaviour between profiles, so Rust makes you
declare intent:

- `wrapping_add` — wrap around. What Java always does.
- `checked_add` — returns `Option`, `None` on overflow. (Used in `framing.rs` as
  `checked_sub`.)
- `saturating_add` — clamp at the maximum. (Used in `proxy.rs` as
  `saturating_mul`.)
- `overflowing_add` — returns the value plus a `bool` flag.

Java gives you exactly one behaviour, silently, and integer overflow bugs are
famously invisible. Rust makes you choose per call site. Here, wrapping is
correct: connection IDs are just labels and wrapping after 1.8×10¹⁹ connections
is fine.

```rust
                let id = conn_id;
                Stats::inc(&stats.conns_accepted);
                stats.conns_active.fetch_add(1, Ordering::Relaxed);
```

`let id = conn_id;` — this is a **copy**, not a move. `u64` implements the `Copy`
trait, meaning it's a plain bit pattern with no ownership semantics. All
primitives are `Copy`. Non-`Copy` types (`String`, `Vec`, `Bytes`) move.

`fetch_add(1, Ordering::Relaxed)` — atomic increment. See §8 for the full
treatment of memory ordering; the short version is that `Relaxed` is the
cheapest possible atomic and Java's `AtomicLong` has no equivalent.

```rust
                let peer: Arc<str> = Arc::from(addr.to_string().as_str());
```

**Look closely at this line — it is a deliberate memory optimisation.**

`Arc<str>` is not `Arc<String>`. The difference:

```
Arc<String>:  [Arc box: strong|weak|String{ptr,len,cap}] ──> [heap bytes "127.0.0.1:5000"]
              TWO allocations, and TWO pointer hops to read a byte.

Arc<str>:     [Arc box: strong|weak|"127.0.0.1:5000"]
              ONE allocation, ONE pointer hop.
```

`str` is an **unsized type** — a raw sequence of UTF-8 bytes with no capacity
field. `Arc<str>` stores the length in the pointer itself (a "fat pointer":
address + length, 16 bytes on the stack), and the bytes live *inline* in the Arc
allocation.

This matters because `peer` is cloned into **every single `Event::Data`**
message that goes through the tee. With `Arc<str>` the clone is one atomic
increment. With `String` it would be a fresh allocation and memcpy per message.
With `Arc<String>` it would be one increment but every read of the string would
chase an extra pointer and the initial allocation is doubled.

Java has no equivalent choice. A `String` is always an object header + a
reference to a `byte[]` (which is itself a header + length + data) — permanently
the `Arc<String>` shape, two allocations, with no option to flatten it.

`Arc::from(&str)` copies the bytes into a new Arc allocation once, at
connect time. Perfect: pay once per connection, save on every message.

```rust
                let cfg = Arc::clone(&cfg);
                let tee = tee.clone();
                let stats2 = Arc::clone(&stats);
```

Preparing owned handles to move into the task. Note `let cfg = Arc::clone(&cfg);`
**shadows** the outer `cfg` with a new binding of the same name. Shadowing is
idiomatic Rust and has no Java equivalent (Java forbids shadowing a local).
`stats2` is named differently only because the outer `stats` is needed again
after the loop.

`tee.clone()` — `Tee` derives `Clone` and holds `Arc`s internally, so this is
several refcount bumps and a couple of `bool` copies. Cheap by construction.

```rust
                tokio::spawn(async move {
                    proxy::handle(sock, peer, id, cfg, tee, Arc::clone(&stats2)).await;
                    stats2.conns_active.fetch_sub(1, Ordering::Relaxed);
                    drop(permit);
                });
```

- `async move { ... }` — an **async block**: an inline future, like a lambda body
  that can `.await`.
- **`move`** forces the closure to capture by *value* (take ownership) rather
  than by reference. It is mandatory here: the task outlives this loop
  iteration, so it cannot borrow anything from it — and the compiler proves that
  and rejects the non-`move` version. In Java a lambda captures effectively-final
  locals by value automatically and you never think about it, but Java also
  can't have a dangling reference because of the GC. **Rust's `move` is where you
  see the borrow checker preventing a use-after-free at compile time.**
- The order of the last three lines is exact: serve the connection, decrement the
  active gauge, *then* release the permit. If the permit were released first,
  `drain()` could observe all permits free while `conns_active` was still
  nonzero.

This is the fan-out point: **one tokio task per connection**. Same shape as one
virtual thread per connection in Java 21, but the task is a compiler-generated
struct sized to the locals of `proxy::handle`, not a growable stack.

```rust
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received; no longer accepting");
                break;
            }
```

The other select branch. `_` discards the value (it's `()`). `break` exits the
`loop`, which drops the listener — no new connections accepted from this
instant.

```rust
    drain(&stats, &permits, cfg.listen.max_connections).await;

    if let Some(p) = publisher {
        tracing::info!("flushing kafka producer");
        p.flush(KAFKA_FLUSH_TIMEOUT);
    }
```

**`if let Some(p) = publisher`** — pattern matching in an `if`. Reads as "if
`publisher` matches the pattern `Some(p)`, bind the inner value to `p` and run
the block." This is the idiomatic single-case `match`.

Java: `if (publisher != null) { ... }` or `publisher.ifPresent(p -> ...)`. The
Rust version is checked — you cannot reach `p` on the `None` path, because `p`
doesn't exist there.

Note this **moves** `publisher` (consumes the `Option`). That's fine, it's the
last use.

```rust
    tracing::info!("stopped");
    Ok(())
}
```

`Ok(())` — construct a successful `Result` carrying unit. **No `return`
keyword**: the last expression of a block is its value. `return` exists but is
only used for early exit. Note the missing semicolon — a trailing semicolon
would turn it into a statement evaluating to `()`, and you'd get a type error.
That semicolon rule catches beginners constantly.

```rust
/// Waits for in-flight connections to finish, bounded by DRAIN_TIMEOUT.
///
/// Acquiring every permit means every connection task has completed.
async fn drain(stats: &Arc<Stats>, permits: &Arc<Semaphore>, total: usize) {
```

`///` is a **doc comment** — Javadoc. It's Markdown, and `cargo doc` renders it.
Crucially, **code blocks inside doc comments are compiled and run as tests** by
`cargo test`. Javadoc snippets rot silently; Rust doc examples cannot.

`&Arc<Stats>` — a borrow *of* an Arc handle. We don't need our own handle, we're
not keeping it, so we don't clone. A tiny thing, but it's the habit: clone only
when you need to own.

```rust
    let active = Stats::get(&stats.conns_active);
    if active == 0 {
        return;
    }
    tracing::info!(active, "draining in-flight connections");
```

`tracing::info!(active, ...)` — shorthand for `active = active`. Field name
inferred from the variable name.

```rust
    let all = permits.acquire_many(total as u32);
    match tokio::time::timeout(DRAIN_TIMEOUT, all).await {
        Ok(_) => tracing::info!("all connections drained"),
        Err(_) => tracing::warn!(
            remaining = Stats::get(&stats.conns_active),
            "drain timed out; closing anyway"
        ),
    }
```

The clever bit, and it's worth pausing on: **acquiring all N permits is
equivalent to waiting for all tasks to finish**, because each running task holds
one. No task registry, no `CountDownLatch`, no join set — the semaphore already
encodes the answer.

- `total as u32` — explicit cast from `usize`. Required; no implicit narrowing.
- `acquire_many(...)` returns a future, not awaited yet — stored in `all`. **This
  is the inertness of Rust futures being used deliberately:** we construct the
  future, then hand it to `timeout` to be driven. In Java, calling an async
  method starts the work immediately and you'd need `orTimeout` on the resulting
  `CompletableFuture`.
- `tokio::time::timeout(d, fut)` — wraps a future, returns
  `Result<T, Elapsed>`. On timeout the inner future is **dropped**, i.e.
  cancelled, for free.
- Both arms return `()`, so the `match` typechecks as a statement.

```rust
#[cfg(unix)]
async fn shutdown_signal() {
```

**Conditional compilation.** `#[cfg(unix)]` means this function only exists when
building for a Unix target. On Windows it is not compiled at all — not compiled
and dead-stripped; *never parsed into the build*.

Java's equivalent is a runtime `if (System.getProperty("os.name")...)` with both
branches always present in the bytecode. Rust's is compile-time, so you can call
platform-specific APIs that wouldn't even link on the other platform. That's
exactly what's happening: `tokio::signal::unix` does not exist on Windows.

```rust
    use tokio::signal::unix::{signal, SignalKind};
```

A `use` **inside a function body**. Perfectly legal, scoped to the function.
Java imports are file-level only.

```rust
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot install SIGTERM handler");
            return std::future::pending().await;
        }
    };
```

`std::future::pending()` — a future that **never completes**. If we can't install
the SIGTERM handler, this branch of the `select!` simply never fires, so the
accept loop runs forever and only `ctrl_c` can stop it. Degraded but alive,
which matches the project's philosophy everywhere else.

`return ....await` in a function returning `()` works because `pending::<()>()`
resolves to `()`... which it never does. The types work out; the code never
reaches the return.

```rust
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
```

Wait for SIGINT or SIGTERM, whichever comes first, then return. Both arms are
empty blocks — we only care *that* it happened.

```rust
#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
```

The Windows version. `let _ = ...` **explicitly discards** the `Result`.

Why bother? Because `Result` is marked `#[must_use]` — ignoring one produces a
compiler warning. `let _ =` is the way to say "I have considered this error and
I am choosing to ignore it." Java has no such mechanism; ignored return values
are silent. You'll see `let _ =` many times in this codebase, always at a point
where failure genuinely doesn't matter (best-effort sends, socket shutdowns).

---

# Part 3 — `proxy.rs`: the hot path

This is the file where the memory lesson lives. Read the comments in it
carefully; they document a real 10x regression that was found and fixed.

```rust
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::time::timeout;
```

`use tokio::io::{AsyncReadExt, AsyncWriteExt};` — these are **extension traits**,
and this is a Rust idiom with no Java equivalent worth understanding.

`AsyncReadExt` adds convenience methods (`read_buf`, `read_exact`, ...) to
*anything* implementing the base `AsyncRead` trait. You must import the trait to
call its methods. If you ever see "method `read_buf` not found", the cause is
almost always a missing trait import, not a missing method.

Java's closest analogue is a static utility class (`Files.readAllBytes(x)`), but
that reads backwards. Kotlin extension functions are the real match.

```rust
/// A read filling less than 1/Nth of the buffer is copied rather than
/// sliced, so it cannot pin the whole allocation in the tee queue.
const SMALL_READ_RATIO: usize = 4;
```

Remember this constant; the payoff is at line 162.

```rust
pub async fn handle(
    client: TcpStream,
    peer: Arc<str>,
    conn_id: u64,
    cfg: Arc<Config>,
    tee: Tee,
    stats: Arc<Stats>,
) {
```

`pub` — visible outside this module. Rust's default visibility is **private to
the module**, which is stricter than Java's package-private default. Other
levels: `pub(crate)` (whole binary, ≈ Java's package-private at build scope),
`pub(super)` (parent module).

**Every parameter here is taken by value — this function takes ownership of all
six.** That is deliberate: the task owns everything it needs, so nothing it
touches can be freed by anyone else, and the compiler proves the task can safely
outlive the accept loop. The `Arc`s make "owning" cheap.

```rust
    let connect = TcpStream::connect(&cfg.upstream.addr);
    let upstream = match timeout(
        Duration::from_millis(cfg.upstream.connect_timeout_ms),
        connect,
    )
    .await
    {
        Ok(Ok(s)) => s,
```

`Ok(Ok(s))` — **nested pattern matching**, one of Rust's genuinely nice
features. `timeout` returns `Result<Result<TcpStream, io::Error>, Elapsed>`: the
outer is "did we time out", the inner is "did the connect succeed".

One `match` destructures both layers and gives you three distinct arms:

```rust
        Ok(Ok(s)) => s,                              // connected
        Ok(Err(e)) => { /* connect refused/failed */ }
        Err(_) => { /* timed out */ }
```

In Java this is a try/catch for `IOException` nested inside a
`try/catch(TimeoutException)` on a `Future.get(timeout)`, and the two failures
end up in structurally different places. Here they're three arms of one
expression, and the compiler guarantees you covered all three.

Each failure arm increments the same counter, logs with different wording, and
`return`s. Note `return` with no value in a function returning `()`.

```rust
    if cfg.proxy.nodelay {
        let _ = client.set_nodelay(true);
        let _ = upstream.set_nodelay(true);
    }
```

Disable Nagle's algorithm — `Socket.setTcpNoDelay(true)`. The comment explains
the stakes: Nagle delays small writes waiting for more data, adding up to 40 ms.
On a payments authorization path that dwarfs everything else in this program.

`let _ =` again: if setting the option fails we do not care enough to abort a
payment.

```rust
    tee.open(conn_id);

    let (client_rd, client_wr) = client.into_split();
    let (up_rd, up_wr) = upstream.into_split();
```

**`into_split()` is pure ownership thinking, and it's a good one to sit with.**

A TCP socket is full-duplex: you can read and write simultaneously. But
`&mut TcpStream` is exclusive — the borrow checker allows only one at a time. So
how do you run reads and writes concurrently?

`into_split()` **consumes** the `TcpStream` (note `into_`, the naming convention
for "takes `self` by value") and returns two separate owned values,
`OwnedReadHalf` and `OwnedWriteHalf`, each of which can be moved into a different
task. The underlying fd is shared via an internal `Arc`; the type system
guarantees only the read half can read and only the write half can write.

The Java equivalent is `socket.getInputStream()` and `socket.getOutputStream()`,
which returns two streams over the same socket and *trusts you* not to do
something incoherent. Rust makes the split explicit and the exclusivity
provable.

Naming conventions worth knowing, since they're consistent across all Rust code:
- `into_x` — consumes `self`, converts. Ownership transferred.
- `to_x` — borrows `self`, produces an owned copy. Allocates.
- `as_x` — borrows `self`, produces a borrowed view. Free.

```rust
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
```

Spawn one task per direction — so a connection is **three tasks**: `handle`, and
one `pump` each way. The doc comment explains why: separate tasks can be
scheduled on separate cores by tokio's work-stealing scheduler, so VP→FMS and
FMS→VP genuinely run in parallel.

Note `pump(...)` is called to *build* the future, then handed to `spawn`. Again:
calling it runs nothing.

Each `Arc::clone` is one atomic increment. Eight arguments, four refcount bumps,
a `bool` pair, and an enum byte. That's the entire cost of setting up a
direction.

```rust
    for (dir, joined) in [
        (Direction::VpToFms.as_str(), to_fms.await),
        (Direction::FmsToVp.as_str(), to_vp.await),
    ] {
```

An **array literal of tuples**, iterated with destructuring. `to_fms.await` awaits
the `JoinHandle` — `Future.get()` in Java, but non-blocking.

Subtle and worth noticing: the array elements are evaluated when the array is
built, so **both `.await`s happen before the loop body runs at all**, in
sequence. `to_fms` is awaited first, then `to_vp`. Since we want both to finish
regardless, that's correct — but it is not obvious from the shape, and it's the
kind of thing that would bite you if the arms had side effects that mattered.

```rust
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::debug!(conn_id, direction = dir, error = %e, "stream ended"),
            Err(e) => tracing::error!(conn_id, direction = dir, error = %e, "pump task panicked"),
        }
    }

    tee.close(conn_id);
}
```

Nested match again, with a specific meaning:

- `Ok(Ok(()))` — task finished, pump returned `Ok(())`. Clean close. Note you
  can pattern-match the unit value `()` itself.
- `Ok(Err(e))` — task finished, pump returned an IO error. Logged at `debug`,
  because a connection reset is completely routine.
- `Err(e)` — **the task panicked**. `JoinHandle`'s error type is `JoinError`.
  Logged at `error`, because a panic is a bug.

This is where the `Cargo.toml` decision cashes out: because panics unwind rather
than abort, a bug in one connection surfaces here as a log line while every
other connection keeps running.

`tee.close(conn_id)` after both directions end, so the tee worker can free its
per-connection state.

## The `pump` loop — read the comments

```rust
/// Copies one direction of the stream.
///
/// The ordering here IS the design: bytes reach the far socket before the tee is
/// offered anything, and the tee call cannot await. Kafka may be down, slow, or
/// absent without adding a microsecond to this loop.
async fn pump(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    ...
) -> io::Result<()> {
```

`mut rd` — **a mutable parameter binding**. Reading from a socket requires
`&mut self`, so the local binding must be `mut`. Java's parameters are
effectively mutable by default and nobody thinks about it; here it's declared.

`io::Result<()>` is an alias for `Result<(), io::Error>`.

```rust
    let cap = cfg.proxy.read_buffer_bytes;
    let idle = match cfg.proxy.idle_timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
```

**`match` on an integer with a binding arm.** `0 => None` matches the literal;
`ms => ...` is a catch-all that *binds* the value to `ms`. Java's switch can't
bind the matched value like this.

Encoding "0 means disabled" as `Option<Duration>` at the top means the hot loop
never re-checks a magic number — it matches on a type. The config's sentinel
convention is converted into a type once and never thought about again. This is
a small but very Rust habit: **push the messy representation to the boundary,
carry a precise type inside.**

```rust
    let byte_counter: &AtomicU64 = match dir {
        Direction::VpToFms => &stats.bytes_vp_to_fms,
        Direction::FmsToVp => &stats.bytes_fms_to_vp,
    };
```

Resolve which counter to use **once**, outside the loop, and hold a borrow of it.

The explicit `: &AtomicU64` annotation is a borrow of a field of a struct behind
an `Arc`, held across every `.await` in the loop. The borrow checker verifies
that `stats` (the `Arc` handle) lives at least as long as this reference — and
it does, because `pump` owns it for the whole function. **In Java you'd hold a
field reference and rely on the GC.** Here the compiler proves the target
outlives the reference, statically, with no runtime cost and no possibility of a
dangling pointer.

```rust
    let mut buf = BytesMut::with_capacity(cap);

    loop {
        buf.reserve(cap);
```

`BytesMut` is a growable byte buffer — Netty's `ByteBuf`, or a
`ByteBuffer`/`ByteArrayOutputStream` hybrid.

`reserve(cap)` ensures at least `cap` bytes of *spare* capacity. Critically, if
the buffer already has room it does **nothing** — no allocation. This is what
makes the buffer reusable across loop iterations.

```rust
        let n = match idle {
            Some(d) => match timeout(d, rd.read_buf(&mut buf)).await {
                Ok(r) => r?,
                Err(_) => {
                    tracing::debug!(conn_id, direction = dir.as_str(), "idle timeout");
                    let _ = wr.shutdown().await;
                    return Ok(());
                }
            },
            None => rd.read_buf(&mut buf).await?,
        };
```

- `rd.read_buf(&mut buf)` — read into the buffer, appending. Takes `&mut buf`,
  an exclusive borrow, for the duration of the call. While that borrow is
  outstanding you cannot touch `buf` at all — the compiler forbids it. A
  concurrent-modification bug is a compile error rather than a runtime surprise.
- `Ok(r) => r?` — the outer `Ok` means "didn't time out"; `r` is the inner
  `io::Result<usize>`; `?` propagates an IO error out of `pump`, where `handle`
  logs it.
- On timeout: `wr.shutdown().await` sends a FIN, then return cleanly. `let _ =`
  because if shutdown fails the socket is already gone.
- `dir.as_str()` — the `&'static str` conversion from `tee.rs`. **Zero
  allocation**: it returns a pointer to a string literal baked into the binary's
  read-only data section. Java's `enum.name()` returns a cached `String` object,
  which is also allocation-free after the first call, but it's an object with a
  header rather than a raw pointer to `.rodata`.

```rust
        if n == 0 {
            let _ = wr.shutdown().await;
            return Ok(());
        }
```

`read` returning 0 means EOF. The comment is the important part: rather than
tearing the whole connection down, we propagate the FIN to the other side so
that direction can finish. If VP half-closes after sending a request, FMS's
response still gets delivered. This is correct TCP proxy behaviour and it is
frequently got wrong.

```rust
        wr.write_all(&buf[..n]).await?;
        Stats::add(byte_counter, n as u64);
```

**This is the line the whole program exists to protect.** Bytes go to the far
socket *first*, before any tee work.

- `&buf[..n]` — a **slice**. `[..n]` is range syntax for "from 0 up to `n`". A
  slice is a borrowed view: a pointer and a length, 16 bytes on the stack, **no
  copy, no allocation**.

  Java's nearest equivalent is `ByteBuffer.slice()` (an object, allocated) or
  `Arrays.copyOfRange` (a full copy). `String.substring` was a zero-copy view
  until Java 7u6, when it was changed to copy — precisely because a small
  substring could pin a huge backing array. **That is exactly the bug documented
  30 lines below.** Java solved it by always copying; Rust lets you choose, and
  this code chooses per-read based on size.
- `write_all` — loops until all bytes are written. Java's `OutputStream.write`
  already guarantees this for blocking streams; NIO channels do not, and
  forgetting the loop is a classic NIO bug.
- `n as u64` — `usize` → `u64` cast. Explicit, always.

```rust
        if tee.should_offer(dir) {
```

Cheap gate. If Kafka is off, or this direction isn't published, we skip all the
buffer work below. `should_offer` is `#[inline]` and reads two `bool`s, so after
LTO this is a single predictable branch.

## The memory lesson, lines 151–172

```rust
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
            let chunk = if n.saturating_mul(SMALL_READ_RATIO) < cap {
                let exact = Bytes::copy_from_slice(&buf[..n]);
                buf.clear(); // keeps the allocation for the next read
                exact
            } else {
                buf.split().freeze()
            };
            tee.offer(conn_id, dir, &peer, chunk);
        } else {
            buf.clear();
        }
    }
}
```

**Understand `Bytes` and `BytesMut` first.**

- `BytesMut` — a *unique*, mutable, growable buffer. Owns its allocation.
- `Bytes` — an *immutable, reference-counted, shareable* view of a byte region.
  Cloning a `Bytes` is a refcount bump, not a copy. Two `Bytes` can point at
  different, overlapping regions **of the same allocation**, and the allocation
  survives until the last one drops.

`buf.split().freeze()` takes everything written so far out of the `BytesMut` and
converts it to a `Bytes` — **with no copy**. It's the fastest possible operation.

And that is exactly what caused the bug.

Trace it concretely:

1. `BytesMut::with_capacity(16384)` → one 16 KiB heap allocation.
2. A 120-byte payments message arrives. `n = 120`.
3. `buf.split().freeze()` → a `Bytes` describing bytes `[0..120]`, but the
   refcount is on the **whole 16 KiB allocation**.
4. That `Bytes` is pushed into the tee queue (capacity 8192, × 4 shards).
5. As long as it sits in the queue, **16 KiB is retained to hold 120 bytes** —
   a 136:1 waste ratio.
6. `buf` is now empty, so the next `reserve(cap)` allocates a *fresh* 16 KiB.

At 256 connections with a backed-up queue: **409 MB RSS, ~146 MB of it pinned
buffers**, per `BENCHMARK.md`. Worse than the Java build, which is what made the
team look.

The fix, line 162:

```rust
let chunk = if n.saturating_mul(SMALL_READ_RATIO) < cap {
```

"If the read filled less than a quarter of the buffer" (`n × 4 < cap`), copy
into an exactly-sized `Bytes` instead:

```rust
let exact = Bytes::copy_from_slice(&buf[..n]);   // 120-byte alloc + 120-byte memcpy
buf.clear();                                     // reuse the SAME 16 KiB next time
exact
```

- `Bytes::copy_from_slice` allocates exactly 120 bytes and copies. A ~120-byte
  `memcpy` is on the order of tens of nanoseconds — far below the noise floor of
  a network hop.
- `buf.clear()` sets the length to 0 but **keeps the capacity**. The next
  `reserve(cap)` is a no-op. The same 16 KiB allocation is reused for the entire
  life of the connection. Java's `ByteBuffer.clear()` does exactly this too.
- Otherwise (a genuinely large read filling most of the buffer), the copy would
  be the expensive option, so take the zero-copy path.

`saturating_mul` rather than `*`: if `n` were enormous, `n * 4` could overflow
`usize` and wrap to a small number, making the condition true and triggering a
huge pointless copy. `saturating_mul` clamps at `usize::MAX`, so the condition is
correctly false. It costs nothing (one `cmov`) and eliminates a whole class of
bug.

**Result: peak RSS 409 MB → 43 MB, with no measurable latency cost.**

> **Why this could not happen in Java, and what Java pays for that.**
> Java has no `Bytes`-style refcounted slice in the standard library. You'd
> either copy (`Arrays.copyOfRange` — always) or use Netty's `ByteBuf` with
> manual `retain()`/`release()` refcounting, which has the *identical* pinning
> hazard plus the risk of leaks and double-frees that Netty's leak detector
> exists to catch.
>
> **Rust's pro:** zero-copy sharing is available, safe, and impossible to
> leak or double-free — the refcount is managed by the type.
> **Rust's con:** "safe" does not mean "efficient". The borrow checker
> guaranteed no use-after-free and no data race; it said nothing about
> *retention*. Holding a small view of a large allocation is memory-safe and
> memory-wasteful, and only a benchmark found it.
>
> **This is the most valuable lesson in the codebase.** Rust eliminates
> memory-safety bugs by construction. It does not eliminate memory-*efficiency*
> bugs. Those still need measurement — exactly as in Java.

The `else { buf.clear(); }` on line 171 handles the not-publishing case: reuse
the buffer, allocate nothing, ever. Steady-state allocation for a
non-publishing connection is **zero bytes per message**.

---

# Part 4 — `framing.rs`

```rust
use bytes::{Buf, Bytes, BytesMut};

use crate::config::Framing as FramingCfg;
```

`as FramingCfg` — import alias, exactly Kotlin's `import x as y`. Java has no
import aliasing at all (you must fully qualify one of them). Used here because
`Framing` the config struct and `Framer` the state machine would read confusingly
side by side.

`Buf` is another extension trait, imported for `.advance()`.

```rust
pub struct Framer {
    cfg: FramingCfg,
    buf: BytesMut,
    /// Once desynced we cannot trust any byte offset in this stream again, so we
    /// stop emitting rather than publish garbage into a payments topic.
    desynced: bool,
}
```

A struct — a class with only fields, no methods (methods go in a separate `impl`
block). Fields are **private by default**; no `pub` here means only this module
can touch them.

Note there is **no lock and no `volatile`**. A `Framer` is owned by exactly one
`StreamState`, owned by exactly one `Worker`'s `HashMap`, owned by exactly one
task. The compiler proved single-ownership, so single-threaded access is
guaranteed. In Java you would write a comment saying "not thread-safe, confined
to the worker thread" and hope.

```rust
pub enum Step {
    Frames(Vec<Bytes>),
    Desynced(&'static str),
    Ignored,
}
```

A three-variant sum type, each carrying different data (or none):

- `Frames(Vec<Bytes>)` — a tuple variant carrying a vector of frames.
- `Desynced(&'static str)` — carries a static string reason. `&'static str`
  means "a string reference that lives for the entire program" — i.e. a literal
  compiled into the binary. **Zero allocation, and it can be stored in the enum
  as a 16-byte fat pointer.** You physically cannot put a runtime-built `String`
  here, which the compiler enforces — a deliberate constraint keeping the error
  path allocation-free.
- `Ignored` — a unit variant, no payload.

Java 21:

```java
sealed interface Step {
    record Frames(List<byte[]> frames) implements Step {}
    record Desynced(String reason) implements Step {}
    record Ignored() implements Step {}
}
```

Semantically identical; mechanically very different. Java: an allocation per
`Step`, plus an interface pointer, plus GC. Rust: one flat value returned in
registers or by a small `memcpy`, sized as `max(variant) + tag`, no allocation.
Returning `Step::Ignored` from `push()` allocates **nothing at all**.

```rust
impl Framer {
    pub fn new(cfg: FramingCfg) -> Self {
        Self { cfg, buf: BytesMut::new(), desynced: false }
    }
```

`impl Framer { ... }` — methods live in a separate block from fields. Rust
deliberately separates data layout from behaviour.

`Self` (capital S) is an alias for the implementing type. `-> Self` is idiomatic
for constructors.

`fn new(...)` — a plain associated function, **not a language-level
constructor**. Rust has no `new` keyword and no constructors; `new` is a naming
convention only. Called as `Framer::new(cfg)`.

`Self { cfg, buf: ..., desynced: false }` — struct literal. `cfg` alone is
**field init shorthand** for `cfg: cfg`, same as JavaScript.

Note `cfg: FramingCfg` by value — the `Framer` takes ownership of its config. It
does not borrow it, so there's no lifetime to thread through, and each `Framer`
holds its own copy. That's why `tee.rs` does `self.framing.clone()` before
building one.

```rust
    pub fn push(&mut self, chunk: &[u8]) -> Step {
```

**The signature carries the whole contract.**

- `&mut self` — an exclusive borrow of the `Framer`. The method mutates it.
  While this call is running, nothing else in the program can touch this
  `Framer`. Compiler-enforced. Java's `synchronized` gives you the same
  guarantee at runtime cost; Rust gives it at compile time for free.
- `chunk: &[u8]` — a **shared borrow of a byte slice**. Read-only, no ownership,
  no copy. The caller keeps its `Bytes`. This is a pointer + length.
- `-> Step` — returns an owned `Step` by value.

The Java signature `Step push(byte[] chunk)` says none of this: is `chunk`
retained? mutated? is `push` thread-safe? You'd need Javadoc, and Javadoc lies.
The Rust signature is a machine-checked specification.

```rust
        if self.desynced {
            return Step::Ignored;
        }
        if self.cfg.mode == "raw" {
            return Step::Frames(vec![Bytes::copy_from_slice(chunk)]);
        }
```

`vec![...]` — a macro building a `Vec`, like `List.of(...)`.

Note the string comparison `self.cfg.mode == "raw"` on a hot-ish path. `==` on
strings in Rust is a **content comparison** (via the `PartialEq` trait), which is
what you want — unlike Java, where `==` on `String` is reference identity and the
source of the most famous Java beginner bug in existence. Rust has no such trap:
`==` is always structural, and identity comparison requires the explicit
`std::ptr::eq`.

(Comparing a string per call is slightly wasteful — an enum parsed at config load
would be cleaner, as `kafka.rs` does with `PayloadEncoding`. It's off the
forwarding path, so it doesn't matter.)

```rust
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        let n = self.cfg.prefix_bytes;
```

Append the new bytes to the reassembly buffer. `Vec::new()` **does not
allocate** — an empty `Vec` is `(dangling_ptr, len=0, cap=0)`, allocating lazily
on first push. So in the common case of a chunk with no complete frame, this
line costs nothing. Java's `new ArrayList<>()` allocates the object immediately
(though it defers the backing array).

```rust
        loop {
            if self.buf.len() < n {
                break;
            }
            let declared = match n {
                2 => {
                    let b = &self.buf[..2];
                    if self.cfg.big_endian {
                        u16::from_be_bytes([b[0], b[1]]) as usize
                    } else {
                        u16::from_le_bytes([b[0], b[1]]) as usize
                    }
                }
                4 => { /* same with u32 */ }
                _ => unreachable!("prefix_bytes validated at config load"),
            };
```

- `u16::from_be_bytes([b[0], b[1]])` — big-endian decode from a fixed-size array.
  Java: `ByteBuffer.wrap(b).getShort()` (big-endian by default) or manual
  shifting. Rust's version is a compiler intrinsic — on x86 it's a 16-bit load
  plus one `bswap`; on a big-endian target it's just a load. Explicit, and the
  array size is checked at compile time.
- **`if` is an expression here**, producing the value assigned to `declared`.
  Java needs a ternary or a statement + assignment.
- `as usize` — explicit widening, mandatory.
- `unreachable!(...)` — a macro that panics if reached. It's an assertion that
  this branch is impossible, with a message documenting *why* (config validation
  guarantees 2 or 4). It also satisfies the exhaustiveness checker, which
  requires `match` on an integer to have a catch-all.

  Java's `throw new IllegalStateException("unreachable")` in a `default:` case is
  the same idea. Rust's is a recognised idiom with a dedicated macro, and it also
  gives the optimiser a hint.

```rust
            let body_len = if self.cfg.length_includes_prefix {
                match declared.checked_sub(n) {
                    Some(v) => v,
                    None => {
                        self.desynced = true;
                        return Step::Desynced("declared length shorter than prefix");
                    }
                }
            } else {
                declared
            };
```

**`checked_sub` — this is a genuinely important line.**

`declared` and `n` are `usize`, which is **unsigned**. If `declared = 1` and
`n = 2`, then `declared - n` in a release build wraps to `18446744073709551615`.
That value would sail past the `> max_frame_bytes` check... actually no, it
would fail that check — but in a slightly differently ordered version of this
code it would be used as a length and the program would try to read 18 exabytes.

`checked_sub` returns `Option<usize>`: `None` on underflow. The `None` arm marks
the stream desynced.

Java has no `checked_sub`. `1 - 2` on `int` gives `-1` silently; on unsigned
semantics faked with `long` it gives garbage. The class of bug where a
length-prefix underflows is the origin of an enormous number of real CVEs in
network parsers. **Rust doesn't prevent it automatically — but it gives you a
one-word way to prevent it, and it panics in debug builds if you forget.**

```rust
            if body_len == 0 {
                self.desynced = true;
                return Step::Desynced("zero-length frame");
            }
            if body_len > self.cfg.max_frame_bytes {
                self.desynced = true;
                return Step::Desynced("frame exceeds max_frame_bytes");
            }
            if self.buf.len() < n + body_len {
                break; // partial frame, wait for more bytes
            }
```

Three guards:

- Zero-length would loop forever without consuming — a hang, not a crash.
- Oversized means a bogus length prefix; without this, one garbage read allocates
  gigabytes. This is *the* classic length-prefix DoS.
- Partial frame: `break` out and wait for more bytes. The already-consumed frames
  in `out` are still returned. Correct incremental parsing.

```rust
            self.buf.advance(n);
            out.push(self.buf.split_to(body_len).freeze());
        }

        Step::Frames(out)
    }
}
```

- `advance(n)` — skip the prefix by moving the buffer's start pointer forward.
  No copy, no shifting of remaining bytes. Java: `ByteBuffer.position(pos + n)`.
- `split_to(body_len)` — remove the first `body_len` bytes and return them as
  their own `BytesMut`. **No copy** — it splits the allocation's ownership in
  two.
- `.freeze()` — convert `BytesMut` → `Bytes`, making it immutable and shareable.
  Also no copy.

So reassembling a frame from the buffer is **pointer arithmetic and a refcount**.
Zero bytes are copied from the moment they land in `self.buf`.

The same pinning caveat from `proxy.rs` applies in principle, but `self.buf`
grows only to the size of a partially received frame, so the ratio is bounded and
small.

Netty's `ByteBuf.readSlice()` is the direct Java equivalent, but it requires
manual `retain`/`release` and its own leak detector. Plain `ByteBuffer` cannot do
this safely at all.

## The tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
```

`#[cfg(test)]` — this module is compiled **only** during `cargo test`. It does not
exist in the release binary. Not "stripped later" — never compiled.

Java puts tests in `src/test/java` and Maven excludes them from the jar; the
effect is similar. But the Rust version is in the *same file as the code*, which
means:

- Tests can access **private** items. `Framer.desynced` is private, and the test
  module can still reach it. In Java you'd need package-private visibility (which
  weakens your API for everyone) or reflection.
- `use super::*` imports everything from the parent module, including privates.

The trade: source files get long. The payoff: tests sit next to the code they
test and never go stale from a rename.

```rust
    #[test]
    fn reassembles_across_arbitrary_chunk_boundaries() {
        let mut f = Framer::new(cfg(false));
        let wire = [0x00, 0x03, b'a', b'b', b'c', 0x00, 0x03, b'x', b'y', b'z'];
        let mut got = Vec::new();
        for b in wire {
            got.extend(frames(f.push(&[b])));
        }
        assert_eq!(got, vec![Bytes::from_static(b"abc"), Bytes::from_static(b"xyz")]);
    }
```

- `#[test]` — `@Test`. Built into the language; no JUnit dependency, no test
  runner to configure. `cargo test` finds and runs them.
- `b'a'` — a **byte literal**, type `u8` (value 97). `b"abc"` is a byte-string
  literal, type `&'static [u8; 3]`. Java has neither; you'd write `(byte) 'a'`
  and `"abc".getBytes(UTF_8)` (which allocates, every call).
- `Bytes::from_static(b"abc")` — a `Bytes` pointing directly at the binary's
  read-only data. **No allocation and no refcount** — the static case is special.
- `assert_eq!` — asserts equality and prints both values on failure. The macro
  captures the source expressions, so failure output shows what was compared
  without you writing a message.
- This test is the important one: it feeds the wire **one byte at a time**,
  proving the framer reassembles across arbitrary TCP segmentation. That is the
  #1 bug in hand-rolled protocol code — assuming one `read()` yields one message.

```rust
    #[test]
    fn oversized_frame_desyncs_and_stays_desynced() {
        let mut f = Framer::new(cfg(false));
        assert!(matches!(f.push(&[0xFF, 0xFF]), Step::Desynced(_)));
        assert!(matches!(f.push(&[0x00, 0x01, b'a']), Step::Ignored));
    }
```

`matches!(expr, pattern)` — a macro returning `bool` if the expression matches
the pattern. `Step::Desynced(_)` means "the `Desynced` variant, don't care about
the payload". Java 16+ `instanceof` patterns are the closest thing, but they
can't destructure enum variants like this.

The second assertion encodes the *sticky* property: once desynced, always
desynced. That's a behavioural invariant, not just a return value, and it's
tested.

Note `fn frames(step: Step) -> Vec<Bytes>` above panics on the wrong variant.
Panicking in a test helper is fine and idiomatic — a panic fails the test.

---

# Part 5 — `tee.rs`: sharding without locks

This file is where Rust's concurrency model pays off most visibly.

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction {
    VpToFms,
    FmsToVp,
}
```

Six traits derived in one line. What each buys you:

- `Clone` — explicit `.clone()`.
- **`Copy`** — the type is a plain bit pattern, so assignment *copies* instead of
  moving. This is why `dir` can be passed around freely without `&` or `.clone()`
  and remain usable. Only valid for types with no ownership (no heap, no
  `Drop`). This is why `as_str(self)` takes `self` by value below.
- `PartialEq, Eq` — `equals()`. Needed for `==` and for use as a `HashMap` key.
- `Hash` — `hashCode()`. Needed as a `HashMap` key.
- `Debug` — a developer-facing `toString()`, used by `{:?}` and `tracing`'s `?`
  sigil.

**Java's `enum` gives you all of these automatically** and there's no way to opt
out. Rust makes you list them, which is more verbose but means a type only gets
capabilities you asked for — you cannot accidentally put something in a `HashSet`
that has meaningless equality.

**Size:** this enum is **1 byte**. A Java enum constant is a heap object (16-byte
header + fields) referenced by a 4-or-8-byte pointer. Inside `Event::Data`, this
field costs 1 byte and no indirection.

```rust
impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::VpToFms => "vp_to_fms",
            Direction::FmsToVp => "fms_to_vp",
        }
    }
}
```

`self` by value (not `&self`) because `Direction` is `Copy` — passing it is one
byte in a register, cheaper than passing a pointer.

Returns `&'static str` — a pointer into the binary's `.rodata`. **No allocation
ever.** Java's `name()` returns a `String` object; the object already exists so
it's cheap, but it's still an object with a header, and `toString()` on a custom
enum often builds a new one.

```rust
enum Event {
    Open { conn_id: u64, at: Instant },
    Data { conn_id: u64, dir: Direction, peer: Arc<str>, chunk: Bytes },
    Close { conn_id: u64, at: Instant },
}
```

**Struct variants** — named fields inside an enum variant, rather than
positional. Note no `pub`: `Event` is private to this module. The whole
event-passing protocol is an implementation detail invisible to the rest of the
crate. Rust's module privacy makes this trivially enforceable.

Size analysis, because it matters for the queue:
- `Data` is the largest: `u64` (8) + `Direction` (1, padded) + `Arc<str>` (16,
  fat pointer) + `Bytes` (32) ≈ 64 bytes, plus a discriminant tag.
- The `mpsc` channel with capacity 8192 pre-allocates ~8192 × 64 B ≈ 512 KiB per
  shard. Four shards ≈ 2 MiB. **Flat, contiguous, no per-message allocation.**

The Java equivalent — `BlockingQueue<Event>` with record instances — allocates a
new object per event, per message, forever, and every one of them becomes garbage
a millisecond later. That's continuous young-gen pressure directly proportional
to payment volume.

```rust
#[derive(Clone)]
pub struct Tee {
    shards: Arc<Vec<mpsc::Sender<Event>>>,
    stats: Arc<Stats>,
    publish_vp_to_fms: bool,
    publish_fms_to_vp: bool,
    active: bool,
}
```

The **cloneable handle** pattern, ubiquitous in Rust. `Tee` is a lightweight
handle; cloning it bumps two refcounts and copies three bytes. Every pump task
gets its own.

`Arc<Vec<mpsc::Sender<Event>>>` — one shared, immutable vector of channel
senders. Because it's never mutated after construction, no lock is needed to read
it: **shared + immutable is always safe**, and the compiler knows that from the
absence of `&mut`.

```rust
impl Tee {
    pub fn spawn(cfg: &Config, publisher: Option<Arc<Publisher>>, stats: Arc<Stats>) -> Tee {
        let (pub_v2f, pub_f2v) = match cfg.tee.publish_directions.as_str() {
            "vp_to_fms" => (true, false),
            "fms_to_vp" => (false, true),
            _ => (true, true),
        };
```

`match` on a `&str` returning a **tuple**, destructured into two bindings in one
statement. Java would need a two-field record or two separate switches. Tuples
are free — no type declaration, no allocation, returned in registers.

```rust
        let publisher = match publisher {
            Some(p) => p,
            None => {
                return Tee {
                    shards: Arc::new(Vec::new()),
                    ...
                    active: false,
                }
            }
        };
```

Unwrap the `Option`, or early-return an inert `Tee`. After this line, `publisher`
is shadowed as a plain `Arc<Publisher>` — **the `Option` is gone from the type**,
so the rest of the function cannot possibly forget to handle `None`. The compiler
tracks this.

This is the "parse, don't validate" idiom: convert uncertainty into a certain
type at the boundary, and the rest of the code is simpler *and* provably correct.

`Arc::new(Vec::new())` for the inert case — an empty `Vec` doesn't allocate, and
the `Arc` is one small allocation. Negligible.

```rust
        let mut senders = Vec::with_capacity(cfg.tee.shards);
        for shard in 0..cfg.tee.shards {
            let (tx, rx) = mpsc::channel(cfg.tee.queue_capacity);
            senders.push(tx);
```

- `Vec::with_capacity(n)` — pre-size, avoid regrowth. `new ArrayList<>(n)`.
- `0..cfg.tee.shards` — an exclusive `Range`, which is an `Iterator`. Compiles to
  a plain counted loop.
- `mpsc::channel(cap)` — a bounded **m**ulti-**p**roducer **s**ingle-**c**onsumer
  channel. Returns a `(Sender, Receiver)` pair, destructured.

  **The "single consumer" part is enforced by the type system**: `Receiver` is
  not `Clone`. There is exactly one, and it gets moved into exactly one worker.
  Java's `BlockingQueue` lets any number of threads call `take()`, and
  single-consumer discipline is a comment you hope people read.

```rust
            let worker = Worker {
                shard,
                rx,
                publisher: Arc::clone(&publisher),
                framing: cfg.framing.clone(),
                timing_cfg: cfg.timing.clone(),
                stats: Arc::clone(&stats),
                state: HashMap::new(),
                timing: HashMap::new(),
            };
            tokio::spawn(worker.run());
        }
```

**`tokio::spawn(worker.run())` — look at the signature of `run`:**

```rust
async fn run(mut self) { ... }
```

`mut self`, **by value**. The method *consumes* the `Worker`. The struct — its
receiver, its two `HashMap`s, its config copies — is **moved into the future**,
which is moved into the task.

The consequences are exactly what makes this design work:

1. The `Worker` and everything it owns lives inside one task.
2. **Nothing else in the program has a reference to it.** The compiler proved
   this by taking ownership.
3. Therefore `self.state` and `self.timing` are accessed by exactly one task,
   and `&mut self` methods need **no lock, no `ConcurrentHashMap`, no atomics**.

The equivalent Java is thread confinement by convention, and you'd probably
reach for `ConcurrentHashMap` anyway because you can't *prove* the confinement.
That's a per-operation CAS and a more complex data structure, for a guarantee
Rust gives you for free at compile time.

`cfg.framing.clone()` — each worker gets its own copy of the framing config,
avoiding shared state entirely. It's a handful of small `String`s cloned four
times at startup. Buying independence with a trivial one-time cost is very
idiomatic.

```rust
    #[inline]
    fn wants(&self, dir: Direction) -> bool {
        match dir {
            Direction::VpToFms => self.publish_vp_to_fms,
            Direction::FmsToVp => self.publish_fms_to_vp,
        }
    }
```

`#[inline]` — a hint that this should be inlined even **across crate
boundaries** (within a crate, LLVM decides on its own). Java's JIT inlines based
on runtime profiling and can inline far more aggressively than any static
compiler in theory — but only after the method is hot, and it can deoptimise.
Rust decides at build time and it's decided forever.

`dir: Direction` by value — 1 byte, `Copy`.

```rust
    #[inline]
    pub fn offer(&self, conn_id: u64, dir: Direction, peer: &Arc<str>, chunk: Bytes) {
        if !self.active || !self.wants(dir) {
            return;
        }
        let shard = &self.shards[(conn_id as usize) % self.shards.len()];
        let ev = Event::Data { conn_id, dir, peer: Arc::clone(peer), chunk };
        match shard.try_send(ev) {
            Ok(()) => Stats::inc(&self.stats.tee_accepted),
            Err(_) => Stats::inc(&self.stats.tee_dropped),
        }
    }
```

**The most safety-critical function in the program.** Read the signature:

- `&self` — shared borrow. `offer` is called concurrently from every pump task
  with no synchronisation, because it only *reads* `self`. Compiler-verified.
- `chunk: Bytes` **by value** — takes ownership. The caller gives up its handle.
  Since `Bytes` is refcounted, this is a pointer move, not a copy.
- **No `async`, no `.await`, returns `()`.** This is the entire architecture in
  one signature:
  - Not `async` → **cannot suspend**. It runs to completion on the calling
    thread, always.
  - Returns `()`, not `Result` → **cannot fail upward**. The caller has no error
    to handle, so it cannot be tempted to retry or propagate.

  The compiler enforces both. Someone who later tries to add `.await` here would
  have to change the signature, which would break `pump`, which would force them
  to confront the design. **The rule "Kafka must never affect VP↔FMS" is encoded
  in a function signature.**

- `(conn_id as usize) % self.shards.len()` — shard by connection ID. This is
  what guarantees both directions of one connection land on the **same worker**,
  which is why `ConnTiming` needs no locking (line 162's comment).
- `Event::Data { conn_id, dir, peer: Arc::clone(peer), chunk }` — field init
  shorthand for three of four fields. `Arc::clone(peer)` is one atomic
  increment. The whole event is built on the stack and moved into the queue.
  **Zero allocations.**
- `try_send` — non-blocking. Returns `Err` immediately if the queue is full. Java:
  `BlockingQueue.offer()` (as opposed to `put()`).
- `Err(_) => Stats::inc(&self.stats.tee_dropped)` — **the load-shedding
  decision, in one line.** Kafka is behind → drop the audit copy, keep
  forwarding the payment. The comment says exactly why. This is the correct
  trade, and it's testable via the counter.

```rust
    pub fn open(&self, conn_id: u64) {
        if !self.active { return; }
        let shard = &self.shards[(conn_id as usize) % self.shards.len()];
        let _ = shard.try_send(Event::Open { conn_id, at: Instant::now() });
    }
```

`Instant::now()` — a **monotonic** clock reading, `System.nanoTime()`. It cannot
go backwards and is unaffected by NTP or clock changes. Rust makes this a
*different type* from `SystemTime` (wall clock) so you **cannot accidentally
subtract a wall-clock time from a monotonic one** — a type error, not a
production incident. Java gives you two `long`s and hopes.

`let _ =` — best-effort, and the comment says what degradation looks like.

## The `Worker`

```rust
struct Worker {
    shard: usize,
    rx: mpsc::Receiver<Event>,
    publisher: Arc<Publisher>,
    framing: crate::config::Framing,
    timing_cfg: crate::config::Timing,
    stats: Arc<Stats>,
    state: HashMap<(u64, Direction), StreamState>,
    timing: HashMap<u64, ConnTiming>,
}
```

`HashMap<(u64, Direction), StreamState>` — **a tuple as a composite key.** This
works because `(u64, Direction)` gets `Hash` and `Eq` automatically from its
components (both derived `Hash + Eq`).

In Java you'd need a record `ConnDir(long id, Direction dir)` — a class
declaration plus a heap allocation per lookup, since the key must be boxed.
Rust's tuple key is 16 bytes on the stack, hashed in place, **no allocation per
lookup**. On a per-message path that's the difference between steady garbage and
none.

```rust
impl Worker {
    async fn run(mut self) {
        let mut sweep = tokio::time::interval(IDLE_SWEEP);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
```

`interval` — a periodic timer, `ScheduledExecutorService.scheduleAtFixedRate`.

`MissedTickBehavior::Skip` — if ticks are missed (worker was busy), don't fire
them all back-to-back; skip to the next scheduled slot. Java's
`scheduleAtFixedRate` **does** fire them back-to-back, which is a classic
production surprise: a paused service resumes and immediately runs the task 50
times. Rust forces you to choose the policy explicitly.

```rust
        loop {
            tokio::select! {
                ev = self.rx.recv() => match ev {
                    Some(ev) => self.handle(ev),
                    None => break,
                },
                _ = sweep.tick() => {
```

`recv()` returns `Option<Event>`: `Some(ev)` for a message, **`None` when every
`Sender` has been dropped**. That is the shutdown signal — no poison pill, no
sentinel value, no `AtomicBoolean running`. When `main` exits and drops the
`Tee`, the senders drop, the refcount hits zero, `recv()` returns `None`, the
worker breaks and logs "tee worker stopped". **Shutdown falls out of the
ownership model for free.**

Note the interaction with cancellation safety: `select!` may cancel `recv()` when
the sweep branch wins. Tokio's `mpsc::Receiver::recv` is documented as
cancellation-safe — no message is lost. That's a property you have to check per
API, and it's the sharpest edge in async Rust.

```rust
                    let now = Instant::now();
                    let before = self.state.len() + self.timing.len();
                    self.state.retain(|_, s| now.duration_since(s.last_touched) < IDLE_MAX);
                    self.timing.retain(|_, t| now.duration_since(t.last_touched) < IDLE_MAX);
                    let reclaimed = before - (self.state.len() + self.timing.len());
```

`retain(|k, v| bool)` — keep entries where the closure returns true.
`map.entrySet().removeIf(...)` inverted.

This is a **manual leak guard**: if a `Close` event were dropped (the queue was
full), that connection's state would live forever. The sweep bounds it at 10
minutes. Note that Rust's ownership model does *not* save you here — this is a
logical leak, not a memory-safety issue, and Java would need exactly the same
sweep. Worth internalising: **Rust prevents use-after-free, not
forgot-to-remove-from-the-map.**

```rust
            Event::Data { conn_id, dir, peer, chunk } => {
                let framing = self.framing.clone();
                let entry = self.state.entry((conn_id, dir)).or_insert_with(|| StreamState {
                    framer: Framer::new(framing),
                    seq: 0,
                    last_touched: Instant::now(),
                });
```

`entry(key).or_insert_with(closure)` — `computeIfAbsent`. Returns `&mut
StreamState`, a mutable reference into the map.

The `let framing = self.framing.clone();` on the line before exists for a
**borrow checker** reason worth understanding, because you will hit this exact
pattern:

- `self.state.entry(...)` takes `&mut self.state`.
- The closure would need `&self.framing` to build the `Framer`.
- The compiler's borrow analysis at closure-capture granularity can't always
  prove `self.state` and `self.framing` are disjoint when both are reached
  through `self`, so it rejects the overlapping borrows.
- Cloning `framing` into a local first breaks the dependency: the closure now
  captures a local, not `self`.

This is the borrow checker being conservative, and the fix is a small clone. It
is the most common friction a Java developer hits in Rust. The honest assessment:

- **Con:** you sometimes restructure code, or pay a small clone, to satisfy a
  checker that is stricter than strictly necessary.
- **Pro:** the checker also *never* lets you ship a data race or a
  use-after-free, and the same discipline is what makes the lock-free worker
  design above sound.

```rust
                match entry.framer.push(&chunk) {
                    Step::Ignored => {}
                    Step::Desynced(reason) => {
                        Stats::inc(&self.stats.framer_desyncs);
                        tracing::warn!(conn_id, direction = dir.as_str(), reason, ...);
                    }
                    Step::Frames(frames) => {
```

Exhaustive match over the three `Step` variants. `push(&chunk)` passes a shared
borrow — `Bytes` derefs to `&[u8]` automatically (deref coercion again).

`Step::Desynced(reason)` binds the `&'static str` out of the variant. Note
`reason` in the `tracing` call uses the shorthand `reason = reason`.

```rust
                    Step::Frames(frames) => {
                        let ts = now_ms();
                        let key = conn_id.to_string();
                        for frame in frames {
                            entry.seq += 1;
```

`conn_id.to_string()` — one `String` allocation per *chunk*, not per frame,
hoisted out of the loop deliberately. It's the Kafka partition key, so both
directions of a connection land on the same partition and stay ordered.

`for frame in frames` — this **consumes** the `Vec` (an `IntoIterator` by value),
yielding owned `Bytes`. After the loop, `frames` is gone. If you wanted to keep
it you'd write `for frame in &frames`. Java's for-each always borrows; Rust makes
the distinction visible, and consuming avoids a refcount bump per frame.

```rust
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
                            self.publisher.publish(&key, &meta, &frame);
```

**`Meta` is a borrowing struct — see `kafka.rs` for its `<'a>` lifetime.**
`peer: &peer` stores a *reference*, not a clone. No refcount bump, no
allocation. `Meta` is built on the stack, passed by reference, and dies at the
end of the iteration.

Java cannot express this. Every field of a Java object that references another
object is a full GC-tracked reference, and a short-lived metadata holder is a
real heap allocation and real garbage. Here `Meta` costs **zero heap bytes**.

```rust
    fn measure(
        timing: &mut HashMap<u64, ConnTiming>,
        cfg: &crate::config::Timing,
        stats: &Stats,
        conn_id: u64,
        dir: Direction,
        now: Instant,
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
```

**Note this is an associated function taking `&mut HashMap` explicitly, not a
method taking `&mut self`.** That's another borrow-checker accommodation: the
caller is already holding `entry` (a `&mut` into `self.state`), so it cannot also
hand out `&mut self`. Passing the specific fields it needs (`&mut self.timing`,
`&self.timing_cfg`, `&self.stats`) lets the compiler see they're disjoint —
which it can do for *direct field access*, just not through a whole-`self`
borrow.

The idiom: **when the borrow checker complains about `&mut self`, pass individual
fields instead.** It's a mechanical fix once you recognise it.

Returns a 3-tuple of `Option<f64>` — three optional measurements, no wrapper
class, no allocation, returned in registers.

```rust
        let t = timing.entry(conn_id).or_insert_with(|| ConnTiming::new(now));
        t.last_touched = now;

        let conn_age_ms = ms(now.saturating_duration_since(t.opened));
        let gap_ms = t.last_frame.map(|prev| ms(now.saturating_duration_since(prev)));
        t.last_frame = Some(now);
```

- `saturating_duration_since` — clamps to zero rather than panicking if `t.opened`
  is somehow later than `now`. `Duration` is unsigned; the plain `-` operator on
  `Instant` panics on a negative result. Explicit overflow discipline again.
- `t.last_frame.map(|prev| ...)` — **`Option::map`**. If `Some`, apply the
  closure; if `None`, stay `None`. Exactly `Optional.map`. Produces
  `Option<f64>` in one expression with no branch written by hand and no
  allocation.

```rust
                Direction::VpToFms => {
                    if t.pending.len() >= cfg.max_pending {
                        t.pending.pop_front();
                        Stats::inc(&stats.rtt_unmatched);
                    }
                    t.pending.push_back(now);
                }
                Direction::FmsToVp => {
                    if let Some(sent) = t.pending.pop_front() {
                        let d = now.saturating_duration_since(sent);
                        rtt_ms = Some(ms(d));
```

`VecDeque` — `ArrayDeque`. A ring buffer, so `pop_front` is O(1) with no shifting.

The FIFO pairing assumes responses come back in request order — documented
honestly as an assumption in `config.rs`, with a config flag to disable it. Good
engineering: the assumption is written down at the config site where an operator
will read it.

`pop_front()` returns `Option<Instant>` — `None` if empty, so an unsolicited
response doesn't corrupt anything. The `if let Some(sent)` handles it in one
construct.

`t.pending.pop_front()` on overflow is the bounded-memory guard: if FMS stops
answering, `pending` cannot grow past `max_pending`.

```rust
fn ms(d: Duration) -> f64 {
    (d.as_micros() as f64) / 1000.0
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
```

Module-level private functions. No class needed — Rust has free functions, and
this is idiomatic rather than a code smell.

`ms` converts a `Duration` to fractional milliseconds via microseconds, so you
get 0.001 ms resolution rather than integer milliseconds.

`now_ms` — `System.currentTimeMillis()`. Returns `Result` because
`SystemTime::now()` can theoretically be *before* the Unix epoch if the system
clock is absurd. `.map(...).unwrap_or(0)` handles it without a branch.

Note the two clock types in use: `Instant` (monotonic) for durations, `SystemTime`
(wall clock) for timestamps published to Kafka. The right tool for each, and the
type system will not let you mix them up.

---

# Part 6 — `kafka.rs`

```rust
use base64::Engine as _;
```

**`as _` — import the trait for its methods, but don't bind its name.** We need
`Engine`'s methods to be callable, but we never write `Engine` in this file, so
binding the name would be an unused import warning. This says "bring the trait
into scope anonymously".

There's no Java analogue because Java has no extension-method resolution.

```rust
pub struct CountingContext {
    stats: Arc<Stats>,
    log_every: u64,
}

impl ClientContext for CountingContext {}
```

**An empty impl block.** `ClientContext` has default implementations for every
method, so implementing it requires no code. It's a marker — "this type is
eligible to be a client context".

Java's equivalent is an interface with all-`default` methods, which has existed
since Java 8. Same idea.

```rust
impl ProducerContext for CountingContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
```

**`type DeliveryOpaque = ();`** is an **associated type**. The trait declares
"implementors must specify a type `DeliveryOpaque`"; we specify `()` — we attach
no per-message user data.

Java's equivalent is a generic type parameter on the interface
(`interface ProducerContext<D>`), but associated types differ importantly: a type
can implement `ProducerContext` only *once*, with one choice of
`DeliveryOpaque`. With Java generics you could implement `ProducerContext<String>`
and `ProducerContext<Integer>`... actually you can't, due to erasure, which is
its own limitation. Rust's version is deliberate: use an associated type when
there's one natural choice per implementor, a generic parameter when there are
many.

`DeliveryResult<'_>` — the `'_` is an **anonymous lifetime**. It says "this type
has a lifetime parameter; infer it". Without it you'd write
`DeliveryResult<'a>` and declare `'a`. It's saying "borrowed from somewhere, and
I don't need to name the source".

`_: ()` — a parameter matched and discarded. It's the unit-typed opaque we don't
use.

**This callback runs on librdkafka's own background C thread.** `&self` means it
only reads `CountingContext`, and the compiler enforces that. The `Arc<Stats>`
is shared across the thread boundary safely because `Stats` contains only
atomics, which makes it `Sync`.

> **`Send` and `Sync` — the two traits that make this safe, and Java's biggest
> missing feature.**
>
> - `Send` = "this type can be moved to another thread."
> - `Sync` = "`&T` can be shared across threads", i.e. `T` is safe for
>   concurrent access.
>
> These are **auto-traits**: the compiler derives them structurally. A struct is
> `Sync` if all its fields are. `AtomicU64` is `Sync`; `Cell<u64>` is not.
> `Arc<T>` is `Send + Sync` only if `T: Send + Sync`. `Rc<T>` (the
> non-atomic refcount) is neither, so **you cannot compile a program that shares
> an `Rc` across threads**.
>
> `tokio::spawn` requires its future to be `Send`. So if you accidentally hold a
> non-thread-safe value across an `.await`, **the program does not compile**.
>
> **Java has no equivalent whatsoever.** `@ThreadSafe` is a comment. Sharing a
> `HashMap` across threads compiles perfectly and fails in production at 3am
> under load, intermittently. This is, in my view, the single strongest argument
> for Rust in a concurrent system — stronger than the memory numbers.

```rust
            Err((err, _msg)) => {
                let n = self.stats.kafka_delivery_failed.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 || n % self.log_every == 0 {
                    tracing::warn!(failures = n, error = %err, "kafka delivery failing");
                }
            }
```

`Err((err, _msg))` — **destructuring a tuple inside the `Err` variant**. Two
levels of pattern in one expression. `_msg` is bound but unused; the leading
underscore silences the unused-variable warning while documenting what the field
is. (`_` alone would discard it without naming it.)

`fetch_add` returns the value *before* the addition, hence `+ 1`. Same as Java's
`getAndIncrement()`.

Log rate limiting: first failure, then every 1000th. The comment names the real
risk — during a broker outage you'd otherwise emit one log line per
transaction, and **log I/O is the one thing left that could starve the runtime**.
That's a real production failure mode: monitoring code taking down the thing it
monitors.

```rust
impl PayloadEncoding {
    fn parse(s: &str) -> Self {
        match s {
            "hex" => PayloadEncoding::Hex,
            "utf8" => PayloadEncoding::Utf8,
            _ => PayloadEncoding::Base64,
        }
    }
```

String → enum **once, at startup**. Everything downstream matches on a 1-byte
enum. Compare `framing.rs`, which compares strings per call; this is the better
pattern, and it's on the hotter path where it matters more.

```rust
    fn encode(self, bytes: &[u8]) -> String {
        match self {
            PayloadEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
            PayloadEncoding::Hex => {
                let mut s = String::with_capacity(bytes.len() * 2);
                for b in bytes {
                    s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
                    s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
                }
                s
            }
            PayloadEncoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        }
    }
```

- `String::with_capacity(bytes.len() * 2)` — **exactly one allocation** for the
  whole hex string, correctly sized up front. Java's `StringBuilder` grows by
  doubling from 16, meaning ~7 reallocations and copies for a 1 KiB frame.
- `for b in bytes` — iterating `&[u8]` yields `&u8`; `b >> 4` auto-dereferences.
- The comment on avoiding `format!("{:02x}", b)` per byte is correct and
  measurable: `format!` builds a formatter, parses the format spec at runtime,
  and allocates a `String` per call. Per byte, at payments volume, that's real.
  Java's `String.format("%02x", b)` is worse still.
- `char::from_digit(v, 16)` returns `Option<char>` — `None` if `v >= 16`. Here
  `b >> 4` on a `u8` is always ≤ 15, so `.unwrap()` is provably safe.

  **`.unwrap()` panics on `None`.** It is the "I know this can't fail" escape
  hatch. Used correctly here. Used carelessly it is the #1 source of Rust
  panics — the moral equivalent of `Optional.get()`.
- `String::from_utf8_lossy(bytes)` returns a **`Cow<str>`** — "Clone on Write":
  either a borrow of the original bytes (if they were already valid UTF-8, **no
  allocation**) or an owned `String` (if replacement characters had to be
  inserted). `.into_owned()` forces it to owned.

  **Java has no `Cow`.** `new String(bytes, UTF_8)` always allocates and always
  copies, even when the input was already valid. `Cow` is a genuinely useful type
  with no Java counterpart: it lets an API be zero-copy in the common case
  without changing its signature.

```rust
pub struct Meta<'a> {
    pub conn_id: u64,
    pub direction: &'a str,
    pub seq: u64,
    pub peer: &'a str,
    ...
}
```

**`<'a>` is a lifetime parameter, and this is the concept with no Java analogue
at all.**

`'a` is a *generic parameter over lifetimes*. `Meta<'a>` holds two `&'a str`
references, and the declaration means: **"a `Meta<'a>` may not outlive `'a`."**
The compiler checks this at every use site.

Concretely, in `tee.rs`:

```rust
let meta = kafka::Meta { ..., peer: &peer, ... };
self.publisher.publish(&key, &meta, &frame);
```

`peer` lives in the enclosing scope. `meta` borrows it. The compiler verifies
`meta` dies before `peer` does. If you tried to stash `meta` in a field that
outlives the loop, **it would not compile**.

**Why this matters here, per the doc comment ("Borrowed rather than owned so
publishing a frame allocates only the JSON body"):**

- `Meta` owns nothing. It is **stack-allocated**, ~72 bytes, freed by moving the
  stack pointer.
- The alternative — `String` fields — would mean two heap allocations and two
  memcpys **per frame**.
- At payments volume that's the difference between zero and millions of
  short-lived allocations.

Java **cannot express this**. Any Java object holding a `String` field holds a
GC-tracked reference; the object itself is heap-allocated with a header; and it
becomes garbage. Java's escape analysis *can* sometimes stack-allocate such an
object, but only if the JIT proves non-escape after profiling, only in compiled
code, and it silently stops working when the code shape changes.

- **Rust pro:** zero-allocation borrowed views are guaranteed, statically,
  always, from the first instruction.
- **Rust con:** lifetimes are the hardest part of the language. They infect
  signatures (`Meta<'a>`, `Envelope<'a>`, `DeliveryResult<'_>`), and "fighting
  the borrow checker" over lifetimes is the classic beginner experience.

```rust
#[derive(Serialize)]
struct Envelope<'a> {
    conn_id: u64,
    direction: &'a str,
    ...
    #[serde(skip_serializing_if = "Option::is_none")]
    conn_age_ms: Option<f64>,
    ...
    fields: Option<&'a BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_error: Option<&'a str>,
    encoding: &'a str,
    payload: &'a str,
}
```

- `#[derive(Serialize)]` — generates the JSON writer at compile time. See Part 1.
- **Field order is wire order**, and the doc comment says so deliberately. Serde
  emits fields in declaration order. Jackson's order is unspecified unless you
  add `@JsonPropertyOrder`.
- `#[serde(skip_serializing_if = "Option::is_none")]` — omit the field entirely
  when `None`. Jackson's `@JsonInclude(NON_NULL)`.

  Note the **function path given as a string literal**. It's resolved at compile
  time by the macro — a typo is a compile error, not a runtime one. Contrast
  Jackson, where an annotation naming a bad class fails at runtime.
- `Option<&'a BTreeMap<String, String>>` — an optional *borrowed* map. Not
  cloned. Note it's 8 bytes (niche optimisation on the reference).
- **Every string field is a borrow.** Serialising this envelope allocates
  exactly one thing: the output `Vec<u8>`. Nothing else.

`BTreeMap` rather than `HashMap` — a `TreeMap`. Chosen because iteration order is
sorted and **deterministic**, so the JSON field order is stable across runs. A
`HashMap` in Rust uses a randomly-seeded hasher (SipHash-1-3), so its iteration
order genuinely varies run to run — a deliberate choice to make HashDoS attacks
impractical and to stop anyone depending on the order. Java's `HashMap` uses a
fixed `hashCode()` and mitigates collision attacks by treeifying dense buckets
(Java 8+) rather than by randomising the hash.

```rust
        let mut cc = ClientConfig::new();
        // `.as_str()` throughout: ClientConfig::set is generic over Into<String>,
        // and a generic bound gets no deref coercion from &String.
        cc.set("bootstrap.servers", cfg.brokers.as_str())
```

**This comment documents a genuinely subtle rule that will bite you.**

Normally `&String` coerces to `&str` automatically (deref coercion), so you can
pass a `&String` anywhere a `&str` is expected. But coercion happens during
*type checking against a concrete expected type*. When the parameter is generic —
`fn set<K: Into<String>>(k: K, ...)` — the compiler must first *infer* `K`. It
infers `K = &String`, then asks "does `&String` implement `Into<String>`?" It
does not. Error.

`.as_str()` performs the conversion explicitly, so `K = &str`, which does
implement `Into<String>`.

Rule of thumb: **deref coercion works for concrete parameter types, not for
generic bounds.** Java has nothing comparable because it has no user-definable
implicit conversions at all — arguably simpler, definitely less powerful.

```rust
        match cc.create_with_context::<CountingContext, ThreadedProducer<CountingContext>>(ctx) {
```

**`::<...>` is the "turbofish".** It supplies generic type arguments explicitly
when inference can't determine them. Java writes `Foo.<String>bar()`; Rust needs
the `::` because `Foo<String>` would be ambiguous with the less-than operator in
expression position.

Here it's needed because `create_with_context` is generic over both the context
type and the producer type, and only the return type would determine the latter.

`ThreadedProducer` — librdkafka's producer that runs its own background polling
thread. So there are **real OS threads** in this process beyond tokio's: tokio's
worker threads plus librdkafka's internal threads. The delivery callback fires on
one of librdkafka's, which is exactly why `Send`/`Sync` matter here.

```rust
            Err(e) => {
                tracing::error!(error = %e, "kafka producer creation failed; continuing WITHOUT publishing");
                None
            }
```

**Producer creation failure returns `None`, not an error.** The function's return
type is `Option<Arc<Publisher>>`, not `Result<...>`, so a caller *cannot*
propagate a Kafka failure into the startup path. The type system makes the
architectural rule unforgettable.

```rust
    pub fn publish(&self, key: &str, meta: &Meta<'_>, frame: &[u8]) {
```

`&Meta<'_>` — anonymous lifetime again: "a `Meta` borrowed from somewhere, I
don't need to relate its lifetime to anything else."

```rust
                let parsed = self.parser.as_ref().map(|p| p.parse(frame));
                let (fields, parse_error) = match &parsed {
                    Some(Ok(map)) => (Some(map), None),
                    Some(Err(reason)) => { ...; (None, Some(reason.as_str())) }
                    None => (None, None),
                };
```

- `self.parser` is `Option<KvParser>`. `.as_ref()` converts `&Option<T>` →
  `Option<&T>` — you get an `Option` of a *borrow*, without moving the parser
  out of `self` (which `&self` forbids anyway). This is an extremely common
  idiom.
- `.map(|p| p.parse(frame))` — produces `Option<Result<BTreeMap, String>>`.
- `match &parsed` — matching on a *reference*, so the arms bind references
  (`map: &BTreeMap`, `reason: &String`) rather than moving out. This is how you
  inspect a value without consuming it.
- The three arms cover: parser enabled and succeeded / enabled and failed / not
  enabled. Exhaustively, by the compiler.
- The tuple return assigns two variables from one match. No mutable temporaries,
  no `null` initialisation. In Java this would be two mutable locals initialised
  to `null` and assigned in branches.

Note `let parsed` is bound to a variable **before** the match, deliberately: the
`BTreeMap` inside it must outlive the `Envelope` that borrows it. If you inlined
the expression into the match, the temporary would be dropped at the end of the
statement and the borrow would dangle — **which the compiler would reject.** The
binding is load-bearing. In Java this would work by accident, because the GC keeps
the map alive as long as anything references it.

```rust
    fn send(&self, key: &str, value: &[u8], headers: OwnedHeaders) {
        let record: BaseRecord<'_, str, [u8], ()> =
            BaseRecord::to(&self.topic).key(key).payload(value).headers(headers);
```

`BaseRecord<'_, str, [u8], ()>` — four generic parameters: a lifetime, the key
type, the payload type, and the delivery-opaque type.

Note `str` and `[u8]` are **unsized types** used directly as type parameters.
That works because `BaseRecord` declares them as `?Sized` (opting out of the
default "must have a known compile-time size" bound). Java can't express this;
every type parameter is a reference to a sized object.

The explicit annotation is needed because inference can't determine the opaque
type `()` from usage.

```rust
    fn headers(&self, meta: &Meta<'_>) -> OwnedHeaders {
        OwnedHeaders::new()
            .insert(Header { key: "conn_id", value: Some(&meta.conn_id.to_string()) })
```

Builder chaining; each `insert` consumes and returns `Self`. Java builders return
`this` and mutate; Rust's move-based builders mean the intermediate value can't
be reused by accident.

`value: Some(&meta.conn_id.to_string())` — `Option<&String>` where the `String` is
a **temporary**. It lives until the end of the enclosing statement, which is long
enough for `insert` to copy the bytes into the headers. The compiler verifies
this; if `insert` tried to *retain* the reference, it wouldn't compile.

This does allocate three `String`s per message (conn_id, seq, ts_ms), which is a
minor cost the design accepts. It's on the tee worker, not the hot path.

```rust
    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            tracing::warn!(error = %e, "kafka flush incomplete on shutdown");
        }
    }
```

`if let Err(e) = ...` — the `Err`-side counterpart to `if let Some(x)`. Reads as
"if this returned an error, bind it and log". The success case is implicitly
ignored. Concise and complete.

---

# Part 7 — `parse.rs`

```rust
pub struct KvParser {
    pair_delim: u8,
    kv_delim: u8,
    trim: bool,
    max_fields: usize,
}
```

Delimiters stored as `u8`, not `String` — resolved once at construction. **The
struct is 24 bytes with no heap allocation at all** and can live inline inside
`Publisher`.

The Java equivalent holds `String` fields: two object references, two `String`
objects, two `byte[]`s. Six allocations for two characters.

```rust
    pub fn new(cfg: &ParseCfg) -> Option<Self> {
        if cfg.mode != "key_value" {
            return None;
        }
        Some(KvParser { ... })
    }
```

**A constructor returning `Option<Self>`** — "there may be no parser". Because
the return type is `Option`, every caller is forced to handle absence.

Java's equivalent is returning `null` from a factory and hoping the caller checks,
or `Optional<KvParser>` which most people won't bother with. Here it's the
natural way to write it.

```rust
    pub fn parse(&self, frame: &[u8]) -> Result<BTreeMap<String, String>, String> {
```

`Result<Map, String>` — the error type is a plain `String` message. Fine for a
diagnostic that gets embedded in the envelope; a library would use a typed error
enum.

```rust
        let text = std::str::from_utf8(frame).map_err(|e| format!("not valid utf-8: {e}"))?;
```

**`std::str::from_utf8` is worth dwelling on.**

It **validates** the bytes are UTF-8 and returns `Result<&str, Utf8Error>` —
`&str`, a *borrowed* view of the original bytes. **No allocation, no copy.** The
returned `&str` points into `frame`.

Java's `new String(bytes, UTF_8)`:
- always allocates a new `String` and copies,
- and **silently replaces invalid sequences with U+FFFD** rather than telling you.

That second point is a real correctness difference. In a payments context,
silently corrupting a byte you couldn't decode is much worse than being told the
frame wasn't text. Rust makes you choose: `from_utf8` (strict, borrowed, fails
loudly) or `from_utf8_lossy` (replaces, may borrow). The code here uses the
strict one and reports the failure into the envelope.

`.map_err(...)?` converts `Utf8Error` → `String`, then propagates.

```rust
        for (i, pair) in text.split(self.pair_delim as char).enumerate() {
```

- `.split(char)` — returns a **lazy iterator of `&str` slices**, all borrowing
  from `text`, which borrows from `frame`. **Zero allocations for the entire
  split.**

  Java's `String.split(String)` compiles a regex (or takes a fast path for
  single chars), then allocates a `String[]` **plus a new `String` per element**.
  For a 5-field message that's 6 allocations per parse, per message. Rust does
  zero.
- `.enumerate()` — pairs each item with its index, yielding `(usize, &str)`.
  Destructured directly in the `for` pattern. Java needs a manual counter or
  `IntStream.range`.
- `self.pair_delim as char` — `u8` → `char` cast. Valid for ASCII.

```rust
            let pair = if self.trim { pair.trim() } else { pair };
```

**Shadowing.** The new `pair` (trimmed) replaces the old one for the rest of the
loop body. Both are `&str` slices into the same buffer — trimming is just moving
the start/end pointers, **no allocation**.

Java's `String.trim()` allocates a new `String` unless nothing changed.

Shadowing like this is idiomatic Rust and often produces cleaner code than
`pairTrimmed`. Java forbids it outright.

```rust
            match pair.split_once(self.kv_delim as char) {
                Some((k, v)) => {
```

**`split_once` — split at the FIRST occurrence only**, returning
`Option<(&str, &str)>`.

The test at line 113 explains exactly why this matters:

```rust
    #[test]
    fn keeps_equals_signs_inside_the_value() {
        // split_once, not split -- a base64 value ending in '=' must survive.
        let got = p.parse(b"token=YWJjZA==,bankCode=1234").unwrap();
        assert_eq!(got.get("token").unwrap(), "YWJjZA==");
    }
```

With `split('=')` you'd get `["token", "YWJjZA", "", ""]` and mangle the base64
padding. `split_once` yields `("token", "YWJjZA==")`. Correct.

Java's equivalent is `split("=", 2)` — the limit argument, which everyone
forgets. Rust gives the safe operation its own name, so you have to opt *into*
the dangerous one.

`Some((k, v))` — destructuring the tuple inside the `Option`, in the pattern.

```rust
                    out.insert(k.to_string(), v.to_string());
```

`.to_string()` — here we finally **do** allocate, because the `BTreeMap` must own
its data (the borrowed `&str` points into `frame`, which dies at end of
`publish`). Two allocations per field, and it's unavoidable given the ownership
requirement.

Everything up to this point was zero-allocation. The allocation happens exactly
once, exactly where ownership genuinely transfers. **That is the ownership model
paying off: it tells you precisely where the costs are.**

```rust
fn first_byte(s: &str, fallback: u8) -> u8 {
    s.as_bytes().first().copied().unwrap_or(fallback)
}
```

A four-combinator chain worth unpacking:

- `.as_bytes()` — `&str` → `&[u8]`. Free; a `str` *is* UTF-8 bytes.
- `.first()` — `Option<&u8>`. `None` for an empty slice, no exception.
- `.copied()` — `Option<&u8>` → `Option<u8>`, dereferencing. (`u8` is `Copy`.)
- `.unwrap_or(fallback)` — the value or the default.

One line, total, no branches written by hand, no possible panic. The Java version
is `s.isEmpty() ? fallback : (byte) s.charAt(0)`, which is fine, but note that
the Rust version composes: each step is a small named operation you can reason
about independently.

---

# Part 8 — `stats.rs` and atomics

```rust
/// All counters are plain relaxed atomics. They are incremented on the hot path,
/// so ordering guarantees are deliberately the weakest available -- we only ever
/// read them for reporting, never to make a decision.
#[derive(Default)]
pub struct Stats {
    pub conns_accepted: AtomicU64,
    pub conns_active: AtomicU64,
    ...
}
```

**Memory layout, and this is a real difference:**

Rust: `Stats` is **one contiguous struct**. 21 `AtomicU64` fields = 168 bytes,
in a single `Arc` allocation. Field access is a fixed offset from a base
pointer — no indirection at all.

Java: 21 separate `AtomicLong` **objects**, each with a 12–16 byte header plus an
8-byte `volatile long` plus padding = ~24 bytes each, ~504 bytes total, **plus**
21 reference fields in the containing object, **plus** a pointer dereference on
every single access, **plus** they're scattered across the heap wherever the
allocator put them, so they're on 21 different cache lines.

That's roughly **3x the memory and 21 pointer chases**, and it's a fair
microcosm of the whole idle-RSS difference.

(A caveat in Rust's favour that also cuts against it: because these counters are
packed together, several land on the same 64-byte cache line, so concurrent
increments from different cores cause **false sharing**. At this volume it's
irrelevant, but at extreme rates you'd add `#[repr(align(64))]` padding. Java's
scattered layout accidentally avoids this — and `LongAdder` addresses it
explicitly with per-thread cells.)

```rust
    #[inline]
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
```

**`Ordering::Relaxed` is the concept to take away from this file.**

Rust exposes the full C++11 memory model. You choose per operation:

| Ordering | Guarantee | Cost |
|---|---|---|
| `Relaxed` | Atomicity only. No ordering with respect to other memory operations. | Cheapest possible atomic |
| `Acquire` / `Release` | One-way barriers; pairs to establish happens-before | Moderate |
| `AcqRel` | Both | Moderate |
| `SeqCst` | Total global order across all threads | Most expensive |

**Java's `AtomicLong.incrementAndGet()` is always sequentially consistent.** You
cannot ask for less. (Java 9's `VarHandle` added `getAndAddRelease` and
`getAndAddOpaque`, which give you roughly `Relaxed`, but almost nobody uses them
and `LongAdder`/`AtomicLong` don't.)

**Does it matter?** Depends on the architecture, and you're on ARM:

- **x86-64:** `fetch_add` compiles to `lock xadd` regardless of ordering. No
  difference for RMW.
- **ARM64 (your Apple Silicon Mac, and Graviton in AWS):** genuinely different.
  `Relaxed` → `ldxr`/`stxr` loop with no barriers. `SeqCst` → `ldaxr`/`stlxr`
  with acquire/release semantics, plus possibly a `dmb`. On a hot path with
  several counters per message, that's real.

The doc comment justifies the choice precisely: *"we only ever read them for
reporting, never to make a decision."* Relaxed is correct because no code
branches on a counter. If `conns_active` gated admission, you'd need stronger
ordering. **Stating the invariant in the comment is what makes this reviewable.**

- **Rust pro:** you can choose the cheapest correct ordering, and the choice is
  documented at the call site.
- **Rust con:** you *must* choose, on every single atomic operation, and choosing
  wrong gives you a bug that reproduces only on weakly-ordered hardware, under
  load, rarely. The C++11 memory model is genuinely hard. Java's "always
  sequentially consistent" is slower but essentially unfootgunnable.

```rust
    #[inline]
    pub fn max(counter: &AtomicU64, n: u64) {
        counter.fetch_max(n, Ordering::Relaxed);
    }
```

`fetch_max` — atomic "set to the max of current and n", one instruction on
platforms that support it, a CAS loop otherwise. **Java has no
`AtomicLong.getAndUpdateMax`**; you'd write the CAS loop by hand:

```java
long prev;
do { prev = max.get(); if (n <= prev) break; } while (!max.compareAndSet(prev, n));
```

Five lines of easily-mis-written code versus one method call.

```rust
    pub fn observe(count: &AtomicU64, sum: &AtomicU64, max: &AtomicU64, value: u64) {
        Self::inc(count);
        Self::add(sum, value);
        Self::max(max, value);
    }
```

Three separate atomics, deliberately **not** atomic as a group. A scraper could
observe `count` incremented but `sum` not yet. That's fine for metrics — and the
doc comment at the field declarations explains the deeper reason for the
sum/count/max shape rather than a stored average:

> *"An average since process start is almost useless — it never recovers from one
> bad hour."*

Correct, and the standard Prometheus pattern: export monotonic counters, let the
scraper compute `rate(sum) / rate(count)` over any window.

```rust
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("conns_accepted", Self::get(&self.conns_accepted)),
            ...
        ]
    }
```

Returns a `Vec` of `(&'static str, u64)` tuples. The names are pointers into
`.rodata` — **no string allocation**. One `Vec` allocation for 21 tuples of 24
bytes each. Called only when `/metrics` is scraped.

Java: `Map<String, Long>` — a `HashMap` allocation, 21 `Long` boxes (or
`LinkedHashMap` entries), 21 `Map.Entry` objects. Roughly 20x the allocation for
the same data, though again, once per scrape, so it doesn't matter here. It's
just illustrative of the constant factor.

---

# Part 9 — `admin.rs`

```rust
/// Hand-rolled rather than pulling in a web framework. This process sits inline
/// on an authorization path, so every dependency is one more thing that can
/// allocate, block, or spawn threads next to the hot path.
pub async fn serve(addr: String, stats: Arc<Stats>) {
```

The rationale is sound and is a genuine Rust-ecosystem consideration: adding
`axum` or `actix-web` pulls in `hyper`, `tower`, `http`, and a hundred
transitive crates, some of which spawn their own threads. For three endpoints
returning static text, 40 lines of hand-rolled HTTP is the right call.

`addr: String` by value — takes ownership. That's why `main.rs` wrote
`cfg.admin.addr.clone()`.

```rust
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%addr, error = %e, "admin bind failed; continuing without metrics");
            return;
        }
    };
```

**Bind failure returns instead of propagating.** Losing metrics must not stop the
proxy. Same philosophy as Kafka. The function returns `()`, so it *structurally
cannot* report failure to its caller — the design is in the signature.

```rust
        let (mut sock, _) = match listener.accept().await {
```

`(mut sock, _)` — bind the socket as mutable, discard the peer address entirely.
The `_` doesn't just ignore the value, it **drops it immediately**.

```rust
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
```

`[0u8; 1024]` — a **fixed-size array**, `[u8; 1024]`, allocated **on the stack**
(well, inside the task's state machine struct). Size is part of the type.

Java's `new byte[1024]` is always a **heap** allocation with a header and a
length field, and always becomes garbage. Rust's is 1024 bytes in the task's
frame, freed by dropping the task, zero GC involvement.

This is a small but very representative example of "cheap memory": in Rust,
**fixed-size buffers are free**. In Java, every array is a heap object.

```rust
            let n = match sock.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req.split_whitespace().nth(1).unwrap_or("/");
```

- `&mut buf` — exclusive borrow of the array for the read.
- `String::from_utf8_lossy(&buf[..n])` → `Cow<str>`. For a valid ASCII HTTP
  request this **borrows the stack buffer with no allocation at all**.
- `split_whitespace().nth(1)` — parse `GET /metrics HTTP/1.1`, take the second
  token. `Option<&str>`, defaulting to `"/"`. Two combinators, and a malformed
  request can't panic.

This is a deliberately minimal parser: one read, first packet only, no
`Content-Length` handling, no keep-alive. Adequate for a localhost-bound admin
endpoint, and the doc comment is honest that it's minimal.

```rust
            let (content_type, body) = match path {
                "/healthz" => ("text/plain", "ok\n".to_string()),
                "/metrics" => ("text/plain; version=0.0.4", prometheus(&stats)),
                _ => ("application/json", json(&stats)),
            };
```

`match` on a `&str` against literal patterns, returning a tuple. Java 21's
pattern-matching switch on strings is now equivalent; before 14 you'd need a
statement switch with mutable locals.

Note both arms must have the same type, so `"ok\n"` gets `.to_string()` to match
the `String` returned by `prometheus()`. The compiler catches the mismatch.

```rust
fn prometheus(stats: &Stats) -> String {
    let mut out = String::with_capacity(2048);
    for (name, value) in stats.snapshot() {
        out.push_str(&format!("# TYPE vp_interceptor_{name} counter\n"));
        out.push_str(&format!("vp_interceptor_{name} {value}\n"));
    }
    out
}
```

`with_capacity(2048)` pre-sizes correctly, but then each `format!` allocates a
temporary `String` that's immediately copied and dropped — 42 pointless
allocations per scrape. `write!(out, "...")` with `std::fmt::Write` would append
in place with zero temporaries.

It's called once per scrape interval, so it genuinely doesn't matter. Worth
noticing as an example of the general Rust idiom: `format!` allocates, `write!`
appends.

`for (name, value) in stats.snapshot()` — consumes the `Vec` by value and
destructures each tuple in the loop pattern.

```rust
fn json(stats: &Stats) -> String {
    let fields: Vec<String> = stats
        .snapshot()
        .into_iter()
        .map(|(k, v)| format!("\"{k}\":{v}"))
        .collect();
    format!("{{{}}}\n", fields.join(","))
}
```

An **iterator chain** — Java Streams, near-identically:

```java
String fields = stats.snapshot().entrySet().stream()
    .map(e -> "\"" + e.getKey() + "\":" + e.getValue())
    .collect(Collectors.joining(","));
```

Differences:

- `.into_iter()` consumes the `Vec`, yielding owned items. Java streams always
  borrow.
- `.map(|(k, v)| ...)` destructures the tuple **in the closure parameter**. Java
  needs `e.getKey()` / `e.getValue()`.
- `.collect()` is generic over the target collection, chosen by the type
  annotation `Vec<String>`. **Return-type-driven dispatch** — Java requires you
  to name a `Collector` explicitly.
- Rust's chain **compiles to a single loop** with no intermediate objects. Java's
  stream creates a pipeline of `Spliterator` objects and virtual `accept` calls,
  which the JIT often but not always flattens.

`format!("{{{}}}\n", ...)` — `{{` and `}}` are escaped literal braces, so this
produces `{...}`. Same escaping rule as Java's `MessageFormat`.

Hand-rolling JSON here is safe only because every key is a fixed identifier and
every value is a `u64`. Note the contrast with `kafka.rs`, where a test exists
specifically to prevent someone "optimising" the envelope into string
concatenation, since that data is attacker-influenced.

---

# Part 10 — `config.rs`

```rust
#[derive(Debug, Deserialize)]
pub struct Config {
    pub listen: Listen,
    pub upstream: Upstream,
    #[serde(default)]
    pub proxy: Proxy,
    ...
}
```

`#[derive(Deserialize)]` generates a TOML/JSON/whatever parser at compile time.
The struct definition **is** the schema.

**Required vs optional is expressed by the absence or presence of
`#[serde(default)]`:**

- `listen` and `upstream` have no `default` → the field is **mandatory**. A
  config missing `[listen]` fails to parse with a precise message.
- `proxy`, `tee`, `framing`, etc. have `#[serde(default)]` → use the type's
  `Default` impl if the section is absent.

Compare Jackson: you'd annotate with `@JsonProperty(required = true)`, which
Jackson only honours for creator parameters, and validation is largely runtime
and partial. Here it's structural.

**Field types encode constraints too:**
- `pub addr: String` — must be present and a string.
- `pub max_connections: usize` — must be a non-negative integer.

Note there is **no `Optional` and no `null`** anywhere in this config. Every field
is a concrete value by the time `load` returns. Nothing downstream ever
null-checks a config value. That is a substantial ongoing simplification.

```rust
#[derive(Debug, Deserialize)]
pub struct Listen {
    pub addr: String,
    #[serde(default = "d_max_conns")]
    pub max_connections: usize,
}
```

`#[serde(default = "d_max_conns")]` — **the name of a function, as a string.**

Rust attributes cannot take arbitrary expressions, so serde takes a path as a
string literal and resolves it at macro-expansion time. Misspell it and you get a
**compile error**, not a runtime one.

Java's `@JsonProperty(defaultValue = "4096")` is a `String` that Jackson mostly
ignores (it's documentation-only for most types!) — a genuine Jackson footgun.
Rust's version actually works and is checked.

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct Framing { ... }
```

Note `Clone` here but not on `Config`. `Framing` and `Timing` and `Parse` are
`Clone` because each tee worker takes its own copy. `Config` is not `Clone`
because it's only ever shared via `Arc`. **The derived traits document the
intended sharing model.**

```rust
    /// ASSUMES responses return in request order on a given connection. True for
    /// a strict request/response link; if VP pipelines and FMS may answer out of
    /// order, `rtt_ms` values will be mismatched -- set this false.
    #[serde(default = "d_true")]
    pub pair_request_response: bool,
```

An assumption documented **on the config field**, where an operator reading the
config will encounter it, with the remedy stated. This is exactly right, and it's
the kind of thing a doc comment is for. `cargo doc` will render it.

```rust
impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {path}: {e}"))?;
        let cfg: Config = toml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }
```

- `read_to_string` → `Result<String>`; `map_err` adds the path to the message; `?`
  propagates.
- `let cfg: Config = toml::from_str(&raw)?;` — **the type annotation drives the
  deserialisation.** `from_str` is generic over its return type
  `T: Deserialize`, and the annotation picks `T = Config`.

  Java's `mapper.readValue(raw, Config.class)` must pass the class token because
  erasure means the return type isn't available at runtime. Rust resolves it at
  compile time. This is inference flowing *backwards* from the annotation into
  the call, which Java's inference cannot do.
- `?` on `toml::Error` works because `anyhow::Error` implements `From<E>` for any
  standard error type — the `?` operator automatically applies the conversion.
  That's the `From`/`Into` trait doing implicit error widening, the one place
  Rust does implicit conversion.

```rust
    fn validate(&self) -> anyhow::Result<()> {
        if self.tee.shards == 0 {
            anyhow::bail!("tee.shards must be >= 1");
        }
```

`anyhow::bail!(msg)` — a macro expanding to `return Err(anyhow!(msg));`. Reads
like a `throw`, is a return.

Every message names the exact TOML key and the constraint. Config errors are
read by operators at 3am; this is the right level of care.

```rust
        match self.framing.mode.as_str() {
            "raw" => {}
            "length_prefix" => {
                if !matches!(self.framing.prefix_bytes, 2 | 4) {
                    anyhow::bail!("framing.prefix_bytes must be 2 or 4");
                }
            }
            other => anyhow::bail!("framing.mode must be 'length_prefix' or 'raw', got '{other}'"),
        }
```

- `matches!(x, 2 | 4)` — **or-patterns**. Reads as "is `x` either 2 or 4". Java
  21's `case 2, 4 ->` is equivalent in a switch, but Rust's works as a `bool`
  expression anywhere.
- `other => ...` — the catch-all arm **binds** the unmatched value, so the error
  message can quote what the operator actually typed. Small thing; enormously
  helpful in practice.
- This validation is what makes `framing.rs`'s
  `_ => unreachable!("prefix_bytes validated at config load")` sound. **The
  invariant is established at the boundary and relied on in the core.** That's
  the pattern the whole file follows.

```rust
impl Default for Proxy {
    fn default() -> Self {
        Self { read_buffer_bytes: d_read_buf(), nodelay: true, idle_timeout_ms: 0 }
    }
}
```

A **manual** `Default` impl, because `#[derive(Default)]` would produce all-zeros
(`nodelay: false`, `read_buffer_bytes: 0`), which is wrong and would fail
validation. Written out explicitly, reusing the same `d_*` functions serde uses,
so the two paths cannot disagree.

Java's field initialisers do this implicitly; Rust makes the default value a
trait impl you can see, test, and reason about.

```rust
fn d_true() -> bool { true }
fn d_max_conns() -> usize { 4096 }
fn d_connect_timeout() -> u64 { 3000 }
...
fn d_qbuf_msgs() -> u64 { 100_000 }
```

Every default in one greppable block. Terse `d_` naming keeps them visually
distinct from real logic.

- `100_000` — underscores as digit separators, exactly like Java's `100_000`.
- `"both".into()` — `&'static str` → `String`, an allocation, at config-load time
  only.
- Single-line function bodies. `rustfmt` normally expands these; the file must
  have `#[rustfmt::skip]` somewhere or they were hand-kept. Either way, for 24
  one-line constants it reads much better compressed.

They're functions rather than constants because serde's `default = "..."`
attribute takes a **function path**. A `const` wouldn't work with that mechanism.

---

# Part 11 — Where the cheap memory actually comes from

Your measured numbers, from `BENCHMARK.md`:

| Metric | Rust | Java | Ratio |
|---|---:|---:|---:|
| Startup to healthy | **30 ms** | 1185 ms | 40x |
| RSS idle | **4.0 MB** | 89.2 MB | 22x |
| RSS after warmup | **20.9 MB** | 151.7 MB | 7.3x |
| RSS peak under load | **43.1 MB** | 197.6 MB | 4.6x |
| CPU ms per 1k messages | **28.4** | 45.1 | 1.6x |
| Binary size | **3.4 MB** | 19 MB | 5.6x |

Here is precisely where each of those comes from, tied to lines in your code.

## 11.1 The 4 MB idle floor — no runtime

Java's ~89 MB at idle, before your code does anything, is:

- JVM metaspace (loaded class metadata) — thousands of classes from the JDK,
  Kafka client, Jackson, Netty.
- JIT code cache and the compiler threads' own working memory.
- GC data structures: card tables, remembered sets, and heap **reserved** by the
  collector whether or not it's in use.
- Thread stacks (default 1 MB reserved each) for GC threads, JIT compiler
  threads, the finalizer, the signal dispatcher.

Rust's 4 MB is: your code, plus statically-linked librdkafka, plus tokio's worker
threads, plus the allocator's arenas. **There is no runtime underneath.** The
binary is the program.

This also explains the **30 ms vs 1185 ms startup**: there is no classloading, no
verification, no JIT warm-up, no reflective serialiser construction. `main` runs
fully optimised machine code from the first instruction.

## 11.2 Per-message allocation: zero on the hot path

Trace one 120-byte payments message through `pump` in `proxy.rs` and count heap
allocations:

| Step | Allocations |
|---|---|
| `buf.reserve(cap)` — buffer already sized | **0** |
| `rd.read_buf(&mut buf)` — into existing buffer | **0** |
| `wr.write_all(&buf[..n])` — writes a slice | **0** |
| `Stats::add(byte_counter, n)` — atomic add | **0** |
| `Bytes::copy_from_slice(&buf[..n])` | **1** (120 bytes, exact) |
| `buf.clear()` — keeps capacity | **0** |
| `tee.offer(...)` — `Arc::clone` + `try_send` into a preallocated slot | **0** |

**One 120-byte allocation per message on the forwarding path.** If the tee is
disabled, **zero**.

The Java path for the same message:
- `ByteBuffer`/`byte[]` for the read (reusable, so ~0 if pooled),
- a `byte[]` copy for the queue,
- an `Event` object,
- a `String` peer (or a shared reference),
- a queue node object,
- and every one of those becomes young-gen garbage a millisecond later.

Java's allocation is *fast* — a pointer bump in TLAB — but the **collection** is
not free, and the young gen must be sized to absorb the churn. That sizing is a
big part of the 90 → 150 → 198 MB progression under load.

## 11.3 Struct layout: flat, not pointer graphs

The single biggest structural difference, illustrated with `Stats`:

```
Rust:  Arc<Stats> ──> [strong|weak| c1 | c2 | c3 | ... | c21 ]
                       one allocation, 21 counters inline, 168 bytes

Java:  Stats ──> [header | ref | ref | ref | ... | ref ]  (21 references)
                     │      │     │
                     │      │     └──> [header|value] AtomicLong
                     │      └────────> [header|value] AtomicLong
                     └───────────────> [header|value] AtomicLong
                       22 allocations, ~500+ bytes, 21 pointer chases
```

Java **cannot** embed one object inside another. Every non-primitive field is a
reference to a separately allocated object with its own 12–16 byte header.
(Project Valhalla will eventually give Java value types; it is not here yet.)

This compounds everywhere in this codebase:

- `Event::Data` — a flat ~64-byte value in a preallocated queue slot. Java: an
  object per event, forever.
- `Meta<'a>` — entirely on the stack, 0 heap bytes. Java: a heap object per
  frame.
- `(u64, Direction)` as a `HashMap` key — 16 stack bytes hashed in place. Java: a
  boxed record allocated per lookup.
- `[0u8; 1024]` in `admin.rs` — stack. Java: heap.
- `Direction` — 1 byte. Java: a reference to a heap singleton.

## 11.4 The specific lines that buy the most memory

**`proxy.rs:162–168` — the exact-size copy heuristic.** The largest single win
measured on this project: **peak RSS 409 MB → 43 MB, ~10x, with no latency
cost.** A 120-byte `memcpy` per message in exchange for not pinning 16 KiB per
queued message.

**`main.rs:88` — `Arc<str>` rather than `String` or `Arc<String>`.** The peer
address is cloned into every event. `Arc<str>` makes that clone one atomic
increment and stores the bytes in a single allocation rather than two.

**`kafka.rs:104` and `kafka.rs:125` — `Meta<'a>` and `Envelope<'a>` borrow
instead of own.** Per the doc comment: *"publishing a frame allocates only the
JSON body."* Two structs with 8 and 13 fields, built per frame, costing zero heap
bytes.

**`proxy.rs:164, 171` — `buf.clear()` rather than reallocating.** The same
16 KiB buffer serves a connection for its entire lifetime.

**`tee.rs:72` — `mpsc::channel(capacity)` is a preallocated ring.** Queue slots
are allocated once at startup. Java's `ArrayBlockingQueue` preallocates the array
of *references*, but the `Event` objects themselves are still allocated per
message.

**`parse.rs:33–46` — parsing borrows throughout.** `from_utf8`, `split`, `trim`,
and `split_once` all return `&str` views into the original frame. The only
allocations are the final `to_string()` calls where the map must own its data.

**`Cargo.toml` features** — unused parts of tokio and rdkafka are never compiled.
That's a chunk of the 3.4 MB vs 19 MB.

## 11.5 The CPU number, and why it's only 1.6x

1.6x is a modest ratio, and it's the *most reproducible* number in your
benchmark. That's the honest picture: **for I/O-bound work, a good JIT gets close
to native.** The JIT compiles hot loops to good machine code, and both programs
spend most of their time in `read`/`write` syscalls.

Rust's 1.6x comes from:
- No GC cycles at all (this is most of it),
- No JIT compilation threads competing for CPU,
- No safepoint polls or write barriers on every reference store,
- Serde's compile-time serialisation vs Jackson's reflective walk,
- LTO inlining across crate boundaries.

**Where Rust would *not* win:** long-running CPU-bound code where the JIT's
profile-guided optimisation (devirtualising a megamorphic call site based on
observed types, for instance) can beat static compilation. Static compilers must
be conservative about things the JIT can simply observe.

## 11.6 What Rust does NOT give you — read this part

Your `BENCHMARK.md` is admirably honest here, and so should this section be:

1. **The tail-latency argument failed.** It was the original justification for
   Rust and proved unmeasurable on the test rig. Argue Rust on memory, CPU, and
   startup — not on p99, unless you measure it on real hardware.

2. **Rust's first attempt used *more* memory than Java** (409 MB vs 167 MB). The
   borrow checker prevented use-after-free; it did not prevent a badly-chosen
   retention strategy. **Memory safety ≠ memory efficiency.** Only the benchmark
   found it.

3. **Logical leaks are still leaks.** `tee.rs`'s idle sweep exists because a
   dropped `Close` event would leave `HashMap` entries forever. Ownership doesn't
   help; you need the same sweep you'd write in Java.

4. **Compile times.** A clean build of this project with `lto = "fat"`,
   `codegen-units = 1`, and cmake-building librdkafka is minutes. Java's
   edit-compile-test loop is dramatically faster.

5. **The borrow checker costs development time**, especially early. The two
   `clone()`s in `tee.rs` (`self.framing.clone()`) and the `measure()` signature
   taking individual fields instead of `&mut self` are both accommodations to the
   checker rather than things you'd write naturally.

6. **Async Rust is harder than Java 21 virtual threads.** Function colouring,
   cancellation safety, `Pin`, and lifetime errors inside async blocks are all
   real friction that Java 21 simply does not have. If this project were written
   today with `Thread.ofVirtual()`, the *code* would be simpler. It would just
   use 20x the memory and start 40x slower.

**The honest summary:** Rust wins decisively on memory footprint, startup time,
and compile-time-guaranteed concurrency correctness (`Send`/`Sync` is worth more
than the memory numbers, in my view). Java wins on development velocity, compile
times, ecosystem breadth, and — since 21 — a genuinely nicer concurrency
programming model. For a process that sits inline on a payments path and must be
dense, restart instantly, and never race, the trade in this project's favour is
the right one, and `BENCHMARK.md` argues it on the correct grounds.

---

# Part 12 — Java → Rust cheat sheet

## Types

| Java | Rust | Note |
|---|---|---|
| `int` / `long` | `i32` / `i64` | Rust also has `u8..u128` unsigned |
| `long` for a size | `usize` | Pointer-sized; used for indices/lengths |
| `boolean` | `bool` | |
| `char` | `char` | Rust's is a full 4-byte Unicode scalar; Java's is a 2-byte UTF-16 code unit |
| `byte[]` | `[u8; N]` (stack) or `Vec<u8>` (heap) or `&[u8]` (borrowed) | Three distinct types for one Java concept |
| `String` | `String` (owned, heap) or `&str` (borrowed view) | Rust splits owned vs borrowed |
| `String` constant | `&'static str` | Points into `.rodata`, no object |
| `Optional<T>` | `Option<T>` | No allocation; `null` doesn't exist |
| `List<T>` / `ArrayList<T>` | `Vec<T>` | |
| `HashMap<K,V>` | `HashMap<K,V>` | Rust's is HashDoS-resistant by default |
| `TreeMap<K,V>` | `BTreeMap<K,V>` | B-tree, better cache behaviour |
| `ArrayDeque<T>` | `VecDeque<T>` | |
| `AtomicLong` | `AtomicU64` | Rust lets you pick memory ordering |
| An object reference | `Arc<T>` (shared) or `Box<T>` (unique) | Rust makes sharing explicit |
| `void` | `()` | A real type, usable in generics |
| — | `&T` / `&mut T` | Borrows. No Java equivalent |
| — | `Cow<'a, str>` | Borrow-or-own. No Java equivalent |
| — | `Bytes` | Refcounted slice. Netty's `ByteBuf` is closest |

## Syntax

| Java | Rust |
|---|---|
| `final var x = 5;` | `let x = 5;` |
| `var x = 5;` | `let mut x = 5;` |
| `static final int X = 5;` | `const X: i32 = 5;` |
| `class Foo { int a; }` | `struct Foo { a: i32 }` |
| methods inside the class | `impl Foo { fn bar(&self) {} }` — separate block |
| `interface Foo {}` | `trait Foo {}` |
| `class A implements B` | `impl B for A` |
| `enum Color { RED }` | `enum Color { Red }` |
| sealed interface + records | `enum` with data-carrying variants |
| `new Foo(1, 2)` | `Foo::new(1, 2)` — convention, not a keyword |
| `x.foo()` | `x.foo()` |
| `Foo.bar()` | `Foo::bar()` |
| `import a.b.C;` | `use a::b::C;` |
| `import a.b.C as D;` (impossible) | `use a::b::C as D;` |
| `package a.b;` | `mod b;` in the parent — explicit |
| `x -> x + 1` | `\|x\| x + 1` |
| `() -> 5` | `\|\| 5` |
| `@Override` etc. | `#[derive(...)]`, `#[inline]`, `#[test]` |
| `String.format("%d", x)` | `format!("{x}")` — compile-time checked |
| `switch (x) { case A -> ...; }` | `match x { A => ..., }` — exhaustive |
| `if (x instanceof Foo f)` | `if let Foo(f) = x` |
| `throw new E()` | `return Err(e)` |
| `try { } catch (E e) { }` | `match result { Ok(v) =>, Err(e) => }` |
| implicit propagation | `?` — explicit at every call site |
| `try (var r = ...) { }` | scope end — automatic, for every value |
| `executor.submit(task)` | `tokio::spawn(future)` |
| `future.get()` | `future.await` |
| `Thread.ofVirtual().start(r)` | `tokio::spawn(async move { .. })` |
| `synchronized` / `ReentrantLock` | usually unnecessary; `Mutex<T>` when needed |
| `BlockingQueue.offer()` | `sender.try_send()` |
| `BlockingQueue.take()` | `receiver.recv().await` |
| `list.stream().map(f).collect(..)` | `vec.iter().map(f).collect()` |
| `map.computeIfAbsent(k, f)` | `map.entry(k).or_insert_with(f)` |
| `map.entrySet().removeIf(p)` | `map.retain(\|k, v\| !p)` |

## The five things to internalise

1. **Read every signature as a contract.** `&self` vs `&mut self` vs `self`, and
   `&T` vs `T`, tell you about mutability, thread-safety, and whether the callee
   retains the argument. `Tee::offer` taking `&self`, not being `async`, and
   returning `()` **is** the architecture.

2. **`Result` and `Option` replace exceptions and `null`, and cost nothing.**
   Every `?` is a visible error edge. Every `match` is checked for exhaustiveness.

3. **Borrow by default, clone deliberately, own when you must.** `parse.rs` shows
   the ideal: borrow all the way through, allocate exactly once at the point
   where ownership genuinely transfers.

4. **Ownership gives you lock-free concurrency for free.** `tee.rs`'s `Worker`
   owns its state, so it needs no locks and no concurrent collections. The
   compiler proved it. `Send`/`Sync` mean you cannot compile a data race.

5. **Memory safety is not memory efficiency.** `proxy.rs:151–172` is a 10x memory
   regression that compiled perfectly and passed every test. Benchmark anyway.
