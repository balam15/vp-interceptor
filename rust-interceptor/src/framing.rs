// ============================================================================
// framing.rs -- reassembles the TCP byte stream into application messages.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 4.
// ============================================================================

// LEARN: `Buf` is another EXTENSION TRAIT, imported purely so `.advance()` is
//   callable further down. Import the trait or the method does not exist.
use bytes::{Buf, Bytes, BytesMut};

// LEARN: `as FramingCfg` is an IMPORT ALIAS.
// JAVA: Java has NO import aliasing at all -- you must fully qualify one of the
//   two clashing names. (Kotlin's `import x as y` is the direct equivalent.)
//   Used here because `Framing` the config struct and `Framer` the state machine
//   would read confusingly side by side.
use crate::config::Framing as FramingCfg;

/// Reassembles the byte stream into application messages for publishing.
///
/// This runs on the tee side only. It never sees the socket and can never delay
/// or alter what is forwarded to FMS -- if it desyncs, we stop publishing for
/// that stream and the proxy keeps running untouched.
// LEARN: a STRUCT is a class with ONLY fields. Methods live in a separate `impl`
//   block below -- Rust deliberately separates data layout from behaviour.
// LEARN: fields are PRIVATE BY DEFAULT. No `pub` here means only this module can
//   touch them (stricter than Java's package-private default).
// LEARN: NOTE THE ABSENCE OF ANY LOCK OR `volatile`. A Framer is owned by exactly
//   one StreamState, owned by exactly one Worker's HashMap, owned by exactly one
//   task. The compiler PROVED single-ownership, so single-threaded access is
//   guaranteed.
// JAVA: you would write a comment saying "not thread-safe, confined to the
//   worker thread" and hope everyone reads it.
pub struct Framer {
    cfg: FramingCfg,
    buf: BytesMut,
    /// Once desynced we cannot trust any byte offset in this stream again, so we
    /// stop emitting rather than publish garbage into a payments topic.
    desynced: bool,
}

// LEARN: a Rust `enum` is a TAGGED UNION (a sum type), not a fixed list of
//   singletons. Each variant carries different data -- or none.
// JAVA: the Java 21 equivalent is a sealed interface with records:
//     sealed interface Step {
//         record Frames(List<byte[]> frames) implements Step {}
//         record Desynced(String reason)     implements Step {}
//         record Ignored()                   implements Step {}
//     }
//   Semantically identical; MECHANICALLY VERY DIFFERENT.
//     Java: an allocation per Step, plus an interface pointer, plus GC.
//     Rust: ONE FLAT VALUE returned in registers or by a small memcpy, sized as
//           max(variant sizes) + a discriminant tag. NO ALLOCATION AT ALL --
//           returning Step::Ignored below allocates literally nothing.
pub enum Step {
    /// Frames reassembled from this chunk.
    // LEARN: a TUPLE VARIANT carrying a vector of frames.
    Frames(Vec<Bytes>),
    /// Stream just desynced; caller should count it and log once.
    // LEARN: &'static str means "a string reference that lives for the ENTIRE
    //   PROGRAM" -- i.e. a literal compiled into the binary. Zero allocation,
    //   stored in the enum as a 16-byte fat pointer. You physically CANNOT put a
    //   runtime-built String here, and the compiler enforces that -- a
    //   deliberate constraint that keeps this error path allocation-free.
    Desynced(&'static str),
    /// Already desynced, nothing to do.
    // LEARN: a UNIT VARIANT -- no payload.
    Ignored,
}

// LEARN: `impl Framer { ... }` holds the methods. Separate from the fields above.
impl Framer {
    // LEARN: `Self` (capital S) aliases the implementing type. `-> Self` is the
    //   idiomatic constructor signature.
    // LEARN: Rust has NO `new` KEYWORD AND NO CONSTRUCTORS. `new` is a plain
    //   associated function and purely a naming convention. Called as
    //   `Framer::new(cfg)`.
    pub fn new(cfg: FramingCfg) -> Self {
        // LEARN: a STRUCT LITERAL. `cfg` alone is FIELD INIT SHORTHAND for
        //   `cfg: cfg` (same idea as JavaScript's shorthand).
        // LEARN: `cfg: FramingCfg` is taken BY VALUE, so the Framer OWNS its
        //   config rather than borrowing it. That means no lifetime parameter to
        //   thread through the type -- which is why tee.rs calls
        //   `self.framing.clone()` before building one.
        Self { cfg, buf: BytesMut::new(), desynced: false }
    }

    // LEARN: THE SIGNATURE CARRIES THE WHOLE CONTRACT. Read it carefully:
    //   `&mut self`   -- an EXCLUSIVE borrow. This method mutates the Framer, and
    //                    while the call runs NOTHING ELSE in the program can
    //                    touch it. Compiler-enforced.
    //   `chunk: &[u8]`-- a SHARED BORROW of a byte slice. Read-only, no
    //                    ownership, no copy. The caller keeps its Bytes. This is
    //                    just a pointer + length.
    //   `-> Step`     -- returns an owned Step by value.
    // JAVA: the signature `Step push(byte[] chunk)` says NONE of this. Is chunk
    //   retained? mutated? is push thread-safe? You would need Javadoc, and
    //   Javadoc lies. The Rust signature is a MACHINE-CHECKED SPECIFICATION.
    pub fn push(&mut self, chunk: &[u8]) -> Step {
        if self.desynced {
            return Step::Ignored;
        }
        // LEARN: `==` ON STRINGS IN RUST IS A CONTENT COMPARISON (via the
        //   PartialEq trait). There is no reference-identity trap.
        // JAVA: `==` on String is REFERENCE IDENTITY and is the single most
        //   famous Java beginner bug in existence. Rust has no such trap;
        //   identity comparison requires the explicit std::ptr::eq.
        // LEARN: comparing a string per call is slightly wasteful -- an enum
        //   parsed once at config load would be cleaner, as kafka.rs does with
        //   PayloadEncoding. It is off the forwarding path, so it does not matter.
        if self.cfg.mode == "raw" {
            // LEARN: `vec![...]` is a MACRO that builds a Vec.
            // JAVA: List.of(...)
            return Step::Frames(vec![Bytes::copy_from_slice(chunk)]);
        }

        self.buf.extend_from_slice(chunk);
        // LEARN: `Vec::new()` DOES NOT ALLOCATE. An empty Vec is
        //   (dangling_ptr, len=0, cap=0) and allocates lazily on first push. So
        //   in the common case of a chunk containing no complete frame, this line
        //   costs nothing at all.
        // JAVA: `new ArrayList<>()` allocates the object immediately (though it
        //   does defer the backing array).
        let mut out = Vec::new();
        let n = self.cfg.prefix_bytes;

        loop {
            if self.buf.len() < n {
                break;
            }
            // LEARN: `if` IS AN EXPRESSION here, producing the value assigned to
            //   `declared`. Java needs a ternary, or a statement plus assignment.
            let declared = match n {
                2 => {
                    let b = &self.buf[..2];
                    if self.cfg.big_endian {
                        // LEARN: `u16::from_be_bytes([b[0], b[1]])` is a BIG-ENDIAN
                        //   decode from a FIXED-SIZE ARRAY (the size is checked at
                        //   compile time). It is a compiler intrinsic: on x86 it
                        //   is a 16-bit load plus one bswap; on a big-endian
                        //   target it is just a load.
                        // JAVA: ByteBuffer.wrap(b).getShort() (big-endian by
                        //   default) or manual shifting. Rust's version is
                        //   explicit about endianness at the call site.
                        // LEARN: `as usize` -- explicit widening, mandatory.
                        u16::from_be_bytes([b[0], b[1]]) as usize
                    } else {
                        u16::from_le_bytes([b[0], b[1]]) as usize
                    }
                }
                4 => {
                    let b = &self.buf[..4];
                    if self.cfg.big_endian {
                        u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize
                    } else {
                        u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize
                    }
                }
                // LEARN: `unreachable!(...)` is a macro that PANICS if reached.
                //   It asserts this branch is impossible AND documents why. It
                //   also satisfies the exhaustiveness checker, which requires a
                //   catch-all when matching on an integer, and gives the
                //   optimiser a hint.
                // JAVA: `throw new IllegalStateException("unreachable")` in a
                //   default: case is the same idea; Rust gives it a dedicated
                //   macro because the idiom is recognised.
                // LEARN: THE INVARIANT THIS RELIES ON IS ESTABLISHED IN
                //   config.rs::validate(). Boundary validates, core assumes.
                _ => unreachable!("prefix_bytes validated at config load"),
            };

            // Normalise to "bytes of body following the prefix".
            let body_len = if self.cfg.length_includes_prefix {
                // LEARN: `checked_sub` IS A GENUINELY IMPORTANT LINE.
                //   `declared` and `n` are usize, which is UNSIGNED. If declared
                //   is 1 and n is 2, then `declared - n` in a RELEASE build wraps
                //   to 18446744073709551615. A length-prefix that underflows and
                //   is then used as a length is the origin of an enormous number
                //   of real CVEs in network parsers.
                //   checked_sub returns Option<usize>: None on underflow.
                // JAVA: there is no checked_sub. `1 - 2` on int gives -1 silently;
                //   faked unsigned semantics give garbage. Rust does not prevent
                //   this automatically -- but it gives you a ONE-WORD way to
                //   prevent it, and it PANICS IN DEBUG BUILDS if you forget.
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

            // LEARN: three guards, each preventing a distinct failure mode:
            // LEARN: zero-length would loop forever without consuming -- a HANG,
            //   not a crash, which is worse to diagnose.
            if body_len == 0 {
                self.desynced = true;
                return Step::Desynced("zero-length frame");
            }
            // LEARN: oversized means a bogus length prefix. Without this, one
            //   garbage read tries to allocate gigabytes. THE classic
            //   length-prefix DoS.
            if body_len > self.cfg.max_frame_bytes {
                self.desynced = true;
                return Step::Desynced("frame exceeds max_frame_bytes");
            }
            if self.buf.len() < n + body_len {
                break; // partial frame, wait for more bytes
                // LEARN: note the already-completed frames in `out` are still
                //   returned below. Correct incremental parsing.
            }

            // LEARN: `advance(n)` skips the prefix by moving the buffer's START
            //   POINTER forward. No copy, no shifting of the remaining bytes.
            // JAVA: ByteBuffer.position(pos + n).
            self.buf.advance(n);
            // LEARN: `split_to(body_len)` removes the first body_len bytes and
            //   returns them as their own BytesMut -- NO COPY, it splits the
            //   allocation's ownership in two. `.freeze()` converts BytesMut ->
            //   Bytes (immutable, shareable), also no copy.
            //   So reassembling a frame is POINTER ARITHMETIC AND A REFCOUNT.
            //   Zero bytes are copied from the moment they land in self.buf.
            // LEARN: the pinning caveat from proxy.rs applies in principle here
            //   too, but self.buf only grows to the size of a partially received
            //   frame, so the ratio is bounded and small.
            // JAVA: Netty's ByteBuf.readSlice() is the direct equivalent, but
            //   needs manual retain/release and its own leak detector. Plain
            //   ByteBuffer cannot do this safely at all.
            out.push(self.buf.split_to(body_len).freeze());
        }

        Step::Frames(out)
    }
}

// LEARN: `#[cfg(test)]` means THIS MODULE IS COMPILED ONLY DURING `cargo test`.
//   Not "stripped later" -- never compiled into the release binary at all.
// JAVA: Java puts tests in src/test/java and Maven excludes them from the jar;
//   the effect is similar. But the Rust version lives IN THE SAME FILE, which
//   means tests can access PRIVATE items (Framer.desynced is private and the
//   test module still reaches it). In Java you would need package-private
//   visibility -- weakening your API for everyone -- or reflection.
//   TRADE: source files get long. PAYOFF: tests sit beside the code they test
//   and never go stale from a rename.
#[cfg(test)]
mod tests {
    // LEARN: `use super::*` imports everything from the parent module, INCLUDING
    //   private items.
    use super::*;

    fn cfg(includes_prefix: bool) -> FramingCfg {
        FramingCfg {
            mode: "length_prefix".into(),
            prefix_bytes: 2,
            big_endian: true,
            length_includes_prefix: includes_prefix,
            max_frame_bytes: 1024,
        }
    }

    // LEARN: panicking in a test helper is fine and idiomatic -- a panic fails
    //   the test, which is exactly what you want on the wrong variant.
    fn frames(step: Step) -> Vec<Bytes> {
        match step {
            Step::Frames(f) => f,
            _ => panic!("expected frames"),
        }
    }

    // LEARN: `#[test]` is @Test. BUILT INTO THE LANGUAGE -- no JUnit dependency,
    //   no test runner to configure. `cargo test` finds and runs them.
    #[test]
    fn reassembles_across_arbitrary_chunk_boundaries() {
        let mut f = Framer::new(cfg(false));
        // Two 3-byte frames, delivered one byte at a time.
        //
        // LEARN: `b'a'` is a BYTE LITERAL of type u8 (value 97). `b"abc"` is a
        //   byte-string literal of type &'static [u8; 3].
        // JAVA: has neither. You write `(byte) 'a'` and `"abc".getBytes(UTF_8)`,
        //   and the latter ALLOCATES on every call.
        let wire = [0x00, 0x03, b'a', b'b', b'c', 0x00, 0x03, b'x', b'y', b'z'];
        let mut got = Vec::new();
        // LEARN: THIS IS THE IMPORTANT TEST. It feeds the wire ONE BYTE AT A
        //   TIME, proving the framer reassembles across arbitrary TCP
        //   segmentation. Assuming one read() yields one message is the #1 bug in
        //   hand-rolled protocol code.
        for b in wire {
            got.extend(frames(f.push(&[b])));
        }
        // LEARN: `Bytes::from_static(b"abc")` points DIRECTLY at the binary's
        //   read-only data -- no allocation AND no refcount; the static case is
        //   special-cased.
        // LEARN: `assert_eq!` prints BOTH values on failure. The macro captures
        //   the source expressions, so the failure output shows what was compared
        //   without you writing a message.
        assert_eq!(got, vec![Bytes::from_static(b"abc"), Bytes::from_static(b"xyz")]);
    }

    #[test]
    fn splits_multiple_frames_in_one_read() {
        let mut f = Framer::new(cfg(false));
        let got = frames(f.push(&[0x00, 0x01, b'a', 0x00, 0x02, b'b', b'c']));
        assert_eq!(got, vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]);
    }

    #[test]
    fn honours_length_includes_prefix() {
        let mut f = Framer::new(cfg(true));
        // declared 5 = 2 prefix + 3 body
        let got = frames(f.push(&[0x00, 0x05, b'a', b'b', b'c']));
        assert_eq!(got, vec![Bytes::from_static(b"abc")]);
    }

    #[test]
    fn oversized_frame_desyncs_and_stays_desynced() {
        let mut f = Framer::new(cfg(false));
        // LEARN: `matches!(expr, pattern)` is a macro returning bool if the
        //   expression matches the pattern. `Step::Desynced(_)` means "the
        //   Desynced variant, don't care about the payload".
        // JAVA: Java 16+ `instanceof` patterns are closest, but they cannot
        //   destructure enum variants like this.
        assert!(matches!(f.push(&[0xFF, 0xFF]), Step::Desynced(_)));
        // LEARN: this second assertion encodes the STICKY property -- once
        //   desynced, always desynced. A behavioural invariant, not just a
        //   return value, and it is tested.
        assert!(matches!(f.push(&[0x00, 0x01, b'a']), Step::Ignored));
    }

    #[test]
    fn raw_mode_passes_chunks_through() {
        let mut c = cfg(false);
        c.mode = "raw".into();
        let mut f = Framer::new(c);
        assert_eq!(frames(f.push(b"anything")), vec![Bytes::from_static(b"anything")]);
    }
}
