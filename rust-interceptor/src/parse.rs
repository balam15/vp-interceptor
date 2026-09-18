// ============================================================================
// parse.rs -- optional key=value field extraction for the Kafka envelope.
//
// THIS FILE IS THE CLEANEST DEMONSTRATION OF BORROWING IN THE CODEBASE: the
// entire parse is zero-allocation right up to the final .to_string() calls,
// which happen exactly where ownership genuinely has to transfer.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 7.
// ============================================================================

use std::collections::BTreeMap;

use crate::config::Parse as ParseCfg;

/// Splits a `key=value` delimited frame into named fields for the Kafka envelope.
///
/// Runs on a tee shard worker, never on the forwarding path, so nothing here can
/// affect VP<->FMS latency. A parse failure is reported in the envelope and never
/// suppresses the publish -- a message you cannot parse is still evidence.
// LEARN: delimiters stored as u8, NOT String -- resolved once at construction.
//   THE WHOLE STRUCT IS 24 BYTES WITH NO HEAP ALLOCATION AT ALL, so it can live
//   inline inside Publisher.
// JAVA: the equivalent holds String fields: two object references, two String
//   objects, two byte[]s. SIX ALLOCATIONS FOR TWO CHARACTERS.
pub struct KvParser {
    pair_delim: u8,
    kv_delim: u8,
    trim: bool,
    max_fields: usize,
}

impl KvParser {
    // LEARN: A CONSTRUCTOR RETURNING Option<Self> -- "there may be no parser".
    //   Because the return type is Option, EVERY CALLER IS FORCED to handle
    //   absence.
    // JAVA: you would return null from a factory and hope the caller checks, or
    //   return Optional<KvParser> which most people will not bother with. Here
    //   it is simply the natural way to write it.
    pub fn new(cfg: &ParseCfg) -> Option<Self> {
        if cfg.mode != "key_value" {
            return None;
        }
        Some(KvParser {
            pair_delim: first_byte(&cfg.pair_delimiter, b','),
            kv_delim: first_byte(&cfg.kv_delimiter, b'='),
            trim: cfg.trim,
            max_fields: cfg.max_fields,
        })
    }

    /// `Ok(fields)` on success, `Err(reason)` on anything malformed. The caller
    /// publishes either way.
    // LEARN: the error type is a plain String message. Fine for a diagnostic that
    //   gets embedded in the envelope; a library would use a typed error enum.
    pub fn parse(&self, frame: &[u8]) -> Result<BTreeMap<String, String>, String> {
        // LEARN: `std::str::from_utf8` VALIDATES that the bytes are UTF-8 and
        //   returns Result<&str, Utf8Error> -- a &str, a BORROWED VIEW of the
        //   original bytes. NO ALLOCATION, NO COPY. The returned &str points
        //   into `frame`.
        // JAVA: `new String(bytes, UTF_8)`
        //   (a) ALWAYS allocates a new String and copies, and
        //   (b) SILENTLY REPLACES invalid sequences with U+FFFD rather than
        //       telling you.
        //   Point (b) is a real correctness difference: in a payments context,
        //   silently corrupting a byte you could not decode is much worse than
        //   being told the frame was not text. Rust makes you CHOOSE:
        //     from_utf8       -- strict, borrowed, fails loudly  (used here)
        //     from_utf8_lossy -- replaces, may borrow            (see kafka.rs)
        // LEARN: `.map_err(...)?` converts Utf8Error -> String, then propagates.
        let text = std::str::from_utf8(frame).map_err(|e| format!("not valid utf-8: {e}"))?;

        let mut out = BTreeMap::new();
        // LEARN: `.split(char)` returns a LAZY ITERATOR OF &str SLICES, all
        //   borrowing from `text`, which borrows from `frame`. ZERO ALLOCATIONS
        //   FOR THE ENTIRE SPLIT.
        // JAVA: String.split(String) compiles a regex (or takes a fast path for a
        //   single char), then allocates a String[] PLUS A NEW String PER
        //   ELEMENT. For a 5-field message that is 6 allocations per parse, per
        //   message. Rust does zero.
        // LEARN: `.enumerate()` pairs each item with its index, yielding
        //   (usize, &str), destructured directly in the `for` pattern.
        // JAVA: needs a manual counter or IntStream.range.
        // LEARN: `self.pair_delim as char` -- u8 to char cast, valid for ASCII.
        for (i, pair) in text.split(self.pair_delim as char).enumerate() {
            // LEARN: SHADOWING -- the new `pair` (trimmed) replaces the old one
            //   for the rest of the loop body. Both are &str slices INTO THE SAME
            //   BUFFER: trimming just moves the start/end pointers, NO ALLOCATION.
            // JAVA: String.trim() allocates a new String unless nothing changed.
            //   And Java FORBIDS shadowing a local outright, so you would need a
            //   second name like `pairTrimmed`.
            let pair = if self.trim { pair.trim() } else { pair };
            if pair.is_empty() {
                continue; // tolerate trailing or doubled delimiters
            }
            if i >= self.max_fields {
                return Err(format!("more than {} fields", self.max_fields));
            }
            // LEARN: `split_once` SPLITS AT THE FIRST OCCURRENCE ONLY, returning
            //   Option<(&str, &str)>. The test below explains exactly why this
            //   matters: with split('=') a base64 value like "YWJjZA==" is
            //   mangled into ["YWJjZA", "", ""]. split_once yields the whole
            //   value intact.
            // JAVA: the equivalent is split("=", 2) -- THE LIMIT ARGUMENT THAT
            //   EVERYONE FORGETS. Rust gives the SAFE operation its own name, so
            //   you must opt INTO the dangerous one.
            // LEARN: `Some((k, v))` destructures the tuple INSIDE the Option, in
            //   the pattern itself.
            match pair.split_once(self.kv_delim as char) {
                Some((k, v)) => {
                    let (k, v) = if self.trim {
                        (k.trim(), v.trim())
                    } else {
                        (k, v)
                    };
                    if k.is_empty() {
                        return Err(format!("empty key in segment {i}"));
                    }
                    // LEARN: HERE we finally DO allocate, because the BTreeMap
                    //   must OWN its data -- the borrowed &str points into
                    //   `frame`, which dies at the end of publish(). Two
                    //   allocations per field, unavoidable given the ownership
                    //   requirement.
                    // LEARN: THE POINT -- everything up to this line was
                    //   zero-allocation. The allocation happens exactly once,
                    //   exactly where ownership genuinely transfers. THAT IS THE
                    //   OWNERSHIP MODEL PAYING OFF: it tells you precisely where
                    //   the costs are, instead of scattering them invisibly.
                    out.insert(k.to_string(), v.to_string());
                }
                None => {
                    return Err(format!(
                        "segment {i} has no '{}' separator",
                        self.kv_delim as char
                    ))
                }
            }
        }

        if out.is_empty() {
            // LEARN: `.into()` converts &str -> String, target type inferred from
            //   the function's declared error type.
            return Err("no fields found".into());
        }
        Ok(out)
    }
}

// LEARN: a four-combinator chain worth unpacking:
//   .as_bytes()  -- &str -> &[u8]. FREE; a `str` IS UTF-8 bytes.
//   .first()     -- Option<&u8>. None for an empty slice, no exception thrown.
//   .copied()    -- Option<&u8> -> Option<u8>, dereferencing (u8 is Copy).
//   .unwrap_or() -- the value, or the default.
//   One line, no hand-written branches, NO POSSIBLE PANIC.
// JAVA: `s.isEmpty() ? fallback : (byte) s.charAt(0)` -- fine, but note the Rust
//   version COMPOSES: each step is a small named operation you can reason about
//   independently.
fn first_byte(s: &str, fallback: u8) -> u8 {
    s.as_bytes().first().copied().unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ParseCfg {
        ParseCfg {
            mode: "key_value".into(),
            pair_delimiter: ",".into(),
            kv_delimiter: "=".into(),
            trim: true,
            max_fields: 64,
        }
    }

    #[test]
    fn parses_the_documented_shape() {
        // LEARN: `.unwrap()` PANICS on None/Err. It is the "I know this cannot
        //   fail" escape hatch -- the moral equivalent of Optional.get().
        //   Fine in a test (a panic fails the test, which is what you want).
        //   Used carelessly in production code it is the #1 source of Rust panics.
        let p = KvParser::new(&cfg()).unwrap();
        let got = p
            .parse(b"accountName=Zacky,accountNumber=11020134353,bankCode=1234")
            .unwrap();
        assert_eq!(got.get("accountName").unwrap(), "Zacky");
        assert_eq!(got.get("accountNumber").unwrap(), "11020134353");
        assert_eq!(got.get("bankCode").unwrap(), "1234");
    }

    #[test]
    fn trims_whitespace_around_pairs_and_values() {
        let p = KvParser::new(&cfg()).unwrap();
        let got = p.parse(b"accountName = Zacky , bankCode = 1234 ").unwrap();
        assert_eq!(got.get("accountName").unwrap(), "Zacky");
        assert_eq!(got.get("bankCode").unwrap(), "1234");
    }

    #[test]
    fn tolerates_trailing_and_doubled_delimiters() {
        let p = KvParser::new(&cfg()).unwrap();
        let got = p.parse(b"a=1,,b=2,").unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn keeps_equals_signs_inside_the_value() {
        // split_once, not split -- a base64 value ending in '=' must survive.
        let p = KvParser::new(&cfg()).unwrap();
        let got = p.parse(b"token=YWJjZA==,bankCode=1234").unwrap();
        assert_eq!(got.get("token").unwrap(), "YWJjZA==");
    }

    #[test]
    fn reports_missing_separator_rather_than_guessing() {
        let p = KvParser::new(&cfg()).unwrap();
        // LEARN: `.is_err()` -- Result has is_ok()/is_err() predicates.
        assert!(p.parse(b"accountName=Zacky,garbage").is_err());
    }

    #[test]
    fn reports_non_utf8_rather_than_panicking() {
        let p = KvParser::new(&cfg()).unwrap();
        // LEARN: THE TEST NAME IS THE CONTRACT. Java's `new String(bytes, UTF_8)`
        //   would silently produce replacement characters here instead.
        assert!(p.parse(&[0xff, 0xfe, 0x00]).is_err());
    }

    #[test]
    fn disabled_when_mode_is_none() {
        let mut c = cfg();
        c.mode = "none".into();
        assert!(KvParser::new(&c).is_none());
    }
}
