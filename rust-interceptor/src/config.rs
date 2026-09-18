// ============================================================================
// config.rs -- the TOML schema, its defaults, and its validation.
//
// THE STRUCT DEFINITIONS BELOW *ARE* THE SCHEMA. There is no separate schema
// file, no runtime reflection, and no null anywhere in the result.
//
// Comment tags: // LEARN: Rust semantics   // JAVA: the comparison
// Everything else is original design rationale. See ../RUST_EXPLAINED.md Part 10.
// ============================================================================

use std::collections::BTreeMap;

use serde::Deserialize;

// LEARN: `#[derive(Deserialize)]` GENERATES A TOML PARSER AT COMPILE TIME (see
//   the long serde note in kafka.rs). No reflection, no runtime schema building.
//
// LEARN: REQUIRED VS OPTIONAL IS EXPRESSED BY THE ABSENCE OR PRESENCE OF
//   #[serde(default)]:
//     `listen` and `upstream` have NO default -> the section is MANDATORY, and a
//       config missing [listen] fails to parse with a precise message.
//     the rest have #[serde(default)] -> use the type's Default impl if absent.
// JAVA: Jackson's @JsonProperty(required = true) is only honoured for creator
//   parameters, and validation is largely runtime and partial. Here it is
//   structural and total.
//
// LEARN: THERE IS NO Optional AND NO null ANYWHERE IN THIS CONFIG. Every field
//   is a concrete value by the time load() returns, so NOTHING DOWNSTREAM EVER
//   NULL-CHECKS A CONFIG VALUE. That is a substantial ongoing simplification you
//   get for free from the type system.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub listen: Listen,
    pub upstream: Upstream,
    #[serde(default)]
    pub proxy: Proxy,
    #[serde(default)]
    pub tee: Tee,
    #[serde(default)]
    pub framing: Framing,
    #[serde(default)]
    pub parse: Parse,
    #[serde(default)]
    pub timing: Timing,
    #[serde(default)]
    pub debug_payload: DebugPayload,
    pub kafka: Kafka,
    #[serde(default)]
    pub admin: Admin,
}

#[derive(Debug, Deserialize)]
pub struct Listen {
    // LEARN: the FIELD TYPE encodes a constraint too -- `String` means must be
    //   present and a string; `usize` means must be a non-negative integer.
    pub addr: String,
    // LEARN: `#[serde(default = "d_max_conns")]` takes THE NAME OF A FUNCTION AS
    //   A STRING. Rust attributes cannot hold arbitrary expressions, so serde
    //   takes a path as a string literal and RESOLVES IT AT MACRO-EXPANSION TIME.
    //   Misspell it and you get a COMPILE ERROR, not a runtime one.
    // JAVA: @JsonProperty(defaultValue = "4096") is a String that Jackson MOSTLY
    //   IGNORES -- for most types it is documentation-only. A genuine Jackson
    //   footgun. Rust's version actually works and is checked.
    #[serde(default = "d_max_conns")]
    pub max_connections: usize,
}

#[derive(Debug, Deserialize)]
pub struct Upstream {
    pub addr: String,
    #[serde(default = "d_connect_timeout")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct Proxy {
    #[serde(default = "d_read_buf")]
    pub read_buffer_bytes: usize,
    #[serde(default = "d_true")]
    pub nodelay: bool,
    // LEARN: bare `#[serde(default)]` on a field uses the FIELD TYPE's Default
    //   (0 for u64), rather than naming a function.
    // LEARN: proxy.rs converts this "0 means disabled" sentinel into an
    //   Option<Duration> once, at the top of pump(), so the hot loop never
    //   re-checks a magic number.
    #[serde(default)]
    pub idle_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct Tee {
    #[serde(default = "d_shards")]
    pub shards: usize,
    #[serde(default = "d_queue_cap")]
    pub queue_capacity: usize,
    #[serde(default = "d_directions")]
    pub publish_directions: String,
}

// LEARN: NOTE `Clone` HERE BUT NOT ON `Config`. Framing, Timing and Parse are
//   Clone because each tee worker takes its own copy (tee.rs). Config is NOT
//   Clone because it is only ever shared via Arc. THE DERIVED TRAITS DOCUMENT
//   THE INTENDED SHARING MODEL, and the compiler enforces it -- you cannot
//   accidentally deep-copy a Config.
#[derive(Debug, Clone, Deserialize)]
pub struct Framing {
    #[serde(default = "d_framing_mode")]
    pub mode: String,
    #[serde(default = "d_prefix_bytes")]
    pub prefix_bytes: usize,
    #[serde(default = "d_true")]
    pub big_endian: bool,
    #[serde(default)]
    pub length_includes_prefix: bool,
    #[serde(default = "d_max_frame")]
    pub max_frame_bytes: usize,
}

/// Per-connection timing measurement. Computed on the tee workers from
/// timestamps taken as frames are reassembled, so it never touches the hot path.
#[derive(Debug, Clone, Deserialize)]
pub struct Timing {
    #[serde(default = "d_true")]
    pub enabled: bool,
    /// Pair each `vp_to_fms` frame with the next `fms_to_vp` frame on the same
    /// connection to produce `rtt_ms`.
    ///
    /// ASSUMES responses return in request order on a given connection. True for
    /// a strict request/response link; if VP pipelines and FMS may answer out of
    /// order, `rtt_ms` values will be mismatched -- set this false.
    //
    // LEARN: AN ASSUMPTION DOCUMENTED ON THE CONFIG FIELD, where an operator
    //   reading the config will actually encounter it, with the remedy stated.
    //   This is exactly what a doc comment is for, and `cargo doc` renders it.
    #[serde(default = "d_true")]
    pub pair_request_response: bool,
    /// Cap on outstanding unmatched requests per connection. Prevents unbounded
    /// growth if responses stop arriving.
    #[serde(default = "d_max_pending")]
    pub max_pending: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DebugPayload {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "d_debug_payload_max_bytes")]
    pub max_bytes: usize,
}

/// Optional field extraction for the Kafka envelope. Never affects forwarding.
#[derive(Debug, Clone, Deserialize)]
pub struct Parse {
    /// none | key_value
    #[serde(default = "d_parse_mode")]
    pub mode: String,
    #[serde(default = "d_pair_delim")]
    pub pair_delimiter: String,
    #[serde(default = "d_kv_delim")]
    pub kv_delimiter: String,
    #[serde(default = "d_true")]
    pub trim: bool,
    /// Guard against a desynced stream producing an unbounded field map.
    #[serde(default = "d_max_fields")]
    pub max_fields: usize,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    #[serde(default = "d_true")]
    pub enabled: bool,
    pub brokers: String,
    pub topic: String,
    /// json | raw
    #[serde(default = "d_value_format")]
    pub value_format: String,
    /// base64 | hex | utf8 -- how the raw frame is represented inside the JSON
    /// envelope. Ignored when value_format = "raw".
    #[serde(default = "d_payload_encoding")]
    pub payload_encoding: String,
    #[serde(default = "d_acks")]
    pub acks: String,
    #[serde(default = "d_compression")]
    pub compression: String,
    #[serde(default = "d_linger")]
    pub linger_ms: u64,
    #[serde(default = "d_msg_timeout")]
    pub message_timeout_ms: u64,
    #[serde(default = "d_qbuf_msgs")]
    pub queue_buffering_max_messages: u64,
    #[serde(default = "d_qbuf_kb")]
    pub queue_buffering_max_kbytes: u64,
    #[serde(default)]
    pub filter: KafkaFilter,
    // LEARN: BTreeMap (a TreeMap) rather than HashMap, so the escape-hatch
    //   properties are applied in a DETERMINISTIC ORDER. Rust's HashMap uses a
    //   randomly-seeded hasher, so its iteration order genuinely varies run to
    //   run -- a deliberate choice that makes HashDoS impractical and stops
    //   anyone depending on the order.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Configurable Kafka frame filter.
///
/// `any_of` is OR across groups, `all_of` is AND within a group, and `mti` /
/// `de70` are exact-match sets.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KafkaFilter {
    #[serde(default)]
    pub any_of: Vec<KafkaFilterAnyOf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct KafkaFilterAnyOf {
    #[serde(default)]
    pub all_of: Vec<KafkaFilterAllOf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct KafkaFilterAllOf {
    #[serde(default)]
    pub mti: Vec<String>,
    #[serde(default)]
    pub de70: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Admin {
    #[serde(default = "d_admin_addr")]
    pub addr: String,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {path}: {e}"))?;
        // LEARN: THE TYPE ANNOTATION DRIVES THE DESERIALISATION. `from_str` is
        //   generic over its RETURN type (T: Deserialize), and the `: Config`
        //   annotation picks T = Config. Inference flows BACKWARDS from the
        //   annotation into the call.
        // JAVA: `mapper.readValue(raw, Config.class)` MUST pass the class token,
        //   because erasure means the return type is not available at runtime.
        //   Rust resolves it entirely at compile time.
        let cfg: Config = toml::from_str(&raw)?;
        // LEARN: `?` works on toml::Error here because anyhow::Error implements
        //   From<E> for any standard error type, and THE `?` OPERATOR
        //   AUTOMATICALLY APPLIES THAT CONVERSION. The From/Into traits doing
        //   implicit error widening is the one place Rust does an implicit
        //   conversion.
        cfg.validate()?;
        Ok(cfg)
    }

    // LEARN: THIS FUNCTION IS WHY framing.rs CAN WRITE
    //   `_ => unreachable!("prefix_bytes validated at config load")`.
    //   THE INVARIANT IS ESTABLISHED AT THE BOUNDARY AND RELIED ON IN THE CORE.
    //   That is the pattern the whole file follows, and it is what "parse, don't
    //   validate" means in practice.
    fn validate(&self) -> anyhow::Result<()> {
        // LEARN: `anyhow::bail!(msg)` is a macro expanding to
        //   `return Err(anyhow!(msg));`. It READS like a throw and IS a return.
        // LEARN: every message names the exact TOML key and the constraint.
        //   Config errors are read by operators at 3am; this is the right level
        //   of care.
        if self.tee.shards == 0 {
            anyhow::bail!("tee.shards must be >= 1");
        }
        if self.tee.queue_capacity == 0 {
            anyhow::bail!("tee.queue_capacity must be >= 1");
        }
        if self.proxy.read_buffer_bytes < 512 {
            anyhow::bail!("proxy.read_buffer_bytes must be >= 512");
        }
        match self.framing.mode.as_str() {
            "raw" => {}
            "length_prefix" => {
                // LEARN: `matches!(x, 2 | 4)` uses OR-PATTERNS -- "is x either 2
                //   or 4". Java 21's `case 2, 4 ->` is equivalent inside a switch,
                //   but Rust's works as a bool EXPRESSION anywhere.
                if !matches!(self.framing.prefix_bytes, 2 | 4) {
                    anyhow::bail!("framing.prefix_bytes must be 2 or 4");
                }
            }
            // LEARN: the catch-all arm BINDS the unmatched value, so the error
            //   message can quote what the operator actually typed. Small thing;
            //   enormously helpful in practice.
            other => anyhow::bail!("framing.mode must be 'length_prefix' or 'raw', got '{other}'"),
        }
        if !matches!(
            self.tee.publish_directions.as_str(),
            "both" | "vp_to_fms" | "fms_to_vp"
        ) {
            anyhow::bail!("tee.publish_directions must be both|vp_to_fms|fms_to_vp");
        }
        if !matches!(self.parse.mode.as_str(), "none" | "key_value") {
            anyhow::bail!(
                "parse.mode must be 'none' or 'key_value', got '{}'",
                self.parse.mode
            );
        }
        if self.parse.pair_delimiter.is_empty() || self.parse.kv_delimiter.is_empty() {
            anyhow::bail!("parse.pair_delimiter and parse.kv_delimiter must be non-empty");
        }
        if self.parse.max_fields == 0 {
            anyhow::bail!("parse.max_fields must be >= 1");
        }
        if self.debug_payload.enabled && self.debug_payload.max_bytes == 0 {
            anyhow::bail!(
                "debug_payload.max_bytes must be >= 1 when debug_payload.enabled is true"
            );
        }
        if !matches!(self.kafka.value_format.as_str(), "json" | "raw") {
            anyhow::bail!(
                "kafka.value_format must be 'json' or 'raw', got '{}'",
                self.kafka.value_format
            );
        }
        if !matches!(
            self.kafka.payload_encoding.as_str(),
            "base64" | "hex" | "utf8"
        ) {
            anyhow::bail!(
                "kafka.payload_encoding must be base64|hex|utf8, got '{}'",
                self.kafka.payload_encoding
            );
        }
        validate_kafka_filter(&self.kafka.filter)?;
        Ok(())
    }
}

fn validate_kafka_filter(filter: &KafkaFilter) -> anyhow::Result<()> {
    for (i, any_of) in filter.any_of.iter().enumerate() {
        if any_of.all_of.is_empty() {
            anyhow::bail!("kafka.filter.any_of[{i}].all_of must contain at least one rule");
        }
        for (j, all_of) in any_of.all_of.iter().enumerate() {
            if all_of.mti.is_empty() && all_of.de70.is_empty() {
                anyhow::bail!("kafka.filter.any_of[{i}].all_of[{j}] must set mti and/or de70");
            }
            for (k, mti) in all_of.mti.iter().enumerate() {
                if mti.len() != 4 || !mti.chars().all(|c| c.is_ascii_digit()) {
                    anyhow::bail!(
                        "kafka.filter.any_of[{i}].all_of[{j}].mti[{k}] must be a 4-digit MTI, got '{mti}'"
                    );
                }
            }
            for (k, de70) in all_of.de70.iter().enumerate() {
                if de70.len() != 3 || !de70.chars().all(|c| c.is_ascii_digit()) {
                    anyhow::bail!(
                        "kafka.filter.any_of[{i}].all_of[{j}].de70[{k}] must be a 3-digit code, got '{de70}'"
                    );
                }
            }
        }
    }
    Ok(())
}

// LEARN: MANUAL `Default` IMPLS, not #[derive(Default)]. Why? Because derive
//   would produce ALL ZEROS -- nodelay: false, read_buffer_bytes: 0 -- which is
//   wrong and would fail validate(). Written out explicitly, REUSING THE SAME
//   d_* FUNCTIONS SERDE USES, so the two paths cannot disagree.
// JAVA: field initialisers do this implicitly and invisibly. Rust makes the
//   default value a trait impl you can see, test, and reason about.
impl Default for Proxy {
    fn default() -> Self {
        Self {
            read_buffer_bytes: d_read_buf(),
            nodelay: true,
            idle_timeout_ms: 0,
        }
    }
}

impl Default for Tee {
    fn default() -> Self {
        Self {
            shards: d_shards(),
            queue_capacity: d_queue_cap(),
            publish_directions: d_directions(),
        }
    }
}

impl Default for Framing {
    fn default() -> Self {
        Self {
            mode: d_framing_mode(),
            prefix_bytes: d_prefix_bytes(),
            big_endian: true,
            length_includes_prefix: false,
            max_frame_bytes: d_max_frame(),
        }
    }
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            enabled: true,
            pair_request_response: true,
            max_pending: d_max_pending(),
        }
    }
}

impl Default for DebugPayload {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: d_debug_payload_max_bytes(),
        }
    }
}

impl Default for Parse {
    fn default() -> Self {
        Self {
            mode: d_parse_mode(),
            pair_delimiter: d_pair_delim(),
            kv_delimiter: d_kv_delim(),
            trim: true,
            max_fields: d_max_fields(),
        }
    }
}

impl Default for Admin {
    fn default() -> Self {
        Self {
            addr: d_admin_addr(),
        }
    }
}

// LEARN: EVERY DEFAULT IN ONE GREPPABLE BLOCK. The terse `d_` prefix keeps them
//   visually distinct from real logic.
// LEARN: they are FUNCTIONS rather than constants because serde's
//   `default = "..."` attribute takes a FUNCTION PATH. A `const` would not work
//   with that mechanism.
// LEARN: `100_000` uses UNDERSCORES AS DIGIT SEPARATORS -- exactly like Java's
//   100_000.
// LEARN: `"both".into()` converts &'static str -> String, an allocation, but
//   only ever at config-load time.
fn d_true() -> bool {
    true
}
fn d_max_conns() -> usize {
    4096
}
fn d_connect_timeout() -> u64 {
    3000
}
fn d_read_buf() -> usize {
    16384
}
fn d_shards() -> usize {
    4
}
fn d_queue_cap() -> usize {
    8192
}
fn d_directions() -> String {
    "both".into()
}
fn d_framing_mode() -> String {
    "length_prefix".into()
}
fn d_prefix_bytes() -> usize {
    2
}
fn d_max_frame() -> usize {
    65536
}
fn d_max_pending() -> usize {
    256
}
fn d_debug_payload_max_bytes() -> usize {
    4096
}
fn d_parse_mode() -> String {
    "key_value".into()
}
fn d_pair_delim() -> String {
    ",".into()
}
fn d_kv_delim() -> String {
    "=".into()
}
fn d_max_fields() -> usize {
    64
}
fn d_value_format() -> String {
    "json".into()
}
fn d_payload_encoding() -> String {
    "base64".into()
}
fn d_acks() -> String {
    "1".into()
}
fn d_compression() -> String {
    "lz4".into()
}
fn d_linger() -> u64 {
    5
}
fn d_msg_timeout() -> u64 {
    5000
}
fn d_qbuf_msgs() -> u64 {
    100_000
}
fn d_qbuf_kb() -> u64 {
    262_144
}
fn d_admin_addr() -> String {
    "127.0.0.1:9101".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_kafka_filter() {
        let mut cfg = Config {
            listen: Listen {
                addr: "0.0.0.0:9100".into(),
                max_connections: d_max_conns(),
            },
            upstream: Upstream {
                addr: "127.0.0.1:8583".into(),
                connect_timeout_ms: d_connect_timeout(),
            },
            proxy: Proxy::default(),
            tee: Tee::default(),
            framing: Framing::default(),
            parse: Parse::default(),
            timing: Timing::default(),
            debug_payload: DebugPayload::default(),
            kafka: Kafka {
                enabled: true,
                brokers: "localhost:9092".into(),
                topic: "vp.fms.iso8583".into(),
                value_format: d_value_format(),
                payload_encoding: d_payload_encoding(),
                acks: d_acks(),
                compression: d_compression(),
                linger_ms: d_linger(),
                message_timeout_ms: d_msg_timeout(),
                queue_buffering_max_messages: d_qbuf_msgs(),
                queue_buffering_max_kbytes: d_qbuf_kb(),
                filter: KafkaFilter {
                    any_of: vec![KafkaFilterAnyOf {
                        all_of: vec![KafkaFilterAllOf {
                            mti: vec!["0800".into(), "0810".into()],
                            de70: vec!["301".into()],
                        }],
                    }],
                },
                properties: BTreeMap::new(),
            },
            admin: Admin::default(),
        };
        assert!(cfg.validate().is_ok());
        cfg.kafka.filter.any_of[0].all_of[0].mti = vec!["81".into()];
        assert!(cfg.validate().is_err());
    }
}
