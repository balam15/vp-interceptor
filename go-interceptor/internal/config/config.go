// Package config reads and validates the TOML schema.
//
// THE STRUCT DEFINITIONS BELOW *ARE* THE SCHEMA. There is no separate schema
// file and no runtime reflection beyond the decoder.
//
// Reads the SAME ../config.toml as the Rust and Java builds -- deliberately, so
// a comparison between the three implementations cannot be skewed by one of them
// quietly running different settings.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the whole file.
//
// GO: unlike serde, the TOML decoder has no per-field "default" attribute. The
// equivalent is Defaults() below: it returns a fully-populated Config and the
// decoder overwrites ONLY the keys present in the file. Same result -- every
// field is a concrete value by the time Load returns, so NOTHING DOWNSTREAM EVER
// NIL-CHECKS A CONFIG VALUE -- and the defaults live in one greppable place.
type Config struct {
	Listen   Listen   `toml:"listen"`
	Upstream Upstream `toml:"upstream"`
	Proxy    Proxy    `toml:"proxy"`
	Tee      Tee      `toml:"tee"`
	Framing  Framing  `toml:"framing"`
	Parse    Parse    `toml:"parse"`
	Timing   Timing   `toml:"timing"`
	Kafka    Kafka    `toml:"kafka"`
	Admin    Admin    `toml:"admin"`
}

type Listen struct {
	// Addr is where VP connects instead of connecting to FMS directly.
	Addr string `toml:"addr"`
	// MaxConnections is a hard cap. Excess connections are accepted and closed
	// immediately rather than queued, so VP sees a fast failure.
	MaxConnections int `toml:"max_connections"`
}

type Upstream struct {
	// Addr is the real FMS endpoint.
	Addr             string `toml:"addr"`
	ConnectTimeoutMs int    `toml:"connect_timeout_ms"`
}

func (u Upstream) ConnectTimeout() time.Duration {
	return time.Duration(u.ConnectTimeoutMs) * time.Millisecond
}

type Proxy struct {
	ReadBufferBytes int `toml:"read_buffer_bytes"`
	// Nodelay sets TCP_NODELAY on both sockets. Leave it on: Nagle can add ~40ms
	// to small ISO 8583 messages, which dwarfs everything else this program does.
	Nodelay bool `toml:"nodelay"`
	// IdleTimeoutMs closes an idle connection after this long. 0 disables.
	IdleTimeoutMs int `toml:"idle_timeout_ms"`
}

// IdleTimeout converts the "0 means disabled" sentinel into a duration once, at
// the boundary, so the hot loop never re-checks a magic number.
func (p Proxy) IdleTimeout() time.Duration {
	return time.Duration(p.IdleTimeoutMs) * time.Millisecond
}

type Tee struct {
	// Shards is the number of framing/publish workers. Connections are assigned
	// conn_id % shards, so each worker owns its framer state without locking.
	Shards int `toml:"shards"`
	// QueueCapacity is the per-shard bound. When full, frames are DROPPED and
	// counted. THIS BOUND IS THE ISOLATION BOUNDARY between Kafka and the hot
	// path.
	QueueCapacity int `toml:"queue_capacity"`
	// PublishDirections is one of: both | vp_to_fms | fms_to_vp.
	PublishDirections string `toml:"publish_directions"`
}

type Framing struct {
	// Mode is length_prefix (reassemble whole messages -- what you want for
	// ISO 8583) or raw (publish socket reads as-is).
	Mode string `toml:"mode"`
	// PrefixBytes is 2 or 4.
	PrefixBytes          int  `toml:"prefix_bytes"`
	BigEndian            bool `toml:"big_endian"`
	LengthIncludesPrefix bool `toml:"length_includes_prefix"`
	// MaxFrameBytes: a frame larger than this means we are desynced.
	MaxFrameBytes int `toml:"max_frame_bytes"`
}

// Timing is per-connection measurement, computed on the tee workers from
// timestamps taken as frames are reassembled, so it never touches the hot path.
type Timing struct {
	Enabled bool `toml:"enabled"`
	// PairRequestResponse pairs each vp_to_fms frame with the next fms_to_vp
	// frame on the same connection to produce rtt_ms.
	//
	// ASSUMES responses return in request order. True for a strict
	// request/response link; if VP pipelines and FMS may answer out of order,
	// rtt_ms values will be mismatched -- set this false and rely on gap_ms.
	PairRequestResponse bool `toml:"pair_request_response"`
	// MaxPending caps outstanding unmatched requests per connection, so memory
	// cannot grow without bound when responses stop arriving.
	MaxPending int `toml:"max_pending"`
}

// Parse is optional field extraction for the Kafka envelope. Never affects
// forwarding, and a parse failure never suppresses a publish.
type Parse struct {
	// Mode is none | key_value.
	Mode          string `toml:"mode"`
	PairDelimiter string `toml:"pair_delimiter"`
	KvDelimiter   string `toml:"kv_delimiter"`
	Trim          bool   `toml:"trim"`
	// MaxFields guards against a desynced stream producing an unbounded map.
	MaxFields int `toml:"max_fields"`
}

type Kafka struct {
	Enabled bool   `toml:"enabled"`
	Brokers string `toml:"brokers"`
	Topic   string `toml:"topic"`
	// ValueFormat is json (metadata envelope) or raw (the frame byte-for-byte,
	// metadata in headers only).
	ValueFormat string `toml:"value_format"`
	// PayloadEncoding is base64 | hex | utf8 -- how the frame is represented
	// inside the JSON envelope. Ignored when ValueFormat is raw.
	PayloadEncoding string `toml:"payload_encoding"`
	// Acks is "0", "1" or "all". Fire-and-forget: acks=1 plus a short message
	// timeout means a struggling broker sheds load instead of backing up.
	Acks                      string `toml:"acks"`
	Compression               string `toml:"compression"`
	LingerMs                  int    `toml:"linger_ms"`
	MessageTimeoutMs          int    `toml:"message_timeout_ms"`
	QueueBufferingMaxMessages int    `toml:"queue_buffering_max_messages"`
	QueueBufferingMaxKbytes   int    `toml:"queue_buffering_max_kbytes"`
	// Properties is the librdkafka-style escape hatch shared with the Rust
	// build. See internal/kafka: the security-related keys are translated to
	// their kafka-go equivalents, and anything unrecognised is logged and
	// ignored rather than silently dropped.
	Properties map[string]string `toml:"properties"`
}

type Admin struct {
	// Addr serves Prometheus text on /metrics, JSON on /, liveness on /healthz.
	Addr string `toml:"addr"`
}

// Defaults returns a fully-populated Config. Every default in the program lives
// here and nowhere else.
//
// Note the values match rust-interceptor/src/config.rs exactly, including
// parse.mode defaulting to key_value while the shipped config.toml sets it to
// none (see the long comment there -- ISO 8583 must not be key=value parsed).
func Defaults() *Config {
	return &Config{
		Listen:   Listen{Addr: "0.0.0.0:9100", MaxConnections: 4096},
		Upstream: Upstream{Addr: "127.0.0.1:8583", ConnectTimeoutMs: 3000},
		Proxy:    Proxy{ReadBufferBytes: 16384, Nodelay: true, IdleTimeoutMs: 0},
		Tee:      Tee{Shards: 4, QueueCapacity: 8192, PublishDirections: "both"},
		Framing: Framing{
			Mode:                 "length_prefix",
			PrefixBytes:          2,
			BigEndian:            true,
			LengthIncludesPrefix: false,
			MaxFrameBytes:        65536,
		},
		Parse: Parse{
			Mode:          "key_value",
			PairDelimiter: ",",
			KvDelimiter:   "=",
			Trim:          true,
			MaxFields:     64,
		},
		Timing: Timing{Enabled: true, PairRequestResponse: true, MaxPending: 256},
		Kafka: Kafka{
			Enabled:                   true,
			ValueFormat:               "json",
			PayloadEncoding:           "base64",
			Acks:                      "1",
			Compression:               "lz4",
			LingerMs:                  5,
			MessageTimeoutMs:          5000,
			QueueBufferingMaxMessages: 100_000,
			QueueBufferingMaxKbytes:   262_144,
			Properties:                map[string]string{},
		},
		Admin: Admin{Addr: "127.0.0.1:9101"},
	}
}

// Load reads, decodes and validates the file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}
	cfg := Defaults()
	md, err := toml.Decode(string(raw), cfg)
	if err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	// Required keys. The Rust build expresses these as fields WITHOUT a serde
	// default, so a missing [listen] fails to parse. Here the decoder cannot
	// tell "absent" from "defaulted", so we ask the metadata directly.
	for _, key := range [][]string{
		{"listen", "addr"},
		{"upstream", "addr"},
		{"kafka", "brokers"},
		{"kafka", "topic"},
	} {
		if !md.IsDefined(key...) {
			return nil, fmt.Errorf("config %s: missing required key [%s] %s", path, key[0], key[1])
		}
	}

	// Unknown keys are a config typo, and a typo in a payments config is worth
	// a log line rather than a silent no-op. Not fatal: an operator adding a key
	// for a newer build must not be unable to start this one.
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		for _, k := range undecoded {
			fmt.Fprintf(os.Stderr, "config %s: unknown key %q (ignored)\n", path, k.String())
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate is why framing.Framer can assume PrefixBytes is 2 or 4 without
// re-checking. THE INVARIANT IS ESTABLISHED AT THE BOUNDARY AND RELIED ON IN THE
// CORE.
//
// Every message names the exact TOML key and the constraint: config errors are
// read by operators at 3am.
func (c *Config) Validate() error {
	if c.Tee.Shards < 1 {
		return fmt.Errorf("tee.shards must be >= 1")
	}
	if c.Tee.QueueCapacity < 1 {
		return fmt.Errorf("tee.queue_capacity must be >= 1")
	}
	if c.Proxy.ReadBufferBytes < 512 {
		return fmt.Errorf("proxy.read_buffer_bytes must be >= 512")
	}
	switch c.Framing.Mode {
	case "raw":
	case "length_prefix":
		if c.Framing.PrefixBytes != 2 && c.Framing.PrefixBytes != 4 {
			return fmt.Errorf("framing.prefix_bytes must be 2 or 4")
		}
	default:
		return fmt.Errorf("framing.mode must be 'length_prefix' or 'raw', got %q", c.Framing.Mode)
	}
	if c.Framing.MaxFrameBytes < 1 {
		return fmt.Errorf("framing.max_frame_bytes must be >= 1")
	}
	switch c.Tee.PublishDirections {
	case "both", "vp_to_fms", "fms_to_vp":
	default:
		return fmt.Errorf("tee.publish_directions must be both|vp_to_fms|fms_to_vp, got %q",
			c.Tee.PublishDirections)
	}
	switch c.Parse.Mode {
	case "none", "key_value":
	default:
		return fmt.Errorf("parse.mode must be 'none' or 'key_value', got %q", c.Parse.Mode)
	}
	if c.Parse.PairDelimiter == "" || c.Parse.KvDelimiter == "" {
		return fmt.Errorf("parse.pair_delimiter and parse.kv_delimiter must be non-empty")
	}
	if c.Parse.MaxFields < 1 {
		return fmt.Errorf("parse.max_fields must be >= 1")
	}
	switch c.Kafka.ValueFormat {
	case "json", "raw":
	default:
		return fmt.Errorf("kafka.value_format must be 'json' or 'raw', got %q", c.Kafka.ValueFormat)
	}
	switch c.Kafka.PayloadEncoding {
	case "base64", "hex", "utf8":
	default:
		return fmt.Errorf("kafka.payload_encoding must be base64|hex|utf8, got %q",
			c.Kafka.PayloadEncoding)
	}
	if c.Timing.MaxPending < 1 {
		return fmt.Errorf("timing.max_pending must be >= 1")
	}
	return nil
}
