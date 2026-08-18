// Package kafka builds the producer and publishes frames.
//
// The contract with the rest of the program: this package never blocks the
// caller for long, never fails upward, and never connects eagerly. Kafka
// outcomes are counted and logged, never acted upon.
package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/parse"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
)

// enqueueTimeoutCap bounds how long a single Publish may wait on the writer.
//
// THIS IS THE GO EQUIVALENT OF THE JAVA BUILD'S max.block.ms=0. kafka-go's
// WriteMessages resolves topic metadata before batching, and that lookup is a
// network call. Async:true makes everything after it non-blocking, but the
// lookup itself would otherwise stall a tee worker for the full write timeout
// whenever the metadata cache is cold or the broker is gone. Capping the context
// turns "wait" into "shed and count", which is the trade this design always
// makes.
const enqueueTimeoutCap = 1 * time.Second

// Meta is the per-message metadata that accompanies every frame.
//
// The three timing fields are pointers so that "not measured" and "measured as
// zero" stay distinguishable, and so an absent one is omitted from the envelope
// rather than published as a misleading 0.
type Meta struct {
	ConnID    uint64
	Direction string
	Seq       uint64
	Peer      string
	TsMs      int64
	// ConnAgeMs is milliseconds since this connection was accepted.
	ConnAgeMs *float64
	// GapMs is milliseconds since the previous frame on this connection, either
	// direction. On a strict request/response link this is the FMS think-time.
	GapMs *float64
	// RttMs is request-to-response time, present on fms_to_vp frames only.
	RttMs *float64
}

// envelope is the Kafka message value when value_format = "json".
//
// FIELD ORDER HERE IS THE FIELD ORDER ON THE WIRE -- encoding/json emits struct
// fields in declaration order -- so keep the identifying fields first. It makes
// kafka-console-consumer output readable without piping through jq, and it keeps
// the three builds byte-identical.
type envelope struct {
	ConnID    uint64 `json:"conn_id"`
	Direction string `json:"direction"`
	Seq       uint64 `json:"seq"`
	Peer      string `json:"peer"`
	TsMs      int64  `json:"ts_ms"`
	Length    int    `json:"length"`

	ConnAgeMs *float64 `json:"conn_age_ms,omitempty"`
	GapMs     *float64 `json:"gap_ms,omitempty"`
	RttMs     *float64 `json:"rtt_ms,omitempty"`

	// Fields holds parsed key=value pairs when [parse] is enabled and the frame
	// parses. encoding/json sorts map keys, matching the Rust build's BTreeMap.
	Fields map[string]string `json:"fields,omitempty"`
	// ParseError says why parsing failed. Present INSTEAD OF Fields; the payload
	// is published either way.
	ParseError string `json:"parse_error,omitempty"`

	Encoding string `json:"encoding"`
	Payload  string `json:"payload"`
}

// Publisher is the fire-and-forget producer.
type Publisher struct {
	writer   *kgo.Writer
	client   *kgo.Client
	topic    string
	json     bool
	encoding string
	parser   *parse.KvParser
	stats    *stats.Stats
	log      *slog.Logger

	enqueueTimeout time.Duration

	enqueueFailLog  atomic.Uint64
	parseFailLog    atomic.Uint64
	deliveryFailLog atomic.Uint64
}

// Build creates the producer WITHOUT connecting: kafka-go resolves and connects
// lazily on first use. A broker that is down, unreachable, or rejecting the
// handshake cannot fail this call and therefore cannot delay or prevent the
// proxy from serving VP.
//
// Returns nil when Kafka is disabled, and logs-and-degrades rather than
// returning an error when the CONFIGURATION is unusable. Losing the audit feed
// is strictly better than dropping the payment path -- which is why the
// signature has no error to return in the first place.
func Build(cfg *config.Config, st *stats.Stats, log *slog.Logger) *Publisher {
	if !cfg.Kafka.Enabled {
		log.Info("kafka disabled by config; running as plain proxy")
		return nil
	}

	transport, err := buildTransport(cfg.Kafka, log)
	if err != nil {
		log.Error("kafka transport config unusable; continuing WITHOUT publishing", "error", err)
		return nil
	}

	brokers := splitBrokers(cfg.Kafka.Brokers)
	if len(brokers) == 0 {
		log.Error("kafka.brokers is empty; continuing WITHOUT publishing")
		return nil
	}

	p := &Publisher{
		topic:          cfg.Kafka.Topic,
		json:           cfg.Kafka.ValueFormat == "json",
		encoding:       cfg.Kafka.PayloadEncoding,
		parser:         parse.New(cfg.Parse),
		stats:          st,
		log:            log,
		enqueueTimeout: min(time.Duration(cfg.Kafka.MessageTimeoutMs)*time.Millisecond, enqueueTimeoutCap),
	}

	p.writer = &kgo.Writer{
		Addr:  kgo.TCP(brokers...),
		Topic: cfg.Kafka.Topic,
		// Key the partition on conn_id, so both directions of one connection
		// land on the same partition and stay ordered.
		Balancer:     &kgo.Hash{},
		RequiredAcks: requiredAcks(cfg.Kafka.Acks),
		Compression:  compression(cfg.Kafka.Compression, log),
		// THE setting this whole file exists to get right. Async means
		// WriteMessages hands the message to the writer's batching goroutines
		// and returns, instead of waiting for the broker to acknowledge.
		Async: true,
		// linger.ms. Must be > 0 or kafka-go substitutes its own 1s default.
		BatchTimeout:           max(time.Duration(cfg.Kafka.LingerMs)*time.Millisecond, time.Millisecond),
		WriteTimeout:           time.Duration(cfg.Kafka.MessageTimeoutMs) * time.Millisecond,
		BatchBytes:             1 << 20,
		Transport:              transport,
		AllowAutoTopicCreation: false,
		Completion:             p.completion,
		ErrorLogger:            kgo.LoggerFunc(p.logWriterError),
	}
	p.client = &kgo.Client{Addr: kgo.TCP(brokers...), Transport: transport}

	if cfg.Kafka.QueueBufferingMaxMessages > 0 || cfg.Kafka.QueueBufferingMaxKbytes > 0 {
		// Said once, at startup, rather than left as a silent no-op: these are
		// librdkafka accumulator bounds with no kafka-go equivalent. The
		// isolation boundary in this build is tee.queue_capacity.
		log.Info("kafka.queue_buffering_max_* have no kafka-go equivalent and are ignored",
			"isolation_bound", "tee.queue_capacity")
	}

	p.warmUpMetadataInBackground()

	log.Info("kafka producer created (lazy connect)",
		"brokers", cfg.Kafka.Brokers,
		"topic", cfg.Kafka.Topic,
		"value_format", cfg.Kafka.ValueFormat,
		"payload_encoding", cfg.Kafka.PayloadEncoding,
		"field_parsing", p.parser != nil)
	return p
}

// warmUpMetadataInBackground fetches topic metadata off-thread so the first real
// publishes do not pay for it under the capped enqueue timeout and get shed.
//
// The Java build needs this for the same reason (max.block.ms=0 makes a
// pre-metadata send fail instantly); librdkafka buffers instead and needs no
// warm-up. Runs in its own goroutine, so a dead broker still cannot delay
// serving.
func (p *Publisher) warmUpMetadataInBackground() {
	go func() {
		for i := 0; i < 60; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := p.client.Metadata(ctx, &kgo.MetadataRequest{Topics: []string{p.topic}})
			cancel()
			if err == nil {
				p.log.Info("kafka metadata ready", "topic", p.topic)
				return
			}
			time.Sleep(time.Second)
		}
		p.log.Warn("kafka metadata still unavailable; publishing will shed until the broker returns",
			"topic", p.topic)
	}()
}

// Publish enqueues one frame. Runs on a tee shard worker, never on the
// forwarding path, so the JSON and base64 allocations here cannot affect
// VP<->FMS latency.
//
// frame must be an independent copy -- the framer guarantees that.
func (p *Publisher) Publish(key string, meta *Meta, frame []byte) {
	value := frame
	if p.json {
		body, err := p.buildEnvelope(meta, frame)
		if err != nil {
			// Should be unreachable: every field is a plain scalar or an
			// already-encoded string.
			p.stats.KafkaEnqueueFailed.Add(1)
			p.log.Error("json encode failed; dropping frame", "error", err)
			return
		}
		value = body
	}

	msg := kgo.Message{
		Key:   []byte(key),
		Value: value,
		// Headers duplicate the envelope's metadata on purpose: a consumer can
		// route or filter on them without deserializing the body, and they still
		// work when value_format = "raw".
		Headers: []kgo.Header{
			{Key: "conn_id", Value: []byte(strconv.FormatUint(meta.ConnID, 10))},
			{Key: "direction", Value: []byte(meta.Direction)},
			{Key: "seq", Value: []byte(strconv.FormatUint(meta.Seq, 10))},
			{Key: "peer", Value: []byte(meta.Peer)},
			{Key: "ts_ms", Value: []byte(strconv.FormatInt(meta.TsMs, 10))},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.enqueueTimeout)
	defer cancel()
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		// Metadata missing, message too large, or the writer is shutting down.
		// Shed, never wait.
		p.stats.KafkaEnqueueFailed.Add(1)
		if n := p.enqueueFailLog.Add(1); n == 1 || n%1000 == 0 {
			p.log.Warn("kafka enqueue rejected; dropping", "failures", n, "error", err)
		}
		return
	}
	p.stats.KafkaEnqueued.Add(1)
}

func (p *Publisher) buildEnvelope(meta *Meta, frame []byte) ([]byte, error) {
	env := envelope{
		ConnID:    meta.ConnID,
		Direction: meta.Direction,
		Seq:       meta.Seq,
		Peer:      meta.Peer,
		TsMs:      meta.TsMs,
		Length:    len(frame),
		ConnAgeMs: meta.ConnAgeMs,
		GapMs:     meta.GapMs,
		RttMs:     meta.RttMs,
		Encoding:  p.encoding,
		Payload:   EncodePayload(p.encoding, frame),
	}

	// Parsing never gates publishing: a frame we cannot read is still published,
	// with the reason attached.
	if p.parser != nil {
		fields, err := p.parser.Parse(frame)
		if err != nil {
			env.ParseError = err.Error()
			if n := p.parseFailLog.Add(1); n == 1 || n%1000 == 0 {
				p.log.Warn("frame did not parse; publishing unparsed",
					"failures", n, "reason", err)
			}
		} else {
			env.Fields = fields
		}
	}

	return marshalEnvelope(&env)
}

// marshalEnvelope encodes without Go's default HTML escaping.
//
// GO: encoding/json rewrites <, > and & as < etc. unless told otherwise.
// serde does not, so leaving it on would make the Go build's messages differ
// from the other two for any payload or parsed field containing those bytes --
// a real possibility with utf8 encoding. The Encoder appends a newline, which is
// trimmed back off.
func marshalEnvelope(env *envelope) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(env.Payload) + 256)
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(env); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// EncodePayload renders the raw frame for the JSON envelope.
//
//	base64 -> safe for binary ISO 8583 (the default)
//	hex    -> the traditional ISO 8583 dump format, easier to read against a spec
//	utf8   -> text protocols ONLY; lossy on binary, do not use for evidence
func EncodePayload(encoding string, frame []byte) string {
	switch encoding {
	case "hex":
		const digits = "0123456789abcdef"
		// Exactly one allocation for the whole string, correctly sized up front.
		// A fmt.Sprintf per byte is surprisingly costly at payments volume.
		out := make([]byte, 0, len(frame)*2)
		for _, b := range frame {
			out = append(out, digits[b>>4], digits[b&0x0f])
		}
		return string(out)
	case "utf8":
		return string(frame)
	default:
		return base64.StdEncoding.EncodeToString(frame)
	}
}

// completion runs on kafka-go's background goroutines. We only count -- there is
// deliberately no retry or recovery logic, because the contract with the proxy
// is that Kafka outcomes are never acted upon.
func (p *Publisher) completion(msgs []kgo.Message, err error) {
	if err == nil {
		p.stats.KafkaDelivered.Add(uint64(len(msgs)))
		return
	}
	p.stats.KafkaDeliveryFailed.Add(uint64(len(msgs)))
	// Rate-limited: a broker outage would otherwise produce one log line per
	// transaction, and log I/O is the one thing that could still starve the
	// runtime -- monitoring code taking down the thing it monitors.
	if n := p.deliveryFailLog.Add(1); n == 1 || n%1000 == 0 {
		p.log.Warn("kafka delivery failing", "failures", n, "messages", len(msgs), "error", err)
	}
}

func (p *Publisher) logWriterError(msg string, args ...any) {
	if n := p.deliveryFailLog.Add(1); n == 1 || n%1000 == 0 {
		p.log.Warn("kafka writer error", "failures", n, "detail", fmt.Sprintf(msg, args...))
	}
}

// Flush closes the writer, which drains whatever is batched. Bounded, because
// shutdown must not hang on an unreachable broker.
func (p *Publisher) Flush(timeout time.Duration) {
	done := make(chan error, 1)
	go func() { done <- p.writer.Close() }()
	select {
	case err := <-done:
		if err != nil {
			p.log.Warn("kafka flush incomplete on shutdown", "error", err)
		}
	case <-time.After(timeout):
		p.log.Warn("kafka flush timed out on shutdown", "timeout", timeout)
	}
}

// ---------------------------------------------------------------------------
// config translation

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

func requiredAcks(acks string) kgo.RequiredAcks {
	switch strings.TrimSpace(acks) {
	case "0":
		return kgo.RequireNone
	case "all", "-1":
		return kgo.RequireAll
	default:
		return kgo.RequireOne
	}
}

func compression(name string, log *slog.Logger) kgo.Compression {
	switch strings.TrimSpace(name) {
	case "", "none":
		return 0
	case "gzip":
		return kgo.Gzip
	case "snappy":
		return kgo.Snappy
	case "lz4":
		return kgo.Lz4
	case "zstd":
		return kgo.Zstd
	default:
		log.Warn("unknown kafka.compression; sending uncompressed", "value", name)
		return 0
	}
}

// buildTransport translates the librdkafka-style [kafka.properties] escape hatch
// shared with the Rust build.
//
// Only the security-related keys have kafka-go equivalents. Anything else is
// reported rather than silently ignored: an operator who sets a property and
// gets no behaviour change and no message will assume it took effect, and on a
// payments link that assumption is expensive.
func buildTransport(cfg config.Kafka, log *slog.Logger) (*kgo.Transport, error) {
	t := &kgo.Transport{DialTimeout: 5 * time.Second}

	get := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := cfg.Properties[k]; ok {
				return strings.TrimSpace(v)
			}
		}
		return ""
	}
	used := map[string]bool{}
	mark := func(keys ...string) {
		for _, k := range keys {
			if _, ok := cfg.Properties[k]; ok {
				used[k] = true
			}
		}
	}

	protocol := strings.ToUpper(get("security.protocol"))
	mark("security.protocol")

	if strings.Contains(protocol, "SSL") {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if ca := get("ssl.ca.location"); ca != "" {
			pem, err := os.ReadFile(ca)
			if err != nil {
				return nil, fmt.Errorf("ssl.ca.location: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("ssl.ca.location %s: no certificates found", ca)
			}
			tlsCfg.RootCAs = pool
		}
		if strings.EqualFold(get("ssl.endpoint.identification.algorithm"), "none") {
			// Verification off is a deliberate, logged decision, never a default.
			log.Warn("kafka TLS hostname verification disabled by ssl.endpoint.identification.algorithm=none")
			tlsCfg.InsecureSkipVerify = true
		}
		t.TLS = tlsCfg
	}
	mark("ssl.ca.location", "ssl.endpoint.identification.algorithm")

	if strings.HasPrefix(protocol, "SASL") {
		mechName := strings.ToUpper(get("sasl.mechanisms", "sasl.mechanism"))
		user := get("sasl.username")
		pass := get("sasl.password")
		var (
			mech sasl.Mechanism
			err  error
		)
		switch mechName {
		case "", "PLAIN":
			mech = plain.Mechanism{Username: user, Password: pass}
		case "SCRAM-SHA-256":
			mech, err = scram.Mechanism(scram.SHA256, user, pass)
		case "SCRAM-SHA-512":
			mech, err = scram.Mechanism(scram.SHA512, user, pass)
		default:
			return nil, fmt.Errorf("unsupported sasl.mechanisms %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", mechName)
		}
		if err != nil {
			return nil, fmt.Errorf("sasl %s: %w", mechName, err)
		}
		t.SASL = mech
	}
	mark("sasl.mechanisms", "sasl.mechanism", "sasl.username", "sasl.password")

	for k := range cfg.Properties {
		if !used[k] {
			log.Warn("kafka.properties key has no kafka-go equivalent and is ignored", "key", k)
		}
	}
	return t, nil
}
