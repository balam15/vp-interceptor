package com.vynamic.interceptor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/** Fire-and-forget Kafka publishing. Never blocks the caller, ever. */
public final class Publisher {
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);
    private static final JsonFactory JSON = new JsonFactory();

    public record Meta(long connId, String direction, long seq, String peer, long tsMs,
                       Double connAgeMs, Double gapMs, Double rttMs) {}

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final boolean json;
    private final String encoding;
    private final KvParser parser;
    private final Stats stats;
    private final AtomicLong enqueueFailLog = new AtomicLong();
    private final AtomicLong parseFailLog = new AtomicLong();

    private Publisher(KafkaProducer<String, byte[]> producer, Config cfg, Stats stats) {
        this.producer = producer;
        this.topic = cfg.topic;
        this.json = cfg.valueFormat.equals("json");
        this.encoding = cfg.payloadEncoding;
        this.parser = KvParser.create(cfg);
        this.stats = stats;
    }

    /**
     * Builds the producer without connecting. A broker that is down, unreachable
     * or rejecting the handshake cannot fail this call, and a bad configuration
     * degrades to proxy-only rather than exiting.
     */
    public static Publisher build(Config cfg, Stats stats) {
        if (!cfg.kafkaEnabled) {
            LOG.info("kafka disabled by config; running as plain proxy");
            return null;
        }
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.brokers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, cfg.acks);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.compression);
        p.put(ProducerConfig.LINGER_MS_CONFIG, cfg.lingerMs);
        // Kafka enforces delivery.timeout.ms >= linger.ms + request.timeout.ms.
        // Violating it throws ConfigException at construction, which this class
        // catches -- so the whole service would silently run proxy-only.
        int requestTimeout = Math.min(cfg.messageTimeoutMs, 30000);
        int deliveryTimeout = Math.max(cfg.messageTimeoutMs, cfg.lingerMs + requestTimeout + 1000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, cfg.queueBufferingMaxKbytes * 1024);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        // THE critical setting. KafkaProducer.send() blocks for up to
        // max.block.ms when the buffer is full or metadata is missing -- which
        // would put Kafka directly into the payment path. Zero makes send()
        // throw instantly instead, which we catch and count.
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
        cfg.extraProperties.forEach(p::put);

        try {
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(p);
            Publisher pub = new Publisher(producer, cfg, stats);
            pub.warmUpMetadataInBackground();
            LOG.info("kafka producer created (lazy connect) brokers={} topic={} value_format={} encoding={} parsing={}",
                    cfg.brokers, cfg.topic, cfg.valueFormat, cfg.payloadEncoding, pub.parser != null);
            return pub;
        } catch (Exception e) {
            LOG.error("kafka producer creation failed; continuing WITHOUT publishing", e);
            return null;
        }
    }

    /**
     * With max.block.ms=0 a send issued before topic metadata is known fails
     * immediately. librdkafka buffers instead, so without this warm-up the Java
     * build would drop the first messages after startup. Runs off-thread so a
     * dead broker still cannot delay serving.
     */
    private void warmUpMetadataInBackground() {
        Thread.ofVirtual().name("kafka-metadata-warmup").start(() -> {
            for (int i = 0; i < 60; i++) {
                try {
                    producer.partitionsFor(topic);
                    LOG.info("kafka metadata ready for topic {}", topic);
                    return;
                } catch (Exception e) {
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { return; }
                }
            }
            LOG.warn("kafka metadata still unavailable; publishing will shed until the broker returns");
        });
    }

    public void publish(String key, Meta meta, byte[] frame) {
        byte[] value;
        if (json) {
            KvParser.Result parsed = parser == null ? null : parser.parse(frame);
            if (parsed != null && parsed.error != null) {
                long n = parseFailLog.incrementAndGet();
                if (n == 1 || n % 1000 == 0) {
                    LOG.warn("frame did not parse; publishing unparsed (failures={} reason={})", n, parsed.error);
                }
            }
            value = envelope(meta, frame, parsed);
        } else {
            value = frame;
        }

        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, null, key, value);
        rec.headers().add(new RecordHeader("conn_id", String.valueOf(meta.connId()).getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader("direction", meta.direction().getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader("seq", String.valueOf(meta.seq()).getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader("peer", meta.peer().getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader("ts_ms", String.valueOf(meta.tsMs()).getBytes(StandardCharsets.UTF_8)));

        try {
            producer.send(rec, (md, ex) -> {
                if (ex == null) {
                    stats.kafkaDelivered.increment();
                } else {
                    long n = stats.kafkaDeliveryFailed.sum();
                    stats.kafkaDeliveryFailed.increment();
                    if (n == 0 || n % 1000 == 0) LOG.warn("kafka delivery failing: {}", ex.toString());
                }
            });
            stats.kafkaEnqueued.increment();
        } catch (Exception e) {
            // Buffer full or metadata missing. Shed, never wait.
            stats.kafkaEnqueueFailed.increment();
            long n = enqueueFailLog.incrementAndGet();
            if (n == 1 || n % 1000 == 0) {
                LOG.warn("kafka enqueue rejected; dropping (failures={} {})", n, e.toString());
            }
        }
    }

    private byte[] envelope(Meta meta, byte[] frame, KvParser.Result parsed) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(frame.length * 2 + 256);
        try (JsonGenerator g = JSON.createGenerator(out)) {
            g.writeStartObject();
            g.writeNumberField("conn_id", meta.connId());
            g.writeStringField("direction", meta.direction());
            g.writeNumberField("seq", meta.seq());
            g.writeStringField("peer", meta.peer());
            g.writeNumberField("ts_ms", meta.tsMs());
            g.writeNumberField("length", frame.length);
            if (meta.connAgeMs() != null) g.writeNumberField("conn_age_ms", meta.connAgeMs());
            if (meta.gapMs() != null) g.writeNumberField("gap_ms", meta.gapMs());
            if (meta.rttMs() != null) g.writeNumberField("rtt_ms", meta.rttMs());
            if (parsed != null && parsed.fields != null) {
                g.writeObjectFieldStart("fields");
                for (Map.Entry<String, String> e : parsed.fields.entrySet()) {
                    g.writeStringField(e.getKey(), e.getValue());
                }
                g.writeEndObject();
            }
            if (parsed != null && parsed.error != null) g.writeStringField("parse_error", parsed.error);
            g.writeStringField("encoding", encoding);
            g.writeStringField("payload", encodePayload(frame));
            g.writeEndObject();
        } catch (Exception e) {
            LOG.error("json encode failed", e);
            return frame;
        }
        return out.toByteArray();
    }

    private String encodePayload(byte[] frame) {
        return switch (encoding) {
            case "hex" -> {
                StringBuilder sb = new StringBuilder(frame.length * 2);
                for (byte b : frame) {
                    sb.append(Character.forDigit((b >> 4) & 0xf, 16));
                    sb.append(Character.forDigit(b & 0xf, 16));
                }
                yield sb.toString();
            }
            case "utf8" -> new String(frame, StandardCharsets.UTF_8);
            default -> Base64.getEncoder().encodeToString(frame);
        };
    }

    public void close(Duration timeout) {
        try {
            producer.close(timeout);
        } catch (Exception e) {
            LOG.warn("kafka close incomplete: {}", e.toString());
        }
    }
}
