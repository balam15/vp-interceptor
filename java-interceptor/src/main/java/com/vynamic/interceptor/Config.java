package com.vynamic.interceptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.toml.TomlMapper;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads the same config.toml as the Rust build.
 *
 * <p>Deliberately shares the file rather than defining a Java-specific format:
 * a benchmark between the two implementations is only meaningful if neither can
 * quietly be running different settings.
 */
public final class Config {

    // [listen]
    public final String listenAddr;
    public final int maxConnections;
    // [upstream]
    public final String upstreamAddr;
    public final int connectTimeoutMs;
    // [proxy]
    public final int readBufferBytes;
    public final boolean nodelay;
    public final int idleTimeoutMs;
    // [tee]
    public final int shards;
    public final int queueCapacity;
    public final String publishDirections;
    // [framing]
    public final String framingMode;
    public final int prefixBytes;
    public final boolean bigEndian;
    public final boolean lengthIncludesPrefix;
    public final int maxFrameBytes;
    // [timing]
    public final boolean timingEnabled;
    public final boolean pairRequestResponse;
    public final int maxPending;
    // [parse]
    public final String parseMode;
    public final char pairDelimiter;
    public final char kvDelimiter;
    public final boolean trim;
    public final int maxFields;
    // [kafka]
    public final boolean kafkaEnabled;
    public final String brokers;
    public final String topic;
    public final String valueFormat;
    public final String payloadEncoding;
    public final String acks;
    public final String compression;
    public final int lingerMs;
    public final int messageTimeoutMs;
    public final long queueBufferingMaxMessages;
    public final long queueBufferingMaxKbytes;
    public final Map<String, String> extraProperties = new LinkedHashMap<>();
    // [admin]
    public final String adminAddr;

    public Config(File file) throws IOException {
        JsonNode root = new TomlMapper().readTree(file);

        JsonNode listen = node(root, "listen");
        listenAddr = str(listen, "addr", "0.0.0.0:9100");
        maxConnections = intOf(listen, "max_connections", 4096);

        JsonNode upstream = node(root, "upstream");
        upstreamAddr = str(upstream, "addr", "127.0.0.1:8583");
        connectTimeoutMs = intOf(upstream, "connect_timeout_ms", 3000);

        JsonNode proxy = node(root, "proxy");
        readBufferBytes = intOf(proxy, "read_buffer_bytes", 16384);
        nodelay = boolOf(proxy, "nodelay", true);
        idleTimeoutMs = intOf(proxy, "idle_timeout_ms", 0);

        JsonNode tee = node(root, "tee");
        shards = intOf(tee, "shards", 4);
        queueCapacity = intOf(tee, "queue_capacity", 8192);
        publishDirections = str(tee, "publish_directions", "both");

        JsonNode framing = node(root, "framing");
        framingMode = str(framing, "mode", "length_prefix");
        prefixBytes = intOf(framing, "prefix_bytes", 2);
        bigEndian = boolOf(framing, "big_endian", true);
        lengthIncludesPrefix = boolOf(framing, "length_includes_prefix", false);
        maxFrameBytes = intOf(framing, "max_frame_bytes", 65536);

        JsonNode timing = node(root, "timing");
        timingEnabled = boolOf(timing, "enabled", true);
        pairRequestResponse = boolOf(timing, "pair_request_response", true);
        maxPending = intOf(timing, "max_pending", 256);

        JsonNode parse = node(root, "parse");
        parseMode = str(parse, "mode", "key_value");
        pairDelimiter = firstChar(str(parse, "pair_delimiter", ","), ',');
        kvDelimiter = firstChar(str(parse, "kv_delimiter", "="), '=');
        trim = boolOf(parse, "trim", true);
        maxFields = intOf(parse, "max_fields", 64);

        JsonNode kafka = node(root, "kafka");
        kafkaEnabled = boolOf(kafka, "enabled", true);
        brokers = str(kafka, "brokers", "localhost:9092");
        topic = str(kafka, "topic", "vp.fms.iso8583");
        valueFormat = str(kafka, "value_format", "json");
        payloadEncoding = str(kafka, "payload_encoding", "base64");
        acks = str(kafka, "acks", "1");
        compression = str(kafka, "compression", "lz4");
        lingerMs = intOf(kafka, "linger_ms", 5);
        messageTimeoutMs = intOf(kafka, "message_timeout_ms", 5000);
        queueBufferingMaxMessages = intOf(kafka, "queue_buffering_max_messages", 100000);
        queueBufferingMaxKbytes = intOf(kafka, "queue_buffering_max_kbytes", 262144);
        JsonNode props = kafka.get("properties");
        if (props != null) {
            props.fieldNames().forEachRemaining(k -> extraProperties.put(k, props.get(k).asText()));
        }

        adminAddr = str(node(root, "admin"), "addr", "127.0.0.1:9101");

        validate();
    }

    private void validate() {
        if (shards < 1) throw new IllegalArgumentException("tee.shards must be >= 1");
        if (queueCapacity < 1) throw new IllegalArgumentException("tee.queue_capacity must be >= 1");
        if (!framingMode.equals("raw") && prefixBytes != 2 && prefixBytes != 4) {
            throw new IllegalArgumentException("framing.prefix_bytes must be 2 or 4");
        }
        if (!valueFormat.equals("json") && !valueFormat.equals("raw")) {
            throw new IllegalArgumentException("kafka.value_format must be json|raw");
        }
    }

    public static String host(String hostPort) {
        return hostPort.substring(0, hostPort.lastIndexOf(':'));
    }

    public static int port(String hostPort) {
        return Integer.parseInt(hostPort.substring(hostPort.lastIndexOf(':') + 1));
    }

    private static JsonNode node(JsonNode root, String name) {
        JsonNode n = root.get(name);
        return n == null ? new TomlMapper().createObjectNode() : n;
    }

    private static String str(JsonNode n, String k, String dflt) {
        JsonNode v = n.get(k);
        return v == null ? dflt : v.asText();
    }

    private static int intOf(JsonNode n, String k, int dflt) {
        JsonNode v = n.get(k);
        return v == null ? dflt : v.asInt();
    }

    private static boolean boolOf(JsonNode n, String k, boolean dflt) {
        JsonNode v = n.get(k);
        return v == null ? dflt : v.asBoolean();
    }

    private static char firstChar(String s, char dflt) {
        return s.isEmpty() ? dflt : s.charAt(0);
    }
}
