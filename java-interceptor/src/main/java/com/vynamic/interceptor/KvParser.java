package com.vynamic.interceptor;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

/**
 * Splits a {@code key=value} delimited frame into named fields for the Kafka
 * envelope.
 *
 * <p>Runs on a tee shard worker, never on the forwarding path. A parse failure
 * is reported in the envelope and never suppresses the publish -- a message you
 * cannot parse is still evidence.
 */
public final class KvParser {

    public static final class Result {
        public final TreeMap<String, String> fields;
        public final String error;

        private Result(TreeMap<String, String> f, String e) { fields = f; error = e; }
        static Result ok(TreeMap<String, String> f) { return new Result(f, null); }
        static Result err(String e) { return new Result(null, e); }
    }

    private final char pairDelim;
    private final char kvDelim;
    private final boolean trim;
    private final int maxFields;

    private KvParser(Config cfg) {
        this.pairDelim = cfg.pairDelimiter;
        this.kvDelim = cfg.kvDelimiter;
        this.trim = cfg.trim;
        this.maxFields = cfg.maxFields;
    }

    /** Returns null when parsing is disabled. */
    public static KvParser create(Config cfg) {
        return cfg.parseMode.equals("key_value") ? new KvParser(cfg) : null;
    }

    public Result parse(byte[] frame) {
        String text;
        try {
            // Strict decode: must reject malformed input the way Rust's
            // std::str::from_utf8 does, not silently substitute U+FFFD.
            CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            CharBuffer cb = dec.decode(ByteBuffer.wrap(frame));
            text = cb.toString();
        } catch (CharacterCodingException e) {
            return Result.err("not valid utf-8: " + e.getMessage());
        }

        TreeMap<String, String> out = new TreeMap<>();
        int index = 0;
        int from = 0;
        while (from <= text.length()) {
            int at = text.indexOf(pairDelim, from);
            String pair = at < 0 ? text.substring(from) : text.substring(from, at);
            from = at < 0 ? text.length() + 1 : at + 1;

            if (trim) pair = pair.trim();
            if (pair.isEmpty()) { index++; continue; }   // tolerate doubled/trailing delimiters
            if (index >= maxFields) return Result.err("more than " + maxFields + " fields");

            int eq = pair.indexOf(kvDelim);
            if (eq < 0) return Result.err("segment " + index + " has no '" + kvDelim + "' separator");

            String k = pair.substring(0, eq);
            String v = pair.substring(eq + 1);      // keeps '=' inside the value
            if (trim) { k = k.trim(); v = v.trim(); }
            if (k.isEmpty()) return Result.err("empty key in segment " + index);
            out.put(k, v);
            index++;
        }

        if (out.isEmpty()) return Result.err("no fields found");
        return Result.ok(out);
    }
}
