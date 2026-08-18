package com.vynamic.interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Reassembles the byte stream into application messages for publishing.
 *
 * <p>Runs on the tee side only. It never sees the socket and can never delay or
 * alter what is forwarded to FMS -- if it desyncs, publishing stops for that
 * stream and the proxy keeps running untouched.
 */
public final class Framer {

    public static final class Result {
        public final List<byte[]> frames;
        public final String desyncReason;   // null unless this call caused a desync
        public final boolean ignored;       // already desynced

        private Result(List<byte[]> frames, String desyncReason, boolean ignored) {
            this.frames = frames;
            this.desyncReason = desyncReason;
            this.ignored = ignored;
        }

        static Result of(List<byte[]> f) { return new Result(f, null, false); }
        static Result desync(String why) { return new Result(List.of(), why, false); }
        static Result ignored() { return new Result(List.of(), null, true); }
    }

    private final boolean raw;
    private final int prefixLen;
    private final boolean bigEndian;
    private final boolean lengthIncludesPrefix;
    private final int maxFrame;

    private byte[] buf = new byte[0];
    private boolean desynced = false;

    public Framer(Config cfg) {
        this.raw = cfg.framingMode.equals("raw");
        this.prefixLen = cfg.prefixBytes;
        this.bigEndian = cfg.bigEndian;
        this.lengthIncludesPrefix = cfg.lengthIncludesPrefix;
        this.maxFrame = cfg.maxFrameBytes;
    }

    public Result push(byte[] chunk, int len) {
        if (desynced) return Result.ignored();
        if (raw) {
            byte[] copy = new byte[len];
            System.arraycopy(chunk, 0, copy, 0, len);
            return Result.of(List.of(copy));
        }

        byte[] merged = new byte[buf.length + len];
        System.arraycopy(buf, 0, merged, 0, buf.length);
        System.arraycopy(chunk, 0, merged, buf.length, len);
        buf = merged;

        List<byte[]> out = new ArrayList<>();
        int pos = 0;
        while (buf.length - pos >= prefixLen) {
            int declared = readPrefix(buf, pos);
            int bodyLen = lengthIncludesPrefix ? declared - prefixLen : declared;

            if (bodyLen <= 0) {
                desynced = true;
                trim(pos);
                return Result.desync(bodyLen == 0 ? "zero-length frame"
                                                  : "declared length shorter than prefix");
            }
            if (bodyLen > maxFrame) {
                desynced = true;
                trim(pos);
                return Result.desync("frame exceeds max_frame_bytes");
            }
            if (buf.length - pos < prefixLen + bodyLen) break;   // partial frame

            byte[] frame = new byte[bodyLen];
            System.arraycopy(buf, pos + prefixLen, frame, 0, bodyLen);
            out.add(frame);
            pos += prefixLen + bodyLen;
        }
        trim(pos);
        return Result.of(out);
    }

    private void trim(int consumed) {
        if (consumed == 0) return;
        byte[] rest = new byte[buf.length - consumed];
        System.arraycopy(buf, consumed, rest, 0, rest.length);
        buf = rest;
    }

    private int readPrefix(byte[] b, int off) {
        if (prefixLen == 2) {
            int hi = b[off] & 0xff, lo = b[off + 1] & 0xff;
            return bigEndian ? (hi << 8) | lo : (lo << 8) | hi;
        }
        int b0 = b[off] & 0xff, b1 = b[off + 1] & 0xff, b2 = b[off + 2] & 0xff, b3 = b[off + 3] & 0xff;
        return bigEndian ? (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
                         : (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    }
}
