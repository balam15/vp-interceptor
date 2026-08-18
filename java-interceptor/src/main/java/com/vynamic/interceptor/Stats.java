package com.vynamic.interceptor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters, mirroring the Rust build's metric names exactly so the same scrapes
 * and dashboards work against either implementation.
 *
 * <p>LongAdder rather than AtomicLong for the hot counters: under contention it
 * trades read cost for write cost, which is the right way round here.
 */
public final class Stats {
    public final LongAdder connsAccepted = new LongAdder();
    public final LongAdder connsActive = new LongAdder();
    public final LongAdder connsRejected = new LongAdder();
    public final LongAdder upstreamConnectFailed = new LongAdder();

    public final LongAdder bytesVpToFms = new LongAdder();
    public final LongAdder bytesFmsToVp = new LongAdder();

    public final LongAdder teeAccepted = new LongAdder();
    public final LongAdder teeDropped = new LongAdder();

    public final LongAdder framesEmitted = new LongAdder();
    public final LongAdder framerDesyncs = new LongAdder();

    public final LongAdder kafkaEnqueued = new LongAdder();
    public final LongAdder kafkaEnqueueFailed = new LongAdder();
    public final LongAdder kafkaDelivered = new LongAdder();
    public final LongAdder kafkaDeliveryFailed = new LongAdder();

    public final LongAdder rttCount = new LongAdder();
    public final LongAdder rttSumUs = new LongAdder();
    public final AtomicLong rttMaxUs = new AtomicLong();
    public final LongAdder rttUnmatched = new LongAdder();
    public final LongAdder connDurationCount = new LongAdder();
    public final LongAdder connDurationSumMs = new LongAdder();
    public final AtomicLong connDurationMaxMs = new AtomicLong();

    public static void observe(LongAdder count, LongAdder sum, AtomicLong max, long value) {
        count.increment();
        sum.add(value);
        max.accumulateAndGet(value, Math::max);
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("conns_accepted", connsAccepted.sum());
        m.put("conns_active", connsActive.sum());
        m.put("conns_rejected", connsRejected.sum());
        m.put("upstream_connect_failed", upstreamConnectFailed.sum());
        m.put("bytes_vp_to_fms", bytesVpToFms.sum());
        m.put("bytes_fms_to_vp", bytesFmsToVp.sum());
        m.put("tee_accepted", teeAccepted.sum());
        m.put("tee_dropped", teeDropped.sum());
        m.put("frames_emitted", framesEmitted.sum());
        m.put("framer_desyncs", framerDesyncs.sum());
        m.put("kafka_enqueued", kafkaEnqueued.sum());
        m.put("kafka_enqueue_failed", kafkaEnqueueFailed.sum());
        m.put("kafka_delivered", kafkaDelivered.sum());
        m.put("kafka_delivery_failed", kafkaDeliveryFailed.sum());
        m.put("rtt_count", rttCount.sum());
        m.put("rtt_sum_us", rttSumUs.sum());
        m.put("rtt_max_us", rttMaxUs.get());
        m.put("rtt_unmatched", rttUnmatched.sum());
        m.put("conn_duration_count", connDurationCount.sum());
        m.put("conn_duration_sum_ms", connDurationSumMs.sum());
        m.put("conn_duration_max_ms", connDurationMaxMs.get());
        return m;
    }
}
