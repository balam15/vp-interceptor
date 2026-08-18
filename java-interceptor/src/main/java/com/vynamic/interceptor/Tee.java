package com.vynamic.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The isolation boundary between the payment path and Kafka.
 *
 * <p>{@link #offer} uses a bounded queue and a non-blocking {@code offer()}:
 * when Kafka is behind, frames are dropped and counted rather than applying
 * backpressure to the socket.
 */
public final class Tee {
    private static final Logger LOG = LoggerFactory.getLogger(Tee.class);
    private static final long IDLE_MAX_MS = 600_000;
    private static final long SWEEP_MS = 60_000;

    public enum Direction {
        VP_TO_FMS("vp_to_fms"), FMS_TO_VP("fms_to_vp");
        public final String label;
        Direction(String l) { this.label = l; }
    }

    private sealed interface Event permits Open, Data, Close {}
    private record Open(long connId, long atNanos) implements Event {}
    private record Data(long connId, Direction dir, String peer, byte[] chunk, int len) implements Event {}
    private record Close(long connId, long atNanos) implements Event {}

    private final BlockingQueue<Event>[] queues;
    private final Thread[] workers;
    private final Stats stats;
    private final boolean active;
    private final boolean pubVpToFms;
    private final boolean pubFmsToVp;

    @SuppressWarnings("unchecked")
    public Tee(Config cfg, Publisher publisher, Stats stats) {
        this.stats = stats;
        this.active = publisher != null;
        this.pubVpToFms = !cfg.publishDirections.equals("fms_to_vp");
        this.pubFmsToVp = !cfg.publishDirections.equals("vp_to_fms");

        if (!active) {
            this.queues = new BlockingQueue[0];
            this.workers = new Thread[0];
            return;
        }

        this.queues = new BlockingQueue[cfg.shards];
        this.workers = new Thread[cfg.shards];
        for (int i = 0; i < cfg.shards; i++) {
            queues[i] = new ArrayBlockingQueue<>(cfg.queueCapacity);
            int shard = i;
            // Platform threads, not virtual: these are long-lived, CPU-bound
            // workers. Virtual threads buy nothing here and would add scheduling
            // overhead on every frame.
            workers[i] = Thread.ofPlatform().name("tee-shard-" + shard).daemon(true)
                    .start(new Worker(shard, queues[shard], publisher, cfg, stats));
        }
        LOG.info("tee workers started shards={} queue_capacity={}", cfg.shards, cfg.queueCapacity);
    }

    public void open(long connId) {
        if (!active) return;
        queues[(int) (connId % queues.length)].offer(new Open(connId, System.nanoTime()));
    }

    public void close(long connId) {
        if (!active) return;
        queues[(int) (connId % queues.length)].offer(new Close(connId, System.nanoTime()));
    }

    /**
     * Hand a copy of the wire bytes to the publish path. Called from the
     * forwarding hot path: must not block or throw.
     */
    public void offer(long connId, Direction dir, String peer, byte[] chunk, int len) {
        if (!active) return;
        if (dir == Direction.VP_TO_FMS ? !pubVpToFms : !pubFmsToVp) return;

        // Java has no refcounted slice equivalent to Rust's Bytes, so the tee
        // must copy here. This is the one unavoidable extra cost of the JVM
        // implementation on the hot path.
        byte[] copy = new byte[len];
        System.arraycopy(chunk, 0, copy, 0, len);

        if (queues[(int) (connId % queues.length)].offer(new Data(connId, dir, peer, copy, len))) {
            stats.teeAccepted.increment();
        } else {
            stats.teeDropped.increment();
        }
    }

    public void shutdown() {
        for (Thread t : workers) t.interrupt();
    }

    // ------------------------------------------------------------------

    private static final class StreamState {
        final Framer framer;
        long seq;
        long lastTouchedMs = System.currentTimeMillis();
        StreamState(Config cfg) { this.framer = new Framer(cfg); }
    }

    private static final class ConnTiming {
        final long openedNanos;
        Long lastFrameNanos;
        final ArrayDeque<Long> pending = new ArrayDeque<>();
        long lastTouchedMs = System.currentTimeMillis();
        ConnTiming(long at) { this.openedNanos = at; }
    }

    private record StreamKey(long connId, Direction dir) {}

    private static final class Worker implements Runnable {
        private final int shard;
        private final BlockingQueue<Event> queue;
        private final Publisher publisher;
        private final Config cfg;
        private final Stats stats;
        private final Map<StreamKey, StreamState> state = new HashMap<>();
        private final Map<Long, ConnTiming> timing = new HashMap<>();
        private long lastSweepMs = System.currentTimeMillis();

        Worker(int shard, BlockingQueue<Event> queue, Publisher publisher, Config cfg, Stats stats) {
            this.shard = shard; this.queue = queue; this.publisher = publisher;
            this.cfg = cfg; this.stats = stats;
        }

        @Override public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Event ev = queue.poll(1, TimeUnit.SECONDS);
                    if (ev != null) handle(ev);
                    sweepIfDue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // A bug in framing or publishing must not kill the worker
                    // and silently stop the feed.
                    LOG.error("tee shard {} error", shard, e);
                }
            }
            LOG.info("tee worker {} stopped", shard);
        }

        private void sweepIfDue() {
            long now = System.currentTimeMillis();
            if (now - lastSweepMs < SWEEP_MS) return;
            lastSweepMs = now;
            int before = state.size() + timing.size();
            state.entrySet().removeIf(e -> now - e.getValue().lastTouchedMs > IDLE_MAX_MS);
            timing.entrySet().removeIf(e -> now - e.getValue().lastTouchedMs > IDLE_MAX_MS);
            int reclaimed = before - (state.size() + timing.size());
            if (reclaimed > 0) LOG.debug("shard {} swept {} idle entries", shard, reclaimed);
        }

        private void handle(Event ev) {
            if (ev instanceof Open o) {
                if (cfg.timingEnabled) timing.put(o.connId(), new ConnTiming(o.atNanos()));
                return;
            }
            if (ev instanceof Close c) {
                state.remove(new StreamKey(c.connId(), Direction.VP_TO_FMS));
                state.remove(new StreamKey(c.connId(), Direction.FMS_TO_VP));
                ConnTiming t = timing.remove(c.connId());
                if (t != null) {
                    long ms = (c.atNanos() - t.openedNanos) / 1_000_000;
                    Stats.observe(stats.connDurationCount, stats.connDurationSumMs, stats.connDurationMaxMs, ms);
                    if (!t.pending.isEmpty()) stats.rttUnmatched.add(t.pending.size());
                }
                return;
            }

            Data d = (Data) ev;
            StreamKey key = new StreamKey(d.connId(), d.dir());
            StreamState st = state.computeIfAbsent(key, k -> new StreamState(cfg));
            st.lastTouchedMs = System.currentTimeMillis();

            Framer.Result r = st.framer.push(d.chunk(), d.len());
            if (r.ignored) return;
            if (r.desyncReason != null) {
                stats.framerDesyncs.increment();
                LOG.warn("framing desync conn={} dir={} reason={} (proxy unaffected)",
                        d.connId(), d.dir().label, r.desyncReason);
                return;
            }

            long tsMs = System.currentTimeMillis();
            String kkey = Long.toString(d.connId());
            for (byte[] frame : r.frames) {
                st.seq++;
                stats.framesEmitted.increment();

                Double connAge = null, gap = null, rtt = null;
                if (cfg.timingEnabled) {
                    long now = System.nanoTime();
                    ConnTiming t = timing.computeIfAbsent(d.connId(), k -> new ConnTiming(now));
                    t.lastTouchedMs = tsMs;
                    connAge = (now - t.openedNanos) / 1_000_000.0;
                    if (t.lastFrameNanos != null) gap = (now - t.lastFrameNanos) / 1_000_000.0;
                    t.lastFrameNanos = now;

                    if (cfg.pairRequestResponse) {
                        if (d.dir() == Direction.VP_TO_FMS) {
                            if (t.pending.size() >= cfg.maxPending) {
                                t.pending.pollFirst();
                                stats.rttUnmatched.increment();
                            }
                            t.pending.addLast(now);
                        } else {
                            Long sent = t.pending.pollFirst();
                            if (sent != null) {
                                long us = (now - sent) / 1000;
                                rtt = us / 1000.0;
                                Stats.observe(stats.rttCount, stats.rttSumUs, stats.rttMaxUs, us);
                            }
                        }
                    }
                }

                publisher.publish(kkey,
                        new Publisher.Meta(d.connId(), d.dir().label, st.seq, d.peer(), tsMs, connAge, gap, rtt),
                        frame);
            }
        }
    }
}
