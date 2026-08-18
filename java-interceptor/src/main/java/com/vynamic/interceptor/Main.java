package com.vynamic.interceptor;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final Duration KAFKA_FLUSH_TIMEOUT = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        String path = args.length > 0 ? args[0] : "config.toml";
        Config cfg = new Config(new File(path));
        Stats stats = new Stats();

        // Kafka first, but this cannot fail startup: a broker that is down,
        // unreachable, or rejecting the handshake yields a producer that
        // connects lazily; a bad config yields null (proxy-only mode).
        Publisher publisher = Publisher.build(cfg, stats);
        Tee tee = new Tee(cfg, publisher, stats);
        HttpServer admin = Admin.start(cfg, stats);

        ServerSocket server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(Config.host(cfg.listenAddr), Config.port(cfg.listenAddr)), 1024);

        LOG.info("interceptor ready listen={} upstream={} max_connections={}",
                cfg.listenAddr, cfg.upstreamAddr, cfg.maxConnections);

        Semaphore permits = new Semaphore(cfg.maxConnections);
        AtomicLong connId = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutdown signal received; no longer accepting");
            running.set(false);
            try { server.close(); } catch (Exception ignored) {}
            tee.shutdown();
            if (publisher != null) {
                LOG.info("flushing kafka producer");
                publisher.close(KAFKA_FLUSH_TIMEOUT);
            }
            if (admin != null) admin.stop(0);
            LOG.info("stopped");
        }));

        while (running.get()) {
            Socket client;
            try {
                client = server.accept();
            } catch (Exception e) {
                if (!running.get()) break;
                LOG.warn("accept failed: {}", e.toString());
                Thread.sleep(20);
                continue;
            }

            if (!permits.tryAcquire()) {
                // Close immediately rather than queue. VP gets a fast,
                // unambiguous failure instead of a mystery timeout.
                stats.connsRejected.increment();
                LOG.warn("connection limit reached; rejecting {}", client.getRemoteSocketAddress());
                Proxy.closeQuietly(client);
                continue;
            }

            long id = connId.incrementAndGet();
            String peer = String.valueOf(client.getRemoteSocketAddress()).replace("/", "");
            stats.connsAccepted.increment();
            stats.connsActive.increment();

            Thread.ofVirtual().name("conn-" + id).start(() -> {
                try {
                    Proxy.handle(client, peer, id, cfg, tee, stats);
                } finally {
                    stats.connsActive.decrement();
                    permits.release();
                }
            });
        }
    }
}
