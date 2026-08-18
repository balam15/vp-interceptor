package com.vynamic.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

/**
 * Serves one VP connection: dials FMS, then runs each direction on its own
 * virtual thread.
 *
 * <p>Virtual threads make blocking IO cheap enough to use one per direction --
 * the same shape as the Rust build's task-per-direction, without an async
 * framework.
 */
public final class Proxy {
    private static final Logger LOG = LoggerFactory.getLogger(Proxy.class);

    public static void handle(Socket client, String peer, long connId,
                              Config cfg, Tee tee, Stats stats) {
        Socket upstream = new Socket();
        try {
            upstream.connect(new InetSocketAddress(Config.host(cfg.upstreamAddr),
                    Config.port(cfg.upstreamAddr)), cfg.connectTimeoutMs);
        } catch (Exception e) {
            stats.upstreamConnectFailed.increment();
            LOG.warn("upstream connect failed conn={} peer={} upstream={} : {}",
                    connId, peer, cfg.upstreamAddr, e.toString());
            closeQuietly(client);
            closeQuietly(upstream);
            return;
        }

        try {
            if (cfg.nodelay) {
                // Nagle would coalesce small messages and add tens of ms.
                client.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
            }
            if (cfg.idleTimeoutMs > 0) {
                client.setSoTimeout(cfg.idleTimeoutMs);
                upstream.setSoTimeout(cfg.idleTimeoutMs);
            }
        } catch (Exception e) {
            LOG.debug("socket option failed conn={}: {}", connId, e.toString());
        }

        tee.open(connId);

        CountDownLatch done = new CountDownLatch(2);
        Thread t1 = Thread.ofVirtual().name("pump-" + connId + "-v2f").start(
                () -> pump(client, upstream, Tee.Direction.VP_TO_FMS, connId, peer, cfg, tee, stats, done));
        Thread t2 = Thread.ofVirtual().name("pump-" + connId + "-f2v").start(
                () -> pump(upstream, client, Tee.Direction.FMS_TO_VP, connId, peer, cfg, tee, stats, done));

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            t1.interrupt();
            t2.interrupt();
        }

        closeQuietly(client);
        closeQuietly(upstream);
        tee.close(connId);
    }

    /**
     * Copies one direction.
     *
     * <p>Ordering is the core of the design: bytes reach the far socket BEFORE
     * the tee is offered anything, and the tee call never blocks.
     */
    private static void pump(Socket from, Socket to, Tee.Direction dir, long connId, String peer,
                             Config cfg, Tee tee, Stats stats, CountDownLatch done) {
        byte[] buf = new byte[cfg.readBufferBytes];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            while (true) {
                int n = in.read(buf);
                if (n < 0) {
                    try { to.shutdownOutput(); } catch (Exception ignored) {}
                    break;
                }

                out.write(buf, 0, n);
                out.flush();

                if (dir == Tee.Direction.VP_TO_FMS) stats.bytesVpToFms.add(n);
                else stats.bytesFmsToVp.add(n);

                tee.offer(connId, dir, peer, buf, n);
            }
        } catch (Exception e) {
            LOG.debug("stream ended conn={} dir={}: {}", connId, dir.label, e.toString());
            try { to.shutdownOutput(); } catch (Exception ignored) {}
        } finally {
            done.countDown();
        }
    }

    static void closeQuietly(Socket s) {
        try { s.close(); } catch (Exception ignored) {}
    }
}
