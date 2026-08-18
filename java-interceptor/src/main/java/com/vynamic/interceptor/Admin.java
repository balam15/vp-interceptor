package com.vynamic.interceptor;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** {@code /metrics}, {@code /healthz} -- JDK built-in server, no web framework. */
public final class Admin {
    private static final Logger LOG = LoggerFactory.getLogger(Admin.class);

    public static HttpServer start(Config cfg, Stats stats) {
        try {
            HttpServer server = HttpServer.create(
                    new InetSocketAddress(Config.host(cfg.adminAddr), Config.port(cfg.adminAddr)), 0);

            server.createContext("/", ex -> {
                String path = ex.getRequestURI().getPath();
                String body;
                String type;
                if (path.equals("/healthz")) {
                    body = "ok\n";
                    type = "text/plain";
                } else if (path.equals("/metrics")) {
                    StringBuilder sb = new StringBuilder(2048);
                    for (Map.Entry<String, Long> e : stats.snapshot().entrySet()) {
                        sb.append("# TYPE vp_interceptor_").append(e.getKey()).append(" counter\n");
                        sb.append("vp_interceptor_").append(e.getKey()).append(' ').append(e.getValue()).append('\n');
                    }
                    body = sb.toString();
                    type = "text/plain; version=0.0.4";
                } else {
                    StringBuilder sb = new StringBuilder("{");
                    boolean first = true;
                    for (Map.Entry<String, Long> e : stats.snapshot().entrySet()) {
                        if (!first) sb.append(',');
                        sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
                        first = false;
                    }
                    body = sb.append("}\n").toString();
                    type = "application/json";
                }
                byte[] out = body.getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().add("Content-Type", type);
                ex.sendResponseHeaders(200, out.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(out); }
            });

            server.setExecutor(r -> Thread.ofVirtual().start(r));
            server.start();
            LOG.info("admin listening (/metrics, /healthz) addr={}", cfg.adminAddr);
            return server;
        } catch (Exception e) {
            LOG.error("admin bind failed; continuing without metrics: {}", e.toString());
            return null;
        }
    }
}
