// Package admin serves /metrics, /healthz and / (JSON).
//
// Uses net/http from the standard library and nothing else. This process sits
// inline on an authorization path, so every dependency is one more thing that
// can allocate, block, or spawn threads next to the hot path -- and for three
// endpoints returning plain text, a router adds nothing.
//
// LOOPBACK-ONLY BY DEFAULT, AND THERE IS NO AUTH. 127.0.0.1 means a browser on
// this host can reach it and another machine cannot. If you need it scraped
// remotely, change config.toml rather than the code, and put something in front
// of it: these endpoints expose operational internals next to a payments path.
package admin

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/stats"
)

// Serve starts the admin listener and returns the server so it can be shut down.
//
// A BIND FAILURE HERE DOES NOT STOP THE PROXY: it logs and returns nil. You lose
// metrics, not availability.
func Serve(addr string, st *stats.Stats, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var contentType, body string
		switch r.URL.Path {
		case "/healthz":
			contentType, body = "text/plain", "ok\n"
		case "/metrics":
			contentType, body = "text/plain; version=0.0.4", prometheus(st)
		default:
			// Everything else, including the /favicon.ico your browser will ask
			// for, gets the JSON dump. Harmless, and it is why you may see two
			// hits per page load.
			contentType, body = "application/json", jsonBody(st)
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write([]byte(body))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("admin bind failed; continuing without metrics", "addr", addr, "error", err)
		}
	}()
	log.Info("admin listening (/metrics, /healthz)", "addr", addr)
	return srv
}

// Shutdown stops the admin server. Never fatal.
func Shutdown(srv *http.Server, timeout time.Duration) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func prometheus(st *stats.Stats) string {
	var b strings.Builder
	b.Grow(2048)
	for _, m := range st.Snapshot() {
		fmt.Fprintf(&b, "# TYPE vp_interceptor_%s counter\n", m.Name)
		fmt.Fprintf(&b, "vp_interceptor_%s %d\n", m.Name, m.Value)
	}
	return b.String()
}

// jsonBody hand-rolls the object. Safe HERE only because every key is a fixed
// identifier and every value is an integer -- contrast internal/kafka, where the
// data is attacker-influenced and a real encoder does the escaping.
func jsonBody(st *stats.Stats) string {
	var b strings.Builder
	b.Grow(1024)
	b.WriteByte('{')
	for i, m := range st.Snapshot() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(m.Name)
		b.WriteString("\":")
		b.WriteString(strconv.FormatUint(m.Value, 10))
	}
	b.WriteString("}\n")
	return b.String()
}
