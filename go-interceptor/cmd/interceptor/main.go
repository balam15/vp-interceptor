// Command interceptor is the VP -> FMS TCP interceptor.
//
// It forwards a TCP stream unchanged and publishes a copy of every reassembled
// message to Kafka, under one rule:
//
//	KAFKA MUST NEVER BE ABLE TO AFFECT VP<->FMS.
//
// Check any change against that sentence first. The mechanism that is easiest to
// break by accident is in internal/proxy: bytes must reach the far socket BEFORE
// anything is handed to the tee, and the tee call must never block.
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/admin"
	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/kafka"
	"github.com/vynamic/vp-fms-interceptor/internal/proxy"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
	"github.com/vynamic/vp-fms-interceptor/internal/tee"
)

const (
	drainTimeout       = 30 * time.Second
	kafkaFlushTimeout  = 5 * time.Second
	adminShutdownGrace = 2 * time.Second
	// acceptBackoff stops a bare retry loop from spinning a core at 100% on
	// EMFILE, where accept fails immediately and forever.
	acceptBackoff = 20 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	log := newLogger()

	path := "config.toml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	st := &stats.Stats{}

	// Kafka first, but note this cannot fail startup: a broker that is down,
	// unreachable, or rejecting the handshake yields a producer that connects
	// lazily in the background, and a bad config yields nil (proxy-only mode).
	publisher := kafka.Build(cfg, st, log)
	t := tee.Spawn(cfg, publisher, st, log)
	adminSrv := admin.Serve(cfg.Admin.Addr, st, log)

	listener, err := net.Listen("tcp", cfg.Listen.Addr)
	if err != nil {
		return fmt.Errorf("binding listen address %s: %w", cfg.Listen.Addr, err)
	}

	log.Info("interceptor ready",
		"listen", cfg.Listen.Addr,
		"upstream", cfg.Upstream.Addr,
		"max_connections", cfg.Listen.MaxConnections)

	// GO: a buffered channel used as a counting semaphore. Non-blocking acquire
	// is a select with a default, exactly like the tee's bounded send -- the same
	// primitive doing the same job at both ends of the program.
	permits := make(chan struct{}, cfg.Listen.MaxConnections)
	var conns sync.WaitGroup
	var connID uint64

	stopping := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		log.Info("shutdown signal received; no longer accepting")
		close(stopping)
		// Closing the listener unblocks Accept below. No flag to poll.
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-stopping:
				// Expected: the signal handler closed the listener.
			default:
				log.Warn("accept failed", "error", err)
				time.Sleep(acceptBackoff)
				continue
			}
			break
		}

		select {
		case permits <- struct{}{}:
		default:
			// Close immediately rather than queue. VP gets a fast, unambiguous
			// failure instead of a mystery timeout.
			st.ConnsRejected.Add(1)
			log.Warn("connection limit reached; rejecting", "peer", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}

		connID++
		id := connID
		peer := conn.RemoteAddr().String()
		st.ConnsAccepted.Add(1)
		st.ConnsActive.Add(1)

		conns.Add(1)
		go func() {
			// The order of these is exact: serve the connection, decrement the
			// active gauge, THEN release the permit and the wait group. Release
			// the permit first and the drain below could observe every permit
			// free while conns_active was still non-zero.
			defer func() {
				st.ConnsActive.Add(^uint64(0)) // atomic decrement
				<-permits
				conns.Done()
			}()
			proxy.Handle(conn, peer, id, cfg, t, st, log)
		}()
	}

	drain(&conns, st, log)
	t.Shutdown()

	if publisher != nil {
		log.Info("flushing kafka producer")
		publisher.Flush(kafkaFlushTimeout)
	}
	admin.Shutdown(adminSrv, adminShutdownGrace)

	log.Info("stopped")
	return nil
}

// drain waits for in-flight connections to finish, bounded by drainTimeout.
func drain(conns *sync.WaitGroup, st *stats.Stats, log *slog.Logger) {
	active := st.ConnsActive.Load()
	if active == 0 {
		return
	}
	log.Info("draining in-flight connections", "active", active)

	done := make(chan struct{})
	go func() {
		conns.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Info("all connections drained")
	case <-time.After(drainTimeout):
		log.Warn("drain timed out; closing anyway", "remaining", st.ConnsActive.Load())
	}
}

// newLogger reads LOG_LEVEL (debug|info|warn|error), defaulting to info. Named
// after the same knob the Rust build exposes as RUST_LOG, which is also accepted
// so one deployment can set one variable for either binary.
func newLogger() *slog.Logger {
	level := slog.LevelInfo
	raw := os.Getenv("LOG_LEVEL")
	if raw == "" {
		raw = os.Getenv("RUST_LOG")
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug", "trace":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
