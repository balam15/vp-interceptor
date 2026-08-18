// Package proxy serves one VP connection: dial FMS, then copy bytes both ways.
//
// THIS IS THE HOT PATH. The ordering inside pump IS the design: bytes reach the
// far socket before the tee is offered anything, and the tee call cannot block.
// Kafka may be down, slow, or absent without adding a microsecond to this loop.
package proxy

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
	"github.com/vynamic/vp-fms-interceptor/internal/tee"
)

// Handle serves one VP connection: dials FMS, then runs the two directions as
// independent goroutines so the scheduler can place them on different cores.
func Handle(client net.Conn, peer string, connID uint64, cfg *config.Config,
	t *tee.Tee, st *stats.Stats, log *slog.Logger) {

	defer client.Close()

	upstream, err := net.DialTimeout("tcp", cfg.Upstream.Addr, cfg.Upstream.ConnectTimeout())
	if err != nil {
		st.UpstreamConnectFailed.Add(1)
		log.Warn("upstream connect failed",
			"conn_id", connID, "peer", peer, "upstream", cfg.Upstream.Addr, "error", err)
		return
	}
	defer upstream.Close()

	if cfg.Proxy.Nodelay {
		// Nagle would coalesce small ISO 8583 messages and add tens of
		// milliseconds -- far more than everything else here combined.
		setNoDelay(client)
		setNoDelay(upstream)
	}

	// Start the connection clock only once FMS is actually connected, so
	// conn_age_ms measures serving time rather than dial time.
	t.Open(connID)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pump(client, upstream, tee.VpToFms, connID, peer, cfg, t, st, log)
	}()
	go func() {
		defer wg.Done()
		pump(upstream, client, tee.FmsToVp, connID, peer, cfg, t, st, log)
	}()
	wg.Wait()

	// After both directions end, so the tee worker can free its per-connection
	// framer and timing state.
	t.Close(connID)
}

// pump copies one direction of the stream.
func pump(src, dst net.Conn, dir tee.Direction, connID uint64, peer string,
	cfg *config.Config, t *tee.Tee, st *stats.Stats, log *slog.Logger) {

	// Resolved once, outside the loop, so the hot path re-checks nothing.
	idle := cfg.Proxy.IdleTimeout()
	byteCounter := &st.BytesVpToFms
	if dir == tee.FmsToVp {
		byteCounter = &st.BytesFmsToVp
	}
	offer := t.ShouldOffer(dir)

	// One buffer for the life of the connection. Because the tee copies what it
	// keeps, this is reused on every read and never reallocated: steady-state
	// allocation on the forwarding path is ZERO BYTES PER MESSAGE.
	buf := make([]byte, cfg.Proxy.ReadBufferBytes)

	for {
		if idle > 0 {
			// Deadline rather than a timer: no goroutine, no allocation, and it
			// is re-armed by the same syscall path that does the read.
			_ = src.SetReadDeadline(time.Now().Add(idle))
		}
		n, err := src.Read(buf)

		if n > 0 {
			// THIS IS THE LINE THE WHOLE PROGRAM EXISTS TO PROTECT. Bytes go to
			// the far socket FIRST, before any tee work happens.
			//
			// GO: net.Conn.Write is defined to write every byte or return an
			// error, so there is no short-write loop to forget here.
			if _, werr := dst.Write(buf[:n]); werr != nil {
				log.Debug("write failed", "conn_id", connID, "direction", dir.String(), "error", werr)
				halfClose(dst)
				return
			}
			byteCounter.Add(uint64(n))

			if offer {
				t.Offer(connID, dir, peer, buf[:n])
			}
		}

		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// Half-close: propagate FIN so the peer can finish its own
				// direction rather than having an in-flight response torn down.
				// If VP half-closes after sending a request, FMS's response
				// still gets delivered. Correct TCP proxy behaviour, and
				// frequently got wrong.
			case isTimeout(err):
				log.Debug("idle timeout", "conn_id", connID, "direction", dir.String())
			default:
				// A connection reset is completely routine, hence debug.
				log.Debug("stream ended", "conn_id", connID, "direction", dir.String(), "error", err)
			}
			halfClose(dst)
			return
		}
	}
}

// halfClose sends FIN on the write side while leaving reads open, so the other
// direction can drain. Falls back to a full close for anything that is not TCP.
func halfClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
		return
	}
	_ = c.Close()
}

func setNoDelay(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		// Best effort: a failed socket option is not worth aborting a payment.
		_ = tc.SetNoDelay(true)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
