package proxy

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
	"github.com/vynamic/vp-fms-interceptor/internal/tee"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// startEchoUpstream stands in for FMS: it echoes every byte back.
func startEchoUpstream(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

// tcpPair returns a connected (client, server) pair of REAL TCP sockets.
// net.Pipe would not do: these tests exercise CloseWrite and SetNoDelay, which
// only exist on *net.TCPConn.
func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type accepted struct {
		c   net.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatal(a.err)
	}
	return client, a.c
}

func testConfig(upstream string) *config.Config {
	cfg := config.Defaults()
	cfg.Upstream.Addr = upstream
	cfg.Upstream.ConnectTimeoutMs = 2000
	cfg.Proxy.ReadBufferBytes = 4096
	return cfg
}

// The core contract: bytes go through unaltered, in both directions, with the
// tee inert (Kafka disabled). If this fails, nothing else matters.
func TestForwardsBothDirections(t *testing.T) {
	up := startEchoUpstream(t)
	cfg := testConfig(up.Addr().String())
	st := &stats.Stats{}
	// A nil publisher yields an inert tee -- proxy-only mode.
	tr := tee.Spawn(cfg, nil, st, quietLogger())

	client, server := tcpPair(t)
	go Handle(server, "127.0.0.1:5000", 1, cfg, tr, st, quietLogger())

	want := []byte{0x00, 0x03, 'a', 'b', 'c'}
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got % x, want % x", got, want)
	}

	client.Close()

	// Byte counters must reflect both directions.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st.BytesVpToFms.Load() == uint64(len(want)) && st.BytesFmsToVp.Load() == uint64(len(want)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("counters: vp_to_fms=%d fms_to_vp=%d, want %d each",
		st.BytesVpToFms.Load(), st.BytesFmsToVp.Load(), len(want))
}

// A refused upstream must be counted and must not hang or panic.
func TestUpstreamConnectFailureIsCountedAndClosesTheClient(t *testing.T) {
	// Bind then immediately close, so the port is almost certainly refusing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := testConfig(addr)
	cfg.Upstream.ConnectTimeoutMs = 500
	st := &stats.Stats{}
	tr := tee.Spawn(cfg, nil, st, quietLogger())

	client, server := tcpPair(t)
	done := make(chan struct{})
	go func() { Handle(server, "127.0.0.1:5000", 1, cfg, tr, st, quietLogger()); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Handle did not return after upstream connect failure")
	}
	if st.UpstreamConnectFailed.Load() != 1 {
		t.Fatalf("upstream_connect_failed = %d, want 1", st.UpstreamConnectFailed.Load())
	}
	// The client socket must be closed, not left dangling.
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the client connection to be closed")
	}
	client.Close()
}

// Half-close must propagate: VP sending FIN after a request must still get the
// response back rather than having it torn down.
func TestHalfCloseLetsTheResponseArrive(t *testing.T) {
	up := startEchoUpstream(t)
	cfg := testConfig(up.Addr().String())
	st := &stats.Stats{}
	tr := tee.Spawn(cfg, nil, st, quietLogger())

	client, server := tcpPair(t)
	go Handle(server, "127.0.0.1:5000", 1, cfg, tr, st, quietLogger())

	req := []byte("request")
	if _, err := client.Write(req); err != nil {
		t.Fatal(err)
	}
	// Half-close the client's write side.
	if tc, ok := client.(*net.TCPConn); ok {
		if err := tc.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}

	got := make([]byte, len(req))
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("response lost after half-close: %v", err)
	}
	if string(got) != string(req) {
		t.Fatalf("got %q, want %q", got, req)
	}
	client.Close()
}
