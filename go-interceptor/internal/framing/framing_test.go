package framing

import (
	"bytes"
	"testing"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
)

func cfg(includesPrefix bool) config.Framing {
	return config.Framing{
		Mode:                 "length_prefix",
		PrefixBytes:          2,
		BigEndian:            true,
		LengthIncludesPrefix: includesPrefix,
		MaxFrameBytes:        1024,
	}
}

func frames(t *testing.T, s Step) [][]byte {
	t.Helper()
	if s.Ignored {
		t.Fatalf("expected frames, got Ignored")
	}
	if s.Desync != "" {
		t.Fatalf("expected frames, got desync: %s", s.Desync)
	}
	return s.Frames
}

func assertFrames(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d (%q)", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i], []byte(want[i])) {
			t.Fatalf("frame %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// THE important test: it feeds the wire ONE BYTE AT A TIME, proving the framer
// reassembles across arbitrary TCP segmentation. Assuming one read yields one
// message is the #1 bug in hand-rolled protocol code.
func TestReassemblesAcrossArbitraryChunkBoundaries(t *testing.T) {
	f := New(cfg(false))
	wire := []byte{0x00, 0x03, 'a', 'b', 'c', 0x00, 0x03, 'x', 'y', 'z'}
	var got [][]byte
	for _, b := range wire {
		got = append(got, frames(t, f.Push([]byte{b}))...)
	}
	assertFrames(t, got, "abc", "xyz")
}

func TestSplitsMultipleFramesInOneRead(t *testing.T) {
	f := New(cfg(false))
	got := frames(t, f.Push([]byte{0x00, 0x01, 'a', 0x00, 0x02, 'b', 'c'}))
	assertFrames(t, got, "a", "bc")
}

func TestHonoursLengthIncludesPrefix(t *testing.T) {
	f := New(cfg(true))
	// declared 5 = 2 prefix + 3 body
	got := frames(t, f.Push([]byte{0x00, 0x05, 'a', 'b', 'c'}))
	assertFrames(t, got, "abc")
}

func TestDeclaredLengthShorterThanPrefixDesyncs(t *testing.T) {
	f := New(cfg(true))
	// declared 1 with a 2-byte prefix underflows. In Rust this would wrap usize
	// to 1.8e19; here it goes negative. Both must be caught, not used.
	s := f.Push([]byte{0x00, 0x01, 'a'})
	if s.Desync != "declared length shorter than prefix" {
		t.Fatalf("got desync %q", s.Desync)
	}
}

func TestOversizedFrameDesyncsAndStaysDesynced(t *testing.T) {
	f := New(cfg(false))
	if s := f.Push([]byte{0xFF, 0xFF}); s.Desync == "" {
		t.Fatal("expected desync on oversized frame")
	}
	// The STICKY property: once desynced, always desynced.
	if s := f.Push([]byte{0x00, 0x01, 'a'}); !s.Ignored {
		t.Fatalf("expected Ignored after desync, got %+v", s)
	}
}

func TestZeroLengthFrameDesyncs(t *testing.T) {
	f := New(cfg(false))
	// Would otherwise loop forever without consuming -- a hang, not a crash.
	if s := f.Push([]byte{0x00, 0x00}); s.Desync != "zero-length frame" {
		t.Fatalf("got desync %q", s.Desync)
	}
}

func TestRawModePassesChunksThrough(t *testing.T) {
	c := cfg(false)
	c.Mode = "raw"
	f := New(c)
	assertFrames(t, frames(t, f.Push([]byte("anything"))), "anything")
}

func TestLittleEndianPrefix(t *testing.T) {
	c := cfg(false)
	c.BigEndian = false
	f := New(c)
	assertFrames(t, frames(t, f.Push([]byte{0x03, 0x00, 'a', 'b', 'c'})), "abc")
}

func TestFourBytePrefix(t *testing.T) {
	c := cfg(false)
	c.PrefixBytes = 4
	f := New(c)
	assertFrames(t, frames(t, f.Push([]byte{0, 0, 0, 3, 'a', 'b', 'c'})), "abc")
}

// The frames handed to the tee must not alias the framer's buffer, which is
// reused and reallocated on every push.
func TestFramesDoNotAliasTheInternalBuffer(t *testing.T) {
	f := New(cfg(false))
	got := frames(t, f.Push([]byte{0x00, 0x03, 'a', 'b', 'c'}))
	// Feed a lot more data, forcing the internal buffer to grow and move.
	for i := 0; i < 100; i++ {
		f.Push(append([]byte{0x00, 0x03}, 'x', 'y', 'z'))
	}
	assertFrames(t, got, "abc")
}

// A chunk that carries no complete frame must return no frames and no desync,
// and the bytes must survive until the rest arrives.
func TestPartialFrameWaitsForMoreBytes(t *testing.T) {
	f := New(cfg(false))
	assertFrames(t, frames(t, f.Push([]byte{0x00, 0x05, 'a', 'b'})))
	assertFrames(t, frames(t, f.Push([]byte{'c', 'd', 'e'})), "abcde")
}
