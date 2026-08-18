// Package framing reassembles the TCP byte stream into application messages.
//
// This runs on the tee side only. It never sees the socket and can never delay
// or alter what is forwarded to FMS -- if it desyncs, we stop publishing for
// that stream and the proxy keeps running untouched.
package framing

import (
	"encoding/binary"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
)

// Step is the outcome of one Push.
//
// GO: Rust returns an enum (a tagged union) and Java a sealed interface. Go has
// neither, so this is a small struct where exactly one field is meaningful --
// checked in the documented order: Ignored, then Desync, then Frames. It is a
// value type, so returning it allocates nothing.
type Step struct {
	// Frames reassembled from this chunk. Each is an independent copy, safe to
	// hold after the caller's buffer is reused.
	Frames [][]byte
	// Desync is non-empty when THIS call desynced the stream. The caller should
	// count it and log once.
	Desync string
	// Ignored is true when the stream was already desynced; nothing to do.
	Ignored bool
}

// Framer holds the reassembly state for ONE direction of ONE connection.
//
// NOT SAFE FOR CONCURRENT USE, and it does not need to be: a Framer is owned by
// exactly one stream entry in exactly one tee worker's map, and that map is
// touched by exactly one goroutine.
//
// GO: Rust's compiler PROVES that confinement. Here it is a design rule that
// `go test -race` checks empirically -- the honest difference between the two
// builds, and the reason the shard-per-connection assignment in the tee is
// load-bearing rather than an optimisation.
type Framer struct {
	raw                  bool
	prefixLen            int
	bigEndian            bool
	lengthIncludesPrefix bool
	maxFrame             int

	buf []byte
	// Once desynced we cannot trust any byte offset in this stream again, so we
	// stop emitting rather than publish garbage into a payments topic.
	desynced bool
}

func New(cfg config.Framing) *Framer {
	return &Framer{
		raw:                  cfg.Mode == "raw",
		prefixLen:            cfg.PrefixBytes,
		bigEndian:            cfg.BigEndian,
		lengthIncludesPrefix: cfg.LengthIncludesPrefix,
		maxFrame:             cfg.MaxFrameBytes,
	}
}

// Push feeds one socket read into the reassembler.
//
// chunk is READ ONLY and is never retained: everything Push returns is copied
// out, so the caller may reuse the backing array the moment Push returns.
func (f *Framer) Push(chunk []byte) Step {
	if f.desynced {
		return Step{Ignored: true}
	}
	if f.raw {
		return Step{Frames: [][]byte{clone(chunk)}}
	}

	// GO: append reuses f.buf's capacity when it has room, so a steady stream
	// of complete frames settles on one allocation per Framer. The compaction at
	// the bottom is what keeps that true.
	f.buf = append(f.buf, chunk...)

	var out [][]byte
	n := f.prefixLen
	pos := 0

	for len(f.buf)-pos >= n {
		declared := f.readPrefix(f.buf[pos:])

		// Normalise to "bytes of body following the prefix".
		bodyLen := declared
		if f.lengthIncludesPrefix {
			// A length prefix that underflows and is then used as a length is
			// the origin of an enormous number of real CVEs in network parsers.
			// Go's int is signed so this cannot wrap to a huge positive the way
			// Rust's usize would, but it CAN go negative -- guarded below.
			bodyLen = declared - n
		}

		// Three guards, each preventing a distinct failure mode.
		if bodyLen < 0 {
			return f.desync(pos, "declared length shorter than prefix")
		}
		if bodyLen == 0 {
			// Zero-length would loop forever without consuming -- a HANG, not a
			// crash, which is worse to diagnose.
			return f.desync(pos, "zero-length frame")
		}
		if bodyLen > f.maxFrame {
			// A bogus length prefix. Without this, one garbage read tries to
			// allocate gigabytes. THE classic length-prefix DoS.
			return f.desync(pos, "frame exceeds max_frame_bytes")
		}
		if len(f.buf)-pos < n+bodyLen {
			break // partial frame, wait for more bytes
		}

		// GO: this copy is not optional. Rust hands out refcounted views into
		// the buffer; here f.buf is reused and reallocated underneath, so a
		// frame that aliased it would mutate after being queued.
		//
		// It is also the fix the Rust build needed anyway: a small frame that
		// aliased a large read buffer would pin the whole allocation while it
		// sat in the tee queue. See ../../BENCHMARK.md.
		out = append(out, clone(f.buf[pos+n:pos+n+bodyLen]))
		pos += n + bodyLen
	}

	f.compact(pos)
	return Step{Frames: out}
}

// desync marks the stream dead, drops what it consumed, and reports why.
//
// Frames already reassembled during THIS call are discarded with it: once the
// offsets are untrustworthy, so is anything read using them.
func (f *Framer) desync(pos int, reason string) Step {
	f.desynced = true
	f.compact(pos)
	return Step{Desync: reason}
}

// compact drops the first `consumed` bytes, reusing the same backing array.
func (f *Framer) compact(consumed int) {
	if consumed == 0 {
		return
	}
	f.buf = append(f.buf[:0], f.buf[consumed:]...)
}

func (f *Framer) readPrefix(b []byte) int {
	if f.prefixLen == 2 {
		if f.bigEndian {
			return int(binary.BigEndian.Uint16(b))
		}
		return int(binary.LittleEndian.Uint16(b))
	}
	// prefix_bytes is validated to be 2 or 4 at config load, so this is the
	// 4-byte case. int is 64-bit on every platform this runs on, so a 4-byte
	// big-endian length cannot overflow it.
	if f.bigEndian {
		return int(binary.BigEndian.Uint32(b))
	}
	return int(binary.LittleEndian.Uint32(b))
}

// clone returns an exact-size copy. Sized precisely rather than by append, so a
// frame queued for Kafka holds only its own bytes.
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
