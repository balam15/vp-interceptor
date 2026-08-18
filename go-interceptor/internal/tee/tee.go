// Package tee is the publish side: sharded workers that reassemble frames and
// hand them to Kafka, entirely off the forwarding path.
//
// THE ISOLATION BOUNDARY BETWEEN THE PAYMENT PATH AND KAFKA IS Offer'S BOUNDED,
// NON-BLOCKING SEND. When Kafka is behind, frames are dropped and counted rather
// than applying backpressure to the socket.
package tee

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/framing"
	"github.com/vynamic/vp-fms-interceptor/internal/kafka"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
)

// Direction is which way a chunk was travelling.
type Direction uint8

const (
	VpToFms Direction = iota
	FmsToVp
)

// String returns the label used in metrics, logs and the Kafka envelope.
//
// GO: returns a compile-time constant string -- no allocation, and the same two
// labels the Rust and Java builds emit.
func (d Direction) String() string {
	if d == VpToFms {
		return "vp_to_fms"
	}
	return "fms_to_vp"
}

type eventKind uint8

const (
	evOpen eventKind = iota
	evData
	evClose
)

// event is what travels down a shard channel.
//
// GO: a struct VALUE, not a pointer or an interface. A buffered channel of
// values preallocates its whole ring at construction, so steady-state publishing
// costs no per-event allocation -- the same property the Rust build gets from
// its bounded mpsc, and one the Java build cannot have (every Event record is a
// heap object).
type event struct {
	kind   eventKind
	connID uint64
	dir    Direction
	peer   string
	chunk  []byte
	at     time.Time
}

// Tee is the handle every connection pump holds. Safe for concurrent use: after
// construction nothing in it is mutated.
type Tee struct {
	shards []chan event
	stats  *stats.Stats
	log    *slog.Logger
	wg     sync.WaitGroup
	// quit is closed by Shutdown. The shard channels are deliberately NEVER
	// closed: Offer runs on the forwarding path, and a send on a closed channel
	// panics. A late Offer must be able to lose its chunk harmlessly, not take
	// down a connection that is still carrying payments.
	quit chan struct{}

	publishVpToFms bool
	publishFmsToVp bool
	// active is false when Kafka is disabled or unbuildable, so Offer costs one
	// predictable branch and nothing else. Written once, during construction,
	// and read without synchronisation by every pump -- so it must never be
	// mutated afterwards.
	active bool
}

const (
	idleSweep = 60 * time.Second
	idleMax   = 600 * time.Second
)

// Spawn starts the shard workers. publisher may be nil, in which case the
// returned Tee is inert.
func Spawn(cfg *config.Config, publisher *kafka.Publisher, st *stats.Stats, log *slog.Logger) *Tee {
	t := &Tee{
		stats:          st,
		log:            log,
		quit:           make(chan struct{}),
		publishVpToFms: cfg.Tee.PublishDirections != "fms_to_vp",
		publishFmsToVp: cfg.Tee.PublishDirections != "vp_to_fms",
		active:         publisher != nil,
	}
	if !t.active {
		t.publishVpToFms, t.publishFmsToVp = false, false
		return t
	}

	t.shards = make([]chan event, cfg.Tee.Shards)
	for i := range t.shards {
		// Bounded. THIS BOUND IS THE ISOLATION BOUNDARY.
		t.shards[i] = make(chan event, cfg.Tee.QueueCapacity)
		w := &worker{
			shard:     i,
			in:        t.shards[i],
			quit:      t.quit,
			publisher: publisher,
			framing:   cfg.Framing,
			timingCfg: cfg.Timing,
			stats:     st,
			log:       log,
			state:     make(map[streamKey]*streamState),
			timing:    make(map[uint64]*connTiming),
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			w.run()
		}()
	}
	log.Info("tee workers started",
		"shards", cfg.Tee.Shards, "queue_capacity", cfg.Tee.QueueCapacity)
	return t
}

// wants reports whether this direction is published at all.
func (t *Tee) wants(dir Direction) bool {
	if dir == VpToFms {
		return t.publishVpToFms
	}
	return t.publishFmsToVp
}

// ShouldOffer reports whether a chunk in this direction would actually be
// queued. Lets the pump skip work it would only throw away.
func (t *Tee) ShouldOffer(dir Direction) bool {
	return t.active && t.wants(dir)
}

// Offer hands a copy of the wire bytes to the publish path.
//
// ================= THE MOST SAFETY-CRITICAL FUNCTION HERE =================
// Called from the forwarding hot path on every read. It MUST NOT block, and it
// has no error to return, so a caller cannot be tempted to retry or propagate.
// The rule the whole program serves -- KAFKA MUST NEVER BE ABLE TO AFFECT
// VP<->FMS -- lives or dies on the non-blocking send below.
//
// chunk is the pump's reusable read buffer: it is COPIED here, and the caller
// may overwrite it the moment this returns.
// ==========================================================================
func (t *Tee) Offer(connID uint64, dir Direction, peer string, chunk []byte) {
	if !t.active || !t.wants(dir) {
		return
	}

	// Sized exactly, not by append: a small payments message must not keep a
	// 16 KiB read buffer alive while it sits in the queue. The Rust build peaked
	// at 409 MB RSS learning that lesson -- see ../../BENCHMARK.md.
	buf := make([]byte, len(chunk))
	copy(buf, chunk)

	ev := event{kind: evData, connID: connID, dir: dir, peer: peer, chunk: buf}
	// GO: select with a default is the non-blocking send. This one line IS the
	// load-shedding decision: Kafka is behind -> drop the audit copy, keep
	// forwarding the payment. The correct trade, and it is observable.
	select {
	case t.shard(connID) <- ev:
		t.stats.TeeAccepted.Add(1)
	default:
		t.stats.TeeDropped.Add(1)
	}
}

// Open marks a connection as accepted, so conn_age_ms is measured from the real
// accept time rather than from the first frame. Best-effort: if it is dropped,
// the worker falls back to first-frame time.
func (t *Tee) Open(connID uint64) {
	t.send(event{kind: evOpen, connID: connID, at: time.Now()})
}

// Close releases the worker's per-connection framer and timing state.
// Best-effort: a dropped Close only means that state lives until the idle sweep
// reclaims it.
func (t *Tee) Close(connID uint64) {
	t.send(event{kind: evClose, connID: connID, at: time.Now()})
}

func (t *Tee) send(ev event) {
	if !t.active {
		return
	}
	select {
	case t.shard(ev.connID) <- ev:
	default:
	}
}

// shard assigns by connection ID, which is what guarantees BOTH DIRECTIONS OF
// ONE CONNECTION LAND ON THE SAME WORKER -- and that is why the per-connection
// timing state below needs no locking at all.
func (t *Tee) shard(connID uint64) chan event {
	return t.shards[connID%uint64(len(t.shards))]
}

// Shutdown asks the workers to publish what is already queued and stop.
//
// Safe to call while connections are still running -- which matters, because
// the drain before it is bounded by a timeout and may give up with pumps still
// in flight. Those pumps keep calling Offer; their chunks are simply dropped
// once the workers have gone. Idempotent.
func (t *Tee) Shutdown() {
	if !t.active {
		return
	}
	select {
	case <-t.quit: // already shut down
		return
	default:
		close(t.quit)
	}
	t.wg.Wait()
}

// ---------------------------------------------------------------------------

type streamKey struct {
	connID uint64
	dir    Direction
}

type streamState struct {
	framer      *framing.Framer
	seq         uint64
	lastTouched time.Time
}

// connTiming is per-connection measurement state. Both directions of a
// connection hash to the same shard, so this needs no locking.
type connTiming struct {
	opened time.Time
	// lastFrame is the previous frame on this connection, either direction.
	lastFrame time.Time
	// pending holds forwarded requests awaiting a response, oldest first.
	pending     pendingQ
	lastTouched time.Time
}

func newConnTiming(now time.Time) *connTiming {
	return &connTiming{opened: now, lastTouched: now}
}

type worker struct {
	shard     int
	in        <-chan event
	quit      <-chan struct{}
	publisher *kafka.Publisher
	framing   config.Framing
	timingCfg config.Timing
	stats     *stats.Stats
	log       *slog.Logger

	// Owned by this goroutine alone -- no mutex, no sync.Map. That is the whole
	// reason for sharding by connection ID.
	state  map[streamKey]*streamState
	timing map[uint64]*connTiming
}

func (w *worker) run() {
	sweep := time.NewTicker(idleSweep)
	defer sweep.Stop()

	for {
		select {
		case ev := <-w.in:
			w.handle(ev)
		case <-sweep.C:
			w.sweep()
		case <-w.quit:
			// Publish what is already queued before going, so a clean shutdown
			// does not throw away audit records that were already accepted.
			// Anything a still-running pump offers after this is dropped, which
			// is the correct trade at shutdown.
			drained := 0
			for {
				select {
				case ev := <-w.in:
					w.handle(ev)
					drained++
				default:
					w.log.Info("tee worker stopped", "shard", w.shard, "drained", drained)
					return
				}
			}
		}
	}
}

// sweep is a manual leak guard. If a Close event were dropped because a queue
// was full, that connection's state would live forever; this bounds it at ten
// minutes. Note that garbage collection does not help here -- this is a LOGICAL
// leak, and the Rust and Java builds need exactly the same sweep.
func (w *worker) sweep() {
	now := time.Now()
	before := len(w.state) + len(w.timing)
	for k, s := range w.state {
		if now.Sub(s.lastTouched) >= idleMax {
			delete(w.state, k)
		}
	}
	for k, t := range w.timing {
		if now.Sub(t.lastTouched) >= idleMax {
			delete(w.timing, k)
		}
	}
	if reclaimed := before - (len(w.state) + len(w.timing)); reclaimed > 0 {
		w.log.Debug("swept idle connection state", "shard", w.shard, "reclaimed", reclaimed)
	}
}

func (w *worker) handle(ev event) {
	switch ev.kind {
	case evOpen:
		if w.timingCfg.Enabled {
			w.timing[ev.connID] = newConnTiming(ev.at)
		}
	case evClose:
		delete(w.state, streamKey{ev.connID, VpToFms})
		delete(w.state, streamKey{ev.connID, FmsToVp})
		if t, ok := w.timing[ev.connID]; ok {
			delete(w.timing, ev.connID)
			dur := ev.at.Sub(t.opened)
			if dur < 0 {
				dur = 0
			}
			stats.Observe(&w.stats.ConnDurationCount, &w.stats.ConnDurationSumMs,
				&w.stats.ConnDurationMaxMs, uint64(dur.Milliseconds()))
			if n := t.pending.Len(); n > 0 {
				w.stats.RttUnmatched.Add(uint64(n))
				w.log.Debug("connection closed with unmatched requests",
					"conn_id", ev.connID, "duration_ms", ms(dur), "unmatched_requests", n)
			}
		}
	case evData:
		w.handleData(ev)
	}
}

func (w *worker) handleData(ev event) {
	key := streamKey{ev.connID, ev.dir}
	st, ok := w.state[key]
	if !ok {
		st = &streamState{framer: framing.New(w.framing)}
		w.state[key] = st
	}
	st.lastTouched = time.Now()

	step := st.framer.Push(ev.chunk)
	if step.Ignored {
		return
	}
	if step.Desync != "" {
		w.stats.FramerDesyncs.Add(1)
		w.log.Warn("framing desync; publishing suspended for this stream (proxy unaffected)",
			"conn_id", ev.connID, "direction", ev.dir.String(), "reason", step.Desync)
		return
	}
	if len(step.Frames) == 0 {
		return
	}

	tsMs := time.Now().UnixMilli()
	// One string per chunk, not per frame. It is the Kafka partition key, so
	// both directions of a connection land on the same partition and stay
	// ordered.
	pkey := strconv.FormatUint(ev.connID, 10)

	for _, frame := range step.Frames {
		st.seq++
		w.stats.FramesEmitted.Add(1)

		// Timing is measured per frame, not per chunk, so a pipelined read
		// yields one measurement per message.
		var connAge, gap, rtt *float64
		if w.timingCfg.Enabled {
			connAge, gap, rtt = w.measure(ev.connID, ev.dir, time.Now())
		}

		w.publisher.Publish(pkey, &kafka.Meta{
			ConnID:    ev.connID,
			Direction: ev.dir.String(),
			Seq:       st.seq,
			Peer:      ev.peer,
			TsMs:      tsMs,
			ConnAgeMs: connAge,
			GapMs:     gap,
			RttMs:     rtt,
		}, frame)
	}
}

// measure returns (conn_age_ms, gap_ms, rtt_ms) for one frame.
//
// rtt_ms is only produced for fms_to_vp frames, by pairing with the oldest
// unanswered request on the connection.
func (w *worker) measure(connID uint64, dir Direction, now time.Time) (connAge, gap, rtt *float64) {
	t, ok := w.timing[connID]
	if !ok {
		// Falls back to first-frame time if the Open event was dropped.
		t = newConnTiming(now)
		w.timing[connID] = t
	}
	t.lastTouched = now

	connAge = ptr(ms(sub(now, t.opened)))
	if !t.lastFrame.IsZero() {
		gap = ptr(ms(sub(now, t.lastFrame)))
	}
	t.lastFrame = now

	if !w.timingCfg.PairRequestResponse {
		return connAge, gap, nil
	}

	if dir == VpToFms {
		if t.pending.Len() >= w.timingCfg.MaxPending {
			// Responses have stopped arriving. Drop the oldest rather than grow
			// without bound.
			t.pending.PopFront()
			w.stats.RttUnmatched.Add(1)
		}
		t.pending.PushBack(now)
		return connAge, gap, nil
	}

	// An unsolicited response finds nothing to pair with, which is fine.
	//
	// The FIFO pairing ASSUMES responses come back in request order. That
	// assumption is documented on timing.pair_request_response in config.toml,
	// where an operator will actually read it, with the remedy stated.
	if sent, ok := t.pending.PopFront(); ok {
		d := sub(now, sent)
		rtt = ptr(ms(d))
		stats.Observe(&w.stats.RttCount, &w.stats.RttSumUs, &w.stats.RttMaxUs,
			uint64(d.Microseconds()))
	}
	return connAge, gap, rtt
}

// sub clamps at zero rather than returning a negative duration.
func sub(a, b time.Time) time.Duration {
	if d := a.Sub(b); d > 0 {
		return d
	}
	return 0
}

// ms renders a duration as milliseconds with microsecond resolution.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func ptr(f float64) *float64 { return &f }
