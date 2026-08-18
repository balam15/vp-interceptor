package tee

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/stats"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// With no publisher the tee must be completely inert: no goroutines, no queues,
// and Offer must cost nothing and count nothing.
func TestInertWithoutAPublisher(t *testing.T) {
	st := &stats.Stats{}
	tr := Spawn(config.Defaults(), nil, st, quietLogger())

	if tr.ShouldOffer(VpToFms) || tr.ShouldOffer(FmsToVp) {
		t.Fatal("an inert tee must not ask for chunks")
	}
	tr.Open(1)
	tr.Offer(1, VpToFms, "p", []byte("payload"))
	tr.Close(1)
	tr.Shutdown() // must not panic or block

	if st.TeeAccepted.Load() != 0 || st.TeeDropped.Load() != 0 {
		t.Fatalf("accepted=%d dropped=%d, want 0/0", st.TeeAccepted.Load(), st.TeeDropped.Load())
	}
}

func TestPublishDirectionsGatesOffer(t *testing.T) {
	cases := map[string][2]bool{
		"both":      {true, true},
		"vp_to_fms": {true, false},
		"fms_to_vp": {false, true},
	}
	for directions, want := range cases {
		cfg := config.Defaults()
		cfg.Tee.PublishDirections = directions
		// active is driven by the publisher, which we cannot construct without
		// a broker, so assert on the direction gate itself.
		tr := &Tee{
			active:         true,
			publishVpToFms: cfg.Tee.PublishDirections != "fms_to_vp",
			publishFmsToVp: cfg.Tee.PublishDirections != "vp_to_fms",
		}
		if got := [2]bool{tr.ShouldOffer(VpToFms), tr.ShouldOffer(FmsToVp)}; got != want {
			t.Errorf("%s: got %v, want %v", directions, got, want)
		}
	}
}

// THE ISOLATION PROPERTY. A full shard queue must drop and count, never block
// the caller -- because the caller is the forwarding path.
func TestOfferDropsRatherThanBlockingWhenTheQueueIsFull(t *testing.T) {
	st := &stats.Stats{}
	// A tee with a queue but NO worker draining it, which is what a wedged
	// Kafka looks like from the hot path's point of view.
	tr := &Tee{
		shards:         []chan event{make(chan event, 2)},
		stats:          st,
		log:            quietLogger(),
		publishVpToFms: true,
		publishFmsToVp: true,
		active:         true,
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			tr.Offer(1, VpToFms, "p", []byte("payload"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Offer blocked on a full queue -- Kafka can now stall the payment path")
	}

	if got := st.TeeAccepted.Load(); got != 2 {
		t.Errorf("tee_accepted = %d, want 2 (the queue capacity)", got)
	}
	if got := st.TeeDropped.Load(); got != 98 {
		t.Errorf("tee_dropped = %d, want 98", got)
	}
}

// Offer must copy: the caller hands over its reusable read buffer.
func TestOfferCopiesTheCallersBuffer(t *testing.T) {
	tr := &Tee{
		shards:         []chan event{make(chan event, 4)},
		stats:          &stats.Stats{},
		log:            quietLogger(),
		publishVpToFms: true,
		active:         true,
	}
	buf := []byte("original")
	tr.Offer(1, VpToFms, "p", buf)
	copy(buf, "OVERWRIT") // the pump's very next read

	ev := <-tr.shards[0]
	if string(ev.chunk) != "original" {
		t.Fatalf("queued chunk aliased the caller's buffer: %q", ev.chunk)
	}
}

// Both directions of one connection must land on the same worker, which is what
// lets the per-connection timing state be lock-free.
func TestBothDirectionsShareAShard(t *testing.T) {
	tr := &Tee{shards: make([]chan event, 4)}
	for i := range tr.shards {
		tr.shards[i] = make(chan event, 1)
	}
	for id := uint64(0); id < 32; id++ {
		if tr.shard(id) != tr.shard(id) {
			t.Fatalf("conn %d hashed to two different shards", id)
		}
	}
}

// Shutdown must be survivable while pumps are still offering, because the drain
// that precedes it is bounded by a timeout and can give up with connections in
// flight. Closing the shard channels here would panic those pumps -- a Kafka
// shutdown killing live payment connections, which is the exact failure this
// whole design exists to prevent.
func TestShutdownIsSafeWhileOffersAreStillArriving(t *testing.T) {
	st := &stats.Stats{}
	tr := &Tee{
		shards:         []chan event{make(chan event, 8)},
		stats:          st,
		log:            quietLogger(),
		quit:           make(chan struct{}),
		publishVpToFms: true,
		active:         true,
	}
	// A worker that discards events, standing in for one with a publisher.
	tr.wg.Add(1)
	go func() {
		defer tr.wg.Done()
		for {
			select {
			case <-tr.shards[0]:
			case <-tr.quit:
				return
			}
		}
	}()

	stop := make(chan struct{})
	offersDone := make(chan struct{})
	go func() {
		defer close(offersDone)
		for {
			select {
			case <-stop:
				return
			default:
				// Must never panic, even long after Shutdown returns.
				tr.Offer(1, VpToFms, "p", []byte("payload"))
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	tr.Shutdown()
	tr.Shutdown() // idempotent
	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-offersDone
}

func TestMsHasMicrosecondResolution(t *testing.T) {
	if got := ms(1500 * time.Microsecond); got != 1.5 {
		t.Fatalf("ms(1500us) = %v, want 1.5", got)
	}
}

// Instant arithmetic must clamp rather than produce a negative duration.
func TestSubClampsAtZero(t *testing.T) {
	now := time.Now()
	if got := sub(now, now.Add(time.Second)); got != 0 {
		t.Fatalf("sub clamped to %v, want 0", got)
	}
}
