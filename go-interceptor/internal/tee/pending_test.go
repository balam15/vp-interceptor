package tee

import (
	"testing"
	"time"
)

func TestPendingQIsFifo(t *testing.T) {
	var q pendingQ
	base := time.Now()
	for i := 0; i < 3; i++ {
		q.PushBack(base.Add(time.Duration(i) * time.Second))
	}
	if q.Len() != 3 {
		t.Fatalf("Len = %d", q.Len())
	}
	for i := 0; i < 3; i++ {
		got, ok := q.PopFront()
		if !ok || !got.Equal(base.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("pop %d = %v, %v", i, got, ok)
		}
	}
	if _, ok := q.PopFront(); ok {
		t.Fatal("expected empty")
	}
}

// An unsolicited response must find nothing to pair with rather than misbehave.
func TestPendingQPopOnEmpty(t *testing.T) {
	var q pendingQ
	if _, ok := q.PopFront(); ok {
		t.Fatal("expected false on empty queue")
	}
}

// The request/response rhythm must not grow the backing array without bound,
// which is the failure mode a naive `buf = buf[1:]` deque has.
func TestPendingQDoesNotGrowUnbounded(t *testing.T) {
	var q pendingQ
	now := time.Now()
	// Steady state: one outstanding request at a time, ten thousand times.
	for i := 0; i < 10_000; i++ {
		q.PushBack(now)
		q.PopFront()
	}
	if got := cap(q.buf); got > 4 {
		t.Fatalf("cap grew to %d in steady state", got)
	}

	// Saturated state: always one more push than pop, capped by the caller at
	// max_pending. The backing array must compact rather than creep.
	const maxPending = 256
	for i := 0; i < 10_000; i++ {
		if q.Len() >= maxPending {
			q.PopFront()
		}
		q.PushBack(now)
	}
	if q.Len() != maxPending {
		t.Fatalf("Len = %d, want %d", q.Len(), maxPending)
	}
	if got := cap(q.buf); got > 4*maxPending {
		t.Fatalf("cap crept to %d with %d live entries", got, maxPending)
	}
}

func TestDirectionLabels(t *testing.T) {
	if VpToFms.String() != "vp_to_fms" || FmsToVp.String() != "fms_to_vp" {
		t.Fatal("direction labels must match the Rust and Java builds exactly")
	}
}
