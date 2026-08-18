package tee

import "time"

// pendingQ is a FIFO of forwarded-request timestamps awaiting a response.
//
// GO: a slice-backed deque rather than container/list, because this is on the
// per-frame path and container/list allocates an Element for every request. In
// the normal request/response rhythm the queue empties on every response, which
// resets it to offset zero -- so after the first push it never allocates again.
//
// Bounded by the caller at timing.max_pending; compact() keeps the backing array
// from creeping even when the queue never fully drains.
type pendingQ struct {
	buf  []time.Time
	head int
}

func (q *pendingQ) Len() int { return len(q.buf) - q.head }

func (q *pendingQ) PushBack(t time.Time) {
	// Compact instead of growing whenever there is reclaimable space in front.
	if q.head > 0 && len(q.buf)+1 > cap(q.buf) {
		q.buf = append(q.buf[:0], q.buf[q.head:]...)
		q.head = 0
	}
	q.buf = append(q.buf, t)
}

// PopFront returns the oldest timestamp, or false when empty -- so an
// unsolicited response cannot corrupt anything.
func (q *pendingQ) PopFront() (time.Time, bool) {
	if q.head >= len(q.buf) {
		return time.Time{}, false
	}
	t := q.buf[q.head]
	q.head++
	if q.head == len(q.buf) {
		q.buf = q.buf[:0]
		q.head = 0
	}
	return t, true
}
