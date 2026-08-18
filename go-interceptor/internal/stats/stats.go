// Package stats holds the process-wide counters.
//
// Mirrors rust-interceptor/src/stats.rs and java-interceptor Stats.java, with
// the SAME METRIC NAMES, so one dashboard works against all three builds.
package stats

import "sync/atomic"

// Stats is every counter the process exports.
//
// All counters are plain atomics. They are incremented on the hot path, and we
// only ever read them for reporting -- never to make a decision. That invariant
// is what makes the unsynchronised sum/count/max triples below acceptable.
//
// GO: atomic.Uint64 (Go 1.19+) embeds the value rather than boxing it, so this
// struct is ONE allocation of 21 contiguous words -- the same layout as the Rust
// build, and unlike Java's 21 separate AtomicLong/LongAdder objects.
//
// GO: it must always be handled as *Stats. atomic.Uint64 contains a noCopy
// marker, so `go vet` fails the build if anyone ever copies a Stats by value.
type Stats struct {
	ConnsAccepted         atomic.Uint64
	ConnsActive           atomic.Uint64
	ConnsRejected         atomic.Uint64
	UpstreamConnectFailed atomic.Uint64

	BytesVpToFms atomic.Uint64
	BytesFmsToVp atomic.Uint64

	// TeeAccepted counts chunks accepted into a shard queue.
	//
	// Named "accepted", not "offered", because it counts only the successes:
	// total offers are accepted + dropped, which is what a drop-RATE alert needs
	// as its denominator.
	TeeAccepted atomic.Uint64
	// TeeDropped counts chunks discarded because a shard queue was full. This is
	// the number to alert on: it means Kafka is degraded, and it proves the hot
	// path was not.
	TeeDropped atomic.Uint64

	FramesEmitted atomic.Uint64
	FramerDesyncs atomic.Uint64

	KafkaEnqueued atomic.Uint64
	// KafkaEnqueueFailed counts sends the writer refused outright -- shed here
	// rather than blocked on.
	KafkaEnqueueFailed  atomic.Uint64
	KafkaDelivered      atomic.Uint64
	KafkaDeliveryFailed atomic.Uint64

	// -- timing ----------------------------------------------------------
	// Exposed as sum/count/max rather than an average, so a scraper can compute
	// rates over an interval. An average since process start is almost useless:
	// it never recovers from one bad hour.

	// RttCount/RttSumUs/RttMaxUs: FMS round-trip, request forwarded -> matching
	// response seen.
	RttCount atomic.Uint64
	RttSumUs atomic.Uint64
	RttMaxUs atomic.Uint64
	// RttUnmatched counts requests that closed without a matching response.
	RttUnmatched atomic.Uint64
	// ConnDuration*: connection lifetime, accept -> close.
	ConnDurationCount atomic.Uint64
	ConnDurationSumMs atomic.Uint64
	ConnDurationMaxMs atomic.Uint64
}

// Max sets counter to max(counter, n).
//
// GO: there is no atomic fetch_max, so this is the CAS loop Rust gets as one
// instruction. Called once per observation, off the forwarding path.
func Max(counter *atomic.Uint64, n uint64) {
	for {
		prev := counter.Load()
		if n <= prev {
			return
		}
		if counter.CompareAndSwap(prev, n) {
			return
		}
	}
}

// Observe records one observation into a sum/count/max triple.
//
// The three counters are deliberately NOT updated atomically as a group: a
// scraper may see count incremented before sum. That is fine for metrics, and
// making it atomic as a group would need a lock on a per-frame path.
func Observe(count, sum, max *atomic.Uint64, value uint64) {
	count.Add(1)
	sum.Add(value)
	Max(max, value)
}

// Metric is one exported counter. A slice of these rather than a map, because
// the ORDER IS PART OF THE OUTPUT -- /metrics and / must be stable across
// scrapes and identical to the other two builds.
type Metric struct {
	Name  string
	Value uint64
}

// Snapshot reads every counter. Not a consistent snapshot across counters, and
// does not need to be.
func (s *Stats) Snapshot() []Metric {
	return []Metric{
		{"conns_accepted", s.ConnsAccepted.Load()},
		{"conns_active", s.ConnsActive.Load()},
		{"conns_rejected", s.ConnsRejected.Load()},
		{"upstream_connect_failed", s.UpstreamConnectFailed.Load()},
		{"bytes_vp_to_fms", s.BytesVpToFms.Load()},
		{"bytes_fms_to_vp", s.BytesFmsToVp.Load()},
		{"tee_accepted", s.TeeAccepted.Load()},
		{"tee_dropped", s.TeeDropped.Load()},
		{"frames_emitted", s.FramesEmitted.Load()},
		{"framer_desyncs", s.FramerDesyncs.Load()},
		{"kafka_enqueued", s.KafkaEnqueued.Load()},
		{"kafka_enqueue_failed", s.KafkaEnqueueFailed.Load()},
		{"kafka_delivered", s.KafkaDelivered.Load()},
		{"kafka_delivery_failed", s.KafkaDeliveryFailed.Load()},
		{"rtt_count", s.RttCount.Load()},
		{"rtt_sum_us", s.RttSumUs.Load()},
		{"rtt_max_us", s.RttMaxUs.Load()},
		{"rtt_unmatched", s.RttUnmatched.Load()},
		{"conn_duration_count", s.ConnDurationCount.Load()},
		{"conn_duration_sum_ms", s.ConnDurationSumMs.Load()},
		{"conn_duration_max_ms", s.ConnDurationMaxMs.Load()},
	}
}
