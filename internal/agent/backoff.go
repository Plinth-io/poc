package agent

import (
	"math/rand/v2"
	"time"
)

// backoff spaces out reconnect attempts. Jitter keeps many agents from
// hammering the hub in lockstep after a restart.
type backoff struct {
	min time.Duration
	max time.Duration
}

// base is the undithered delay following prev.
func (b backoff) base(prev time.Duration) time.Duration {
	if prev <= 0 {
		return b.min
	}
	next := prev * 2
	if next > b.max {
		return b.max
	}
	return next
}

// next returns the delay to wait before the attempt following prev, dithered
// down by up to half.
func (b backoff) next(prev time.Duration) time.Duration {
	d := b.base(prev)
	return d - time.Duration(rand.Int64N(int64(d/2)+1))
}
