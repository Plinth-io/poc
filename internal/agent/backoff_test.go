package agent

import (
	"testing"
	"time"
)

func TestBackoffDoublesUntilTheCap(t *testing.T) {
	b := backoff{min: 500 * time.Millisecond, max: 8 * time.Second}

	tests := []struct {
		name string
		prev time.Duration
		want time.Duration
	}{
		{name: "first attempt starts at min", prev: 0, want: 500 * time.Millisecond},
		{name: "doubles", prev: 500 * time.Millisecond, want: time.Second},
		{name: "doubles again", prev: 2 * time.Second, want: 4 * time.Second},
		{name: "stops at max", prev: 6 * time.Second, want: 8 * time.Second},
		{name: "stays at max", prev: 8 * time.Second, want: 8 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// next adds jitter, so compare the base before jitter is applied.
			if got := b.base(tc.prev); got != tc.want {
				t.Fatalf("base(%v) = %v, want %v", tc.prev, got, tc.want)
			}
		})
	}
}

func TestBackoffJitterStaysInRange(t *testing.T) {
	b := backoff{min: time.Second, max: time.Second}
	for i := 0; i < 100; i++ {
		got := b.next(time.Second)
		if got < 500*time.Millisecond || got > time.Second {
			t.Fatalf("next = %v, outside [500ms, 1s]", got)
		}
	}
}
