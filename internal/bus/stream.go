package bus

import (
	"sync"

	busv1 "plinth.io/poc/gen/bus/v1"
)

// inboxSize bounds how many envelopes may queue for one stream before the
// connection's read loop blocks.
//
// ponytail: one shared read loop, so a slow stream stalls its neighbours;
// per-stream credit windows are the upgrade if that ever shows up in practice.
const inboxSize = 16

// Stream is one logical channel on the bus. Every termination path — regular
// end, cancel, deadline, panic recovery, connection teardown — funnels through
// Close, which is the only place a stream leaves the registry.
type Stream struct {
	ID uint64
	In chan *busv1.Envelope

	reg  *Registry
	once sync.Once
	done chan struct{}

	mu  sync.Mutex
	err error
}

func newStream(id uint64, reg *Registry) *Stream {
	return &Stream{
		ID:   id,
		In:   make(chan *busv1.Envelope, inboxSize),
		reg:  reg,
		done: make(chan struct{}),
	}
}

// Close records err, unregisters the stream and releases everyone waiting on
// Done. Repeated calls keep the first error.
func (s *Stream) Close(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()

		s.reg.remove(s.ID)
		close(s.done)
	})
}

func (s *Stream) Done() <-chan struct{} { return s.done }

func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
