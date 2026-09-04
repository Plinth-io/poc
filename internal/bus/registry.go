package bus

import (
	"errors"
	"sync"
)

// Side decides the parity of locally allocated stream ids so the hub and the
// agent can both open streams without ever colliding.
type Side uint64

const (
	SideHub   Side = 1
	SideAgent Side = 2
)

// MaxStreams bounds concurrent streams per connection.
const MaxStreams = 256

var (
	ErrTooManyStreams  = errors.New("bus: too many concurrent streams")
	ErrDuplicateStream = errors.New("bus: stream id already in use")
)

// Registry owns the live streams of one connection.
type Registry struct {
	mu   sync.Mutex
	m    map[uint64]*Stream
	next uint64
}

func NewRegistry(side Side) *Registry {
	return &Registry{
		m:    make(map[uint64]*Stream),
		next: uint64(side),
	}
}

// Open allocates the next local stream id. Ids are never reused, so a late
// envelope for a finished stream can never hit a fresh one.
func (r *Registry) Open() (*Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) >= MaxStreams {
		return nil, ErrTooManyStreams
	}
	s := newStream(r.next, r)
	r.m[s.ID] = s
	r.next += 2
	return s, nil
}

// Accept registers a stream opened by the peer.
func (r *Registry) Accept(id uint64) (*Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[id]; exists {
		return nil, ErrDuplicateStream
	}
	if len(r.m) >= MaxStreams {
		return nil, ErrTooManyStreams
	}
	s := newStream(id, r)
	r.m[id] = s
	return s, nil
}

func (r *Registry) Get(id uint64) (*Stream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.m[id]
	return s, ok
}

func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m)
}

// CloseAll ends every stream of the connection, used when the websocket dies.
func (r *Registry) CloseAll(err error) {
	r.mu.Lock()
	streams := make([]*Stream, 0, len(r.m))
	for _, s := range r.m {
		streams = append(streams, s)
	}
	r.mu.Unlock()

	for _, s := range streams {
		s.Close(err)
	}
}

func (r *Registry) remove(id uint64) {
	r.mu.Lock()
	delete(r.m, id)
	r.mu.Unlock()
}
