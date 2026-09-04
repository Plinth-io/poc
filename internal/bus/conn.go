package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
)

// readLimit bounds one websocket message. Payloads are capped at MaxChunk, the
// rest is envelope overhead.
const readLimit = 1 << 20

// outboxSize bounds envelopes waiting for the single writer goroutine. A full
// outbox blocks producers, which is the connection's backpressure.
const outboxSize = 64

// closeWait bounds how long Run waits for a started close handshake to reach
// the wire: long enough for the frame to go out, short enough not to hold Run
// hostage for a peer that never acks.
const closeWait = 250 * time.Millisecond

// ErrConnClosed reports that the underlying websocket is gone.
var ErrConnClosed = errors.New("bus: connection closed")

// OpenFunc handles an envelope that opens a new stream. It runs on its own
// goroutine and owns the stream from then on.
type OpenFunc func(*Stream, *busv1.Envelope)

// Conn multiplexes many logical streams over one websocket. It knows nothing
// about gRPC or HTTP; the relays are its users.
type Conn struct {
	Streams *Registry

	ws     *websocket.Conn
	out    chan *busv1.Envelope
	onOpen OpenFunc

	closeOnce sync.Once
	closed    chan struct{}
	closeDone chan struct{}

	tapMu sync.RWMutex
	tap   func(dir string, env *busv1.Envelope)
}

func NewConn(ws *websocket.Conn, side Side, onOpen OpenFunc) *Conn {
	ws.SetReadLimit(readLimit)
	return &Conn{
		Streams: NewRegistry(side),
		ws:      ws,
		out:     make(chan *busv1.Envelope, outboxSize),
		onOpen:  onOpen,
		closed:  make(chan struct{}),
	}
}

// SetTap installs an observer for every envelope in both directions. The hub UI
// uses it for its live inspector; dir is "in" or "out".
func (c *Conn) SetTap(f func(dir string, env *busv1.Envelope)) {
	c.tapMu.Lock()
	c.tap = f
	c.tapMu.Unlock()
}

func (c *Conn) observe(dir string, env *busv1.Envelope) {
	c.tapMu.RLock()
	f := c.tap
	c.tapMu.RUnlock()
	if f != nil {
		f(dir, env)
	}
}

// Send queues an envelope for the writer goroutine. The caller sets StreamId.
func (c *Conn) Send(ctx context.Context, env *busv1.Envelope) error {
	select {
	case c.out <- env:
		return nil
	case <-c.closed:
		return ErrConnClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run drives the read and write loops until one of them fails, then closes
// every stream of this connection.
func (c *Conn) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var writeErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.writeLoop(ctx); err != nil {
			writeErr = err
			cancel()
		}
	}()

	err := c.readLoop(ctx)
	cancel()
	wg.Wait()

	// readLoop dying only because writeLoop's failure canceled the shared ctx
	// masks the real cause; surface writeLoop's error instead.
	if errors.Is(err, context.Canceled) && writeErr != nil {
		err = writeErr
	}

	if c.closeDone != nil {
		select {
		case <-c.closeDone:
		case <-time.After(closeWait):
		}
	}

	c.closeOnce.Do(func() { close(c.closed) })
	c.Streams.CloseAll(fmt.Errorf("%w: %v", ErrConnClosed, err))
	return err
}

func (c *Conn) writeLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env := <-c.out:
			raw, err := proto.Marshal(env)
			if err != nil {
				return fmt.Errorf("marshal envelope: %w", err)
			}
			c.observe("out", env)
			if err := c.ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
				return err
			}
		}
	}
}

// closeProtocolError starts the close handshake with StatusProtocolError but
// does not block readLoop on it: Close performs a close handshake with a
// fixed internal timeout, and a peer that never acks must not hold Run
// hostage. Run still gives it up to closeWait before returning.
//
// ponytail: close handshake detached, join it if a caller ever needs the fd
// released the moment Run returns.
func (c *Conn) closeProtocolError(reason string) {
	c.closeDone = make(chan struct{})
	done := c.closeDone
	go func() {
		defer close(done)
		if err := c.ws.Close(websocket.StatusProtocolError, reason); err != nil {
			slog.Debug("close handshake after protocol error", "reason", reason, "err", err)
		}
	}()
}

func (c *Conn) readLoop(ctx context.Context) error {
	for {
		typ, raw, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageBinary {
			c.closeProtocolError("expected binary frames")
			return fmt.Errorf("bus: unexpected message type %v", typ)
		}

		env := &busv1.Envelope{}
		if err := proto.Unmarshal(raw, env); err != nil {
			// A corrupt envelope means the bus state is no longer trustworthy,
			// so the whole connection goes rather than a single stream.
			c.closeProtocolError("malformed envelope")
			return fmt.Errorf("bus: malformed envelope: %w", err)
		}
		c.observe("in", env)
		c.dispatch(ctx, env)
	}
}

func (c *Conn) dispatch(ctx context.Context, env *busv1.Envelope) {
	if s, ok := c.Streams.Get(env.GetStreamId()); ok {
		select {
		case s.In <- env:
		case <-s.Done():
		case <-ctx.Done():
		}
		return
	}

	if !opensStream(env) {
		// Late envelopes after a cancel are expected, not an error.
		slog.Debug("envelope for unknown stream", "stream_id", env.GetStreamId())
		return
	}

	s, err := c.Streams.Accept(env.GetStreamId())
	if err != nil {
		slog.Warn("refusing new stream", "stream_id", env.GetStreamId(), "err", err)
		return
	}
	if c.onOpen == nil {
		s.Close(errors.New("bus: no handler for incoming streams"))
		return
	}
	go c.onOpen(s, env)
}

func opensStream(env *busv1.Envelope) bool {
	switch env.GetPayload().(type) {
	case *busv1.Envelope_RpcOpen, *busv1.Envelope_HttpOpen:
		return true
	default:
		return false
	}
}
