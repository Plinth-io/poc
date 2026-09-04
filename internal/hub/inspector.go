package hub

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
)

// inspectorQueue bounds how far a UI subscriber may lag before it loses
// events.
//
// ponytail: dropping is the whole policy — a stalled browser tab must never
// slow the bus down. Per-subscriber replay is the upgrade if anyone needs
// gap-free events.
const inspectorQueue = 128

// Event is one envelope as the UI shows it. Payloads stay out on purpose —
// the inspector demonstrates multiplexing, it is not a debugger.
type Event struct {
	At       time.Time
	AgentID  string
	Dir      string // "in" or "out", seen from the hub
	Kind     string
	StreamID uint64
	Size     int
}

// Inspector keeps the newest envelopes and fans them out to the UI.
type Inspector struct {
	mu       sync.Mutex
	capacity int
	events   []Event
	subs     map[chan Event]struct{}
	now      func() time.Time
}

func NewInspector(capacity int) *Inspector {
	return &Inspector{
		capacity: capacity,
		subs:     make(map[chan Event]struct{}),
		now:      time.Now,
	}
}

// Record adds one envelope to the buffer and fans it out to subscribers. It is
// called from bus.Conn's read and write loops via SetTap, so it must never
// block: the send to each subscriber is non-blocking, exactly like the
// agent's log buffer — a lagging browser tab loses events rather than
// stalling the bus.
func (i *Inspector) Record(agentID, dir string, env *busv1.Envelope) {
	ev := Event{
		At:       i.now(),
		AgentID:  agentID,
		Dir:      dir,
		Kind:     kindOf(env),
		StreamID: env.GetStreamId(),
		Size:     proto.Size(env),
	}
	if p, ok := env.GetPayload().(*busv1.Envelope_RpcMsg); ok {
		ev.Size = len(p.RpcMsg.GetPayload())
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.events = append(i.events, ev)
	if len(i.events) > i.capacity {
		i.events = i.events[len(i.events)-i.capacity:]
	}
	for ch := range i.subs {
		select {
		case ch <- ev:
		default: // a lagging UI loses events rather than stalling the bus
		}
	}
}

func (i *Inspector) Events() []Event {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]Event(nil), i.events...)
}

// Subscribe returns a channel of future events and the function that releases
// it.
func (i *Inspector) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, inspectorQueue)
	i.mu.Lock()
	i.subs[ch] = struct{}{}
	i.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			i.mu.Lock()
			delete(i.subs, ch)
			i.mu.Unlock()
		})
	}
}

func kindOf(env *busv1.Envelope) string {
	switch env.GetPayload().(type) {
	case *busv1.Envelope_Hello:
		return "hello"
	case *busv1.Envelope_RpcOpen:
		return "rpc_open"
	case *busv1.Envelope_RpcMsg:
		return "rpc_msg"
	case *busv1.Envelope_RpcHalfClose:
		return "rpc_half_close"
	case *busv1.Envelope_RpcHead:
		return "rpc_head"
	case *busv1.Envelope_RpcEnd:
		return "rpc_end"
	case *busv1.Envelope_RpcCancel:
		return "rpc_cancel"
	case *busv1.Envelope_HttpOpen:
		return "http_open"
	case *busv1.Envelope_HttpBody:
		return "http_body"
	case *busv1.Envelope_HttpEnd:
		return "http_end"
	case *busv1.Envelope_HttpResponseHead:
		return "http_response_head"
	case *busv1.Envelope_HttpResponseBody:
		return "http_response_body"
	case *busv1.Envelope_HttpResponseEnd:
		return "http_response_end"
	case *busv1.Envelope_HttpCancel:
		return "http_cancel"
	default:
		return "unknown"
	}
}
