package agent

import (
	"strings"
	"sync"
)

// subscriberQueue bounds how far a UI client may lag before it loses lines.
//
// ponytail: dropping is the whole policy — a stalled browser tab must never
// slow the agent down. Per-subscriber replay is the upgrade if anyone needs
// gap-free logs.
const subscriberQueue = 64

// LogBuffer is an io.Writer for a slog handler. It keeps the last lines for
// the UI's initial render and fans new ones out to live subscribers.
type LogBuffer struct {
	mu       sync.Mutex
	capacity int
	lines    []string
	partial  strings.Builder
	subs     map[chan string]struct{}
}

func NewLogBuffer(capacity int) *LogBuffer {
	return &LogBuffer{capacity: capacity, subs: make(map[chan string]struct{})}
}

func (b *LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.partial.Write(p)
	text := b.partial.String()
	for {
		idx := strings.IndexByte(text, '\n')
		if idx < 0 {
			break
		}
		b.appendLine(strings.TrimRight(text[:idx], "\r"))
		text = text[idx+1:]
	}
	b.partial.Reset()
	b.partial.WriteString(text)
	return len(p), nil
}

func (b *LogBuffer) appendLine(line string) {
	b.lines = append(b.lines, line)
	if len(b.lines) > b.capacity {
		b.lines = b.lines[len(b.lines)-b.capacity:]
	}
	for ch := range b.subs {
		select {
		case ch <- line:
		default: // a lagging subscriber loses lines rather than blocking us
		}
	}
}

func (b *LogBuffer) Lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.lines...)
}

// Subscribe returns a channel of future lines and the function that releases it.
func (b *LogBuffer) Subscribe() (<-chan string, func()) {
	ch := make(chan string, subscriberQueue)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, ch)
			b.mu.Unlock()
		})
	}
}
