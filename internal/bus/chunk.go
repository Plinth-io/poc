// Package bus carries tagged envelopes over a single websocket and splits it
// into independent logical streams.
package bus

import "errors"

const (
	// MaxChunk caps one envelope payload. Multiplexing alone does not stop a
	// large message from blocking the other streams, because the websocket
	// stays one ordered pipe — capping the chunk size is what makes the
	// multiplexing real.
	MaxChunk = 64 << 10

	// MaxMessage caps a reassembled gRPC message.
	MaxMessage = 4 << 20
)

var ErrMessageTooLarge = errors.New("bus: message exceeds 4 MiB")

// Chunks splits a payload into pieces of at most MaxChunk bytes. An empty
// payload yields one empty chunk, so a zero-length gRPC message still travels.
//
// The returned slices alias b — nothing is copied. Since Conn.Send only queues
// an envelope for the writer goroutine, the caller must not reuse or overwrite
// b until the chunks built from it have been sent.
func Chunks(b []byte) [][]byte {
	if len(b) == 0 {
		return [][]byte{{}}
	}
	out := make([][]byte, 0, (len(b)+MaxChunk-1)/MaxChunk)
	for len(b) > MaxChunk {
		out = append(out, b[:MaxChunk])
		b = b[MaxChunk:]
	}
	return append(out, b)
}

// Reassembler rebuilds a message from the chunks of one stream and direction.
// It is not safe for concurrent use; each stream owns one per direction.
type Reassembler struct {
	buf []byte
}

// Add appends a chunk. Once more is false it returns the complete payload and
// resets, so the same Reassembler serves every message of its stream.
func (r *Reassembler) Add(chunk []byte, more bool) ([]byte, bool, error) {
	if len(r.buf)+len(chunk) > MaxMessage {
		r.buf = nil
		return nil, false, ErrMessageTooLarge
	}
	r.buf = append(r.buf, chunk...)
	if more {
		return nil, false, nil
	}
	out := r.buf
	r.buf = nil
	return out, true, nil
}
