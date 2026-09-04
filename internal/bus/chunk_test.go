package bus

import (
	"bytes"
	"errors"
	"testing"
)

func TestChunksSplitsAtLimit(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantChunks int
	}{
		{name: "empty yields one empty chunk", size: 0, wantChunks: 1},
		{name: "single byte", size: 1, wantChunks: 1},
		{name: "exactly one chunk", size: MaxChunk, wantChunks: 1},
		{name: "one byte over", size: MaxChunk + 1, wantChunks: 2},
		{name: "two and a half chunks", size: 2*MaxChunk + 7, wantChunks: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := bytes.Repeat([]byte{0xab}, tc.size)
			got := Chunks(in)
			if len(got) != tc.wantChunks {
				t.Fatalf("len(Chunks) = %d, want %d", len(got), tc.wantChunks)
			}
			for i, c := range got {
				if len(c) > MaxChunk {
					t.Fatalf("chunk %d has %d bytes, over the %d limit", i, len(c), MaxChunk)
				}
			}
			var joined []byte
			for _, c := range got {
				joined = append(joined, c...)
			}
			if !bytes.Equal(joined, in) {
				t.Fatal("rejoined chunks differ from input")
			}
		})
	}
}

func TestReassemblerRebuildsPayload(t *testing.T) {
	in := bytes.Repeat([]byte{0x5c}, 3*MaxChunk+11)
	chunks := Chunks(in)

	var r Reassembler
	for i, c := range chunks {
		more := i < len(chunks)-1
		payload, complete, err := r.Add(c, more)
		if err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
		if more {
			if complete {
				t.Fatalf("Add(%d) reported complete while more chunks were announced", i)
			}
			continue
		}
		if !complete {
			t.Fatal("final Add did not report complete")
		}
		if !bytes.Equal(payload, in) {
			t.Fatal("reassembled payload differs from input")
		}
	}
}

func TestReassemblerRejectsOversizedMessage(t *testing.T) {
	var r Reassembler
	full := bytes.Repeat([]byte{0x11}, MaxChunk)
	for sent := 0; sent <= MaxMessage; sent += MaxChunk {
		_, _, err := r.Add(full, true)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("Add error = %v, want ErrMessageTooLarge", err)
		}
		return
	}
	t.Fatalf("no error after exceeding %d bytes", MaxMessage)
}

func TestReassemblerResetsAfterCompletion(t *testing.T) {
	var r Reassembler
	if _, _, err := r.Add([]byte("first"), false); err != nil {
		t.Fatalf("Add: %v", err)
	}
	payload, complete, err := r.Add([]byte("second"), false)
	if err != nil || !complete {
		t.Fatalf("Add = (%v, %v, %v)", payload, complete, err)
	}
	if string(payload) != "second" {
		t.Fatalf("payload = %q, want %q — leftovers from the previous message", payload, "second")
	}
}
