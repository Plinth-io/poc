package hub

import (
	"testing"
	"time"

	busv1 "plinth.io/poc/gen/bus/v1"
)

func TestInspectorRecordsKindAndSize(t *testing.T) {
	insp := NewInspector(10)
	insp.Record("mac-1", "out", &busv1.Envelope{
		StreamId: 7,
		Payload: &busv1.Envelope_RpcMsg{RpcMsg: &busv1.RpcMsg{
			Payload: make([]byte, 128),
		}},
	})

	events := insp.Events()
	if len(events) != 1 {
		t.Fatalf("Events() has %d entries, want 1", len(events))
	}
	got := events[0]
	if got.AgentID != "mac-1" || got.Dir != "out" || got.StreamID != 7 {
		t.Fatalf("event = %+v", got)
	}
	if got.Kind != "rpc_msg" {
		t.Fatalf("Kind = %q, want %q", got.Kind, "rpc_msg")
	}
	if got.Size != 128 {
		t.Fatalf("Size = %d, want 128", got.Size)
	}
}

func TestInspectorKeepsOnlyTheNewestEvents(t *testing.T) {
	insp := NewInspector(3)
	for i := 0; i < 5; i++ {
		insp.Record("mac-1", "in", &busv1.Envelope{
			StreamId: uint64(i),
			Payload:  &busv1.Envelope_RpcHalfClose{RpcHalfClose: &busv1.RpcHalfClose{}},
		})
	}
	events := insp.Events()
	if len(events) != 3 {
		t.Fatalf("Events() has %d entries, want 3", len(events))
	}
	if events[0].StreamID != 2 {
		t.Fatalf("oldest kept event has stream %d, want 2", events[0].StreamID)
	}
}

func TestInspectorSubscriberReceivesEvents(t *testing.T) {
	insp := NewInspector(10)
	ch, stop := insp.Subscribe()
	defer stop()

	insp.Record("mac-1", "in", &busv1.Envelope{
		StreamId: 1,
		Payload:  &busv1.Envelope_HttpOpen{HttpOpen: &busv1.HttpOpen{Method: "GET", Uri: "/"}},
	})
	select {
	case ev := <-ch:
		if ev.Kind != "http_open" {
			t.Fatalf("Kind = %q, want %q", ev.Kind, "http_open")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber never received the event")
	}
}
