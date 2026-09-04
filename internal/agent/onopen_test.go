package agent

import (
	"testing"
	"time"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// TestOnOpenRefusesAStreamWhoseConnectionIsGone reproduces the state a
// connection teardown leaves behind: bus.Conn.dispatch has already spawned the
// goroutine for a freshly accepted stream when conn.Run returns and
// ConnectOnce clears the connection, so session() hands out a nil *bus.Conn.
// Both relay branches used to pass it straight through — the gRPC one panicked
// twice over, once in the relay and once again inside its own recover, which
// killed the agent for good instead of letting it reconnect.
//
// This is a white-box test on purpose: racing a real stream open against a
// real teardown cannot be made deterministic, while the state that race
// produces can be built exactly.
func TestOnOpenRefusesAStreamWhoseConnectionIsGone(t *testing.T) {
	// Targets that refuse connections: with the guard missing, whatever the
	// relay does with them must not be what ends the test.
	a := New(Config{GRPCTarget: "127.0.0.1:1", HTTPTarget: "http://127.0.0.1:1"})
	t.Cleanup(a.Close)

	tests := []struct {
		name string
		env  *busv1.Envelope
	}{
		{
			name: "rpc",
			env: &busv1.Envelope{StreamId: 1, Payload: &busv1.Envelope_RpcOpen{
				RpcOpen: &busv1.RpcOpen{Method: "/demo.v1.Demo/Echo"},
			}},
		},
		{
			name: "http",
			env: &busv1.Envelope{StreamId: 3, Payload: &busv1.Envelope_HttpOpen{
				HttpOpen: &busv1.HttpOpen{Method: "GET", Uri: "/"},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := bus.NewRegistry(bus.SideHub)
			st, err := reg.Accept(tc.env.GetStreamId())
			if err != nil {
				t.Fatalf("Accept: %v", err)
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				a.onOpen(st, tc.env)
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("onOpen did not return for a stream whose connection is already gone")
			}
			if st.Err() == nil {
				t.Fatal("onOpen left the stream open instead of closing it")
			}
			if reg.Len() != 0 {
				t.Fatalf("registry still holds %d streams", reg.Len())
			}
		})
	}
}
