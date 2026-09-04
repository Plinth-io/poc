package relay_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/testenv"
)

func TestCallerDeadlineEndsTheCall(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx, cancel := context.WithTimeout(tunnelCtx(t, env), 50*time.Millisecond)
	defer cancel()

	// Tail with a huge line count outlives the deadline.
	stream, err := client.Tail(ctx, &demov1.TailRequest{Lines: 1 << 20})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("Code = %v, want %v", status.Code(err), codes.DeadlineExceeded)
			}
			return
		}
	}
}

func TestCallerCancelStopsTheAgentSideStream(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx, cancel := context.WithCancel(tunnelCtx(t, env))
	stream, err := client.Tail(ctx, &demov1.TailRequest{Lines: 1 << 20})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	cancel()

	for {
		if _, err := stream.Recv(); err != nil {
			if status.Code(err) != codes.Canceled {
				t.Fatalf("Code = %v, want %v", status.Code(err), codes.Canceled)
			}
			break
		}
	}

	// The stream must be gone on the hub side; nothing may linger in the map.
	ag, ok := env.Hub.Agents().Get(env.AgentID)
	if !ok {
		t.Fatal("agent vanished")
	}
	deadline := time.Now().Add(callTimeout)
	for time.Now().Before(deadline) {
		if ag.Conn.Streams.Len() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("hub still holds %d streams after cancel", ag.Conn.Streams.Len())
}

func TestAgentDisconnectFailsRunningCallsWithUnavailable(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Tail(tunnelCtx(t, env), &demov1.TailRequest{Lines: 1 << 20})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}

	ag, ok := env.Hub.Agents().Get(env.AgentID)
	if !ok {
		t.Fatal("agent not registered")
	}
	ag.Conn.CloseWith(1000, "test tears the tunnel down")

	for {
		if _, err := stream.Recv(); err != nil {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("Code = %v, want %v", status.Code(err), codes.Unavailable)
			}
			return
		}
	}
}
