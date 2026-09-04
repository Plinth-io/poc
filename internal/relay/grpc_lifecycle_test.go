package relay_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/demo"
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
	if demo.TailActive.Load() == 0 {
		t.Fatal("agent's local Tail call already finished before it could be cancelled")
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
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := ag.Conn.Streams.Len(); n != 0 {
		t.Fatalf("hub still holds %d streams after cancel", n)
	}

	// The hub-side registry clearing out says nothing about the agent: that
	// happens as soon as forwardResponses sees the caller's context end,
	// independent of whether the agent honoured the cancel. Only the demo
	// service's own counter proves the agent's local call actually stopped.
	//
	// This poll deliberately uses a bound well below callTimeout, not
	// callTimeout itself: the RpcOpen the agent received also carried the
	// caller's original deadline (from tunnelCtx), so a missing explicit
	// cancel would still self-heal once that deadline passes — masking the
	// bug if this poll were allowed to run that long too. A prompt cancel
	// must clear the call in milliseconds, not wait out its own deadline.
	const cancelPropagationBound = 1 * time.Second
	deadline = time.Now().Add(cancelPropagationBound)
	for demo.TailActive.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent's local Tail call is still running after the caller cancelled")
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	if demo.TailActive.Load() == 0 {
		t.Fatal("agent's local Tail call already finished before the connection died")
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
			break
		}
	}

	// The agent side must stop its local call too, not just report the
	// disconnect to the caller — otherwise the demo service's Tail keeps
	// running unnoticed after the tunnel is gone.
	deadline := time.Now().Add(callTimeout)
	for demo.TailActive.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent's local Tail call is still running after the connection died")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAgentDisconnectStopsAStalledBidiCall covers the one path the other two
// disconnect/cancel tests cannot reach: a continuously-producing call like
// Tail always notices a dead connection on its own next send, so it never
// needs pumpBusToLocal's own <-st.Done() handler. A bidirectional call that
// is stalled in its local Recv, waiting for a next message that will never
// arrive, has no such self-detection — only that handler's cancel() unsticks
// it.
func TestAgentDisconnectStopsAStalledBidiCall(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Chat(tunnelCtx(t, env))
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if err := stream.Send(&demov1.ChatMessage{Text: "hello"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if demo.ChatActive.Load() == 0 {
		t.Fatal("agent's local Chat call already finished before the connection died")
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
			break
		}
	}

	// Bounded well below callTimeout: the caller's original deadline, carried
	// to the agent in RpcOpen, would eventually cancel the stalled call on
	// its own and mask a missing st.Done() handler if this poll ran that long.
	const cleanupBound = 1 * time.Second
	deadline := time.Now().Add(cleanupBound)
	for demo.ChatActive.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent's stalled local Chat call is still running after the connection died")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
