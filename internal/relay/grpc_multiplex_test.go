package relay_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	busv1 "plinth.io/poc/gen/bus/v1"
	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/demo"
	"plinth.io/poc/internal/testenv"
)

// TestFiftyConcurrentStreamsStayIndependent is the multiplexing claim this
// design rests on: one websocket, many independent logical streams, and a
// response can never cross over to a neighbour.
func TestFiftyConcurrentStreamsStayIndependent(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	const streams = 50
	var wg sync.WaitGroup
	errCh := make(chan error, streams)

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Every stream requests a different length, so a mixed-up
			// response cannot pass unnoticed.
			lines := int32(10 + n)
			stream, err := client.Tail(tunnelCtx(t, env), &demov1.TailRequest{Lines: lines})
			if err != nil {
				errCh <- fmt.Errorf("stream %d: Tail: %w", n, err)
				return
			}
			var got int32
			for {
				chunk, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					errCh <- fmt.Errorf("stream %d: Recv at %d: %w", n, got, err)
					return
				}
				if chunk.GetIndex() != got {
					errCh <- fmt.Errorf("stream %d: index %d, want %d", n, chunk.GetIndex(), got)
					return
				}
				got++
			}
			if got != lines {
				errCh <- fmt.Errorf("stream %d: %d chunks, want %d", n, got, lines)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// smallCallBound is how long a small Echo call may take while a large one
// shares the connection. Measured over repeated runs, a correctly interleaved
// small call lands well under 15ms (the first one, racing the large call's
// own first chunks, is the slowest observed at ~12ms); this bound leaves a
// large margin over that noise while staying an order of magnitude under
// callTimeout.
//
// ponytail: catches gross regressions (multi-second stalls, deadlocks) but
// not fine-grained starvation — measured, a fully serialized, unchunked 2 MiB
// write still finishes in ~17.5ms on this loopback transport, well under this
// bound. Telling interleaved from monolithic apart would need injected
// write-path delay or real network latency, neither of which fits here.
const smallCallBound = 500 * time.Millisecond

// TestConcurrentLargeAndSmallCallsDoNotStarveEachOther proves that a call
// spanning many chunks does not block the single writer goroutine long
// enough to stall unrelated calls sharing the same connection.
func TestConcurrentLargeAndSmallCallsDoNotStarveEachOther(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ag, ok := env.Hub.Agents().Get(env.AgentID)
	if !ok {
		t.Fatal("agent not registered")
	}

	// The tap fires for every envelope the hub writes to the agent. A
	// multi-chunk RpcMsg (More is only set on non-final chunks) can only
	// belong to the large request, so it is the signal that the large call
	// is genuinely in flight rather than merely scheduled.
	bigInFlight := make(chan struct{})
	var once sync.Once
	ag.Conn.SetTap(func(dir string, e *busv1.Envelope) {
		if dir != "out" {
			return
		}
		if m, ok := e.GetPayload().(*busv1.Envelope_RpcMsg); ok && m.RpcMsg.GetMore() {
			once.Do(func() { close(bigInFlight) })
		}
	})
	t.Cleanup(func() { ag.Conn.SetTap(nil) })

	padding := make([]byte, 2<<20) // 2 MiB, many chunks
	done := make(chan error, 1)
	go func() {
		_, err := client.Echo(tunnelCtx(t, env), &demov1.EchoRequest{Text: "big", Padding: padding})
		done <- err
	}()

	select {
	case <-bigInFlight:
	case <-time.After(callTimeout):
		t.Fatal("large call never started chunking")
	}

	// While the large call travels, small calls must still complete quickly,
	// not merely "before callTimeout" — see smallCallBound.
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(tunnelCtx(t, env), smallCallBound)
		_, err := client.Echo(ctx, &demov1.EchoRequest{Text: "small"})
		cancel()
		if err != nil {
			t.Fatalf("small call %d starved (bound %v): %v", i, smallCallBound, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("large call: %v", err)
	}
}

// TestStreamLimitIsEnforced opens more long-running streams than
// bus.MaxStreams allows and checks the surplus is refused rather than
// silently queued.
func TestStreamLimitIsEnforced(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	var opened []demov1.Demo_ChatClient
	defer func() { drainChatStreams(t, opened) }()

	var refusal error
	for i := 0; i < 300; i++ {
		stream, err := client.Chat(tunnelCtx(t, env))
		if err != nil {
			refusal = err
			break
		}
		if err := stream.Send(&demov1.ChatMessage{Text: "hold"}); err != nil {
			refusal = err
			break
		}
		if _, err := stream.Recv(); err != nil {
			refusal = err
			break
		}
		opened = append(opened, stream)
	}
	if refusal == nil {
		t.Fatal("no refusal after 300 concurrent streams, limit not enforced")
	}
	// A wrong code (e.g. from the payload-too-large path in grpc_hub.go) or a
	// refusal at the wrong count would both pass a bare "something failed"
	// check; a raised or broken limit must fail this one instead.
	if status.Code(refusal) != codes.ResourceExhausted {
		t.Fatalf("refusal code = %v, want %v (err: %v)", status.Code(refusal), codes.ResourceExhausted, refusal)
	}
	if len(opened) != bus.MaxStreams {
		t.Fatalf("opened %d streams before refusal, want %d (bus.MaxStreams)", len(opened), bus.MaxStreams)
	}
}

// drainChatStreams closes every held Chat stream and waits for the agent's
// local Chat calls to actually finish before returning. demo.ChatActive is a
// process-global counter other tests use as a precondition; leaving these
// calls running would hand the next test a dirty counter.
func drainChatStreams(t *testing.T, streams []demov1.Demo_ChatClient) {
	t.Helper()

	var wg sync.WaitGroup
	for _, s := range streams {
		wg.Add(1)
		go func(s demov1.Demo_ChatClient) {
			defer wg.Done()
			_ = s.CloseSend()
			for {
				if _, err := s.Recv(); err != nil {
					return
				}
			}
		}(s)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(callTimeout + 2*time.Second):
		t.Fatal("draining chat streams timed out")
	}

	// Bounded well below the timeout above: once every stream's Recv has
	// returned, the agent's local Chat calls have nothing left to wait on.
	const cleanupBound = 1 * time.Second
	deadline := time.Now().Add(cleanupBound)
	for demo.ChatActive.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent's local Chat calls are still running after streams closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
