package relay_test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	demov1 "plinth.io/poc/gen/demo/v1"
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

// TestConcurrentLargeAndSmallCallsDoNotStarveEachOther proves that a call
// spanning many chunks does not block the single writer goroutine long
// enough to stall unrelated calls sharing the same connection.
func TestConcurrentLargeAndSmallCallsDoNotStarveEachOther(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	padding := make([]byte, 2<<20) // 2 MiB, many chunks
	done := make(chan error, 1)
	go func() {
		_, err := client.Echo(tunnelCtx(t, env), &demov1.EchoRequest{Text: "big", Padding: padding})
		done <- err
	}()

	// While the large call travels, small calls must still complete.
	for i := 0; i < 20; i++ {
		if _, err := client.Echo(tunnelCtx(t, env), &demov1.EchoRequest{Text: "small"}); err != nil {
			t.Fatalf("small call %d starved: %v", i, err)
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

	var refused bool
	for i := 0; i < 300; i++ {
		stream, err := client.Chat(tunnelCtx(t, env))
		if err != nil {
			refused = true
			break
		}
		if err := stream.Send(&demov1.ChatMessage{Text: "hold"}); err != nil {
			refused = true
			break
		}
		if _, err := stream.Recv(); err != nil {
			refused = true
			break
		}
		opened = append(opened, stream)
	}
	if !refused {
		t.Fatal("no refusal after 300 concurrent streams, limit not enforced")
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
