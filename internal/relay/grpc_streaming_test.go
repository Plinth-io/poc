package relay_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/demo"
	"plinth.io/poc/internal/relay"
	"plinth.io/poc/internal/testenv"
)

// tunnelCtx is callCtx (grpc_test.go) pre-loaded with the agent id, so every
// streaming call stays bound by the same callTimeout.
func tunnelCtx(t *testing.T, env *testenv.Env) context.Context {
	t.Helper()
	return callCtx(t, relay.AgentIDKey, env.AgentID)
}

func TestServerStreamDeliversEveryMessageInOrder(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Tail(tunnelCtx(t, env), &demov1.TailRequest{Lines: 200})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	var got int
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv after %d chunks: %v", got, err)
		}
		if chunk.GetIndex() != int32(got) {
			t.Fatalf("chunk %d has index %d — order broken", got, chunk.GetIndex())
		}
		got++
	}
	if got != 200 {
		t.Fatalf("received %d chunks, want 200", got)
	}
}

func TestBidiStreamInterleavesBothDirections(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Chat(tunnelCtx(t, env))
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	for i := 0; i < 50; i++ {
		text := "msg-" + string(rune('a'+i%26))
		if err := stream.Send(&demov1.ChatMessage{Text: text}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		got, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if got.GetText() != "echo: "+text {
			t.Fatalf("Recv %d = %q, want %q", i, got.GetText(), "echo: "+text)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after CloseSend = %v, want io.EOF", err)
	}
}

func TestHalfCloseEndsTheClientStream(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Chat(tunnelCtx(t, env))
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if err := stream.Send(&demov1.ChatMessage{Text: "nur eine"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want io.EOF — half close did not travel", err)
	}
}

func TestErrorStatusAndTrailerSurviveTheTunnel(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	var trailer metadata.MD
	_, err := client.Fail(tunnelCtx(t, env), &demov1.FailRequest{Reason: "kaputt"},
		grpc.Trailer(&trailer))
	if err == nil {
		t.Fatal("Fail returned no error through the tunnel")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("Code = %v, want %v", st.Code(), codes.FailedPrecondition)
	}
	if st.Message() != "kaputt" {
		t.Fatalf("Message = %q, want %q", st.Message(), "kaputt")
	}
	if got := trailer.Get("x-demo-trailer"); len(got) != 1 || got[0] != "set" {
		t.Fatalf("trailer = %v, want [set]", got)
	}
}

func TestLargeMessagesAreChunkedAndRebuilt(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	// 300 KiB forces several chunks in both directions.
	padding := make([]byte, 300<<10)
	for i := range padding {
		padding[i] = byte(i)
	}
	got, err := client.Echo(tunnelCtx(t, env), &demov1.EchoRequest{Text: "big", Padding: padding})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got.GetPaddingBytes() != int64(len(padding)) {
		t.Fatalf("PaddingBytes = %d, want %d", got.GetPaddingBytes(), len(padding))
	}
}

// TestAbortedServerStreamStopsTheAgentsLocalCall covers cancelAgent's
// abnormal-exit path from Task 7: forwardResponses must tell the agent to
// abandon the call when the caller goes away mid-stream, or the agent's local
// Tail call keeps running unnoticed. Lines is set far beyond anything the
// pipeline's own buffers (bus inbox/outbox, HTTP/2 flow control) could absorb
// before the caller stops reading, so the local call is still in flight when
// it is cancelled.
func TestAbortedServerStreamStopsTheAgentsLocalCall(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, relay.AgentIDKey, env.AgentID)

	stream, err := client.Tail(ctx, &demov1.TailRequest{Lines: 10_000_000})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if demo.TailActive.Load() == 0 {
		t.Fatal("agent's local Tail call already finished before it could be cancelled")
	}

	cancel() // the caller goes away mid-stream

	// Bounded well below callTimeout, not callTimeout itself: ctx here also
	// carries callTimeout as its own deadline, serialized to the agent via
	// RpcOpen, so a leaked local call would self-heal at that same deadline
	// anyway — a callTimeout-bound poll could never fail for the reason it
	// exists, only pass slower.
	const cleanupBound = 1 * time.Second
	deadline := time.Now().Add(cleanupBound)
	for demo.TailActive.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent's local Tail call is still running after the caller went away")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPipelinedLargeChatMessagesDoNotAlias is the regression test for the
// slice-aliasing trap: bus.Chunks returns slices aliasing its input, and
// rawcodec.Codec.Unmarshal reuses the receive buffer's backing array via
// append(buf[:0], data...), so a `var msg []byte` hoisted out of the receive
// loop in forwardRequests or pumpLocalToBus would let one message's chunks
// get overwritten by the next before the writer goroutine sends them. Sending
// several large, distinct messages back-to-back — without draining the
// replies in between — exercises both the caller-to-agent direction
// (forwardRequests) and the agent-to-caller direction (pumpLocalToBus, which
// forwards the demo service's own back-to-back echoes).
func TestPipelinedLargeChatMessagesDoNotAlias(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	stream, err := client.Chat(tunnelCtx(t, env))
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	const n = 4
	msgs := make([]string, n)
	for i := range msgs {
		// Each message is a few bus.MaxChunk-sized chunks of its own distinct
		// byte, so aliasing corrupts it with a neighbour's content instead of
		// silently reproducing the same bytes.
		msgs[i] = strings.Repeat(string(rune('A'+i)), 100<<10)
	}
	for i, m := range msgs {
		if err := stream.Send(&demov1.ChatMessage{Text: m}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	for i, m := range msgs {
		got, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if want := "echo: " + m; got.GetText() != want {
			t.Fatalf("echo %d corrupted: got %d bytes, want message %d's own %d bytes", i, len(got.GetText()), i, len(want))
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after CloseSend = %v, want io.EOF", err)
	}
}
