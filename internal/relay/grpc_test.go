package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/relay"
	"plinth.io/poc/internal/testenv"
)

// callTimeout bounds every tunnelled call so a stalled relay fails the test
// instead of hanging the suite.
const callTimeout = 5 * time.Second

func callCtx(t *testing.T, kv ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

func TestUnaryCallReachesTheLocalService(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx := callCtx(t, relay.AgentIDKey, env.AgentID)
	got, err := client.Echo(ctx, &demov1.EchoRequest{Text: "durch den tunnel"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got.GetText() != "durch den tunnel" {
		t.Fatalf("Text = %q", got.GetText())
	}
}

func TestCallWithoutAgentIDIsRejected(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	_, err := client.Echo(callCtx(t), &demov1.EchoRequest{Text: "x"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestCallForUnknownAgentIsUnavailable(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx := callCtx(t, relay.AgentIDKey, "does-not-exist")
	_, err := client.Echo(ctx, &demov1.EchoRequest{Text: "x"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Code = %v, want %v", status.Code(err), codes.Unavailable)
	}
}

func TestRequestMetadataArrivesAtTheService(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx := callCtx(t, relay.AgentIDKey, env.AgentID, "x-caller", "integration-test")
	// The demo service is wrapped in an interceptor that copies x-caller into
	// the response header, so one call proves both directions.
	var header metadata.MD
	got, err := client.Echo(ctx, &demov1.EchoRequest{Text: "meta"}, grpc.Header(&header))
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got.GetText() != "meta" {
		t.Fatalf("Text = %q", got.GetText())
	}
	if vals := header.Get("x-caller-seen"); len(vals) != 1 || vals[0] != "integration-test" {
		t.Fatalf("x-caller-seen = %v, want [integration-test]", vals)
	}
}

func TestServiceErrorAndTrailerTravelBack(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	ctx := callCtx(t, relay.AgentIDKey, env.AgentID)
	var trailer metadata.MD
	_, err := client.Fail(ctx, &demov1.FailRequest{Reason: "so gewollt"}, grpc.Trailer(&trailer))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Code = %v, want %v", status.Code(err), codes.FailedPrecondition)
	}
	if got := status.Convert(err).Message(); got != "so gewollt" {
		t.Fatalf("Message = %q, want %q", got, "so gewollt")
	}
	if vals := trailer.Get("x-demo-trailer"); len(vals) != 1 || vals[0] != "set" {
		t.Fatalf("x-demo-trailer = %v, want [set]", vals)
	}
}

// TestLargeRequestIsChunkedAndReassembled exercises the multi-chunk path: the
// payload is several times bus.MaxChunk, so it only arrives intact if the
// agent reassembles the chunks in order.
func TestLargeRequestIsChunkedAndReassembled(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	padding := bytes.Repeat([]byte{0xab}, 200<<10)
	ctx := callCtx(t, relay.AgentIDKey, env.AgentID)
	got, err := client.Echo(ctx, &demov1.EchoRequest{Text: "gross", Padding: padding})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got.GetText() != "gross" {
		t.Fatalf("Text = %q", got.GetText())
	}
	if got.GetPaddingBytes() != int64(len(padding)) {
		t.Fatalf("PaddingBytes = %d, want %d", got.GetPaddingBytes(), len(padding))
	}
}

// TestConcurrentCallsStayOnTheirOwnStream is the multiplexing check: every
// response must come back on the stream that asked for it.
func TestConcurrentCallsStayOnTheirOwnStream(t *testing.T) {
	env := testenv.Start(t)
	client := demov1.NewDemoClient(env.Dial(t))

	const calls = 16
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			want := fmt.Sprintf("call-%d", i)
			ctx := callCtx(t, relay.AgentIDKey, env.AgentID)
			got, err := client.Echo(ctx, &demov1.EchoRequest{Text: want})
			if err != nil {
				errs <- fmt.Errorf("%s: %w", want, err)
				return
			}
			if got.GetText() != want {
				errs <- fmt.Errorf("Text = %q, want %q", got.GetText(), want)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// expiredStream is the smallest grpc.ServerStream that HubGRPC needs: a
// method, metadata and a context whose deadline has already passed. Driving
// this through a real gRPC client is not possible — the client refuses to send
// a call whose deadline is already gone, so the hub would never see one.
type expiredStream struct{ ctx context.Context }

func (s expiredStream) SetHeader(metadata.MD) error  { return nil }
func (s expiredStream) SendHeader(metadata.MD) error { return nil }
func (s expiredStream) SetTrailer(metadata.MD)       {}
func (s expiredStream) Context() context.Context     { return s.ctx }
func (s expiredStream) SendMsg(any) error            { return nil }
func (s expiredStream) RecvMsg(any) error            { return io.EOF }

// transportStream carries the method name, which grpc.MethodFromServerStream
// reads back out of the context.
type transportStream struct{ method string }

func (t *transportStream) Method() string                  { return t.method }
func (t *transportStream) SetHeader(metadata.MD) error     { return nil }
func (t *transportStream) SendHeader(metadata.MD) error    { return nil }
func (t *transportStream) SetTrailer(md metadata.MD) error { return nil }

// TestAnAlreadyExpiredDeadlineIsRefused pins that no stream is opened for a
// call with nothing left to spend: the lookup hands out a nil connection, so
// touching it after this point would surface as INTERNAL from the handler's
// own panic recovery instead of DEADLINE_EXCEEDED.
func TestAnAlreadyExpiredDeadlineIsRefused(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	ctx = grpc.NewContextWithServerTransportStream(ctx, &transportStream{method: "/demo.v1.Demo/Echo"})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(relay.AgentIDKey, "mac-1"))

	err := relay.HubGRPC(stubLookup{conn: nil})(nil, expiredStream{ctx: ctx})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Code = %v (%v), want %v", status.Code(err), err, codes.DeadlineExceeded)
	}
}
