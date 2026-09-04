// Package demo implements the example service that travels through the tunnel.
package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
)

// TailActive counts in-flight Tail calls. Tests use it to prove that an
// aborted Tail call actually returns instead of running to completion or
// blocking forever.
//
// ponytail: one process-global counter, fine as long as Tail tests run
// sequentially and drain their stream to EOF; give Tail a per-call counter
// via a server constructor if a Tail test ever needs t.Parallel() or
// concurrent Tail calls within one test.
var TailActive atomic.Int32

// ChatActive counts in-flight Chat calls, for the same reason as TailActive:
// tests use it to prove a stalled bidirectional call actually returns instead
// of blocking forever once its underlying connection is gone.
var ChatActive atomic.Int32

type server struct {
	demov1.UnimplementedDemoServer
}

func NewServer() demov1.DemoServer { return &server{} }

func (s *server) Echo(_ context.Context, req *demov1.EchoRequest) (*demov1.EchoResponse, error) {
	return &demov1.EchoResponse{
		Text:         req.GetText(),
		PaddingBytes: int64(len(req.GetPadding())),
	}, nil
}

func (s *server) Tail(req *demov1.TailRequest, stream demov1.Demo_TailServer) error {
	TailActive.Add(1)
	defer TailActive.Add(-1)
	for i := int32(0); i < req.GetLines(); i++ {
		if err := stream.Send(&demov1.TailChunk{Index: i, Line: fmt.Sprintf("line %d", i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Chat(stream demov1.Demo_ChatServer) error {
	ChatActive.Add(1)
	defer ChatActive.Add(-1)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&demov1.ChatMessage{Text: "echo: " + msg.GetText()}); err != nil {
			return err
		}
	}
}

func (s *server) Fail(ctx context.Context, req *demov1.FailRequest) (*demov1.FailResponse, error) {
	grpc.SetTrailer(ctx, metadata.Pairs("x-demo-trailer", "set"))
	return nil, status.Error(codes.FailedPrecondition, req.GetReason())
}
