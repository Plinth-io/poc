package demo_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/demo"
)

type metadataCollector struct{ md metadata.MD }

func client(t *testing.T) demov1.DemoClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := grpc.NewServer()
	demov1.RegisterDemoServer(srv, demo.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return demov1.NewDemoClient(cc)
}

func TestEchoReturnsInput(t *testing.T) {
	got, err := client(t).Echo(context.Background(), &demov1.EchoRequest{Text: "hallo"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got.GetText() != "hallo" {
		t.Fatalf("Text = %q, want %q", got.GetText(), "hallo")
	}
}

func TestTailStreamsRequestedLines(t *testing.T) {
	stream, err := client(t).Tail(context.Background(), &demov1.TailRequest{Lines: 4})
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	var count int
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if chunk.GetIndex() != int32(count) {
			t.Fatalf("Index = %d, want %d", chunk.GetIndex(), count)
		}
		count++
	}
	if count != 4 {
		t.Fatalf("received %d chunks, want 4", count)
	}
}

func TestChatEchoesEveryMessage(t *testing.T) {
	stream, err := client(t).Chat(context.Background())
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	for _, text := range []string{"eins", "zwei", "drei"} {
		if err := stream.Send(&demov1.ChatMessage{Text: text}); err != nil {
			t.Fatalf("Send(%q): %v", text, err)
		}
		got, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if got.GetText() != "echo: "+text {
			t.Fatalf("Text = %q, want %q", got.GetText(), "echo: "+text)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
}

func TestFailReturnsCodeMessageAndTrailer(t *testing.T) {
	var trailer metadataCollector
	_, err := client(t).Fail(context.Background(), &demov1.FailRequest{Reason: "kaputt"},
		grpc.Trailer(&trailer.md))
	if err == nil {
		t.Fatal("Fail returned no error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a status: %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("Code = %v, want %v", st.Code(), codes.FailedPrecondition)
	}
	if st.Message() != "kaputt" {
		t.Fatalf("Message = %q, want %q", st.Message(), "kaputt")
	}
	if got := trailer.md.Get("x-demo-trailer"); len(got) != 1 || got[0] != "set" {
		t.Fatalf("trailer x-demo-trailer = %v, want [set]", got)
	}
}
