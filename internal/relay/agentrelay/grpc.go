package agentrelay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/relay/wire"
)

// rpcDesc describes every tunnelled call as fully bidirectional. The real
// cardinality is enforced by the endpoints, not by the relay.
var rpcDesc = &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}

// ServeRPC runs one tunnelled call against the local gRPC target. It owns the
// stream and always ends it through st.Close.
func ServeRPC(parent context.Context, st *bus.Stream, conn *bus.Conn, open *busv1.RpcOpen, cc *grpc.ClientConn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if d := open.GetTimeoutNanos(); d > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, time.Duration(d))
		defer stop()
	}
	ctx = metadata.NewOutgoingContext(ctx, wire.MDFromHeaders(open.GetMetadata()))

	defer func() {
		if r := recover(); r != nil {
			sendEnd(conn, st, status.Errorf(codes.Internal, "relay panic: %v", r), nil)
			st.Close(nil)
		}
	}()

	cs, err := cc.NewStream(ctx, rpcDesc, open.GetMethod())
	if err != nil {
		sendEnd(conn, st, err, nil)
		st.Close(nil)
		return
	}

	go pumpBusToLocal(ctx, cancel, st, cs)
	pumpLocalToBus(ctx, conn, st, cs)
	st.Close(nil)
}

// pumpBusToLocal is the single sending goroutine towards the local service. It
// owns the hub to agent direction and therefore its own Reassembler.
func pumpBusToLocal(ctx context.Context, cancel context.CancelFunc, st *bus.Stream, cs grpc.ClientStream) {
	// A panic here would kill the agent: nothing above this goroutine can
	// recover it. Cancelling is enough to end the call — pumpLocalToBus turns
	// the cancelled context into an RpcEnd for the hub.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic while pumping towards the local service", "stream_id", st.ID, "err", r)
			cancel()
		}
	}()

	var re bus.Reassembler
	for {
		select {
		case <-ctx.Done():
			return
		case <-st.Done():
			cancel()
			return
		case env := <-st.In:
			switch p := env.GetPayload().(type) {
			case *busv1.Envelope_RpcMsg:
				payload, complete, err := re.Add(p.RpcMsg.GetPayload(), p.RpcMsg.GetMore())
				if err != nil {
					cancel()
					return
				}
				if complete {
					if err := cs.SendMsg(&payload); err != nil {
						return
					}
				}
			case *busv1.Envelope_RpcHalfClose:
				_ = cs.CloseSend()
			case *busv1.Envelope_RpcCancel:
				cancel()
				return
			}
		}
	}
}

func pumpLocalToBus(ctx context.Context, conn *bus.Conn, st *bus.Stream, cs grpc.ClientStream) {
	if hd, err := cs.Header(); err == nil {
		_ = conn.Send(ctx, &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcHead{
			RpcHead: &busv1.RpcHead{Metadata: wire.HeadersFromMD(hd)},
		}})
	}
	for {
		// msg must stay loop-local: bus.Chunks hands slices of this buffer to
		// the writer goroutine, so reusing it would corrupt queued envelopes.
		var msg []byte
		err := cs.RecvMsg(&msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			sendEnd(conn, st, err, cs.Trailer())
			return
		}
		chunks := bus.Chunks(msg)
		for i, c := range chunks {
			env := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcMsg{
				RpcMsg: &busv1.RpcMsg{Payload: c, More: i < len(chunks)-1},
			}}
			if sendErr := conn.Send(ctx, env); sendErr != nil {
				return
			}
		}
	}
}

// sendEnd reports the final status of the local call. It deliberately sends on
// a background context: the call's own context is usually the thing that just
// expired, and the hub still needs the status.
func sendEnd(conn *bus.Conn, st *bus.Stream, err error, trailer metadata.MD) {
	st2 := status.Convert(err)
	_ = conn.Send(context.Background(), &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcEnd{
		RpcEnd: &busv1.RpcEnd{
			Code:    int32(st2.Code()),
			Message: st2.Message(),
			Trailer: wire.HeadersFromMD(trailer),
		},
	}})
}
