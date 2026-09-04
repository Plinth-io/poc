package relay

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// Lookup resolves an agent id to its bus connection.
type Lookup interface {
	Lookup(id string) (*bus.Conn, bool)
}

// HubGRPC returns the handler for every method the hub does not implement
// itself, which is all of them: the hub is a relay, not a service.
func HubGRPC(l Lookup) grpc.StreamHandler {
	// The named return lets the deferred recover turn a panic into a failed
	// call instead of a dead hub process.
	return func(_ any, ss grpc.ServerStream) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "relay panic: %v", r)
			}
		}()

		method, ok := grpc.MethodFromServerStream(ss)
		if !ok {
			return status.Error(codes.Internal, "cannot determine method")
		}
		md, _ := metadata.FromIncomingContext(ss.Context())
		ids := md.Get(AgentIDKey)
		if len(ids) == 0 || ids[0] == "" {
			return status.Errorf(codes.InvalidArgument, "missing %s metadata", AgentIDKey)
		}
		conn, ok := l.Lookup(ids[0])
		if !ok {
			return status.Errorf(codes.Unavailable, "agent %q is not connected", ids[0])
		}

		st, err := conn.Streams.Open()
		if err != nil {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		defer st.Close(nil)

		var timeout int64
		if dl, ok := ss.Context().Deadline(); ok {
			timeout = int64(time.Until(dl))
		}
		open := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcOpen{
			RpcOpen: &busv1.RpcOpen{
				Method:       method,
				Metadata:     HeadersFromMD(md),
				TimeoutNanos: timeout,
			},
		}}
		if err := conn.Send(ss.Context(), open); err != nil {
			return status.Error(codes.Unavailable, "agent connection closed")
		}

		go forwardRequests(ss, conn, st)
		return forwardResponses(ss, conn, st)
	}
}

// forwardRequests is the single sending goroutine for this stream's caller to
// agent direction, which is what keeps the envelope order intact.
func forwardRequests(ss grpc.ServerStream, conn *bus.Conn, st *bus.Stream) {
	ctx := ss.Context()
	for {
		// msg must stay loop-local: bus.Chunks hands slices of this buffer to
		// the writer goroutine, so reusing it would corrupt queued envelopes.
		var msg []byte
		err := ss.RecvMsg(&msg)
		if errors.Is(err, io.EOF) {
			_ = conn.Send(ctx, &busv1.Envelope{StreamId: st.ID,
				Payload: &busv1.Envelope_RpcHalfClose{RpcHalfClose: &busv1.RpcHalfClose{}}})
			return
		}
		if err != nil {
			_ = conn.Send(ctx, &busv1.Envelope{StreamId: st.ID,
				Payload: &busv1.Envelope_RpcCancel{RpcCancel: &busv1.RpcCancel{Reason: err.Error()}}})
			return
		}
		chunks := bus.Chunks(msg)
		for i, c := range chunks {
			env := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcMsg{
				RpcMsg: &busv1.RpcMsg{Payload: c, More: i < len(chunks)-1},
			}}
			if err := conn.Send(ctx, env); err != nil {
				return
			}
		}
	}
}

// forwardResponses owns the agent to caller direction and therefore its own
// Reassembler; sharing one with the request direction would interleave chunks.
func forwardResponses(ss grpc.ServerStream, conn *bus.Conn, st *bus.Stream) error {
	var re bus.Reassembler
	for {
		select {
		case <-ss.Context().Done():
			_ = conn.Send(context.Background(), &busv1.Envelope{StreamId: st.ID,
				Payload: &busv1.Envelope_RpcCancel{RpcCancel: &busv1.RpcCancel{Reason: "caller cancelled"}}})
			return status.FromContextError(ss.Context().Err()).Err()

		case <-st.Done():
			return status.Error(codes.Unavailable, "connection to agent lost")

		case env := <-st.In:
			switch p := env.GetPayload().(type) {
			case *busv1.Envelope_RpcHead:
				if err := ss.SetHeader(MDFromHeaders(p.RpcHead.GetMetadata())); err != nil {
					return err
				}
			case *busv1.Envelope_RpcMsg:
				payload, complete, err := re.Add(p.RpcMsg.GetPayload(), p.RpcMsg.GetMore())
				if err != nil {
					return status.Error(codes.ResourceExhausted, err.Error())
				}
				if complete {
					if err := ss.SendMsg(&payload); err != nil {
						return err
					}
				}
			case *busv1.Envelope_RpcEnd:
				ss.SetTrailer(MDFromHeaders(p.RpcEnd.GetTrailer()))
				if codes.Code(p.RpcEnd.GetCode()) == codes.OK {
					return nil
				}
				return status.Error(codes.Code(p.RpcEnd.GetCode()), p.RpcEnd.GetMessage())
			}
		}
	}
}
