package hubrelay

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
		// conn and st are declared here, above the recover, so a panic after
		// the stream is open can still tell the agent to abandon the call.
		var conn *bus.Conn
		var st *bus.Stream
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "relay panic: %v", r)
				if st != nil {
					cancelAgent(conn, st, err.Error())
				}
			}
		}()

		method, ok := grpc.MethodFromServerStream(ss)
		if !ok {
			return status.Error(codes.Internal, "cannot determine method")
		}
		md, _ := metadata.FromIncomingContext(ss.Context())
		ids := md.Get(wire.AgentIDKey)
		if len(ids) == 0 || ids[0] == "" {
			return status.Errorf(codes.InvalidArgument, "missing %s metadata", wire.AgentIDKey)
		}
		conn, ok = l.Lookup(ids[0])
		if !ok {
			return status.Errorf(codes.Unavailable, "agent %q is not connected", ids[0])
		}

		// Before opening a stream: a deadline that is already spent has no
		// remaining runtime to carry, and sending zero would mean "no
		// deadline" to the agent — the call would run on unbounded.
		var timeout int64
		if dl, ok := ss.Context().Deadline(); ok {
			timeout = int64(time.Until(dl))
			if timeout <= 0 {
				return status.Error(codes.DeadlineExceeded, "deadline expired before the call was relayed")
			}
		}

		st, err = conn.Streams.Open()
		if err != nil {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		defer st.Close(nil)

		open := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_RpcOpen{
			RpcOpen: &busv1.RpcOpen{
				Method:       method,
				Metadata:     wire.HeadersFromMD(md),
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
//
// It carries its own recover: grpc-go's handler recovery does not cover
// goroutines the handler spawned, so a panic here would take the whole hub
// down. Closing the stream is what surfaces it to the caller — forwardResponses
// turns the recorded status into the call's result.
func forwardRequests(ss grpc.ServerStream, conn *bus.Conn, st *bus.Stream) {
	defer func() {
		if r := recover(); r != nil {
			err := status.Errorf(codes.Internal, "relay panic: %v", r)
			slog.Error("panic while forwarding requests", "stream_id", st.ID, "err", r)
			cancelAgent(conn, st, err.Error())
			st.Close(err)
		}
	}()

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
			cancelAgent(conn, st, "caller cancelled")
			return status.FromContextError(ss.Context().Err()).Err()

		case <-st.Done():
			// A status recorded on the stream comes from forwardRequests'
			// panic recovery and is the real cause; anything else means the
			// bus connection died.
			if err := st.Err(); err != nil {
				if s, ok := status.FromError(err); ok {
					return s.Err()
				}
			}
			// No cancel here: the bus connection is what died, so there is
			// nobody left to tell.
			//
			// ponytail: a status the agent had already queued in st.In can be
			// lost to a simultaneous connection death, turning a real code
			// into Unavailable. Drain st.In non-blockingly before returning if
			// that ever needs to be exact.
			return status.Error(codes.Unavailable, "connection to agent lost")

		case env := <-st.In:
			switch p := env.GetPayload().(type) {
			case *busv1.Envelope_RpcHead:
				if err := ss.SetHeader(wire.MDFromHeaders(p.RpcHead.GetMetadata())); err != nil {
					cancelAgent(conn, st, err.Error())
					return err
				}
			case *busv1.Envelope_RpcMsg:
				payload, complete, err := re.Add(p.RpcMsg.GetPayload(), p.RpcMsg.GetMore())
				if err != nil {
					cancelAgent(conn, st, err.Error())
					return status.Error(codes.ResourceExhausted, err.Error())
				}
				if complete {
					if err := ss.SendMsg(&payload); err != nil {
						cancelAgent(conn, st, err.Error())
						return err
					}
				}
			case *busv1.Envelope_RpcEnd:
				// No cancel on either exit below: the agent sent its final
				// status and closed its own stream, so it has nothing left to
				// abandon.
				ss.SetTrailer(wire.MDFromHeaders(p.RpcEnd.GetTrailer()))
				if codes.Code(p.RpcEnd.GetCode()) == codes.OK {
					return nil
				}
				return status.Error(codes.Code(p.RpcEnd.GetCode()), p.RpcEnd.GetMessage())
			}
		}
	}
}

// cancelAgent tells the agent to abandon the call. Every abnormal exit of
// forwardResponses has to send it: the handler's st.Close stops draining
// st.In right after, so without a cancel the agent's pumps stay parked and its
// local call runs on unnoticed. The only exception is a dead bus connection,
// where there is no peer left to reach. It uses a background context on
// purpose, since the caller's context is usually the thing that just expired.
func cancelAgent(conn *bus.Conn, st *bus.Stream, reason string) {
	_ = conn.Send(context.Background(), &busv1.Envelope{StreamId: st.ID,
		Payload: &busv1.Envelope_RpcCancel{RpcCancel: &busv1.RpcCancel{Reason: reason}}})
}
