package relay

import (
	"context"
	"io"
	"net/http"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// ServeHTTPStream runs one tunnelled HTTP request against the agent's local
// target and streams the response back, unbuffered.
func ServeHTTPStream(parent context.Context, st *bus.Stream, conn *bus.Conn, open *busv1.HttpOpen, target string, client *http.Client) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer st.Close(nil)

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, open.GetMethod(), target+open.GetUri(), pr)
	if err != nil {
		sendResponseEnd(ctx, conn, st, err)
		return
	}
	req.Header = HTTPFromHeaders(open.GetHeaders())

	go pumpHTTPRequestBody(ctx, cancel, st, pw)

	resp, err := client.Do(req)
	if err != nil {
		sendResponseEnd(ctx, conn, st, err)
		return
	}
	defer resp.Body.Close()

	head := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_HttpResponseHead{
		HttpResponseHead: &busv1.HttpResponseHead{
			Status:  int32(resp.StatusCode),
			Headers: HeadersFromHTTP(resp.Header),
		},
	}}
	if err := conn.Send(ctx, head); err != nil {
		return
	}

	buf := make([]byte, bus.MaxChunk)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			// The buffer is reused, so the chunk must be copied before it is
			// queued for the writer goroutine.
			chunk := append([]byte(nil), buf[:n]...)
			env := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_HttpResponseBody{
				HttpResponseBody: &busv1.HttpResponseBody{Chunk: chunk},
			}}
			if err := conn.Send(ctx, env); err != nil {
				return
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				readErr = nil
			}
			sendResponseEnd(ctx, conn, st, readErr)
			return
		}
	}
}

// pumpHTTPRequestBody is the only reader of st.In for this stream. It keeps
// running after HttpEnd so a later HttpCancel still reaches the request.
func pumpHTTPRequestBody(ctx context.Context, cancel context.CancelFunc, st *bus.Stream, pw *io.PipeWriter) {
	bodyOpen := true
	for {
		select {
		case <-ctx.Done():
			if bodyOpen {
				_ = pw.CloseWithError(ctx.Err())
			}
			return
		case <-st.Done():
			if bodyOpen {
				_ = pw.CloseWithError(io.ErrUnexpectedEOF)
			}
			return
		case env := <-st.In:
			switch p := env.GetPayload().(type) {
			case *busv1.Envelope_HttpBody:
				if bodyOpen {
					if _, err := pw.Write(p.HttpBody.GetChunk()); err != nil {
						// The transport gave up on the request body, which is
						// what happens when the target answers before reading
						// it. That response is still valid, so the loop only
						// stops feeding the pipe — cancelling here would abort
						// the response instead. It keeps draining st.In as
						// well: an undrained inbox blocks the connection's
						// single read loop, and with it every other stream.
						bodyOpen = false
					}
				}
			case *busv1.Envelope_HttpEnd:
				if bodyOpen {
					_ = pw.Close()
					bodyOpen = false
				}
			case *busv1.Envelope_HttpCancel:
				if bodyOpen {
					_ = pw.CloseWithError(context.Canceled)
					bodyOpen = false
				}
				cancel()
				return
			}
		}
	}
}

// sendResponseEnd reports the end of the local response. A non-empty message
// means the agent could not finish reading from its target.
//
// It strips the cancellation from ctx on purpose, the same way sendEnd does
// for gRPC: the request context is usually the very thing that just ended, and
// Conn.Send selects over the outbox and ctx.Done() at once, so a cancelled ctx
// would drop this envelope at random. Without it the hub waits for a terminal
// envelope that never comes.
func sendResponseEnd(ctx context.Context, conn *bus.Conn, st *bus.Stream, err error) {
	var msg string
	if err != nil {
		msg = err.Error()
	}
	_ = conn.Send(context.WithoutCancel(ctx), &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_HttpResponseEnd{
		HttpResponseEnd: &busv1.HttpResponseEnd{Error: msg},
	}})
}
