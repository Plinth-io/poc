package relay

import (
	"context"
	"net/http"
	"strings"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// HubHTTP serves /a/{id}/… by tunnelling the request to that agent's local
// HTTP target over the same bus the gRPC relay uses.
func HubHTTP(l Lookup) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agentID := r.PathValue("id")
		conn, ok := l.Lookup(agentID)
		if !ok {
			http.Error(w, "agent is not connected", http.StatusServiceUnavailable)
			return
		}
		st, err := conn.Streams.Open()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer st.Close(nil)

		prefix := "/a/" + agentID
		uri := strings.TrimPrefix(r.URL.RequestURI(), prefix)
		if uri == "" {
			uri = "/"
		}

		// Dropped before conversion so a client cannot smuggle in its own
		// prefix and make the target emit links to somewhere else.
		r.Header.Del(ForwardedPrefixHeader)
		headers := HeadersFromHTTP(r.Header)
		headers = append(headers, &busv1.Header{
			Key:    ForwardedPrefixHeader,
			Values: [][]byte{[]byte(prefix)},
		})

		open := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_HttpOpen{
			HttpOpen: &busv1.HttpOpen{Method: r.Method, Uri: uri, Headers: headers},
		}}
		if err := conn.Send(r.Context(), open); err != nil {
			http.Error(w, "agent connection closed", http.StatusBadGateway)
			return
		}

		go forwardHTTPRequestBody(r, conn, st)
		relayHTTPResponse(w, r, conn, st)
	})
}

func forwardHTTPRequestBody(r *http.Request, conn *bus.Conn, st *bus.Stream) {
	defer r.Body.Close()
	buf := make([]byte, bus.MaxChunk)
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			// The buffer is reused, so the chunk must be copied before it is
			// queued for the writer goroutine.
			chunk := append([]byte(nil), buf[:n]...)
			if sendErr := conn.Send(r.Context(), &busv1.Envelope{StreamId: st.ID,
				Payload: &busv1.Envelope_HttpBody{HttpBody: &busv1.HttpBody{Chunk: chunk}},
			}); sendErr != nil {
				return
			}
		}
		if err != nil {
			_ = conn.Send(r.Context(), &busv1.Envelope{StreamId: st.ID,
				Payload: &busv1.Envelope_HttpEnd{HttpEnd: &busv1.HttpEnd{}}})
			return
		}
	}
}

func relayHTTPResponse(w http.ResponseWriter, r *http.Request, conn *bus.Conn, st *bus.Stream) {
	flusher, _ := w.(http.Flusher)
	var wroteHead bool

	cancelUpstream := func(reason string) {
		_ = conn.Send(context.Background(), &busv1.Envelope{StreamId: st.ID,
			Payload: &busv1.Envelope_HttpCancel{HttpCancel: &busv1.HttpCancel{Reason: reason}}})
	}

	for {
		select {
		case <-r.Context().Done():
			// Without this the agent would keep streaming into a closed tab
			// and leak a goroutine per abandoned request.
			cancelUpstream("client disconnected")
			return

		case <-st.Done():
			if !wroteHead {
				http.Error(w, "agent connection lost", http.StatusBadGateway)
			}
			return

		case env := <-st.In:
			switch p := env.GetPayload().(type) {
			case *busv1.Envelope_HttpResponseHead:
				for k, vs := range HTTPFromHeaders(p.HttpResponseHead.GetHeaders()) {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(int(p.HttpResponseHead.GetStatus()))
				wroteHead = true
				if flusher != nil {
					flusher.Flush()
				}

			case *busv1.Envelope_HttpResponseBody:
				if _, err := w.Write(p.HttpResponseBody.GetChunk()); err != nil {
					cancelUpstream("write to client failed")
					return
				}
				// Flushing per chunk is what makes a live stream arrive at all
				// instead of a page that never finishes loading.
				if flusher != nil {
					flusher.Flush()
				}

			case *busv1.Envelope_HttpResponseEnd:
				if !wroteHead {
					if msg := p.HttpResponseEnd.GetError(); msg != "" {
						http.Error(w, msg, http.StatusBadGateway)
						return
					}
					w.WriteHeader(http.StatusOK)
				}
				return
			}
		}
	}
}
