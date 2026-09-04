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

		// Split the escaped path by segment rather than trimming the decoded
		// id: "/a/mac%2d1/hello" resolves to the registered id "mac-1", so a
		// prefix built from the decoded id would not match the raw URI.
		escID, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.EscapedPath(), "/a/"), "/")
		prefix := "/a/" + escID
		uri := "/" + rest
		if r.URL.RawQuery != "" {
			uri += "?" + r.URL.RawQuery
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

		// net/http declares the request invalid once the handler returns and
		// closes the body itself, so the pump has to be joined here rather
		// than left to race that close. It ends when the client's body ends,
		// which the server would wait for anyway before reusing the socket.
		done := make(chan struct{})
		go forwardHTTPRequestBody(r, conn, st, done)
		relayHTTPResponse(w, r, conn, st)
		<-done
	})
}

func forwardHTTPRequestBody(r *http.Request, conn *bus.Conn, st *bus.Stream, done chan struct{}) {
	defer close(done)
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
				if wroteHead {
					continue
				}
				// WriteHeader panics outside this range, so a peer with an
				// unset or bogus status field must not reach it.
				code := int(p.HttpResponseHead.GetStatus())
				if code < 100 || code > 999 {
					cancelUpstream("agent sent an invalid status")
					http.Error(w, "agent sent an invalid status", http.StatusBadGateway)
					return
				}
				for k, vs := range HTTPFromHeaders(p.HttpResponseHead.GetHeaders()) {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(code)
				wroteHead = true
				if flusher != nil {
					flusher.Flush()
				}

			case *busv1.Envelope_HttpResponseBody:
				if _, err := w.Write(p.HttpResponseBody.GetChunk()); err != nil {
					cancelUpstream("write to client failed")
					return
				}
				wroteHead = true
				// Flushing per chunk is what makes a live stream arrive at all
				// instead of a page that never finishes loading.
				if flusher != nil {
					flusher.Flush()
				}

			case *busv1.Envelope_HttpResponseEnd:
				// An error that arrives after the head is unreportable: the
				// status line is already on the wire and HTTP/1.1 has no
				// trailer the client would read. Such a response ends as a
				// clean EOF, truncated but indistinguishable from complete.
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
