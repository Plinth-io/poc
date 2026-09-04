package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestGRPCConnStaysClosedAfterClose(t *testing.T) {
	a := New(Config{GRPCTarget: "127.0.0.1:1"})
	if _, err := a.grpcConn(); err != nil {
		t.Fatalf("grpcConn: %v", err)
	}
	a.Close()

	if _, err := a.grpcConn(); !errors.Is(err, errClosed) {
		t.Fatalf("err = %v, want %v", err, errClosed)
	}
}

// TestKeepaliveClosesConnectionAfterAMissedPong drives a server that reads the
// hello and then reads nothing else. coder/websocket only answers a ping with
// a pong from inside a Read call, so once the server stops reading, the
// agent's next ping never gets a pong back and keepalive must tear the
// connection down itself.
func TestKeepaliveClosesConnectionAfterAMissedPong(t *testing.T) {
	origInterval, origTimeout := pingInterval, pingTimeout
	pingInterval, pingTimeout = 20*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { pingInterval, pingTimeout = origInterval, origTimeout })

	// A hijacked connection's r.Context() is not cancelled by an abrupt
	// client-side close, so the handler cannot rely on it to return.
	// stopHandler is closed by t.Cleanup instead, guaranteeing the handler
	// goroutine (and, via the deferred CloseNow, its socket) always exits
	// with the test. It is registered after srv.Close below, so cleanup's
	// LIFO order closes stopHandler — and with it the handler's socket —
	// before srv.Close runs.
	stopHandler := make(chan struct{})

	helloRead := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/agent/connect", func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err := ws.Read(r.Context()); err != nil {
			return
		}
		close(helloRead)
		// Stop reading: no further Read call means no automatic pong, however
		// many pings the agent sends.
		select {
		case <-r.Context().Done():
		case <-stopHandler:
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stopHandler) })

	a := New(Config{
		HubURL:  "ws" + strings.TrimPrefix(srv.URL, "http") + "/agent/connect",
		Token:   "secret1",
		AgentID: "mac-1",
		Version: "test",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- a.ConnectOnce(ctx) }()

	select {
	case <-helloRead:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the hello")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ConnectOnce err = nil, want an error from the keepalive teardown")
		}
	case <-time.After(9 * time.Second):
		t.Fatal("ConnectOnce did not return after a missed pong")
	}
}
