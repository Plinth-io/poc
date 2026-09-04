package bus_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// pair starts a websocket server and returns a connected hub/agent pair.
func pair(t *testing.T, onAgentOpen bus.OpenFunc) (*bus.Conn, *bus.Conn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	accepted := make(chan *bus.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		c := bus.NewConn(ws, bus.SideAgent, onAgentOpen)
		accepted <- c
		_ = c.Run(ctx)
	}))
	t.Cleanup(srv.Close)

	ws, _, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	hub := bus.NewConn(ws, bus.SideHub, nil)
	go func() { _ = hub.Run(ctx) }()

	select {
	case agent := <-accepted:
		return hub, agent
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted the connection")
		return nil, nil
	}
}

func TestOpeningEnvelopeCreatesStreamOnPeer(t *testing.T) {
	opened := make(chan *busv1.Envelope, 1)
	hub, _ := pair(t, func(_ *bus.Stream, env *busv1.Envelope) {
		opened <- env
	})

	st, err := hub.Streams.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	env := &busv1.Envelope{
		StreamId: st.ID,
		Payload: &busv1.Envelope_RpcOpen{RpcOpen: &busv1.RpcOpen{
			Method: "/demo.v1.Demo/Echo",
		}},
	}
	if err := hub.Send(context.Background(), env); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-opened:
		if got.StreamId != st.ID {
			t.Fatalf("stream id = %d, want %d", got.StreamId, st.ID)
		}
		if got.GetRpcOpen().GetMethod() != "/demo.v1.Demo/Echo" {
			t.Fatalf("method = %q", got.GetRpcOpen().GetMethod())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer never saw the opening envelope")
	}
}

func TestEnvelopeForKnownStreamLandsInInbox(t *testing.T) {
	ready := make(chan *bus.Stream, 1)
	hub, agent := pair(t, func(s *bus.Stream, _ *busv1.Envelope) { ready <- s })

	st, err := hub.Streams.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := hub.Send(context.Background(), &busv1.Envelope{
		StreamId: st.ID,
		Payload:  &busv1.Envelope_RpcOpen{RpcOpen: &busv1.RpcOpen{Method: "/x/Y"}},
	}); err != nil {
		t.Fatalf("Send open: %v", err)
	}
	var peerStream *bus.Stream
	select {
	case peerStream = <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("peer never opened the stream")
	}

	if err := agent.Send(context.Background(), &busv1.Envelope{
		StreamId: peerStream.ID,
		Payload:  &busv1.Envelope_RpcMsg{RpcMsg: &busv1.RpcMsg{Payload: []byte("hi")}},
	}); err != nil {
		t.Fatalf("Send msg: %v", err)
	}

	select {
	case env := <-st.In:
		if string(env.GetRpcMsg().GetPayload()) != "hi" {
			t.Fatalf("payload = %q", env.GetRpcMsg().GetPayload())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("envelope never reached the stream inbox")
	}
}

func TestEnvelopeForUnknownStreamIsDropped(t *testing.T) {
	// A late envelope after a cancel is normal and must not kill the
	// connection; only a malformed envelope is fatal.
	opened := make(chan *busv1.Envelope, 1)
	hub, _ := pair(t, func(_ *bus.Stream, env *busv1.Envelope) {
		opened <- env
	})

	if err := hub.Send(context.Background(), &busv1.Envelope{
		StreamId: 999,
		Payload:  &busv1.Envelope_RpcMsg{RpcMsg: &busv1.RpcMsg{Payload: []byte("stray")}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	st, err := hub.Streams.Open()
	if err != nil {
		t.Fatalf("Open after stray envelope: %v", err)
	}
	if err := hub.Send(context.Background(), &busv1.Envelope{
		StreamId: st.ID,
		Payload:  &busv1.Envelope_RpcOpen{RpcOpen: &busv1.RpcOpen{Method: "/x/Y"}},
	}); err != nil {
		t.Fatalf("Send open: %v", err)
	}

	select {
	case got := <-opened:
		if got.StreamId != st.ID {
			t.Fatalf("stream id = %d, want %d", got.StreamId, st.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection unusable after a stray envelope")
	}
}

func TestRunClosesAllStreamsWhenPeerDisappears(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ws.Close(websocket.StatusNormalClosure, "bye")
	}))
	defer srv.Close()

	ws, _, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	c := bus.NewConn(ws, bus.SideHub, nil)
	st, err := c.Streams.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = c.Run(ctx)

	select {
	case <-st.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Run returned without closing the open streams")
	}
}

func TestMalformedEnvelopeClosesTheWholeConnection(t *testing.T) {
	// A corrupt envelope means the bus state is untrustworthy, so the whole
	// connection goes — unlike a stray stream id, which is merely dropped.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closed := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		closed <- bus.NewConn(ws, bus.SideAgent, nil).Run(ctx)
	}))
	defer srv.Close()

	ws, _, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, []byte{0xff, 0xff, 0xff, 0xff}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("Run returned nil for a malformed envelope")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection survived a malformed envelope")
	}

	if _, _, err := ws.Read(ctx); err == nil {
		t.Fatal("peer socket still readable after a protocol error")
	}
}
