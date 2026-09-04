package hub_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	busv1 "plinth.io/poc/gen/bus/v1"
)

func TestHubIndexListsConnectedAgents(t *testing.T) {
	h, wsURL := startHub(t)
	ctx, cancel := contextWithCancel(t)
	defer cancel()
	connectAgent(t, ctx, wsURL, "secret1", "mac-1")
	waitForAgent(t, h, "mac-1")

	srv := httptest.NewServer(h.Mux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	page := string(buf[:n])
	if !strings.Contains(page, "mac-1") {
		t.Fatal("index does not list the connected agent")
	}
	if !strings.Contains(page, "/a/mac-1/") {
		t.Fatal("index has no link into the agent UI")
	}
}

func TestEventStreamIsServed(t *testing.T) {
	h, _ := startHub(t)
	srv := httptest.NewServer(h.Mux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events/stream")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}
}

// TestEventStreamShowsEnvelopesCrossingTheBus exercises the tap wiring end to
// end: an envelope sent over a connected agent's bus.Conn must surface on the
// SSE stream. Deleting the conn.SetTap(...) call in handleConnect leaves the
// inspector disconnected from the bus and this test times out.
func TestEventStreamShowsEnvelopesCrossingTheBus(t *testing.T) {
	h, wsURL := startHub(t)
	ctx, cancel := contextWithCancel(t)
	defer cancel()
	connectAgent(t, ctx, wsURL, "secret1", "mac-1")
	waitForAgent(t, h, "mac-1")

	srv := httptest.NewServer(h.Mux())
	defer srv.Close()

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, srv.URL+"/events/stream", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		ag, ok := h.Agents().Get("mac-1")
		if !ok {
			return
		}
		_ = ag.Conn.Send(context.Background(), &busv1.Envelope{
			StreamId: 42,
			Payload:  &busv1.Envelope_HttpCancel{HttpCancel: &busv1.HttpCancel{}},
		})
	}()

	type sseEvent struct {
		AgentID  string
		Dir      string
		StreamID uint64
		Kind     string
	}
	found := make(chan *sseEvent, 1)
	go func() {
		r := bufio.NewReader(resp.Body)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				found <- nil
				return
			}
			data, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				continue
			}
			var ev sseEvent
			if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &ev); err != nil {
				continue
			}
			if ev.StreamID == 42 {
				found <- &ev
				return
			}
		}
	}()

	select {
	case ev := <-found:
		if ev == nil {
			t.Fatal("SSE stream closed before the envelope arrived")
		}
		if ev.AgentID != "mac-1" || ev.Dir != "out" || ev.Kind != "http_cancel" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(3 * time.Second):
		streamCancel() // unblocks the reader goroutine so it doesn't leak
		t.Fatal("envelope sent over the bus never reached the SSE stream")
	}
}
