package hub_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
