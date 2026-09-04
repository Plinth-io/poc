package agent_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"plinth.io/poc/internal/agent"
	"plinth.io/poc/internal/hub"
)

func TestAgentReconnectsAfterTheHubDropsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tokens, err := hub.ParseTokens("mac-1:secret1")
	if err != nil {
		t.Fatalf("ParseTokens: %v", err)
	}
	h := hub.New(hub.Config{Tokens: tokens})
	srv := newHTTPTestServer(t, h.Mux())

	a := agent.New(agent.Config{
		HubURL:     "ws" + strings.TrimPrefix(srv.URL, "http") + "/agent/connect",
		Token:      "secret1",
		AgentID:    "mac-1",
		Version:    "test",
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	go func() { _ = a.Run(ctx) }()
	t.Cleanup(a.Close)

	first := waitForAgentConn(t, h, "mac-1", nil)
	first.Conn.CloseWith(websocket.StatusNormalClosure, "test drops the connection")

	waitForAgentConn(t, h, "mac-1", first)
}

func newHTTPTestServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// waitForAgentConn waits for a registration different from prev.
func waitForAgentConn(t *testing.T, h *hub.Hub, id string, prev *hub.Agent) *hub.Agent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ag, ok := h.Agents().Get(id); ok && ag != prev {
			return ag
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("agent %q did not (re)register", id)
	return nil
}
