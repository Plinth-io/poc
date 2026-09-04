package hub_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"plinth.io/poc/internal/agent"
	"plinth.io/poc/internal/hub"
)

func startHub(t *testing.T) (*hub.Hub, string) {
	t.Helper()
	tokens, err := hub.ParseTokens("mac-1:secret1")
	if err != nil {
		t.Fatalf("ParseTokens: %v", err)
	}
	h := hub.New(hub.Config{Tokens: tokens})
	srv := httptest.NewServer(h.Mux())
	t.Cleanup(srv.Close)
	return h, "ws" + strings.TrimPrefix(srv.URL, "http") + "/agent/connect"
}

func connectAgent(t *testing.T, ctx context.Context, wsURL, token, agentID string) {
	t.Helper()
	a := agent.New(agent.Config{
		HubURL:  wsURL,
		Token:   token,
		AgentID: agentID,
		Version: "test",
	})
	go func() { _ = a.ConnectOnce(ctx) }()
}

func waitForAgent(t *testing.T, h *hub.Hub, id string) *hub.Agent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ag, ok := h.Agents().Get(id); ok {
			return ag
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("agent %q never registered", id)
	return nil
}

func TestValidTokenRegistersAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, wsURL := startHub(t)
	connectAgent(t, ctx, wsURL, "secret1", "mac-1")

	ag := waitForAgent(t, h, "mac-1")
	if ag.Version != "test" {
		t.Fatalf("Version = %q, want %q", ag.Version, "test")
	}
}

func TestUnknownTokenIsRejectedBeforeUpgrade(t *testing.T) {
	_, wsURL := startHub(t)
	httpURL := "http" + strings.TrimPrefix(wsURL, "ws")

	req, err := http.NewRequest(http.MethodGet, httpURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHelloWithForeignAgentIDIsRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, wsURL := startHub(t)
	// Valid token for mac-1, but the agent claims to be someone else.
	connectAgent(t, ctx, wsURL, "secret1", "build-2")

	time.Sleep(200 * time.Millisecond)
	if _, ok := h.Agents().Get("build-2"); ok {
		t.Fatal("agent registered under an id its token does not own")
	}
	if _, ok := h.Agents().Get("mac-1"); ok {
		t.Fatal("agent registered under the token's id despite a mismatching hello")
	}
}

func TestSecondConnectionReplacesTheFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, wsURL := startHub(t)
	connectAgent(t, ctx, wsURL, "secret1", "mac-1")
	first := waitForAgent(t, h, "mac-1")

	connectAgent(t, ctx, wsURL, "secret1", "mac-1")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ag, ok := h.Agents().Get("mac-1"); ok && ag != first {
			if len(h.Agents().List()) != 1 {
				t.Fatalf("List() has %d agents, want 1", len(h.Agents().List()))
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("second connection never replaced the first")
}
