package relay_test

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"plinth.io/poc/internal/testenv"
)

// TestStreamingResponseArrivesBeforeTheHandlerFinishes proves the relay
// flushes each response chunk instead of buffering the whole response. The
// target never returns, so a missing flush anywhere in the chain leaves the
// read blocked until the test's own timeout fires.
func TestStreamingResponseArrivesBeforeTheHandlerFinishes(t *testing.T) {
	env := testenv.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		env.HubHTTPURL+"/a/"+env.AgentID+"/sse", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	lines := make(chan string, 1)
	go func() {
		r := bufio.NewReader(resp.Body)
		line, err := r.ReadString('\n')
		if err == nil {
			lines <- strings.TrimSpace(line)
		}
	}()

	select {
	case line := <-lines:
		if line != "data: tick 0" {
			t.Fatalf("first line = %q, want %q", line, "data: tick 0")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no bytes arrived while the handler was still streaming — a flush is missing")
	}
}

// TestClientDisconnectCancelsTheHandlerBehindTheAgent proves HttpCancel
// travels from the client's disconnect all the way to the handler running
// behind the agent. Asserting only that the client's own read fails would
// prove nothing since the client cancelled it itself; the meaningful signal
// is CancelObserved, set by the target's handler on its own context.
func TestClientDisconnectCancelsTheHandlerBehindTheAgent(t *testing.T) {
	env := testenv.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		env.HubHTTPURL+"/a/"+env.AgentID+"/sse", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	buf := make([]byte, 32)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first Read: %v", err)
	}
	cancel()
	resp.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if env.CancelObserved() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the handler behind the agent never saw the cancellation — HttpCancel does not travel")
}

// TestStreamIsReleasedAfterClientDisconnect proves the hub-side stream is
// deregistered once the client leaves, so an abandoned live view does not
// leak an entry in the agent's stream registry forever.
func TestStreamIsReleasedAfterClientDisconnect(t *testing.T) {
	env := testenv.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		env.HubHTTPURL+"/a/"+env.AgentID+"/sse", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	buf := make([]byte, 32)
	_, _ = resp.Body.Read(buf)
	cancel()
	resp.Body.Close()

	ag, ok := env.Hub.Agents().Get(env.AgentID)
	if !ok {
		t.Fatal("agent not registered")
	}
	// Bounded well below callTimeout: nothing in this path waits out a
	// deadline before cleaning up, so a real leak must show up quickly, not
	// only after a long poll masks it as a slow pass.
	const cleanupBound = 3 * time.Second
	deadline := time.Now().Add(cleanupBound)
	for time.Now().Before(deadline) {
		if ag.Conn.Streams.Len() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%d streams still registered after the client left", ag.Conn.Streams.Len())
}
