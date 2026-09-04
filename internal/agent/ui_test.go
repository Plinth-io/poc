package agent_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"plinth.io/poc/internal/agent"
)

func TestUIIndexRenders(t *testing.T) {
	a := agent.New(agent.Config{AgentID: "mac-1"})
	logs := agent.NewLogBuffer(10)
	srv := httptest.NewServer(a.UIHandler(logs))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestUIUsesForwardedPrefixForLinks(t *testing.T) {
	a := agent.New(agent.Config{AgentID: "mac-1"})
	srv := httptest.NewServer(a.UIHandler(agent.NewLogBuffer(10)))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	req.Header.Set("X-Forwarded-Prefix", "/a/mac-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "/a/mac-1/logs/stream") {
		t.Fatal("page does not use the forwarded prefix, links would break through the tunnel")
	}
}

func TestLogStreamPushesNewLines(t *testing.T) {
	a := agent.New(agent.Config{AgentID: "mac-1"})
	logs := agent.NewLogBuffer(10)
	srv := httptest.NewServer(a.UIHandler(logs))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/logs/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = logs.Write([]byte("frisch\n"))
	}()

	found := make(chan bool, 1)
	go func() {
		r := bufio.NewReader(resp.Body)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				found <- false
				return
			}
			if strings.Contains(line, "frisch") {
				found <- true
				return
			}
		}
	}()

	select {
	case ok := <-found:
		if !ok {
			t.Fatal("SSE stream closed before the new log line arrived")
		}
	case <-time.After(3 * time.Second):
		cancel() // unblocks the reader goroutine so it doesn't leak
		t.Fatal("new log line never reached the SSE stream")
	}
}
