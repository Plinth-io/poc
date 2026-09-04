package relay_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"plinth.io/poc/internal/testenv"
)

// tunnelHTTP bounds every tunnelled request so a stalled relay fails the test
// instead of hanging the suite.
func tunnelHTTP() *http.Client { return &http.Client{Timeout: callTimeout} }

func TestGetThroughTheTunnel(t *testing.T) {
	env := testenv.Start(t)

	req, err := http.NewRequest(http.MethodGet, env.HubHTTPURL+"/a/"+env.AgentID+"/hello", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Caller", "integration-test")

	resp, err := tunnelHTTP().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "hello from the agent target" {
		t.Fatalf("body = %q", body)
	}
	if got := resp.Header.Get("X-Target"); got != "agent" {
		t.Fatalf("X-Target = %q, want %q", got, "agent")
	}
	if got := resp.Header.Get("X-Caller-Seen"); got != "integration-test" {
		t.Fatalf("X-Caller-Seen = %q, want %q", got, "integration-test")
	}
}

func TestQueryAndPathReachTheTarget(t *testing.T) {
	env := testenv.Start(t)

	resp, err := tunnelHTTP().Get(env.HubHTTPURL + "/a/" + env.AgentID + "/echo/path?q=1&q=2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "/echo/path?q=1&q=2" {
		t.Fatalf("target saw %q", body)
	}
}

func TestPostBodyIsForwarded(t *testing.T) {
	env := testenv.Start(t)

	// 300 KiB forces several HttpBody envelopes.
	payload := bytes.Repeat([]byte("x"), 300<<10)
	resp, err := tunnelHTTP().Post(env.HubHTTPURL+"/a/"+env.AgentID+"/size", "application/octet-stream",
		bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != "307200" {
		t.Fatalf("target reported %q bytes, want 307200", body)
	}
}

func TestLargeResponseArrivesIntact(t *testing.T) {
	env := testenv.Start(t)

	resp, err := tunnelHTTP().Get(env.HubHTTPURL + "/a/" + env.AgentID + "/big")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := testenv.BigResponse(); !bytes.Equal(body, want) {
		t.Fatalf("body = %d bytes, want %d identical bytes", len(body), len(want))
	}
}

func TestForwardedPrefixIsSet(t *testing.T) {
	env := testenv.Start(t)

	req, err := http.NewRequest(http.MethodGet, env.HubHTTPURL+"/a/"+env.AgentID+"/prefix", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	// A spoofed prefix from the client must not reach the target.
	req.Header.Set("X-Forwarded-Prefix", "/somewhere/else")

	resp, err := tunnelHTTP().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	want := "/a/" + env.AgentID
	if string(body) != want {
		t.Fatalf("prefix = %q, want %q", body, want)
	}
}

func TestTargetStatusIsRelayed(t *testing.T) {
	env := testenv.Start(t)

	resp, err := tunnelHTTP().Get(env.HubHTTPURL + "/a/" + env.AgentID + "/teapot")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "no coffee here" {
		t.Fatalf("body = %q", body)
	}
}

func TestUnknownAgentReturnsServiceUnavailable(t *testing.T) {
	env := testenv.Start(t)

	resp, err := tunnelHTTP().Get(env.HubHTTPURL + "/a/nobody/hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}
