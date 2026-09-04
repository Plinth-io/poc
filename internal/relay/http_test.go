package relay_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/relay"
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

	tests := []struct {
		name string
		id   string
	}{
		{name: "plain agent id", id: env.AgentID},
		// The caller picks the encoding, not the agent: this still resolves to
		// the registered id, so the path split must not depend on the two
		// spellings being equal.
		{name: "percent-encoded agent id", id: strings.ReplaceAll(env.AgentID, "-", "%2d")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := tunnelHTTP().Get(env.HubHTTPURL + "/a/" + tc.id + "/echo/path?q=1&q=2")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if string(body) != "/echo/path?q=1&q=2" {
				t.Fatalf("target saw %q", body)
			}
		})
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

// TestEarlyResponseSurvivesAnUnreadRequestBody covers a target that answers
// before reading the request: the agent's transport abandons the upload while
// the response is still streaming, and the response must survive that.
//
// The 8 MiB body is load-bearing. Anything small enough for the loopback
// buffers and the server's own post-handler drain to swallow never makes the
// pipe write fail, and the test then passes without reaching the branch it
// exists for.
func TestEarlyResponseSurvivesAnUnreadRequestBody(t *testing.T) {
	env := testenv.Start(t)

	payload := bytes.Repeat([]byte("x"), 8<<20)
	resp, err := tunnelHTTP().Post(env.HubHTTPURL+"/a/"+env.AgentID+"/early",
		"application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := strings.Repeat("chunk\n", 5); string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
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

// stubLookup hands every request to the same connection, so a fake agent needs
// no token, no hello and no registry.
type stubLookup struct{ conn *bus.Conn }

func (s stubLookup) Lookup(string) (*bus.Conn, bool) { return s.conn, true }

// fakeAgent runs a real bus over a real websocket with a scripted peer on the
// agent side. It exists for envelopes a well-behaved agent never sends, which
// is the only way to cover the hub's guards against a buggy or hostile peer.
// It returns the URL of a hub serving HubHTTP.
func fakeAgent(t *testing.T, reply func(conn *bus.Conn, st *bus.Stream, open *busv1.HttpOpen)) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	hubSide := make(chan *bus.Conn, 1)
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		c := bus.NewConn(ws, bus.SideHub, nil)
		hubSide <- c
		_ = c.Run(r.Context())
	}))
	// Cancel before Close: the websocket handler only returns once its Conn
	// does, and Close waits for outstanding requests.
	t.Cleanup(func() { cancel(); wsSrv.Close() })

	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(wsSrv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	var agentConn *bus.Conn
	agentConn = bus.NewConn(ws, bus.SideAgent, func(st *bus.Stream, env *busv1.Envelope) {
		defer st.Close(nil)
		reply(agentConn, st, env.GetHttpOpen())
	})
	go func() { _ = agentConn.Run(ctx) }()

	var conn *bus.Conn
	select {
	case conn = <-hubSide:
	case <-time.After(callTimeout):
		t.Fatal("the hub side of the fake bus never came up")
	}

	mux := http.NewServeMux()
	mux.Handle("/a/{id}/", relay.HubHTTP(stubLookup{conn: conn}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestInvalidStatusFromTheAgentIsRefused(t *testing.T) {
	tests := []struct {
		name   string
		status int32
	}{
		{name: "unset field", status: 0},
		{name: "below the range", status: 99},
		{name: "above the range", status: 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hubURL := fakeAgent(t, func(conn *bus.Conn, st *bus.Stream, _ *busv1.HttpOpen) {
				_ = conn.Send(context.Background(), &busv1.Envelope{StreamId: st.ID,
					Payload: &busv1.Envelope_HttpResponseHead{
						HttpResponseHead: &busv1.HttpResponseHead{Status: tc.status},
					}})
				<-st.Done()
			})

			resp, err := tunnelHTTP().Get(hubURL + "/a/anyone/hello")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502", resp.StatusCode)
			}
		})
	}
}

// fakeHub is fakeAgent's mirror: a real bus whose agent side runs
// ServeHTTPStream, so a test can send an HttpOpen no honest hub would build.
// It returns the hub-side connection.
func fakeHub(t *testing.T, target string) *bus.Conn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		var agentConn *bus.Conn
		agentConn = bus.NewConn(ws, bus.SideAgent, func(st *bus.Stream, env *busv1.Envelope) {
			relay.ServeHTTPStream(r.Context(), st, agentConn, env.GetHttpOpen(), target, tunnelHTTP())
		})
		_ = agentConn.Run(r.Context())
	}))
	// Cancel before Close: the websocket handler only returns once its Conn
	// does, and Close waits for outstanding requests.
	t.Cleanup(func() { cancel(); wsSrv.Close() })

	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(wsSrv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	conn := bus.NewConn(ws, bus.SideHub, nil)
	go func() { _ = conn.Run(ctx) }()
	return conn
}

// TestURIWithoutALeadingSlashIsRefused covers the guard against a uri that
// re-parses the concatenation with the target into a different destination:
// "@example.com/" would demote the target to userinfo. The hub never builds
// such a uri, but the agent trusts nothing it reads off the bus.
func TestURIWithoutALeadingSlashIsRefused(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached.Store(true)
	}))
	defer target.Close()

	conn := fakeHub(t, target.URL)
	st, err := conn.Streams.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close(nil)

	open := &busv1.Envelope{StreamId: st.ID, Payload: &busv1.Envelope_HttpOpen{
		HttpOpen: &busv1.HttpOpen{Method: http.MethodGet, Uri: "@example.com/"},
	}}
	if err := conn.Send(context.Background(), open); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case env := <-st.In:
		end := env.GetHttpResponseEnd()
		if end == nil {
			t.Fatalf("first envelope back is %T, want an HttpResponseEnd", env.GetPayload())
		}
		if end.GetError() == "" {
			t.Fatal("the agent accepted a uri without a leading slash")
		}
	case <-time.After(callTimeout):
		t.Fatal("no terminal envelope for the refused uri")
	}
	if reached.Load() {
		t.Fatal("the refused request still reached a target")
	}
}
