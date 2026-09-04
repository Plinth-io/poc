package hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
)

// rawAgent dials the hub with a valid token and sends the hello, then either
// keeps reading — which is what makes coder/websocket answer a ping with a
// pong — or goes silent, which is what a killed machine looks like on a
// connection TCP has not given up on yet.
func rawAgent(t *testing.T, ctx context.Context, wsURL string, answerPings bool) {
	t.Helper()
	header := http.Header{}
	header.Set("Authorization", "Bearer secret1")
	ws, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.CloseNow() })

	raw, err := proto.Marshal(&busv1.Envelope{Payload: &busv1.Envelope_Hello{
		Hello: &busv1.Hello{AgentId: "mac-1", Version: "test"},
	}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if answerPings {
		go func() {
			for {
				if _, _, err := ws.Read(ctx); err != nil {
					return
				}
			}
		}()
	}
}

// Test binaries ping far more often than production, and they do it from
// here rather than per test: the keepalive goroutine reads these while it
// runs, and it outlives the test that connected it, so a per-test restore
// would be a data race on exactly the connections these tests create.
func init() {
	pingInterval, pingTimeout = 20*time.Millisecond, 100*time.Millisecond
}

func startKeepaliveHub(t *testing.T) (*Hub, string) {
	t.Helper()
	tokens, err := ParseTokens("mac-1:secret1")
	if err != nil {
		t.Fatalf("ParseTokens: %v", err)
	}
	h := New(Config{Tokens: tokens})
	srv := httptest.NewServer(h.Mux())
	t.Cleanup(srv.Close)
	return h, "ws" + strings.TrimPrefix(srv.URL, "http") + "/agent/connect"
}

// waitUntil polls cond and fails with msg once the bound is up, so no wait in
// these tests can hang the suite.
func waitUntil(t *testing.T, bound time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestKeepaliveDropsAnAgentThatStopsAnsweringPings is why the hub pings at
// all: without it the registry keeps handing this connection to callers until
// TCP gives up on it, which takes minutes.
func TestKeepaliveDropsAnAgentThatStopsAnsweringPings(t *testing.T) {
	h, wsURL := startKeepaliveHub(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rawAgent(t, ctx, wsURL, false)
	waitUntil(t, 2*time.Second, func() bool {
		_, ok := h.Agents().Get("mac-1")
		return ok
	}, "agent never registered")

	// The socket teardown behind this still waits out the close handshake,
	// but the registry entry has to go at the moment the pong is missed —
	// this bound would not hold if it waited for the handshake instead.
	waitUntil(t, 2*time.Second, func() bool {
		_, ok := h.Agents().Get("mac-1")
		return !ok
	}, "hub kept a silent agent in the registry")
}

// TestKeepaliveRecordsTheLastPong covers the other half: an agent that is
// merely idle answers from its read loop and must never be dropped, and the
// answer is what the hub UI's "letzter Pong" column shows.
func TestKeepaliveRecordsTheLastPong(t *testing.T) {
	h, wsURL := startKeepaliveHub(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rawAgent(t, ctx, wsURL, true)
	waitUntil(t, 2*time.Second, func() bool {
		_, ok := h.Agents().Get("mac-1")
		return ok
	}, "agent never registered")

	waitUntil(t, 2*time.Second, func() bool {
		ag, ok := h.Agents().Get("mac-1")
		return ok && !ag.LastPong().IsZero()
	}, "no answered ping was recorded for an idle but healthy agent")

	// Several ping intervals later it is still registered: idleness alone
	// must not look like death.
	time.Sleep(5 * pingInterval)
	if _, ok := h.Agents().Get("mac-1"); !ok {
		t.Fatal("hub dropped an idle agent that answers its pings")
	}
}
