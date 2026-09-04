// Package testenv starts a hub, an agent and the demo service in one process
// so the integration tests exercise the real websocket and real gRPC stacks.
// The transport is never mocked — that is where the bugs live.
package testenv

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/agent"
	"plinth.io/poc/internal/demo"
	"plinth.io/poc/internal/hub"
	"plinth.io/poc/internal/relay"
)

const (
	testAgentID = "mac-1"
	testToken   = "secret1"
)

type Env struct {
	Hub                *hub.Hub
	HubGRPCAddr        string
	HubHTTPURL         string
	AgentID            string
	DemoGRPCAddr       string
	AgentHTTPTargetURL string
}

// cancelSeen records whether the /sse route's handler observed its request
// context end, so a test can prove HttpCancel travelled all the way to the
// handler behind the agent.
var cancelSeen atomic.Bool

// CancelObserved reports whether the /sse route's handler has seen its
// request context cancelled since the last Start.
func (e *Env) CancelObserved() bool { return cancelSeen.Load() }

// Start brings the whole chain up and tears it down with the test.
func Start(t *testing.T) *Env {
	t.Helper()
	cancelSeen.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	demoAddr := startDemoService(t)
	targetURL := startAgentTarget(t)
	h, httpSrv := startHub(t)
	grpcAddr := startHubGRPC(t, h)
	agentErr := startAgent(t, ctx, httpSrv.URL, demoAddr, targetURL)

	env := &Env{
		Hub:                h,
		HubGRPCAddr:        grpcAddr,
		HubHTTPURL:         httpSrv.URL,
		AgentID:            testAgentID,
		DemoGRPCAddr:       demoAddr,
		AgentHTTPTargetURL: targetURL,
	}
	waitForAgent(t, h, agentErr)
	return env
}

// Dial returns a plain gRPC client against the hub. It carries no schema of
// the tunnelled service beyond what the caller itself uses.
func (e *Env) Dial(t *testing.T) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(e.HubGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func startDemoService(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(echoCallerHeader))
	demov1.RegisterDemoServer(srv, demo.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// echoCallerHeader copies an incoming x-caller into a response header so the
// tests can prove metadata survived the tunnel in both directions.
func echoCallerHeader(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-caller"); len(vals) > 0 {
			_ = grpc.SetHeader(ctx, metadata.Pairs("x-caller-seen", vals[0]))
		}
	}
	return handler(ctx, req)
}

// startAgentTarget is the plain HTTP service the agent tunnels to. It shares
// nothing with the demo gRPC service: the two prove the same bus carries
// unrelated protocols.
func startAgentTarget(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Target", "agent")
		w.Header().Set("X-Caller-Seen", r.Header.Get("X-Caller"))
		_, _ = io.WriteString(w, "hello from the agent target")
	})
	mux.HandleFunc("/echo/path", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.URL.RequestURI())
	})
	mux.HandleFunc("/size", func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, n)
	})
	mux.HandleFunc("/prefix", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get(relay.ForwardedPrefixHeader))
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(BigResponse())
	})
	mux.HandleFunc("/early", func(w http.ResponseWriter, r *http.Request) {
		// Answers without ever reading the request body, then streams slowly:
		// the transport gives up on the upload while the response is still in
		// flight, which is the window a cancelled context would lose.
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		for i := 0; i < 5; i++ {
			_, _ = io.WriteString(w, "chunk\n")
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
		}
	})
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		// Writes one event immediately and then keeps the response open, so a
		// test can prove that bytes arrive before the handler returns.
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		for i := 0; ; i++ {
			if _, err := fmt.Fprintf(w, "data: tick %d\n\n", i); err != nil {
				return
			}
			flusher.Flush()
			select {
			case <-r.Context().Done():
				cancelSeen.Store(true)
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	})
	mux.HandleFunc("/teapot", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "no coffee here")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// BigResponse is the body of the target's /big route: large enough to span
// several response envelopes and not self-similar, so a chunk that is lost,
// reordered or overwritten by the next read shows up as a mismatch.
func BigResponse() []byte {
	out := make([]byte, 300<<10)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}

func startHub(t *testing.T) (*hub.Hub, *httptest.Server) {
	t.Helper()
	tokens, err := hub.ParseTokens(testAgentID + ":" + testToken)
	if err != nil {
		t.Fatalf("ParseTokens: %v", err)
	}
	h := hub.New(hub.Config{Tokens: tokens})
	srv := httptest.NewServer(h.Mux())
	t.Cleanup(srv.Close)
	return h, srv
}

func startHubGRPC(t *testing.T, h *hub.Hub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := h.GRPCServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// startAgent returns the channel ConnectOnce's result lands in, so a failed
// dial, auth or hello shows up as itself instead of as a registration timeout.
func startAgent(t *testing.T, ctx context.Context, hubHTTPURL, demoAddr, httpTarget string) <-chan error {
	t.Helper()
	a := agent.New(agent.Config{
		HubURL:     "ws" + strings.TrimPrefix(hubHTTPURL, "http") + "/agent/connect",
		Token:      testToken,
		AgentID:    testAgentID,
		Version:    "test",
		GRPCTarget: demoAddr,
		HTTPTarget: httpTarget,
	})
	errc := make(chan error, 1)
	go func() { errc <- a.ConnectOnce(ctx) }()
	t.Cleanup(func() { a.Close() })
	return errc
}

func waitForAgent(t *testing.T, h *hub.Hub, agentErr <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := h.Agents().Get(testAgentID); ok {
			return
		}
		select {
		case err := <-agentErr:
			t.Fatalf("agent stopped before registering: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.Fatal("agent never registered with the hub")
}
