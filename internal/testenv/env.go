// Package testenv starts a hub, an agent and the demo service in one process
// so the integration tests exercise the real websocket and real gRPC stacks.
// The transport is never mocked — that is where the bugs live.
package testenv

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/agent"
	"plinth.io/poc/internal/demo"
	"plinth.io/poc/internal/hub"
)

const (
	testAgentID = "mac-1"
	testToken   = "secret1"
)

type Env struct {
	Hub          *hub.Hub
	HubGRPCAddr  string
	HubHTTPURL   string
	AgentID      string
	DemoGRPCAddr string
}

// Start brings the whole chain up and tears it down with the test.
func Start(t *testing.T) *Env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	demoAddr := startDemoService(t)
	h, httpSrv := startHub(t)
	grpcAddr := startHubGRPC(t, h)
	agentErr := startAgent(t, ctx, httpSrv.URL, demoAddr)

	env := &Env{
		Hub:          h,
		HubGRPCAddr:  grpcAddr,
		HubHTTPURL:   httpSrv.URL,
		AgentID:      testAgentID,
		DemoGRPCAddr: demoAddr,
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
func startAgent(t *testing.T, ctx context.Context, hubHTTPURL, demoAddr string) <-chan error {
	t.Helper()
	a := agent.New(agent.Config{
		HubURL:     "ws" + strings.TrimPrefix(hubHTTPURL, "http") + "/agent/connect",
		Token:      testToken,
		AgentID:    testAgentID,
		Version:    "test",
		GRPCTarget: demoAddr,
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
