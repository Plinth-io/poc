// Package agent connects outwards to the hub and relays what arrives to local
// targets. It never listens for inbound connections.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/rawcodec"
	"plinth.io/poc/internal/relay"
)

type Config struct {
	HubURL     string // ws://127.0.0.1:7001/agent/connect
	Token      string
	AgentID    string
	Version    string
	GRPCTarget string // 127.0.0.1:50052
	HTTPTarget string // http://127.0.0.1:8090
}

type Agent struct {
	cfg Config

	mu      sync.Mutex
	closed  bool
	grpcCC  *grpc.ClientConn
	busConn *bus.Conn
	ctx     context.Context
}

func New(cfg Config) *Agent { return &Agent{cfg: cfg} }

// ConnectOnce dials the hub, announces itself and serves the bus until the
// connection ends. Reconnecting is the caller's job.
func (a *Agent) ConnectOnce(ctx context.Context) error {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+a.cfg.Token)

	ws, _, err := websocket.Dial(ctx, a.cfg.HubURL, &websocket.DialOptions{
		HTTPHeader:      header,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return fmt.Errorf("dial hub: %w", err)
	}

	// Below this point, ws is ours to close until bus.NewConn takes it over;
	// after that, conn.Run owns closing it (see the Conn doc comment). No
	// defer here: a deferred close would race conn.Run's own close exactly
	// like the hub's did (see startClose in internal/bus/conn.go), so the two
	// early returns below close explicitly instead.
	hello := &busv1.Envelope{Payload: &busv1.Envelope_Hello{Hello: &busv1.Hello{
		AgentId: a.cfg.AgentID,
		Version: a.cfg.Version,
		Targets: a.targets(),
	}}}
	raw, err := proto.Marshal(hello)
	if err != nil {
		if cerr := ws.CloseNow(); cerr != nil {
			slog.Debug("close websocket after marshal failure", "err", cerr)
		}
		return fmt.Errorf("marshal hello: %w", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
		if cerr := ws.CloseNow(); cerr != nil {
			slog.Debug("close websocket after failed hello", "err", cerr)
		}
		return fmt.Errorf("send hello: %w", err)
	}

	conn := bus.NewConn(ws, bus.SideAgent, a.onOpen)
	a.setConn(ctx, conn)
	slog.Info("connected to hub", "agent_id", a.cfg.AgentID)
	return conn.Run(ctx)
}

func (a *Agent) targets() []string {
	var out []string
	if a.cfg.GRPCTarget != "" {
		out = append(out, "grpc://"+a.cfg.GRPCTarget)
	}
	if a.cfg.HTTPTarget != "" {
		out = append(out, a.cfg.HTTPTarget)
	}
	return out
}

// setConn records what ConnectOnce built. onOpen only receives the stream, so
// the connection and its context have to be reachable from the Agent.
func (a *Agent) setConn(ctx context.Context, c *bus.Conn) {
	a.mu.Lock()
	a.busConn, a.ctx = c, ctx
	a.mu.Unlock()
}

func (a *Agent) session() (context.Context, *bus.Conn) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ctx == nil {
		return context.Background(), a.busConn
	}
	return a.ctx, a.busConn
}

// grpcConn lazily dials the local gRPC target. ForceCodec keeps the tunnelled
// payload bytes untouched on this hop as well.
func (a *Agent) grpcConn() (*grpc.ClientConn, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errClosed
	}
	if a.grpcCC != nil {
		return a.grpcCC, nil
	}
	cc, err := grpc.NewClient(a.cfg.GRPCTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawcodec.Codec{})),
	)
	if err != nil {
		return nil, err
	}
	a.grpcCC = cc
	return cc, nil
}

// httpClient talks to the local target. Redirects are not followed, so the
// caller sees the target's own 3xx response.
func httpClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// errClosed rejects work that arrives after Close, so a late stream cannot
// revive the connection to the local target.
var errClosed = errors.New("agent: closed")

// Close releases the connection to the local target. It deliberately leaves
// the websocket alone: from bus.NewConn onward the bus.Conn owns that socket.
func (a *Agent) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	if a.grpcCC != nil {
		_ = a.grpcCC.Close()
		a.grpcCC = nil
	}
}

// onOpen handles a stream the hub opened.
func (a *Agent) onOpen(st *bus.Stream, env *busv1.Envelope) {
	switch p := env.GetPayload().(type) {
	case *busv1.Envelope_RpcOpen:
		cc, err := a.grpcConn()
		if err != nil {
			slog.Error("cannot reach local gRPC target", "err", err)
			st.Close(err)
			return
		}
		ctx, conn := a.session()
		relay.ServeRPC(ctx, st, conn, p.RpcOpen, cc)
	case *busv1.Envelope_HttpOpen:
		if a.cfg.HTTPTarget == "" {
			st.Close(errors.New("agent: no local HTTP target configured"))
			return
		}
		ctx, conn := a.session()
		relay.ServeHTTPStream(ctx, st, conn, p.HttpOpen, a.cfg.HTTPTarget, httpClient())
	default:
		st.Close(fmt.Errorf("agent: no handler for %T", env.GetPayload()))
	}
}
