// Package agent connects outwards to the hub and relays what arrives to local
// targets. It never listens for inbound connections.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
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
	defer ws.CloseNow()

	hello := &busv1.Envelope{Payload: &busv1.Envelope_Hello{Hello: &busv1.Hello{
		AgentId: a.cfg.AgentID,
		Version: a.cfg.Version,
		Targets: a.targets(),
	}}}
	raw, err := proto.Marshal(hello)
	if err != nil {
		return fmt.Errorf("marshal hello: %w", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	conn := bus.NewConn(ws, bus.SideAgent, a.onOpen)
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

// onOpen handles a stream the hub opened. Task 7 fills in the gRPC branch and
// Task 11 the HTTP branch.
func (a *Agent) onOpen(st *bus.Stream, env *busv1.Envelope) {
	st.Close(fmt.Errorf("agent: no handler for %T", env.GetPayload()))
}
