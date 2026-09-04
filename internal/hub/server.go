package hub

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
)

// helloTimeout bounds how long a freshly upgraded connection may stay silent.
const helloTimeout = 5 * time.Second

type Config struct {
	Tokens map[string]string // token -> agent id
}

type Hub struct {
	tokens map[string]string
	agents *Agents

	mu     sync.RWMutex
	onOpen bus.OpenFunc
}

func New(cfg Config) *Hub {
	return &Hub{tokens: cfg.Tokens, agents: newAgents()}
}

func (h *Hub) Agents() *Agents { return h.agents }

// SetOpenFunc installs the handler for streams an agent opens towards the hub.
func (h *Hub) SetOpenFunc(f bus.OpenFunc) {
	h.mu.Lock()
	h.onOpen = f
	h.mu.Unlock()
}

func (h *Hub) openFunc() bus.OpenFunc {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.onOpen
}

func (h *Hub) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/agent/connect", h.handleConnect)
	return mux
}

func (h *Hub) handleConnect(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	agentID, ok := h.tokens[token]
	if !ok {
		http.Error(w, "unknown token", http.StatusUnauthorized)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		slog.Warn("websocket upgrade failed", "agent_id", agentID, "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	hello, err := readHello(ctx, ws)
	if err != nil {
		ws.Close(websocket.StatusProtocolError, "expected hello")
		slog.Warn("no hello received", "agent_id", agentID, "err", err)
		return
	}
	if hello.GetAgentId() != agentID {
		ws.Close(websocket.StatusPolicyViolation, "agent id does not match token")
		slog.Warn("hello claims a foreign agent id",
			"token_agent_id", agentID, "claimed", hello.GetAgentId())
		return
	}

	conn := bus.NewConn(ws, bus.SideHub, h.openFunc())
	ag := &Agent{
		ID:      agentID,
		Conn:    conn,
		Since:   time.Now(),
		Version: hello.GetVersion(),
		Targets: hello.GetTargets(),
	}
	if old := h.agents.Add(ag); old != nil {
		old.Conn.CloseWith(websocket.StatusNormalClosure, "replaced by a newer connection")
	}
	defer h.agents.Remove(agentID, ag)

	slog.Info("agent connected", "agent_id", agentID, "version", ag.Version)
	err = conn.Run(ctx)
	slog.Info("agent disconnected", "agent_id", agentID, "err", err)
}

// readHello consumes the first envelope, which must be a hello. It runs before
// the bus takes over the socket, so the connection is authenticated end to end
// before any stream can exist.
func readHello(ctx context.Context, ws *websocket.Conn) (*busv1.Hello, error) {
	ctx, cancel := context.WithTimeout(ctx, helloTimeout)
	defer cancel()

	typ, raw, err := ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("hello: unexpected message type %v", typ)
	}
	env := &busv1.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		return nil, fmt.Errorf("hello: %w", err)
	}
	hello := env.GetHello()
	if hello == nil {
		return nil, fmt.Errorf("hello: first envelope carries %T", env.GetPayload())
	}
	return hello, nil
}
