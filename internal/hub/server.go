package hub

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/rawcodec"
	"plinth.io/poc/internal/relay"
)

// helloTimeout bounds how long a freshly upgraded connection may stay silent.
const helloTimeout = 5 * time.Second

type Config struct {
	Tokens map[string]string // token -> agent id
}

// inspectorCapacity bounds how many past envelopes the hub UI can show on
// load; live updates arrive over /events/stream regardless of this size.
const inspectorCapacity = 500

type Hub struct {
	tokens    map[string]string
	agents    *Agents
	inspector *Inspector

	mu     sync.RWMutex
	onOpen bus.OpenFunc
}

func New(cfg Config) *Hub {
	// Config.Tokens can be built by hand, bypassing ParseTokens' guarantees;
	// an empty key would otherwise match the empty token bearerToken trims a
	// bare "Bearer " down to.
	delete(cfg.Tokens, "")
	return &Hub{tokens: cfg.Tokens, agents: newAgents(), inspector: NewInspector(inspectorCapacity)}
}

func (h *Hub) Agents() *Agents { return h.agents }

// Inspector gives the UI access to the live envelope feed.
func (h *Hub) Inspector() *Inspector { return h.inspector }

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
	mux.HandleFunc("GET /{$}", h.handleIndex)
	mux.HandleFunc("GET /events/stream", h.handleEventStream)
	mux.HandleFunc("/agent/connect", h.handleConnect)
	mux.Handle("/a/{id}/", relay.HubHTTP(h.agents))
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
	// The two rejection paths below use closeAsync directly, since bus.Conn
	// does not own ws yet at that point. From bus.NewConn onward (below),
	// bus.Conn owns ws: conn.Run guarantees the fd is released before it
	// returns, so nothing here needs to close ws again once that call is
	// reached — doing so would race with Run's own close.

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	hello, err := readHello(ctx, ws)
	if err != nil {
		closeAsync(ws, websocket.StatusProtocolError, "expected hello")
		slog.Warn("no hello received", "agent_id", agentID, "err", err)
		return
	}
	if hello.GetAgentId() != agentID {
		closeAsync(ws, websocket.StatusPolicyViolation, "agent id does not match token")
		slog.Warn("hello claims a foreign agent id",
			"token_agent_id", agentID, "claimed", hello.GetAgentId())
		return
	}

	conn := bus.NewConn(ws, bus.SideHub, h.openFunc())
	conn.SetTap(func(dir string, env *busv1.Envelope) {
		h.inspector.Record(agentID, dir, env)
	})
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

// closeAsync starts the close handshake without blocking the caller, the same
// way bus.Conn.CloseWith does: Close can take several seconds waiting for the
// peer's acknowledgement, and a rejected connection must not tie up the
// handler goroutine for that long — a stream of bad tokens would otherwise be
// a cheap way to occupy handlers.
func closeAsync(ws *websocket.Conn, code websocket.StatusCode, reason string) {
	go func() {
		if err := ws.Close(code, reason); err != nil {
			slog.Debug("close handshake", "reason", reason, "err", err)
		}
	}()
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

// GRPCServer accepts every method and relays it. ForceServerCodec keeps the
// payload bytes untouched, so the hub needs no schema of the target service.
func (h *Hub) GRPCServer() *grpc.Server {
	return grpc.NewServer(
		grpc.ForceServerCodec(rawcodec.Codec{}),
		grpc.UnknownServiceHandler(relay.HubGRPC(h.agents)),
	)
}
