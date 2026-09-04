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
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	busv1 "plinth.io/poc/gen/bus/v1"
	"plinth.io/poc/internal/bus"
	"plinth.io/poc/internal/rawcodec"
	"plinth.io/poc/internal/relay"
)

const (
	defaultMinBackoff = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
)

// pingInterval and pingTimeout are vars, not consts, so a test can shrink them
// to make a ping failure observable in well under a second instead of the
// production ~55s worst case.
var (
	pingInterval = 15 * time.Second
	pingTimeout  = 30 * time.Second
)

type Config struct {
	HubURL     string // ws://127.0.0.1:7001/agent/connect
	Token      string
	AgentID    string
	Version    string
	GRPCTarget string // 127.0.0.1:50052
	HTTPTarget string // http://127.0.0.1:8090

	// MinBackoff and MaxBackoff bound the delay between reconnect attempts.
	// Zero picks defaultMinBackoff/defaultMaxBackoff.
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

type Agent struct {
	cfg Config

	mu          sync.Mutex
	closed      bool
	grpcCC      *grpc.ClientConn
	busConn     *bus.Conn
	ctx         context.Context
	connectedAt time.Time
}

func New(cfg Config) *Agent { return &Agent{cfg: cfg} }

// ConnectOnce dials the hub, announces itself and serves the bus until the
// connection ends. Reconnecting is the caller's job.
func (a *Agent) ConnectOnce(ctx context.Context) error {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+a.cfg.Token)

	// ponytail: no TLS. HubURL is whatever the -hub flag says, and against a
	// ws:// hub this token travels in the clear along with everything else.
	// The upgrade is a wss:// URL, which needs no code change here — it is
	// required the moment the hub is not localhost.

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

	pingCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go a.keepalive(pingCtx, ws, conn)

	err = conn.Run(ctx)
	a.clearConn()
	return err
}

// Run keeps a connection to the hub alive until ctx ends. Streams do not
// survive a drop: the calls in flight fail and the next one uses the fresh
// connection.
//
// ponytail: no stream resumption, so a long transfer restarts from the top
// after an outage. Buffering plus sequence numbers on both sides is the
// upgrade, once streams have to outlive their connection.
func (a *Agent) Run(ctx context.Context) error {
	b := backoff{min: a.cfg.MinBackoff, max: a.cfg.MaxBackoff}
	if b.min <= 0 {
		b.min = defaultMinBackoff
	}
	if b.max < b.min {
		b.max = max(defaultMaxBackoff, b.min)
	}

	var delay time.Duration
	for {
		start := time.Now()
		err := a.ConnectOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(start) > b.max {
			delay = 0
		} else {
			delay = b.next(delay)
		}
		slog.Warn("connection to hub ended, retrying", "err", err, "in", delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// keepalive detects a silently dead connection. coder/websocket's Ping blocks
// until the pong arrives, so a timeout is all the liveness check needs. A
// failed ping tears the connection down through conn.CloseWith rather than
// closing ws directly: from bus.NewConn onward the Conn is the socket's only
// closer, and a second closer here would reintroduce the CAS race CloseWith
// exists to avoid.
func (a *Agent) keepalive(ctx context.Context, ws *websocket.Conn, conn *bus.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := ws.Ping(pingCtx)
			cancel()
			if err != nil {
				conn.CloseWith(websocket.StatusNormalClosure, "keepalive: no pong")
				return
			}
		}
	}
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
	a.connectedAt = time.Now()
	a.mu.Unlock()
}

// clearConn drops the reference to a connection that has ended, so
// Status().Connected reports the current state instead of staying true
// forever after the first connection.
func (a *Agent) clearConn() {
	a.mu.Lock()
	a.busConn = nil
	a.mu.Unlock()
}

// isClosed reports whether Close has run, so both branches of onOpen refuse
// late work instead of only the gRPC one.
func (a *Agent) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

// session returns the context and connection that own st, or a nil connection
// if they no longer do. onOpen runs on a goroutine bus.Conn.dispatch spawned,
// so the connection can end — and ConnectOnce clear it — before the stream is
// ever served. A newer connection is no substitute either: its stream ids
// belong to a different registry, so st is only served while it is still the
// registered stream of the current connection.
func (a *Agent) session(st *bus.Stream) (context.Context, *bus.Conn) {
	a.mu.Lock()
	ctx, conn := a.ctx, a.busConn
	a.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if conn == nil {
		return ctx, nil
	}
	if cur, ok := conn.Streams.Get(st.ID); !ok || cur != st {
		return ctx, nil
	}
	return ctx, conn
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
// caller sees the target's own 3xx response. One shared client is enough: its
// zero Transport means every tunnelled request reuses http.DefaultTransport.
var httpClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
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
	// Both relays send on conn unconditionally, so the connection is checked
	// here, once, before either of them ever sees it — a stream that opens
	// while the connection is being torn down must end as a closed stream,
	// not as a nil dereference that takes the whole agent down.
	ctx, conn := a.session(st)
	if conn == nil {
		st.Close(errors.New("agent: connection ended before the stream started"))
		return
	}

	switch p := env.GetPayload().(type) {
	case *busv1.Envelope_RpcOpen:
		cc, err := a.grpcConn()
		if err != nil {
			slog.Error("cannot reach local gRPC target", "err", err)
			st.Close(err)
			return
		}
		relay.ServeRPC(ctx, st, conn, p.RpcOpen, cc)
	case *busv1.Envelope_HttpOpen:
		if a.isClosed() {
			st.Close(errClosed)
			return
		}
		if a.cfg.HTTPTarget == "" {
			st.Close(errors.New("agent: no local HTTP target configured"))
			return
		}
		relay.ServeHTTPStream(ctx, st, conn, p.HttpOpen, a.cfg.HTTPTarget, httpClient)
	default:
		st.Close(fmt.Errorf("agent: no handler for %T", env.GetPayload()))
	}
}
