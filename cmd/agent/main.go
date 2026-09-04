package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"plinth.io/poc/internal/agent"
)

func main() {
	hubURL := flag.String("hub", "ws://127.0.0.1:7001/agent/connect", "hub websocket endpoint")
	agentID := flag.String("id", "mac-1", "agent id, must match the token")
	grpcTarget := flag.String("grpc-target", "127.0.0.1:50052", "local gRPC target")
	uiAddr := flag.String("ui-addr", "127.0.0.1:8090", "address of this agent's own UI")
	flag.Parse()

	logs := agent.NewLogBuffer(1000)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, logs), nil)))

	a := agent.New(agent.Config{
		HubURL:     *hubURL,
		Token:      os.Getenv("AGENT_TOKEN"),
		AgentID:    *agentID,
		Version:    "poc",
		GRPCTarget: *grpcTarget,
		HTTPTarget: "http://" + *uiAddr,
	})
	defer a.Close()

	// The agent serves its own UI and relays it back through its own tunnel,
	// which shows that the bus knows nothing about its targets.
	go func() {
		slog.Info("agent UI listening", "addr", *uiAddr)
		if err := http.ListenAndServe(*uiAddr, a.UIHandler(logs)); err != nil {
			slog.Error("agent ui", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("agent stopped", "err", err)
		os.Exit(1)
	}
}
