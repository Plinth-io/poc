package main

import (
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"plinth.io/poc/internal/hub"
)

// readHeaderTimeout bounds how long a client may take sending request
// headers. readTimeout bounds the whole request read, including the body —
// this is what closes the resource hold on /a/{id}/: without it, a client
// that trickles a request body without ever finishing it can keep a handler
// goroutine, a stream registry entry and a TCP connection alive forever.
// Neither timeout touches response writing, so the long-lived SSE responses
// (/events/stream and the agent's tunnelled /logs/stream) are unaffected;
// a WriteTimeout would cut those off mid-stream and is deliberately not set.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 30 * time.Second
)

func main() {
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:7000", "gRPC listen address for callers")
	httpAddr := flag.String("http-addr", "127.0.0.1:7001", "HTTP listen address for the UI and agents")
	flag.Parse()

	// Callers are not authenticated, so both listeners stay on the loopback
	// interface. Opening them to the network requires caller auth first.
	tokens, err := hub.ParseTokens(os.Getenv("HUB_TOKENS"))
	if err != nil {
		slog.Error("HUB_TOKENS", "err", err)
		os.Exit(1)
	}
	if len(tokens) == 0 {
		slog.Error("HUB_TOKENS is empty, no agent could authenticate")
		os.Exit(1)
	}

	h := hub.New(hub.Config{Tokens: tokens})

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		slog.Error("listen grpc", "addr", *grpcAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("hub gRPC listening", "addr", *grpcAddr)
		if err := h.GRPCServer().Serve(lis); err != nil {
			slog.Error("grpc serve", "err", err)
			os.Exit(1)
		}
	}()

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           h.Mux(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
	}
	slog.Info("hub HTTP listening", "addr", *httpAddr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("http serve", "err", err)
		os.Exit(1)
	}
}
