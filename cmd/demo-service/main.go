package main

import (
	"flag"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"

	demov1 "plinth.io/poc/gen/demo/v1"
	"plinth.io/poc/internal/demo"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50052", "listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	demov1.RegisterDemoServer(srv, demo.NewServer())
	slog.Info("demo service listening", "addr", *addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
