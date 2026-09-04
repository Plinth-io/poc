package agent

import (
	"errors"
	"testing"
)

func TestGRPCConnStaysClosedAfterClose(t *testing.T) {
	a := New(Config{GRPCTarget: "127.0.0.1:1"})
	if _, err := a.grpcConn(); err != nil {
		t.Fatalf("grpcConn: %v", err)
	}
	a.Close()

	if _, err := a.grpcConn(); !errors.Is(err, errClosed) {
		t.Fatalf("err = %v, want %v", err, errClosed)
	}
}
