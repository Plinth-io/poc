package hub_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any goroutine outlives its test. Leaks are the
// most likely defect class here, so they get their own gate.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// grpc-go keeps internal workers alive for the process lifetime.
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
