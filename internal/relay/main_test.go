package relay_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any goroutine outlives its test. Leaks are the
// most likely defect class here, so they get their own gate.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// grpc-go tears these down asynchronously with ClientConn.Close: a
		// ClientConn created and closed within one test can still have one of
		// these winding down at the TestMain boundary.
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		// http.DefaultTransport keeps idle connections alive by design, and
		// the agent's tunnelled HTTP client uses it. These two live as long
		// as the pool does, so they are pool bookkeeping rather than leaked
		// work — at the cost that a genuinely leaked connection of the same
		// shape would hide here too.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
