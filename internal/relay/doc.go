// Package relay is the boundary between the two halves of the tunnel. It holds
// no code of its own; the halves live in its subpackages.
//
//	wire        what both halves need: the routing key, the forwarded-prefix
//	            header, and the conversions between Go's metadata/header types
//	            and the envelope's Header messages
//	hubrelay    the hub's side — accepts callers and puts their calls on the bus
//	agentrelay  the agent's side — takes calls off the bus and replays them
//	            against the local gRPC and HTTP targets
//
// The split exists so neither half can reach into the other by accident. Both
// binaries already link only what they import, so this costs nothing at
// runtime; what it buys is that an agent-side change cannot quietly start
// depending on hub-side code. boundary_test.go enforces that mechanically.
//
// The tests in this directory are the end-to-end ones: they drive both halves
// together over a real WebSocket and real gRPC, which is the only place the
// two are supposed to meet.
package relay
