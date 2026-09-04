// Package relay bridges gRPC and HTTP onto the envelope bus. It is the only
// place that knows both worlds; the bus itself knows neither.
package relay

import (
	"google.golang.org/grpc/metadata"

	busv1 "plinth.io/poc/gen/bus/v1"
)

// AgentIDKey selects the agent a call is routed to.
const AgentIDKey = "x-agent-id"

// notForwarded lists keys the relay consumes itself. grpc-go writes its own
// status details from the returned status, so forwarding the trailer copy
// would duplicate them.
var notForwarded = map[string]bool{
	AgentIDKey:                true,
	"grpc-status-details-bin": true,
}

// HeadersFromMD converts metadata for the wire. Values travel as bytes because
// "-bin" keys hold arbitrary binary data.
func HeadersFromMD(md metadata.MD) []*busv1.Header {
	out := make([]*busv1.Header, 0, len(md))
	for k, vs := range md {
		if notForwarded[k] {
			continue
		}
		values := make([][]byte, 0, len(vs))
		for _, v := range vs {
			values = append(values, []byte(v))
		}
		out = append(out, &busv1.Header{Key: k, Values: values})
	}
	return out
}

func MDFromHeaders(hs []*busv1.Header) metadata.MD {
	md := metadata.MD{}
	for _, h := range hs {
		if notForwarded[h.GetKey()] {
			continue
		}
		for _, v := range h.GetValues() {
			md.Append(h.GetKey(), string(v))
		}
	}
	return md
}
