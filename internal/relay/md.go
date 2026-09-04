// Package relay bridges gRPC and HTTP onto the envelope bus. It is the only
// place that knows both worlds; the bus itself knows neither.
package relay

import (
	"net/http"

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

// ForwardedPrefixHeader tells the agent under which path prefix it is being
// served, so its templates can emit links that work through the tunnel and
// on localhost alike.
const ForwardedPrefixHeader = "X-Forwarded-Prefix"

// hopByHop headers describe one TCP hop and must not be relayed.
var hopByHop = map[string]bool{
	"Connection":        true,
	"Keep-Alive":        true,
	"Proxy-Connection":  true,
	"Te":                true,
	"Trailer":           true,
	"Transfer-Encoding": true,
	"Upgrade":           true,
}

// HeadersFromHTTP converts HTTP headers for the wire, dropping the ones that
// describe only the hop they arrived on.
func HeadersFromHTTP(h http.Header) []*busv1.Header {
	out := make([]*busv1.Header, 0, len(h))
	for k, vs := range h {
		if hopByHop[http.CanonicalHeaderKey(k)] {
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

// HTTPFromHeaders is the inverse of HeadersFromHTTP.
func HTTPFromHeaders(hs []*busv1.Header) http.Header {
	out := http.Header{}
	for _, h := range hs {
		if hopByHop[http.CanonicalHeaderKey(h.GetKey())] {
			continue
		}
		for _, v := range h.GetValues() {
			out.Add(h.GetKey(), string(v))
		}
	}
	return out
}
