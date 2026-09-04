package relay

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestMetadataRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		md   metadata.MD
	}{
		{name: "empty", md: metadata.MD{}},
		{name: "single value", md: metadata.Pairs("x-user", "chris")},
		{name: "repeated key", md: metadata.MD{"x-tag": []string{"a", "b"}}},
		{
			name: "binary value with nulls",
			md:   metadata.MD{"trace-bin": []string{string([]byte{0x00, 0xff, 0x00})}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MDFromHeaders(HeadersFromMD(tc.md))
			if len(got) != len(tc.md) {
				t.Fatalf("got %d keys, want %d", len(got), len(tc.md))
			}
			for k, want := range tc.md {
				gotVals := got.Get(k)
				if len(gotVals) != len(want) {
					t.Fatalf("key %q: got %d values, want %d", k, len(gotVals), len(want))
				}
				for i := range want {
					if gotVals[i] != want[i] {
						t.Fatalf("key %q value %d = %q, want %q", k, i, gotVals[i], want[i])
					}
				}
			}
		})
	}
}

func TestHeadersFromMDDropsRelayOnlyKeys(t *testing.T) {
	md := metadata.Pairs(
		AgentIDKey, "mac-1",
		"grpc-status-details-bin", "should not travel",
		"x-keep", "yes",
	)
	got := MDFromHeaders(HeadersFromMD(md))
	if len(got.Get(AgentIDKey)) != 0 {
		t.Fatal("routing key was forwarded to the agent")
	}
	if len(got.Get("grpc-status-details-bin")) != 0 {
		t.Fatal("status details trailer was forwarded and would be duplicated")
	}
	if got.Get("x-keep")[0] != "yes" {
		t.Fatal("regular metadata was dropped")
	}
}

func TestHTTPHeaderRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		h    http.Header
	}{
		{name: "empty", h: http.Header{}},
		{name: "single value", h: http.Header{"X-User": []string{"chris"}}},
		{name: "repeated key", h: http.Header{"X-Tag": []string{"a", "b"}}},
		{name: "value with a comma", h: http.Header{"Accept": []string{"text/html, */*"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HTTPFromHeaders(HeadersFromHTTP(tc.h))
			if len(got) != len(tc.h) {
				t.Fatalf("got %d keys, want %d", len(got), len(tc.h))
			}
			for k, want := range tc.h {
				gotVals := got.Values(k)
				if len(gotVals) != len(want) {
					t.Fatalf("key %q: got %d values, want %d", k, len(gotVals), len(want))
				}
				for i := range want {
					if gotVals[i] != want[i] {
						t.Fatalf("key %q value %d = %q, want %q", k, i, gotVals[i], want[i])
					}
				}
			}
		})
	}
}

func TestHTTPHeadersDropHopByHopKeys(t *testing.T) {
	// The keys are spelled in three casings on purpose: both directions
	// canonicalise before consulting the drop list.
	h := http.Header{
		"Connection":        []string{"keep-alive"},
		"transfer-encoding": []string{"chunked"},
		"UPGRADE":           []string{"websocket"},
		"X-Keep":            []string{"yes"},
	}
	for _, drop := range []string{"Connection", "Transfer-Encoding", "Upgrade"} {
		t.Run(drop, func(t *testing.T) {
			if got := HTTPFromHeaders(HeadersFromHTTP(h)).Get(drop); got != "" {
				t.Fatalf("%s survived the relay as %q", drop, got)
			}
		})
	}
	if got := HTTPFromHeaders(HeadersFromHTTP(h)).Get("X-Keep"); got != "yes" {
		t.Fatalf("X-Keep = %q, want %q", got, "yes")
	}
}
