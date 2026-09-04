package relay

import (
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
