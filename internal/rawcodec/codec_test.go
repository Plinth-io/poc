package rawcodec

import (
	"bytes"
	"testing"
)

func TestCodecNameIsProto(t *testing.T) {
	// The name decides the wire content-type. Anything but "proto" would make
	// the real endpoints refuse the tunnelled payload.
	if got := (Codec{}).Name(); got != "proto" {
		t.Fatalf("Name() = %q, want %q", got, "proto")
	}
}

func TestCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: []byte{}},
		{name: "single byte", in: []byte{0x01}},
		{name: "binary with nulls", in: []byte{0x00, 0xff, 0x00, 0x7f}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := Codec{}.Marshal(&tc.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var out []byte
			if err := (Codec{}).Unmarshal(raw, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !bytes.Equal(out, tc.in) {
				t.Fatalf("round trip = %v, want %v", out, tc.in)
			}
		})
	}
}

func TestCodecUnmarshalCopiesInput(t *testing.T) {
	// gRPC reuses its read buffers, so Unmarshal must not alias them.
	src := []byte{1, 2, 3}
	var out []byte
	if err := (Codec{}).Unmarshal(src, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	src[0] = 9
	if out[0] != 1 {
		t.Fatal("Unmarshal aliased the caller's buffer")
	}
}

func TestCodecRejectsForeignTypes(t *testing.T) {
	if _, err := (Codec{}).Marshal("not bytes"); err == nil {
		t.Fatal("Marshal(string) = nil error, want error")
	}
	if err := (Codec{}).Unmarshal(nil, new(string)); err == nil {
		t.Fatal("Unmarshal(*string) = nil error, want error")
	}
}
