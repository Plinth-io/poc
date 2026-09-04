// Package rawcodec provides a gRPC codec that moves payloads through a relay
// without touching them.
package rawcodec

import (
	"fmt"

	"google.golang.org/grpc/encoding"
)

// Codec passes gRPC payloads through unchanged. Name reports "proto" on
// purpose: the content-type stays application/grpc+proto, so the endpoints on
// both ends keep serializing normally and the relay never needs their schema.
type Codec struct{}

var _ encoding.Codec = Codec{}

func (Codec) Name() string { return "proto" }

func (Codec) Marshal(v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case *[]byte:
		return *b, nil
	default:
		return nil, fmt.Errorf("rawcodec: cannot marshal %T", v)
	}
}

func (Codec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawcodec: cannot unmarshal into %T", v)
	}
	*p = append((*p)[:0], data...)
	return nil
}
