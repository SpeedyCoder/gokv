package encoding

import (
	"fmt"
)

// Codec encodes/decodes Go values to/from slices of bytes.
type Codec interface {
	// Marshal encodes a Go value to a slice of bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a Go value.
	Unmarshal(data []byte, v interface{}) error
}

// All available codec types
const (
	// JSON is a codec that encodes/decodes Go values to/from JSON.
	JSON = jsonCodec("JSONCodec")
	// Gob is a codec that encodes/decodes Go values to/from gob.
	Gob = gobCodec("GOBCodec")
	// Proto is a codec that encodes/decodes Go values that implement
	// the proto.Message interface to/from.
	Proto = protoCodec("ProtoCodec")
)

// FromString returns encoding corresponding to provided lowercase string.
func FromString(s string) (Codec, error) {
	switch s {
	case "json":
		return JSON, nil
	case "gob":
		return Gob, nil
	case "proto", "protobuf":
		return Proto, nil
	default:
		return nil, fmt.Errorf("unknown encoding type: %s", s)
	}
}
