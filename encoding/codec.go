package encoding

// Codec encodes/decodes Go values to/from slices of bytes.
type Codec interface {
	// Marshal encodes a Go value to a slice of bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a Go value.
	Unmarshal(data []byte, v interface{}) error
}

// Convenience variables
var (
	// JSON is a codec that encodes/decodes Go values to/from JSON.
	JSON Codec = jsonCodec{}
	// Gob is a codec that encodes/decodes Go values to/from gob.
	Gob Codec = gobCodec{}
	// Proto is a codec that encodes/decodes Go values that implement
	// the proto.Message interface to/from.
	Proto Codec = protoCodec{}
)
