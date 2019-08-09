package encoding

import (
	"encoding/json"
)

// jsonCodec encodes/decodes Go values to/from JSON.
type jsonCodec struct{}

// Marshal encodes a Go value to JSON.
func (c jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes a JSON value into a Go value.
func (c jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
