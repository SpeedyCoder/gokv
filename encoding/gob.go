package encoding

import (
	"bytes"
	"encoding/gob"
)

// gobCodec encodes/decodes Go values to/from gob.
type gobCodec string

// Marshal encodes a Go value to gob.
func (c gobCodec) Marshal(v interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Unmarshal decodes a gob value into a Go value.
func (c gobCodec) Unmarshal(data []byte, v interface{}) error {
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(v)
}
