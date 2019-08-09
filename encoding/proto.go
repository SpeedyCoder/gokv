package encoding

import (
	"errors"

	"github.com/golang/protobuf/proto"
)

var errNotAProtoMessage = errors.New("not a protobuf message")

// protoCodec encodes/decodes protobuf messages.
type protoCodec struct{}

// Marshal encodes a protobuf message to a slice of bytes.
func (protoCodec) Marshal(v interface{}) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, errNotAProtoMessage
	}

	return proto.Marshal(m)
}

// Unmarshal decodes a slice of bytes into a protobuf message.
func (protoCodec) Unmarshal(data []byte, v interface{}) error {
	m, ok := v.(proto.Message)
	if !ok {
		return errNotAProtoMessage
	}

	return proto.Unmarshal(data, m)
}
