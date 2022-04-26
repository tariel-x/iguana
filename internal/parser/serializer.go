package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	ErrorInvalidRecord = 87
)

type ProduceResponse struct {
	Topic     string
	Partition int32
	ErrorCode int
	Offset    int64
}

type ProduceResponseSerializer struct {
}

func (s ProduceResponseSerializer) Serialize(resp ProduceResponse) ([]byte, error) {
	ret := struct {
		Partition int32
		ErrorCode int16
		Offset    int64
	}{
		Partition: resp.Partition,
		ErrorCode: int16(resp.ErrorCode),
		Offset:    resp.Offset,
	}
	buffer := bytes.NewBufferString(resp.Topic)
	if err := binary.Write(buffer, binary.BigEndian, ret); err != nil {
		return nil, fmt.Errorf("can not write int fields: %w", err)
	}

	return buffer.Bytes(), nil
}
