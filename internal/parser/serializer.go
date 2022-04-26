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
	Topic         string
	CorrelationID int32
	Partition     int32
	ErrorCode     int
	Offset        int64
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

	payloadBuffer := bytes.NewBuffer(nil)

	if err := binary.Write(payloadBuffer, binary.BigEndian, resp.CorrelationID); err != nil {
		return nil, fmt.Errorf("can not write correlation id: %w", err)
	}

	if err := binary.Write(payloadBuffer, binary.BigEndian, int16(len([]byte(resp.Topic)))); err != nil {
		return nil, fmt.Errorf("can not write topic length: %w", err)
	}

	if err := binary.Write(payloadBuffer, binary.BigEndian, []byte(resp.Topic)); err != nil {
		return nil, fmt.Errorf("can not write topic: %w", err)
	}

	if err := binary.Write(payloadBuffer, binary.BigEndian, ret); err != nil {
		return nil, fmt.Errorf("can not write int fields: %w", err)
	}

	// Write length
	l := int32(len(payloadBuffer.Bytes()))

	lBuffer := bytes.NewBuffer(nil)
	if err := binary.Write(lBuffer, binary.BigEndian, l); err != nil {
		return nil, fmt.Errorf("can not write length: %w", err)
	}
	lBytes := lBuffer.Bytes()

	// TODO: fix insufficient data to decode packet, more bytes expected
	return append(lBytes, payloadBuffer.Bytes()...), nil
}
