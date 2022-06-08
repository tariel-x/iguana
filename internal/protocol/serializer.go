package protocol

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

//TODO support other versions except V3
func (s ProduceResponseSerializer) Serialize(resp ProduceResponse) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var err error
	if err = write(buf, resp.CorrelationID); err != nil {
		return nil, fmt.Errorf("can not write correlation id: %w", err)
	}

	if err := writeArray(buf, func(buf *bytes.Buffer) error {

		var err error
		if err = writeString(buf, resp.Topic); err != nil {
			return fmt.Errorf("can not write topic: %w", err)
		}

		if err = writeArray(buf, func(buffer *bytes.Buffer) error {
			var err error
			if err = write(buf, resp.Partition); err != nil {
				return fmt.Errorf("can not write partition: %w", err)
			}

			if err = write(buf, int16(resp.ErrorCode)); err != nil {
				return fmt.Errorf("can not write error code: %w", err)
			}

			if err = write(buf, resp.Offset); err != nil {
				return fmt.Errorf("can not write offset: %w", err)
			}

			if err = write(buf, int64(-1)); err != nil {
				return fmt.Errorf("can not write log_append_time_ms: %w", err)
			}
			return nil

		}, 1); err != nil {
			return fmt.Errorf("can not write partitions: %w", err)
		}

		return nil
	}, 1); err != nil {
		return nil, fmt.Errorf("can not write topics: %w", err)
	}

	if err = write(buf, int32(0)); err != nil {
		return nil, fmt.Errorf("can not write throttle_time_ms: %w", err)
	}

	return pack(buf.Bytes())
}

func writeString(buf *bytes.Buffer, in string) error {
	var err error
	strData := []byte(in)
	if err = write(buf, int16(len(strData))); err != nil {
		return fmt.Errorf("can not write string length: %w", err)
	}

	if err = write(buf, strData); err != nil {
		return fmt.Errorf("can not write string: %w", err)
	}

	return nil
}

func write(buf *bytes.Buffer, in interface{}) error {
	return binary.Write(buf, binary.BigEndian, in)
}

func pack(data []byte) ([]byte, error) {
	lenBuf := bytes.NewBuffer(nil)
	if err := write(lenBuf, int32(len(data))); err != nil {
		return nil, fmt.Errorf("can not write length: %w", err)
	}
	return append(lenBuf.Bytes(), data...), nil
}

//TODO: rewrite to keep slices with size > 1
func writeArray(buf *bytes.Buffer, callback func(*bytes.Buffer) error, l int) error {
	if err := write(buf, int32(l)); err != nil {
		return fmt.Errorf("can not write arrays length: %w", err)
	}
	for i := 0; i < l; i++ {
		if err := callback(buf); err != nil {
			return err
		}
	}
	return nil
}
