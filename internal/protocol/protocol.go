package protocol

import "fmt"

type Decoder interface {
	Decode([]byte) (int, error)
}

type FieldString struct {
	Value string
}

func (s *FieldString) Decode(data []byte) (int, error) {
	var offset int
	var stringLength int16

	if err := decode(data[offset:offset+2], &stringLength); err != nil {
		return 0, err
	}
	offset = offset + 2

	if stringLength == 0 {
		return 0, nil
	}

	s.Value = string(data[offset : offset+int(stringLength)])

	offset = offset + int(stringLength)
	return offset, nil
}

type FieldInt8 struct {
	Value int8
}

func (s *FieldInt8) Decode(data []byte) (int, error) {
	if err := decode(data[:1], &s.Value); err != nil {
		return 0, err
	}
	return 1, nil
}

type FieldInt16 struct {
	Value int16
}

func (s *FieldInt16) Decode(data []byte) (int, error) {
	if err := decode(data[:2], &s.Value); err != nil {
		return 0, err
	}
	return 2, nil
}

type FieldInt32 struct {
	Value int32
}

func (s *FieldInt32) Decode(data []byte) (int, error) {
	if err := decode(data[:4], &s.Value); err != nil {
		return 0, err
	}
	return 4, nil
}

type FieldInt64 struct {
	Value int64
}

func (s *FieldInt64) Decode(data []byte) (int, error) {
	if err := decode(data[:8], &s.Value); err != nil {
		return 0, err
	}
	return 8, nil
}

type FieldPayload struct {
	Value []byte
}

func (s *FieldPayload) Decode(data []byte) (int, error) {
	s.Value = data
	return len(data), nil
}

type FieldArray struct {
	Value  []interface{}
	Fields Message
	//DecodeElement decodeElementFunc
	//DecodeSize    decodeSizeFunc
}

//TODO: fill logic in array decode
func (s *FieldArray) Decode(data []byte) (int, error) {
	return 0, nil
}

type Field struct {
	Name string
	Type Decoder
}

type Message []Field

func (m Message) Decode(data []byte) (int, error) {
	totalOffset := 0
	for i := range m {
		offset, err := m[i].Type.Decode(data[totalOffset:])
		if err != nil {
			return 0, fmt.Errorf("can not decode %s: %w", m[i].Name, err)
		}
		totalOffset += offset
	}
	return totalOffset, nil
}
