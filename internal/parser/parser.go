package parser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

const (
	CodeProduceRequest          = 0
	CodeFetchRequest            = 1
	CodeOffsetRequest           = 2
	CodeMetadataRequest         = 3
	CodeOffsetCommitRequest     = 8
	CodeOffsetFetchRequest      = 9
	CodeGroupCoordinatorRequest = 10
	CodeJoinGroupRequest        = 11
	CodeHeartbeatRequest        = 12
	CodeLeaveGroupRequest       = 13
	CodeSyncGroupRequest        = 14
	CodeDescribeGroupsRequest   = 15
	CodeListGroupsRequest       = 16
	CodeApiVersions             = 18
)

var RequestNames = map[int]string{
	CodeProduceRequest:          "ProduceRequest",
	CodeFetchRequest:            "FetchRequest",
	CodeOffsetRequest:           "OffsetRequest",
	CodeMetadataRequest:         "MetadataRequest",
	CodeOffsetCommitRequest:     "OffsetCommitRequest",
	CodeOffsetFetchRequest:      "OffsetFetchRequest",
	CodeGroupCoordinatorRequest: "GroupCoordinatorRequest",
	CodeJoinGroupRequest:        "JoinGroupRequest",
	CodeHeartbeatRequest:        "HeartbeatRequest",
	CodeLeaveGroupRequest:       "LeaveGroupRequest",
	CodeSyncGroupRequest:        "SyncGroupRequest",
	CodeDescribeGroupsRequest:   "DescribeGroupsRequest",
	CodeListGroupsRequest:       "ListGroupsRequest",
	CodeApiVersions:             "ApiVersions",
}

func GetRequest(b []byte) (int, string, error) {
	var intKey int16
	buf := bytes.NewReader(b)
	if err := binary.Read(buf, binary.BigEndian, &intKey); err != nil {
		return 0, "", err
	}
	if stringKey, ok := RequestNames[int(intKey)]; ok {
		return int(intKey), stringKey, nil
	}
	return int(intKey), strconv.Itoa(int(intKey)), nil
}

type decodeElementFunc func(data []byte) (int, interface{}, error)

type decodeSizeFunc func(data []byte) (int, int, error)

func decodeVarint(data []byte) (int, int, error) {
	value, read := binary.Varint(data)
	if read == 0 {
		return 0, 0, errors.New("not varint")
	}
	return int(value), read, nil
}

func decodeInt32(data []byte) (int, int, error) {
	var value int32
	if err := decode(data[:4], &value); err != nil {
		return 0, 0, err
	}
	return int(value), 4, nil
}

func decodeArray(data []byte, target interface{}, decodeElement decodeElementFunc, decodeSize decodeSizeFunc) (int, error) {
	valuePtr := reflect.ValueOf(target)
	if reflect.TypeOf(target).Kind() != reflect.Ptr || valuePtr.IsNil() {
		return 0, errors.New("target must be a non-nil pointer")
	}
	if reflect.Indirect(valuePtr).Kind() != reflect.Slice {
		return 0, errors.New("target must be a pointer to slice")
	}

	offset := 0
	if decodeSize == nil {
		decodeSize = decodeInt32
	}
	arraySize, read, err := decodeSize(data[offset:])
	if err != nil {
		return 0, fmt.Errorf("can not decode array size: %w", err)
	}
	offset += read

	value := valuePtr.Elem()

	for i := 0; i < int(arraySize); i++ {

		elementOffset, element, err := decodeElement(data[offset:])
		if err != nil {
			return 0, err
		}
		offset = offset + elementOffset
		value.Set(reflect.Append(value, reflect.ValueOf(element)))
	}

	return offset, nil
}

func decodeString(data []byte) (int, string, error) {
	var offset int
	var stringLength int16

	if err := decode(data[offset:offset+2], &stringLength); err != nil {
		return 0, "", err
	}
	offset = offset + 2

	if stringLength == 0 {
		return 0, "", nil
	}

	ret := string(data[offset : offset+int(stringLength)])

	offset = offset + int(stringLength)
	return offset, ret, nil
}

func decodeData(data []byte) (int, []byte, error) {
	var offset int

	length, read := binary.Varint(data[offset:])
	if read == 0 {
		return 0, nil, errors.New("not varint")
	}
	offset += read

	if length <= 0 {
		return offset, nil, nil
	}

	return offset + int(length), data[offset : offset+int(length)], nil
}

func decode(data []byte, s interface{}) error {
	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.BigEndian, s); err != nil {
		return fmt.Errorf("can not decode data into the provided variable: %w", err)
	}
	return nil
}
