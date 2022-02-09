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

func decodeArray(data []byte, target interface{}, decodeElement decodeElementFunc) (int, error) {
	valuePtr := reflect.ValueOf(target)
	if reflect.TypeOf(target).Kind() != reflect.Ptr || valuePtr.IsNil() {
		return 0, errors.New("target must be a non-nil pointer")
	}
	if reflect.Indirect(valuePtr).Kind() != reflect.Slice {
		return 0, errors.New("target must be a pointer to slice")
	}

	offset := 0
	var arraySize int32
	if err := decode(data[offset:offset+4], &arraySize); err != nil {
		return 0, err
	}
	offset = offset + 4

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
	var topicNameSize int16

	if err := decode(data[offset:offset+2], &topicNameSize); err != nil {
		return 0, "", err
	}
	offset = offset + 2

	ret := string(data[offset : offset+int(topicNameSize)])

	offset = offset + int(topicNameSize)
	return offset, ret, nil
}

func decode(data []byte, s interface{}) error {
	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.BigEndian, s); err != nil {
		return fmt.Errorf("can not decode data into the provided variable: %w", err)
	}
	return nil
}

type ProduceRequest struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
	RequiredAcks  int16
	Timeout       int32
	Topics        []ProduceRequestTopic
}

type ProduceRequestTopic struct {
	TopicName  string
	Partitions []ProduceRequestPartition
}

type ProduceRequestPartition struct {
	Partition  int32
	MessageSet []Message
}

type Message struct {
	Offset         int64
	MessageSize    int32
	LeaderEpoch    int32
	MagicByte      int8
	Crc            int32
	Flags          int16
	LastOffsetData int32
	FirstTimestamp int64
	LastTimestamp  int64
	ProducerID     int64
	ProducerEpoch  int16
	BaseSequence   int32
	Size           int32
}

type ProduceRequestParser struct {
}

func (p *ProduceRequestParser) Parse(data []byte) (*ProduceRequest, error) {
	var offset int
	reqpart1 := struct {
		Size          int32
		ApiKey        int16
		ApiVersion    int16
		CorrelationId int32
	}{}
	if err := decode(data[offset:12], &reqpart1); err != nil {
		return nil, err
	}
	offset += 12
	read, clientID, err := decodeString(data[offset:])
	offset = offset + read

	reqpart2 := struct {
		TransactionID int16
		RequiredAcks  int16
		Timeout       int32
	}{}
	if err := decode(data[offset:offset+8], &reqpart2); err != nil {
		return nil, err
	}
	offset += 8

	var topics []ProduceRequestTopic
	read, err = decodeArray(data[offset:], &topics, p.parseTopic)
	if err != nil {
		return nil, err
	}
	offset += read

	return &ProduceRequest{
		ApiKey:        reqpart1.ApiKey,
		ApiVersion:    reqpart1.ApiVersion,
		CorrelationId: reqpart1.CorrelationId,
		ClientId:      clientID,
		RequiredAcks:  reqpart2.RequiredAcks,
		Timeout:       reqpart2.Timeout,
		Topics:        topics,
	}, nil
}

func (p *ProduceRequestParser) parseTopic(data []byte) (int, interface{}, error) {
	var offset int
	read, topicName, err := decodeString(data[offset:])
	if err != nil {
		return 0, ProduceRequestTopic{}, err
	}
	offset += read

	var partitions []ProduceRequestPartition
	read, err = decodeArray(data[offset:], &partitions, p.parsePartition)
	if err != nil {
		return 0, ProduceRequestTopic{}, err
	}
	offset += read

	return offset, ProduceRequestTopic{
		TopicName:  topicName,
		Partitions: partitions,
	}, nil
}

func (p *ProduceRequestParser) parsePartition(data []byte) (int, interface{}, error) {
	var offset int
	var partitionID int32
	if err := decode(data[offset:offset+4], &partitionID); err != nil {
		return 0, nil, err
	}
	offset = offset + 4

	var messages []Message
	read, err := decodeArray(data[offset:], &messages, p.parseMessage)
	if err != nil {
		return 0, nil, err
	}
	offset += read

	return offset, ProduceRequestPartition{
		Partition:  partitionID,
		MessageSet: nil,
	}, nil
}

func (p *ProduceRequestParser) parseMessage(data []byte) (int, interface{}, error) {
	var offset int
	msg := Message{}
	if err := decode(data[offset:offset+61], &msg); err != nil {
		return 0, nil, err
	}
	offset += 61

	return offset, msg, nil
}
