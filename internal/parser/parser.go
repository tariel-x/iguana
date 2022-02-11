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
	Partition   int32
	RecordBatch RecordBatch
}

type RecordBatch struct {
	FirstOffset          int64
	Length               int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	FirstSequence        int32
	Records              []Record
}

type Record struct {
	Length         int64 //varint
	Attributes     int8
	TimestampDelta int64 //varint
	OffsetDelta    int64 //varint
	Key            []byte
	Value          []byte
	Headers        []Header
}

type Header struct {
	HeaderKey   string
	HeaderValue []byte
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

	read, messages, err := p.parseRecordBatch(data[offset:])
	if err != nil {
		return 0, nil, err
	}
	offset += read

	return offset, ProduceRequestPartition{
		Partition:   partitionID,
		RecordBatch: messages,
	}, nil
}

func (p *ProduceRequestParser) parseRecordBatch(data []byte) (int, RecordBatch, error) {
	var offset int
	var messageSetSize int32
	if err := decode(data[offset:offset+4], &messageSetSize); err != nil {
		return 0, RecordBatch{}, err
	}
	offset += 4

	reqpart := struct {
		FirstOffset          int64
		Length               int32
		PartitionLeaderEpoch int32
		Magic                int8
		CRC                  int32
		Attributes           int16
		LastOffsetDelta      int32
		FirstTimestamp       int64
		MaxTimestamp         int64
		ProducerId           int64
		ProducerEpoch        int16
		FirstSequence        int32
	}{}

	if err := decode(data[offset:offset+57], &reqpart); err != nil {
		return 0, RecordBatch{}, err
	}
	offset += 57

	var records []Record
	read, err := decodeArray(data[offset:], &records, p.parseRecord)
	if err != nil {
		return 0, RecordBatch{}, err
	}
	offset += read

	return offset, RecordBatch{
		FirstOffset:          reqpart.FirstOffset,
		Length:               reqpart.Length,
		PartitionLeaderEpoch: reqpart.PartitionLeaderEpoch,
		Magic:                reqpart.Magic,
		CRC:                  reqpart.CRC,
		Attributes:           reqpart.Attributes,
		LastOffsetDelta:      reqpart.LastOffsetDelta,
		FirstTimestamp:       reqpart.FirstTimestamp,
		MaxTimestamp:         reqpart.MaxTimestamp,
		ProducerId:           reqpart.ProducerId,
		ProducerEpoch:        reqpart.ProducerEpoch,
		FirstSequence:        reqpart.FirstSequence,
		Records:              records,
	}, nil
}

func (p *ProduceRequestParser) parseRecord(data []byte) (int, interface{}, error) {
	var offset int

	length, read := binary.Varint(data)
	if read == 0 {
		return 0, nil, errors.New("not varint")
	}
	offset += read

	var attributes int8
	err := decode(data[offset:offset+1], &attributes)
	if err != nil {
		return 0, nil, fmt.Errorf("not int8: %w", err)
	}
	offset += 1

	timestampDelta, read := binary.Varint(data[offset:])
	if read == 0 {
		return 0, nil, errors.New("not varint")
	}
	offset += read

	offsetDelta, read := binary.Varint(data[offset:])
	if read == 0 {
		return 0, nil, errors.New("not varint")
	}
	offset += read

	read, key, err := decodeData(data[offset:])
	if err != nil {
		return 0, nil, err
	}
	offset += read

	read, value, err := decodeData(data[offset:])
	if err != nil {
		return 0, nil, err
	}
	offset += read

	var headers []Header
	read, err = decodeArray(data[offset:], &headers, p.parseHeader)
	if err != nil {
		return 0, nil, err
	}
	offset += read

	return offset, Record{
		Length:         length,
		Attributes:     attributes,
		TimestampDelta: timestampDelta,
		OffsetDelta:    offsetDelta,
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil
}

func (p *ProduceRequestParser) parseHeader(data []byte) (int, interface{}, error) {
	var offset int

	read, key, err := decodeData(data[offset:])
	if err != nil {
		return 0, nil, err
	}
	offset += read

	read, value, err := decodeData(data[offset:])
	if err != nil {
		return 0, nil, err
	}
	offset += read

	return offset, Header{
		HeaderKey:   string(key),
		HeaderValue: value,
	}, nil
}
