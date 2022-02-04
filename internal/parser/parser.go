package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

func readString(data []byte) string {
	return string(data)
}

func read(data []byte, s interface{}) error {
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
	Crc        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

type ProduceRequestParser struct {
}

func (p *ProduceRequestParser) Parse(data []byte) (*ProduceRequest, error) {
	offset := 0
	reqpart1 := struct {
		Size               int32
		ApiKey             int16
		ApiVersion         int16
		CorrelationId      int32
		ClientIDStringSize int16
	}{}
	if err := read(data[offset:14], &reqpart1); err != nil {
		return nil, err
	}
	offset = 14
	clientID := readString(data[offset : offset+int(reqpart1.ClientIDStringSize)])
	offset = offset + int(reqpart1.ClientIDStringSize)

	reqpart2 := struct {
		TransactionID int16
		RequiredAcks  int16
		Timeout       int32
		TopicsSize    int32
	}{}
	if err := read(data[offset:offset+12], &reqpart2); err != nil {
		return nil, err
	}
	offset = offset + 12

	read, topics, err := p.parseTopics(int(reqpart2.TopicsSize), data[offset:])
	if err != nil {
		return nil, err
	}
	offset = offset + read

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

func (p *ProduceRequestParser) parseTopics(count int, data []byte) (int, []ProduceRequestTopic, error) {
	offset := 0
	topics := make([]ProduceRequestTopic, 0, count)
	for i := 0; i < count; i++ {
		topicOffset, topic, err := p.parseTopic(data[offset:])
		if err != nil {
			return 0, nil, err
		}
		offset = offset + topicOffset
		topics = append(topics, topic)
	}
	return offset, topics, nil
}

func (p *ProduceRequestParser) parseTopic(data []byte) (int, ProduceRequestTopic, error) {
	var topicNameSize int16
	offset := 0
	if err := read(data[offset:offset+2], &topicNameSize); err != nil {
		return 0, ProduceRequestTopic{}, err
	}
	offset = offset + 2
	topicName := readString(data[offset : offset+int(topicNameSize)])
	return offset, ProduceRequestTopic{
		TopicName: topicName,
	}, nil
}
