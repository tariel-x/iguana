package parser

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

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
	Headers        HeaderSet
}

type HeaderSet []Header

func (s HeaderSet) String() string {
	parts := make([]string, 0, len(s))
	for _, header := range s {
		parts = append(parts, fmt.Sprintf("%s:%s", header.HeaderKey, header.HeaderValue))
	}
	return strings.Join(parts, ";")
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
	read, err = decodeArray(data[offset:], &topics, p.parseTopic, nil)
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
	read, err = decodeArray(data[offset:], &partitions, p.parsePartition, nil)
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
	read, err := decodeArray(data[offset:], &records, p.parseRecord, nil)
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
	read, err = decodeArray(data[offset:], &headers, p.parseHeader, decodeVarint)
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
