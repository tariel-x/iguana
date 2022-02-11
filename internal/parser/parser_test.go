package parser

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRequest(t *testing.T) {
	f, err := os.Open("request_test_data.bin")
	if err != nil {
		t.Errorf("can not open file: %s", err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("can not read file: %s", err)
	}

	parser := &ProduceRequestParser{}
	req, err := parser.Parse(data)
	if err != nil {
		t.Errorf("can not parse data: %s", err)
	}

	if req == nil {
		t.Errorf("response is nil")
		return
	}

	if req.ApiKey != CodeProduceRequest {
		t.Errorf("not produce request")
		return
	}

	expected := ProduceRequest{
		ApiKey:        CodeProduceRequest,
		ApiVersion:    6,
		CorrelationId: 2,
		ClientId:      "rdkafka",
		RequiredAcks:  -1,
		Timeout:       5000,
		Topics: []ProduceRequestTopic{
			{
				TopicName: "test-data",
				Partitions: []ProduceRequestPartition{
					{
						Partition: 0,
						RecordBatch: RecordBatch{
							FirstOffset:          0,
							Length:               57,
							PartitionLeaderEpoch: 0,
							Magic:                2,
							CRC:                  243385418,
							Attributes:           0,
							LastOffsetDelta:      0,
							FirstTimestamp:       1643903619549,
							MaxTimestamp:         1643903619549,
							ProducerId:           -1,
							ProducerEpoch:        -1,
							FirstSequence:        -1,
							Records: []Record{
								{
									Length:         7,
									Attributes:     0,
									TimestampDelta: 0,
									OffsetDelta:    0,
									Key:            nil,
									Value:          []byte("1"),
									Headers:        nil,
								},
							},
						},
					},
				},
			},
		},
	}
	assert.NotNil(t, req)
	assert.Equal(t, expected, *req)
}
