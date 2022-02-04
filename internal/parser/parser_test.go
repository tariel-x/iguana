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
		Topics:        nil,
	}
	assert.NotNil(t, req)
	assert.Equal(t, expected, *req)
}
