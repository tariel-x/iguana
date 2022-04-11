package parser

const (
	ErrorInvalidRecord = 87
)

type ProduceResponse struct {
	Topic     string
	Partition int32
	ErrorCode int
	Offset    int64
}

type ProduceResponseSerializer struct {
}

func (s ProduceResponseSerializer) Serialize(resp ProduceResponse) ([]byte, error) {
	//TODO: сделать сериализацию
	return nil, nil
}
