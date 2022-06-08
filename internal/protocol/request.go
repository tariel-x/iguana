package protocol

type Request struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
	Body          []byte
}

//TODO: parse here request headers and write common scheme for produce request
