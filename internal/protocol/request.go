package protocol

type Request struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
	Body          []byte
}

//TODO: parse here request headers and write common scheme for produce request

func RequestMessage() Message {
	return Message{
		Field{Name: "ApiKey", Type: &FieldInt16{}},
		Field{Name: "ApiVersion", Type: &FieldInt16{}},
		Field{Name: "CorrelationId", Type: &FieldInt32{}},
		Field{Name: "ClientId", Type: &FieldString{}},
		Field{Name: "RequiredAcks", Type: &FieldInt16{}},
		Field{Name: "Timeout", Type: &FieldInt32{}},
		//Field{Name: "Payload", Type: &FieldPayload{}},
		Field{Name: "Topics", Type: &FieldArray{
			Fields: Message{
				Field{Name: "TopicName", Type: &FieldString{}},
			},
		}},
	}
}
