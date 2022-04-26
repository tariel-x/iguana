package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/tariel-x/iguana/internal/parser"
)

type Validator interface {
	Validate(context.Context, []byte, string, int) (bool, error)
}

type Proxy struct {
	address   string
	broker    string
	validator Validator
}

const defaultAddress = ":9092"

var ErrNoBroker = errors.New("no broker is set")

func NewProxy(address string, broker string, validator Validator) (*Proxy, error) {
	p := Proxy{
		address:   defaultAddress,
		validator: validator,
	}
	if address != "" {
		p.address = address
	}

	if broker == "" {
		return nil, ErrNoBroker
	}
	p.broker = broker
	return &p, nil
}

func (p *Proxy) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.address)
	if err != nil {
		return err
	}

	conns := make(chan net.Conn, 10000)
	errs := make(chan error, 10000)

	go func() {
		for {
			// Listen for connections
			conn, err := ln.Accept()
			if err != nil {
				errs <- fmt.Errorf("can not listen: %w", err)
				return
			}
			conns <- conn
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errs:
			return err
		case conn := <-conns:
			log.Println("New connection from", conn.RemoteAddr().String())
			go p.handle(ctx, conn)
		}
	}
}

func (p *Proxy) handle(ctx context.Context, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("ERROR CLOSING", err)
		}
	}()

	// Connect to the broker
	brokerConn, err := net.Dial("tcp", p.broker)
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err := brokerConn.Close(); err != nil {
			log.Println("ERROR CLOSING", err)
		}
	}()

	proxymessages := make(chan []byte, 1000)
	toparse := make(chan []byte, 1000)
	returnmessages := make(chan []byte, 1000)
	errch := make(chan error, 10000)

	pipectx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Client to broker pipeline: read, parse, redirect
	go p.listen(pipectx, conn, toparse, errch)
	go p.parse(pipectx, toparse, proxymessages, returnmessages, errch)
	go p.redirect(pipectx, proxymessages, brokerConn, errch)

	// Broker to client pipeline: read, redirect
	go p.listen(pipectx, brokerConn, returnmessages, errch)
	go p.redirect(pipectx, returnmessages, conn, errch)

	select {
	case <-ctx.Done():
		return
	case err := <-errch:
		log.Println("ERROR", err)
		return
	}
}

func (p *Proxy) listen(ctx context.Context, conn net.Conn, messages chan []byte, errch chan error) {
	for {
		proxymessage, err := readMessage(conn)
		if err != nil {
			errch <- err
			return
		}

		_, cmd, err := parser.GetRequest(proxymessage)
		if err != nil {
			errch <- err
			return
		}

		log.Println("from", conn.RemoteAddr(), len(proxymessage), "bytes, cmd: ", cmd)

		messages <- proxymessage
	}
}

func (p *Proxy) redirect(ctx context.Context, messages chan []byte, conn net.Conn, errch chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			if _, err := conn.Write(msg); err != nil {
				errch <- err
				return
			}
		}
	}
}

func (p *Proxy) parse(ctx context.Context, toparse, messages, returnmessages chan []byte, errch chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-toparse:
			// Get request code
			cmdID, _, err := parser.GetRequest(msg)
			if err != nil {
				errch <- err
				return
			}
			// If not produce -- redirect
			if cmdID != parser.CodeProduceRequest {
				messages <- msg
				continue
			}
			// Parse produce request
			req, err := p.parseProduce(msg)
			if err != nil {
				errch <- err
				return
			}
			validationErr, err := p.validate(ctx, req)
			if err != nil {
				errch <- err
				return
			}
			if validationErr != nil {
				refuseMsg, err := p.getRefuseMessage(*validationErr)
				if err != nil {
					errch <- err
					return
				}
				returnmessages <- refuseMsg
				return
			}
			messages <- msg
		}
	}
}

var ErrRecordNotValid = errors.New("record not valid")

type validationError struct {
	topic     string
	partition int32
	offset    int64
	err       error
}

func (p *Proxy) validate(ctx context.Context, req *parser.ProduceRequest) (*validationError, error) {
	for _, topic := range req.Topics {
		for _, partition := range topic.Partitions {
			for _, record := range partition.RecordBatch.Records {
				log.Printf("PRODUCE %s PARTITION %d HEADERS %s KEY %s VALUE %s\n", topic.TopicName, partition.Partition, record.Headers, record.Key, record.Value)
				valid, err := p.validatePayload(ctx, topic.TopicName, record.Value)
				if err != nil {
					return nil, err
				}
				if !valid {
					return &validationError{
						topic:     topic.TopicName,
						partition: partition.Partition,
						offset:    partition.RecordBatch.FirstOffset,
						err:       ErrRecordNotValid,
					}, nil
				}
			}
		}
	}

	return nil, nil
}

func (p *Proxy) validatePayload(ctx context.Context, topic string, message []byte) (bool, error) {
	rawSchemaID := binary.BigEndian.Uint32(message[0:4])
	schemaID := 0
	if rawSchemaID > 0 {
		schemaID = int(rawSchemaID)
	}
	return p.validator.Validate(ctx, message[4:], topic, schemaID)
}

func (p *Proxy) parseProduce(data []byte) (*parser.ProduceRequest, error) {
	req, err := (&parser.ProduceRequestParser{}).Parse(data)
	return req, err
}

func (p *Proxy) getRefuseMessage(validationErr validationError) ([]byte, error) {
	s := parser.ProduceResponseSerializer{}
	resp := parser.ProduceResponse{
		Topic:     validationErr.topic,
		Partition: validationErr.partition,
		ErrorCode: parser.ErrorInvalidRecord,
		Offset:    validationErr.offset,
	}
	return s.Serialize(resp)
}

func readMessage(conn io.Reader) ([]byte, error) {
	sizeBytes := make([]byte, 4)
	_, err := io.ReadAtLeast(conn, sizeBytes, 4)
	if err != nil {
		return nil, err
	}

	var size int32
	buf := bytes.NewReader(sizeBytes)
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return nil, err
	}

	proxymessage := make([]byte, int(size))
	_, err = io.ReadAtLeast(conn, proxymessage, int(size))
	return append(sizeBytes, proxymessage...), nil
}
