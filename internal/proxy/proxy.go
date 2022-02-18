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

type Proxy struct {
	address string
	broker  string
}

const defaultAddress = ":9092"

var ErrNoBroker = errors.New("no broker is set")

func NewProxy(address string, broker string) (*Proxy, error) {
	p := Proxy{address: defaultAddress}
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
			// Открываем порт
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
	brokerConn, err := net.Dial("tcp", p.broker)
	if err != nil {
		log.Println(err)
		return
	}

	proxymessages := make(chan []byte, 1000)
	toparse := make(chan []byte, 1000)
	returnmessages := make(chan []byte, 1000)

	// client to broker
	go p.listen(ctx, conn, proxymessages, toparse)
	go p.parse(ctx, toparse)
	go p.redirect(ctx, brokerConn, proxymessages)

	// broker to client
	go p.listen(ctx, brokerConn, returnmessages, nil)
	go p.redirect(ctx, conn, returnmessages)
}

func (p *Proxy) listen(ctx context.Context, conn net.Conn, messages, toparse chan []byte) error {
	for {
		proxymessage, err := readMessage(conn)
		if err != nil {
			return err
		}

		_, cmd, err := parser.GetRequest(proxymessage)
		if err != nil {
			return err
		}
		if toparse != nil {
			log.Println("From client:", len(proxymessage), "bytes, cmd: ", cmd)
			toparse <- proxymessage
		} else {
			log.Println("To client:", len(proxymessage), "bytes, cmd: ", cmd)
		}

		messages <- proxymessage
	}
}

func (p *Proxy) redirect(ctx context.Context, conn net.Conn, messages chan []byte) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			if _, err := conn.Write(msg); err != nil {
				return err
			}
		}
	}
}

func (p *Proxy) parse(ctx context.Context, messages chan []byte) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			cmdID, _, err := parser.GetRequest(msg)
			if err != nil {
				return err
			}
			if cmdID != parser.CodeProduceRequest {
				continue
			}
			if err := p.parseProduce(msg); err != nil {
				return err
			}
		}
	}
}

func (p *Proxy) parseProduce(data []byte) error {
	req, err := (&parser.ProduceRequestParser{}).Parse(data)
	if err != nil {
		return err
	}
	for _, topic := range req.Topics {
		for _, partition := range topic.Partitions {
			for _, record := range partition.RecordBatch.Records {
				log.Printf("PRODUCE %s PARTITION %d HEADERS %s KEY %s VALUE %s\n", topic.TopicName, partition.Partition, record.Headers, record.Key, record.Value)
			}
		}
	}
	return nil
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
