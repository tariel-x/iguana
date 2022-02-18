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
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("ERROR CLOSING", err)
		}
	}()

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

	// client to broker
	go p.listen(pipectx, conn, toparse, errch)
	go p.parse(pipectx, toparse, proxymessages, errch)
	go p.redirect(pipectx, proxymessages, brokerConn, errch)

	// broker to client
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

func (p *Proxy) parse(ctx context.Context, toparse, messages chan []byte, errch chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-toparse:
			cmdID, _, err := parser.GetRequest(msg)
			if err != nil {
				errch <- err
				return
			}
			if cmdID != parser.CodeProduceRequest {
				messages <- msg
				continue
			}
			if err := p.parseProduce(msg); err != nil {
				errch <- err
				return
			}
			messages <- msg
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
