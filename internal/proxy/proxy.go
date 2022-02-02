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
	"strconv"
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
	returnmessages := make(chan []byte, 1000)

	// proxy to broker
	go p.listen(ctx, conn, proxymessages, true)
	go p.redirect(ctx, brokerConn, proxymessages)

	// proxy to client
	go p.listen(ctx, brokerConn, returnmessages, false)
	go p.redirect(ctx, conn, returnmessages)
}

func (p *Proxy) listen(ctx context.Context, conn net.Conn, messages chan []byte, isclient bool) error {
	for {
		proxymessage, err := readMessage(conn)
		if err != nil {
			return err
		}

		cmd, err := apiKey(proxymessage[4:6])
		if err != nil {
			return err
		}
		if isclient {
			log.Println("From client:", len(proxymessage), "bytes, cmd: ", cmd)
			log.Println("From client:", proxymessage)
		} else {
			log.Println("To client:", len(proxymessage), "bytes, cmd: ", cmd)
			log.Println("To client:", proxymessage)
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

func apiKey(b []byte) (string, error) {
	keys := map[int16]string{
		0:  "ProduceRequest",
		1:  "FetchRequest",
		2:  "OffsetRequest",
		3:  "MetadataRequest",
		8:  "OffsetCommitRequest",
		9:  "OffsetFetchRequest",
		10: "GroupCoordinatorRequest",
		11: "JoinGroupRequest",
		12: "HeartbeatRequest",
		13: "LeaveGroupRequest",
		14: "SyncGroupRequest",
		15: "DescribeGroupsRequest",
		16: "ListGroupsRequest",
		18: "ApiVersions",
	}
	var intKey int16
	buf := bytes.NewReader(b)
	if err := binary.Read(buf, binary.BigEndian, &intKey); err != nil {
		return "", err
	}
	if stringKey, ok := keys[intKey]; ok {
		return stringKey, nil
	}
	return strconv.Itoa(int(intKey)), nil
}
