package raft

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"net"
)

type Transport interface {
	Dial(address string) (conn net.Conn, err error)
	Listen(address string) (ln net.Listener, err error)
}

func TcpTransport() Transport {
	return &tcpTransport{}
}

func TcpTransportWithTLS(serverTLS *tls.Config, clientTLS *tls.Config) Transport {
	return &tcpTransport{
		clientTLS: clientTLS,
		serverTLS: serverTLS,
	}
}

type tcpTransport struct {
	clientTLS *tls.Config
	serverTLS *tls.Config
}

func (tr *tcpTransport) Dial(address string) (conn net.Conn, err error) {
	if tr.clientTLS == nil {
		conn, err = net.Dial("tcp", address)
	} else {
		conn, err = tls.Dial("tcp", address, tr.clientTLS)
	}
	return
}

func (tr *tcpTransport) Listen(address string) (ln net.Listener, err error) {
	if tr.serverTLS == nil {
		ln, err = net.Listen("tcp", address)
	} else {
		ln, err = tls.Listen("tcp", address, tr.serverTLS)
	}
	return
}

const (
	shortMessage = iota + 1
	trunkedMessage
)

type messageKind uint16

type MessageRequestType uint16

func NewMessageWriter(requestType MessageRequestType, data []byte) (msg MessageWriter) {
	if data == nil {
		data = []byte{}
	}
	msg = &Message{
		requestType: requestType,
		kind:        shortMessage,
		data:        data,
		sink:        nil,
	}
	return
}

func NewMessageWriterWithTrunk(requestType MessageRequestType, data []byte, sink io.Reader) (msg MessageWriter) {
	if data == nil {
		data = []byte{}
	}
	msg = &Message{
		requestType: requestType,
		kind:        trunkedMessage,
		data:        data,
		sink:        sink,
	}
	return
}

type MessageWriter interface {
	WriteTo(w io.Writer) (n int64, err error)
}

func NewMessageReader() (msg MessageReader) {
	msg = &Message{
		kind: 0,
		data: nil,
		sink: nil,
	}
	return
}

type MessageReader interface {
	ReadFrom(r io.Reader) (n int64, err error)
	RequestType() (typ MessageRequestType)
	Bytes() (p []byte)
	Trunk() (trunk *Trunk, has bool)
}

const (
	trunkBufferSize = 1048576 // 1 MiB
)

type Trunk struct {
	sink io.Reader
}

func (trunk *Trunk) SetLimiter(limiter uint64) {
	trunk.sink = io.LimitReader(trunk.sink, int64(limiter))
}

func (trunk *Trunk) NextBlock() (p []byte, err error) {
	buf := make([]byte, trunkBufferSize)
	n, readErr := trunk.sink.Read(buf)
	if readErr != nil {
		err = readErr
		return
	}
	p = buf[0:n]
	return
}

func (trunk *Trunk) Read(p []byte) (n int, err error) {
	n, err = trunk.sink.Read(p)
	return
}

/*
Message
+---------------------------------------------------------+-----------+-----------+
| Header                                                  | Body      | Trunk     |
+---------------------+-----------------+-----------------+-----------+-----------+
| 4(BigEndian)     	  | 2(BigEndian)    | 2(BigEndian)    | n         | reader    |
+---------------------+-----------------+-----------------+-----------+-----------+
| Len(data)           | request type    | kind            | data      | snappy    |
+---------------------+-----------------+-----------------+-----------+-----------+
*/
type Message struct {
	requestType MessageRequestType
	kind        messageKind
	data        []byte
	sink        io.Reader
}

func (msg *Message) Trunk() (trunk *Trunk, has bool) {
	has = msg.kind == trunkedMessage
	if has {
		trunk = &Trunk{
			sink: msg.sink,
		}
	}
	return
}

func (msg *Message) Bytes() (p []byte) {
	p = msg.data
	return
}

func (msg *Message) RequestType() (typ MessageRequestType) {
	typ = msg.requestType
	return
}

func (msg *Message) ReadFrom(r io.Reader) (n int64, err error) {
	p := make([]byte, 8)
	hasHeader := false
	idx := 0
	requestType := uint16(0)
	msgKind := uint16(0)
	var body []byte
	for {
		nn, readErr := r.Read(p[idx:])
		if readErr != nil {
			err = readErr
			return
		}
		n = n + int64(nn)
		idx = idx + nn
		if idx < cap(p) {
			continue
		}
		if !hasHeader {
			bodyLen := binary.BigEndian.Uint32(p[0:4])
			requestType = binary.BigEndian.Uint16(p[4:6])
			msgKind = binary.BigEndian.Uint16(p[6:8])
			if msgKind == 0 {
				err = fmt.Errorf("message: read failed cause invalid message kind")
				return
			}
			hasHeader = true
			if bodyLen == 0 {
				err = fmt.Errorf("message: read failed cause invalid message body")
				return
			}
			p = make([]byte, bodyLen)
			idx = 0
			continue
		}
		body = p
		break
	}
	data := make([]byte, 0, 1)
	data, err = snappy.Decode(data, body)
	if err != nil {
		return
	}
	msg.requestType = MessageRequestType(requestType)
	msg.kind = messageKind(msgKind)
	msg.data = data
	if msg.kind == trunkedMessage {
		msg.sink = snappy.NewReader(r)
	}
	return
}

func (msg *Message) WriteTo(w io.Writer) (n int64, err error) {
	data := make([]byte, 0, 1)
	if msg.data != nil && len(msg.data) > 0 {
		data = snappy.Encode(data, msg.data)
	}
	dataLen := uint32(len(data))
	p := make([]byte, 8+dataLen)
	binary.BigEndian.PutUint32(p[0:4], dataLen)
	binary.BigEndian.PutUint16(p[4:6], uint16(msg.requestType))
	binary.BigEndian.PutUint16(p[6:8], uint16(msg.kind))
	if dataLen > 0 {
		copy(p[8:], data)
	}
	for {
		nn, writeErr := w.Write(p)
		n = n + int64(nn)
		if writeErr != nil {
			err = writeErr
			return
		}
		if nn < len(p) {
			p = p[nn:]
			continue
		}
		break
	}
	if msg.kind == trunkedMessage && msg.sink != nil {
		snappyWriter := snappy.NewBufferedWriter(w)
		for {
			trunk := make([]byte, trunkBufferSize)
			trunked, readTrunkErr := msg.sink.Read(trunk)
			if readTrunkErr != nil {
				if readTrunkErr == io.EOF {
					break
				}
				err = fmt.Errorf("message write trunk failed, %v", readTrunkErr)
				return
			}
			if trunked > 0 {
				nn, writeErr := snappyWriter.Write(trunk[0:trunked])
				if writeErr != nil {
					err = writeErr
					return
				}
				n = n + int64(nn)
			}
		}
		flushErr := snappyWriter.Flush()
		if flushErr != nil {
			err = fmt.Errorf("message write trunk failed, %v", flushErr)
			return
		}
		_ = snappyWriter.Close()
	}
	return
}
