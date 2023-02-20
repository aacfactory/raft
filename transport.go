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

type messageKind uint32

func NewRequestMessage(data []byte) (msg RequestMessage) {
	if data == nil {
		data = []byte{}
	}
	msg = &Message{
		kind: shortMessage,
		data: data,
		sink: nil,
	}
	return
}

func NewRequestMessageWithTrunk(data []byte, sink io.Reader) (msg RequestMessage) {
	if data == nil {
		data = []byte{}
	}
	msg = &Message{
		kind: trunkedMessage,
		data: data,
		sink: sink,
	}
	return
}

type RequestMessage interface {
	WriteTo(w io.Writer) (n int64, err error)
}

func NewResponseMessage() (msg ResponseMessage) {
	msg = &Message{
		kind: 0,
		data: nil,
		sink: nil,
	}
	return
}

type ResponseMessage interface {
	ReadFrom(r io.Reader) (n int64, err error)
	Bytes() (p []byte)
	Trunk() (trunk *Trunk, has bool)
}

const (
	trunkBufferSize = 1048576 // 1 MiB
)

type Trunk struct {
	sink io.Reader
}

func (trunk *Trunk) Read() (p []byte, err error) {
	buf := make([]byte, trunkBufferSize)
	n, readErr := trunk.sink.Read(buf)
	if readErr != nil {
		err = readErr
		return
	}
	p = buf[0:n]
	return
}

/*
Message
+---------------------------------------+-----------+
| Header                                | Body      |
+---------------------+-----------------+-----------+
| 4(BigEndian)     	  | 4(BigEndian)    | n         |
+---------------------+----------------- -----------+
| Len(data)           | kind            | data      |
+---------------------+-----------------+-----------+
*/
type Message struct {
	kind messageKind
	data []byte
	sink io.Reader
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

func (msg *Message) ReadFrom(r io.Reader) (n int64, err error) {
	p := make([]byte, 8)
	hasHeader := false
	idx := 0
	msgKind := uint32(0)
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
			msgKind = binary.BigEndian.Uint32(p[4:8])
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
	binary.BigEndian.PutUint32(p[4:8], uint32(msg.kind))
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

func readMessage(reader io.Reader) (msg *Message, n int64, err error) {
	p := make([]byte, 16)
	headRead := false
	idx := 0
	msgType := uint64(0)
	var data []byte
	for {
		nn, readErr := reader.Read(p[idx:])
		if readErr != nil {
			err = readErr
			return
		}
		n = n + int64(nn)
		idx = idx + nn
		if idx < cap(p) {
			continue
		}
		if !headRead {
			bodyLen := binary.BigEndian.Uint64(p[0:8])
			msgType = binary.BigEndian.Uint64(p[8:16])
			if msgType == 0 {
				err = fmt.Errorf("message: read failed cause invalid message type")
				return
			}
			headRead = true
			if bodyLen == 0 {
				break
			}
			p = make([]byte, bodyLen)
			idx = 0
			continue
		}
		data = p
		break
	}
	if data == nil || len(data) == 0 {
		msg = &Message{
			kind: messageKind(msgType),
		}
		return
	}
	decoded := make([]byte, 0, 1)
	decoded, err = snappy.Decode(decoded, data)
	if err != nil {
		return
	}
	msg = &Message{
		kind: messageKind(msgType),
		data: decoded,
	}
	return
}

func writeMessage(msg *Message, writer io.Writer) (n int64, err error) {
	data := make([]byte, 0, 1)
	if msg.data != nil && len(msg.data) > 0 {
		data = snappy.Encode(data, msg.data)
	}
	dataLen := uint64(len(data))
	p := make([]byte, 16+dataLen)
	binary.BigEndian.PutUint64(p[0:8], dataLen)
	binary.BigEndian.PutUint64(p[8:16], uint64(msg.kind))
	if dataLen > 0 {
		copy(p[16:], data)
	}
	for {
		nn, writeErr := writer.Write(p)
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

	return
}
