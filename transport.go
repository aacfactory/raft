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
	LogMessageType = iota + 1
	VoteMessageType
	InstallSnapshotMessageType
	TimeoutNowMessageTyp
	ClientMessageType
	FsmViewCommandMessageType
)

type MessageType uint64

/*
Message
+--------------------------- -----------+-----------+
| Head                                  | Body      |
+---------------------+-----------------+-----------+
| 8(BigEndian)     	  | 8(BigEndian)    | n         |
+---------------------+----------------- -----------+
| Len(Data)           | Type            | Data      |
+---------------------+-----------------+-----------+
*/
type Message struct {
	Type MessageType
	Data []byte
}

func (msg *Message) ReadFrom(r io.Reader) (n int64, err error) {
	var msg0 *Message
	msg0, n, err = readMessage(r)
	if err != nil {
		return
	}
	msg.Type = msg0.Type
	msg.Data = msg0.Data
	return
}

func (msg *Message) WriteTo(w io.Writer) (n int64, err error) {
	n, err = writeMessage(msg, w)
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
			Type: MessageType(msgType),
		}
		return
	}
	decoded := make([]byte, 0, 1)
	decoded, err = snappy.Decode(decoded, data)
	if err != nil {
		return
	}
	msg = &Message{
		Type: MessageType(msgType),
		Data: decoded,
	}
	return
}

func writeMessage(msg *Message, writer io.Writer) (n int64, err error) {
	data := make([]byte, 0, 1)
	if msg.Data != nil && len(msg.Data) > 0 {
		data = snappy.Encode(data, msg.Data)
	}
	dataLen := uint64(len(data))
	p := make([]byte, 16+dataLen)
	binary.BigEndian.PutUint64(p[0:8], dataLen)
	binary.BigEndian.PutUint64(p[8:16], uint64(msg.Type))
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
