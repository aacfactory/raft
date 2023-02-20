package raft_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/raft"
	"testing"
	"time"
)

func TestMessage_RW(t *testing.T) {
	r := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{
			Id:   []byte("id"),
			Addr: []byte("addr"),
		},
		Term:              1,
		PrevLogEntry:      1,
		PrevLogTerm:       0,
		LeaderCommitIndex: 0,
		Entries: []*raft.Log{
			{
				Index:      1,
				Term:       1,
				Type:       raft.LogCommand,
				Key:        []byte("k1"),
				Data:       []byte("data"),
				Extensions: []byte("Extensions"),
				AppendedAt: time.Now(),
			},
			{
				Index:      2,
				Term:       1,
				Type:       raft.LogNoop,
				Key:        nil,
				Data:       nil,
				Extensions: []byte("Extensions"),
				AppendedAt: time.Now(),
			},
		},
	}

	msg := &raft.Message{
		Type: raft.LogMessageType,
		Data: r.Encode(),
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1))

	_, wErr := msg.WriteTo(buf)
	if wErr != nil {
		t.Error(wErr)
		return
	}
	fmt.Println(len(msg.Data), buf.Len()-16, len(msg.Data)+16-buf.Len())

	m := &raft.Message{}
	_, rErr := m.ReadFrom(buf)
	if rErr != nil {
		t.Error(rErr)
		return
	}
	fmt.Println(m.Type, bytes.Equal(msg.Data, m.Data))
}
