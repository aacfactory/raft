package raft_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/raft"
	"testing"
	"time"
)

func TestAppendEntriesRequest_Encode(t *testing.T) {
	r := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{
			Id:   []byte("id"),
			Addr: []byte("addr"),
		},
		Key:               []byte("key"),
		Term:              1,
		PrevLogEntry:      1,
		PrevLogTerm:       0,
		LeaderCommitIndex: 0,
		Entries: []*raft.Log{
			{
				Index:      1,
				Term:       1,
				Type:       raft.LogCommand,
				Data:       []byte("data"),
				Extensions: []byte("Extensions"),
				AppendedAt: time.Now(),
			},
			{
				Index:      2,
				Term:       1,
				Type:       raft.LogNoop,
				Data:       nil,
				Extensions: []byte("Extensions"),
				AppendedAt: time.Now(),
			},
		},
	}
	msg, encodeErr := r.Encode()
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	fmt.Println(msg.WriteTo(bytes.NewBuffer([]byte{})))

}
