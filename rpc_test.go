package raft_test

import (
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
	p := r.Encode()
	fmt.Println("encode:", len(p))
	r2 := &raft.AppendEntriesRequest{}
	decodeErr := r2.Decode(p)
	if decodeErr != nil {
		t.Error(decodeErr)
		return
	}
	fmt.Println(string(r2.Id), string(r2.Addr))
	fmt.Println(r2.Term, r2.PrevLogTerm, r2.PrevLogEntry, r2.LeaderCommitIndex)
	if r2.Entries != nil {
		for _, entry := range r2.Entries {
			fmt.Println(entry.Term, entry.Index, entry.Type, entry.AppendedAt, string(entry.Key), string(entry.Data), string(entry.Extensions))
		}
	}
}
