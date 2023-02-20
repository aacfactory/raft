package raft_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/raft"
	"io"
	"os"
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

	p := r.Encode()

	request := raft.NewRequestMessage(p)

	buf := bytes.NewBuffer(make([]byte, 0, 1))

	_, wErr := request.WriteTo(buf)
	if wErr != nil {
		t.Error(wErr)
		return
	}

	response := raft.NewResponseMessage()
	_, rErr := response.ReadFrom(buf)
	if rErr != nil {
		t.Error(rErr)
		return
	}
	fmt.Println(bytes.Equal(p, response.Bytes()))
}

func TestMessage_Trunk(t *testing.T) {
	p := []byte(time.Now().String())
	fp, readFileErr := os.ReadFile(`./README.md`)
	if readFileErr != nil {
		t.Error(readFileErr)
		return
	}
	f, openErr := os.OpenFile(`./README.md`, os.O_RDONLY, 0600)
	if openErr != nil {
		t.Error(openErr)
		return
	}
	defer f.Close()

	request := raft.NewRequestMessageWithTrunk(p, f)

	buf := bytes.NewBuffer(make([]byte, 0, 1))
	_, wErr := request.WriteTo(buf)
	if wErr != nil {
		t.Error(wErr)
		return
	}

	response := raft.NewResponseMessage()
	_, rErr := response.ReadFrom(buf)
	if rErr != nil {
		t.Error(rErr)
		return
	}
	trunk, hasTrunk := response.Trunk()
	if !hasTrunk {
		t.Error("no trunk")
		return
	}

	pt := bytes.NewBuffer(make([]byte, 0, 1))
	for {
		sp, readTrunkErr := trunk.Read()
		if readTrunkErr != nil {
			if readTrunkErr == io.EOF {
				break
			}
		}
		pt.Write(sp)
	}

	fmt.Println(bytes.Equal(fp, pt.Bytes()), pt.Len())

}
