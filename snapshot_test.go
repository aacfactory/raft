package raft_test

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/aacfactory/raft"
	"os"
	"testing"
)

func TestSnapshot(t *testing.T) {
	config := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				Id:       "1",
				Address:  "1.com",
			},
			{
				Suffrage: raft.Voter,
				Id:       "2",
				Address:  "2.com",
			},
		},
	}
	p, _ := os.ReadFile("D:\\images\\3\\05-wa.png")
	fmt.Println("size: ", len(p))
	snapshots := raft.FileSnapshotStore(`G:\tmp\raft\snapshot`)
	for i := 0; i < 5; i++ {
		sink, createErr := snapshots.Create(uint64(i+1), 1, config, 1)
		if createErr != nil {
			t.Error("sink create:", createErr)
			return
		}
		fmt.Println("sink create:", sink.Id())
		_, writeErr := sink.Write(p)
		if writeErr != nil {
			t.Error("sink write:", writeErr)
			return
		}
		closeErr := sink.Close()
		if closeErr != nil {
			t.Error("sink close:", closeErr)
			return
		}
	}
	metas, listErr := snapshots.List()
	if listErr != nil {
		t.Error("list:", listErr)
		return
	}
	if metas == nil {
		metas = make([]*raft.SnapshotMeta, 0, 1)
	}
	for _, meta := range metas {
		fmt.Println("list:", meta.Id, meta.Index, meta.Term, meta.Size, hex.EncodeToString(meta.CRC), meta.CreateAT)
	}
	if len(metas) > 0 {
		meta, reader, openErr := snapshots.Open(metas[0].Id)
		if openErr != nil {
			t.Error("open:", reader)
			return
		}
		fmt.Println("open:", meta.Id, meta.Index, meta.Term, meta.Size, hex.EncodeToString(meta.CRC), meta.CreateAT)
		file, _ := os.OpenFile(`G:\tmp\raft\snapshot\1.png`, os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0600)
		_, _ = bufio.NewReader(reader).WriteTo(file)
	}
}
