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

	request := raft.NewMessageWriterWithTrunk(1, p, f)

	buf := bytes.NewBuffer(make([]byte, 0, 1))
	_, wErr := request.WriteTo(buf)
	if wErr != nil {
		t.Error(wErr)
		return
	}

	response := raft.NewMessageReader()
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
		sp, readTrunkErr := trunk.NextBlock()
		if readTrunkErr != nil {
			if readTrunkErr == io.EOF {
				break
			}
		}
		pt.Write(sp)
	}

	fmt.Println(bytes.Equal(fp, pt.Bytes()), pt.Len(), response.RequestType())

}
