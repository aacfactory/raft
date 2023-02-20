package raft

import (
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
)

type RPCHeader struct {
	// Id is the ServerId of the node sending the RPC Request or Response
	Id []byte
	// Addr is the ServerAddr of the node sending the RPC Request or Response
	Addr []byte
}

func (header *RPCHeader) Encode() (p []byte) {
	idLen := 0
	if header.Id != nil {
		idLen = len(header.Id)
	}
	addrLen := 0
	if header.Addr != nil {
		addrLen = len(header.Addr)
	}
	p = make([]byte, idLen+addrLen+16)
	binary.BigEndian.PutUint64(p[0:8], uint64(idLen))
	binary.BigEndian.PutUint64(p[8:16], uint64(addrLen))
	idx := 16
	if idLen > 0 {
		copy(p[idx:idx+idLen], header.Id)
		idx = idx + idLen
	}
	if addrLen > 0 {
		copy(p[idx:idx+addrLen], header.Addr)
	}
	return
}

func (header *RPCHeader) Decode(p []byte) (err error) {
	pLen := uint64(len(p))
	if pLen < 16 {
		err = fmt.Errorf("invalid rpc header")
		return
	}
	idLen := binary.BigEndian.Uint64(p[0:8])
	addrLen := binary.BigEndian.Uint64(p[8:16])
	idx := uint64(16)
	if idLen > 0 {
		if pLen < idx+idLen {
			err = fmt.Errorf("invalid rpc header bytes")
			return
		}
		header.Id = p[idx : idx+idLen]
		idx = idx + idLen
	}
	if addrLen > 0 {
		if pLen < idx+addrLen {
			err = fmt.Errorf("invalid rpc header bytes")
			return
		}
		header.Addr = p[idx : idx+addrLen]
	}
	return
}

type AppendEntriesRequest struct {
	RPCHeader
	Term              uint64
	PrevLogEntry      uint64
	PrevLogTerm       uint64
	LeaderCommitIndex uint64
	Entries           []*Log
}

func (request *AppendEntriesRequest) Encode() (p []byte) {
	buf := bytebufferpool.Get()
	_, _ = buf.Write(request.RPCHeader.Encode())
	bep := make([]byte, 40)
	binary.BigEndian.PutUint64(bep[0:8], request.Term)
	binary.BigEndian.PutUint64(bep[8:16], request.PrevLogEntry)
	binary.BigEndian.PutUint64(bep[16:24], request.PrevLogTerm)
	binary.BigEndian.PutUint64(bep[24:32], request.LeaderCommitIndex)
	if request.Entries != nil {
		logLen := uint64(len(request.Entries))
		binary.BigEndian.PutUint64(bep[32:40], logLen)
		_, _ = buf.Write(bep)
		for _, entry := range request.Entries {
			logBytesLen := make([]byte, 8)
			if entry == nil {
				binary.BigEndian.PutUint64(logBytesLen, 0)
				_, _ = buf.Write(logBytesLen)
			} else {
				logBytes := entry.Encode()
				binary.BigEndian.PutUint64(logBytesLen, uint64(len(logBytes)))
				_, _ = buf.Write(logBytesLen)
				_, _ = buf.Write(logBytes)
			}
		}
	} else {
		binary.BigEndian.PutUint64(bep[32:40], 0)
		_, _ = buf.Write(bep)
	}
	p = buf.Bytes()
	bytebufferpool.Put(buf)
	return
}

func (request *AppendEntriesRequest) Decode(p []byte) (err error) {
	decodeHeaderErr := request.RPCHeader.Decode(p)
	if decodeHeaderErr != nil {
		err = decodeHeaderErr
		return
	}
	p = p[16:]
	if request.Id != nil {
		p = p[len(request.Id):]
	}
	if request.Addr != nil {
		p = p[len(request.Addr):]
	}
	request.Term = binary.BigEndian.Uint64(p[0:8])
	request.PrevLogEntry = binary.BigEndian.Uint64(p[8:16])
	request.PrevLogTerm = binary.BigEndian.Uint64(p[16:24])
	request.LeaderCommitIndex = binary.BigEndian.Uint64(p[24:32])

	entriesLen := binary.BigEndian.Uint64(p[32:40])
	if entriesLen > 0 {
		request.Entries = make([]*Log, 0, entriesLen)
		p = p[40:]
		for i := uint64(0); i < entriesLen; i++ {
			entryLen := binary.BigEndian.Uint64(p[0:8])
			if entryLen == 0 {
				p = p[8:]
				continue
			}
			entry := &Log{}
			decodeEntryErr := entry.Decode(p[8:])
			if decodeEntryErr != nil {
				err = decodeEntryErr
				return
			}
			request.Entries = append(request.Entries, entry)
			p = p[8+entryLen:]
		}
	}
	return
}

type AppendEntriesResponse struct {
	RPCHeader
	Term           uint64
	LastLog        uint64
	Success        bool
	NoRetryBackoff bool
}

type VoteRequest struct {
	RPCHeader
	Term               uint64
	LastLogIndex       uint64
	LastLogTerm        uint64
	LeadershipTransfer bool
}

type VoteResponse struct {
	RPCHeader
	Term    uint64
	Granted bool
}

type InstallSnapshotRequest struct {
	RPCHeader
	Term               uint64
	LastLogIndex       uint64
	LastLogTerm        uint64
	ConfigurationIndex uint64
	Size               int64
	Leader             []byte
	Configuration      []byte
}

type InstallSnapshotResponse struct {
	RPCHeader
	Term    uint64
	Success bool
}

type TimeoutNowRequest struct {
	RPCHeader
}

type TimeoutNowResponse struct {
	RPCHeader
}

// todo Client
// todo FsmViewCommand
