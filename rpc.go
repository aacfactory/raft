package raft

import (
	"fmt"
	"github.com/aacfactory/raft/encoding"
	"io"
)

const (
	AppendEntriesRequestType = MessageRequestType(iota + 1)
	AppendEntriesResponseType
	VoteRequestType
	VoteResponseType
	InstallSnapshotRequestType
	InstallSnapshotResponseType
	TimeoutNowRequestType
	TimeoutNowResponseType
	ClusterRequestType
	ClusterResponseType
	FsmRequestType
	FsmResponseType
)

type RPCHeader struct {
	// Id is the ServerId of the node sending the RPC Request or Response
	Id []byte
	// Addr is the ServerAddr of the node sending the RPC Request or Response
	Addr []byte
}

func (header *RPCHeader) encodeTo(encoder *encoding.Encoder) (p []byte) {
	encoder.WriteLengthFieldBasedFrame(header.Id)
	encoder.WriteLengthFieldBasedFrame(header.Addr)
	return
}

func (header *RPCHeader) decodeFrom(decoder *encoding.Decoder) (err error) {
	header.Id, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("decode rpc header failed, %v", err)
		return
	}
	header.Addr, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("decode rpc header failed, %v", err)
		return
	}
	return
}

func decodeRPC(msg MessageReader) (rpc RPC, err error) {
	switch msg.RequestType() {
	case AppendEntriesRequestType:
		rpc = &AppendEntriesRequest{}
		err = rpc.Decode(msg)
		break
	case AppendEntriesResponseType:
		rpc = &AppendEntriesResponse{}
		err = rpc.Decode(msg)
		break
	case VoteRequestType:
		rpc = &VoteRequest{}
		err = rpc.Decode(msg)
		break
	case VoteResponseType:
		rpc = &VoteResponse{}
		err = rpc.Decode(msg)
		break
	case InstallSnapshotRequestType:
		rpc = &InstallSnapshotRequest{}
		err = rpc.Decode(msg)
		break
	case InstallSnapshotResponseType:
		rpc = &InstallSnapshotResponse{}
		err = rpc.Decode(msg)
		break
	case TimeoutNowRequestType:
		rpc = &TimeoutNowRequest{}
		err = rpc.Decode(msg)
		break
	case TimeoutNowResponseType:
		rpc = &TimeoutNowResponse{}
		err = rpc.Decode(msg)
		break
	case ClusterRequestType:
		rpc = &ClusterRequest{}
		err = rpc.Decode(msg)
		break
	case ClusterResponseType:
		rpc = &ClusterResponse{}
		err = rpc.Decode(msg)
		break
	case FsmRequestType:
		rpc = &FsmRequest{}
		err = rpc.Decode(msg)
		break
	case FsmResponseType:
		rpc = &FsmResponse{}
		err = rpc.Decode(msg)
		break
	default:
		err = fmt.Errorf("decode rpc failed, type was unknown")
		break
	}
	if err != nil {
		rpc = nil
	}
	return
}

type RPC interface {
	Encode() (writer MessageWriter, err error)
	Decode(msg MessageReader) (err error)
}

type AppendEntriesRequest struct {
	RPCHeader
	Term              uint64
	PrevLogEntry      uint64
	PrevLogTerm       uint64
	LeaderCommitIndex uint64
	Entries           []*Log
}

func (request *AppendEntriesRequest) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	request.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(request.Term)
	encoder.WriteUint64(request.PrevLogEntry)
	encoder.WriteUint64(request.PrevLogTerm)
	encoder.WriteUint64(request.LeaderCommitIndex)
	entriesLen := uint64(0)
	if request.Entries != nil {
		entriesLen = uint64(len(request.Entries))
	}
	encoder.WriteUint64(entriesLen)
	for i := uint64(0); i < entriesLen; i++ {
		entry := request.Entries[i]
		if entry == nil {
			encoder.WriteLengthFieldBasedFrame([]byte{})
			continue
		}
		entry.encodeTo(encoder)
	}
	writer = NewMessageWriter(AppendEntriesRequestType, encoder.Bytes())
	return
}

func (request *AppendEntriesRequest) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = request.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
		return
	}
	request.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
		return
	}
	request.PrevLogEntry, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
		return
	}
	request.PrevLogTerm, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
		return
	}
	request.LeaderCommitIndex, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
		return
	}
	entriesLen, entriesLenErr := decoder.Uint64()
	if entriesLenErr != nil {
		err = fmt.Errorf("decode AppendEntriesRequest failed, %v", entriesLenErr)
		return
	}
	request.Entries = make([]*Log, 0, 1)
	for i := uint64(0); i < entriesLen; i++ {
		entry := &Log{}
		err = entry.decodeFrom(decoder)
		if err != nil {
			err = fmt.Errorf("decode AppendEntriesRequest failed, %v", err)
			return
		}
		request.Entries = append(request.Entries, entry)
	}
	return
}

type AppendEntriesResponse struct {
	RPCHeader
	Term           uint64
	LastLog        uint64
	Succeed        bool
	NoRetryBackoff bool
}

func (response *AppendEntriesResponse) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	response.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(response.Term)
	encoder.WriteUint64(response.LastLog)
	encoder.WriteBool(response.Succeed)
	encoder.WriteBool(response.NoRetryBackoff)
	writer = NewMessageWriter(AppendEntriesResponseType, encoder.Bytes())
	return
}

func (response *AppendEntriesResponse) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = response.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesResponse failed, %v", err)
		return
	}
	response.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesResponse failed, %v", err)
		return
	}
	response.LastLog, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesResponse failed, %v", err)
		return
	}
	response.Succeed, err = decoder.Bool()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesResponse failed, %v", err)
		return
	}
	response.NoRetryBackoff, err = decoder.Bool()
	if err != nil {
		err = fmt.Errorf("decode AppendEntriesResponse failed, %v", err)
		return
	}
	return
}

type VoteRequest struct {
	RPCHeader
	Term               uint64
	LastLogIndex       uint64
	LastLogTerm        uint64
	LeadershipTransfer bool
}

func (request *VoteRequest) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	request.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(request.Term)
	encoder.WriteUint64(request.LastLogIndex)
	encoder.WriteUint64(request.LastLogTerm)
	encoder.WriteBool(request.LeadershipTransfer)
	writer = NewMessageWriter(VoteRequestType, encoder.Bytes())
	return
}

func (request *VoteRequest) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = request.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode VoteRequest failed, %v", err)
		return
	}
	request.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode VoteRequest failed, %v", err)
		return
	}
	request.LastLogIndex, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode VoteRequest failed, %v", err)
		return
	}
	request.LastLogTerm, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode VoteRequest failed, %v", err)
		return
	}
	request.LeadershipTransfer, err = decoder.Bool()
	if err != nil {
		err = fmt.Errorf("decode VoteRequest failed, %v", err)
		return
	}
	return
}

type VoteResponse struct {
	RPCHeader
	Term    uint64
	Granted bool
}

func (response *VoteResponse) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	response.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(response.Term)
	encoder.WriteBool(response.Granted)
	writer = NewMessageWriter(VoteResponseType, encoder.Bytes())
	return
}

func (response *VoteResponse) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = response.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode VoteResponse failed, %v", err)
		return
	}
	response.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode VoteResponse failed, %v", err)
		return
	}
	response.Granted, err = decoder.Bool()
	if err != nil {
		err = fmt.Errorf("decode VoteResponse failed, %v", err)
		return
	}
	return
}

type InstallSnapshotRequest struct {
	RPCHeader
	Term               uint64
	LastLogIndex       uint64
	LastLogTerm        uint64
	ConfigurationIndex uint64
	Size               uint64
	Leader             []byte
	Configuration      []byte
	Snapshot           io.Reader
}

func (request *InstallSnapshotRequest) Encode() (writer MessageWriter, err error) {
	if request.Snapshot == nil {
		err = fmt.Errorf("encode InstallSnapshotRequest failed, cause snapshot is nil")
		return
	}
	if request.Size == 0 {
		err = fmt.Errorf("encode InstallSnapshotRequest failed, cause snapshot size is zero")
		return
	}
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	request.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(request.Term)
	encoder.WriteUint64(request.LastLogIndex)
	encoder.WriteUint64(request.LastLogTerm)
	encoder.WriteUint64(request.ConfigurationIndex)
	encoder.WriteUint64(request.Size)
	encoder.WriteLengthFieldBasedFrame(request.Leader)
	encoder.WriteLengthFieldBasedFrame(request.Configuration)
	writer = NewMessageWriterWithTrunk(InstallSnapshotRequestType, encoder.Bytes(), request.Snapshot)
	return
}

func (request *InstallSnapshotRequest) Decode(msg MessageReader) (err error) {
	trunk, hasTrunk := msg.Trunk()
	if !hasTrunk {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, no trunk")
		return
	}
	request.Snapshot = trunk

	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = request.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.LastLogIndex, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.LastLogTerm, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.ConfigurationIndex, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.Size, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.Leader, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}
	request.Configuration, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotRequest failed, %v", err)
		return
	}

	return
}

type InstallSnapshotResponse struct {
	RPCHeader
	Term    uint64
	Succeed bool
}

func (response *InstallSnapshotResponse) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	response.RPCHeader.encodeTo(encoder)
	encoder.WriteUint64(response.Term)
	encoder.WriteBool(response.Succeed)
	writer = NewMessageWriter(InstallSnapshotResponseType, encoder.Bytes())
	return
}

func (response *InstallSnapshotResponse) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = response.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotResponse failed, %v", err)
		return
	}
	response.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotResponse failed, %v", err)
		return
	}
	response.Succeed, err = decoder.Bool()
	if err != nil {
		err = fmt.Errorf("decode InstallSnapshotResponse failed, %v", err)
		return
	}
	return
}

type TimeoutNowRequest struct {
	RPCHeader
}

func (request *TimeoutNowRequest) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	request.RPCHeader.encodeTo(encoder)
	writer = NewMessageWriter(TimeoutNowRequestType, encoder.Bytes())
	return
}

func (request *TimeoutNowRequest) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = request.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode TimeoutNowRequest failed, %v", err)
		return
	}
	return
}

type TimeoutNowResponse struct {
	RPCHeader
}

func (response *TimeoutNowResponse) Encode() (writer MessageWriter, err error) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	response.RPCHeader.encodeTo(encoder)
	writer = NewMessageWriter(TimeoutNowResponseType, encoder.Bytes())
	return
}

func (response *TimeoutNowResponse) Decode(msg MessageReader) (err error) {
	decoder := encoding.NewDecoder(msg.Bytes())
	defer decoder.Close()
	err = response.RPCHeader.decodeFrom(decoder)
	if err != nil {
		err = fmt.Errorf("decode TimeoutNowResponse failed, %v", err)
		return
	}
	return
}

type ClusterRequest struct {
}

func (request *ClusterRequest) Encode() (writer MessageWriter, err error) {
	//TODO implement me
	panic("implement me")
}

func (request *ClusterRequest) Decode(msg MessageReader) (err error) {
	//TODO implement me
	panic("implement me")
}

type ClusterResponse struct {
}

func (response *ClusterResponse) Encode() (writer MessageWriter, err error) {
	//TODO implement me
	panic("implement me")
}

func (response *ClusterResponse) Decode(msg MessageReader) (err error) {
	//TODO implement me
	panic("implement me")
}

type FsmRequest struct {
}

func (request *FsmRequest) Encode() (writer MessageWriter, err error) {
	//TODO implement me
	panic("implement me")
}

func (request *FsmRequest) Decode(msg MessageReader) (err error) {
	//TODO implement me
	panic("implement me")
}

type FsmResponse struct {
}

func (response *FsmResponse) Encode() (writer MessageWriter, err error) {
	//TODO implement me
	panic("implement me")
}

func (response *FsmResponse) Decode(msg MessageReader) (err error) {
	//TODO implement me
	panic("implement me")
}
