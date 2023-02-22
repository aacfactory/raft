package raft

import (
	"fmt"
	"github.com/aacfactory/raft/encoding"
	"github.com/aacfactory/rings"
	"time"
)

type LogType uint8

const (
	LogCommand LogType = iota
	LogNoop
	LogConfiguration
)

func (lt LogType) String() string {
	switch lt {
	case LogCommand:
		return "LogCommand"
	case LogNoop:
		return "LogNoop"
	case LogConfiguration:
		return "LogConfiguration"
	default:
		return fmt.Sprintf("%d", lt)
	}
}

type Log struct {
	Index      uint64
	Term       uint64
	Type       LogType
	Data       []byte
	Extensions []byte
	AppendedAt time.Time
}

func (log *Log) encodeTo(encoder *encoding.Encoder) (p []byte) {
	encoder.WriteUint64(log.Index)
	encoder.WriteUint64(log.Term)
	encoder.WriteUint64(uint64(log.Type))
	encoder.WriteLengthFieldBasedFrame(log.Data)
	encoder.WriteLengthFieldBasedFrame(log.Extensions)
	encoder.WriteTime(log.AppendedAt)
	return
}

func (log *Log) decodeFrom(decoder *encoding.Decoder) (err error) {
	log.Index, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("log decode failed, %v", err)
		return
	}
	log.Term, err = decoder.Uint64()
	if err != nil {
		err = fmt.Errorf("log decode failed, %v", err)
		return
	}
	typ, typErr := decoder.Uint64()
	if typErr != nil {
		err = fmt.Errorf("log decode failed, %v", typErr)
		return
	}
	log.Type = LogType(typ)
	log.Data, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("log decode failed, %v", err)
		return
	}
	log.Extensions, err = decoder.LengthFieldBasedFrame()
	if err != nil {
		err = fmt.Errorf("log decode failed, %v", err)
		return
	}
	log.AppendedAt, err = decoder.Time()
	if err != nil {
		err = fmt.Errorf("log decode failed, %v", err)
		return
	}
	return
}

type LogStore interface {
	FirstIndex(key []byte) (uint64, error)
	LastIndex(key []byte) (uint64, error)
	GetLog(index uint64, key []byte) (log *Log, err error)
	StoreLog(log *Log) error
	StoreLogs(logs []*Log) error
	DeleteRange(min, max uint64) error
}

func FileLogStore(path string) (store LogStore) {
	// todo
	store = &fileLogStore{}
	panic("todo")
	return
}

type fileLogStore struct {
}

func (store *fileLogStore) FirstIndex(key []byte) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (store *fileLogStore) LastIndex(key []byte) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (store *fileLogStore) GetLog(index uint64, key []byte) (log *Log, err error) {
	//TODO implement me
	panic("implement me")
}

func (store *fileLogStore) StoreLog(log *Log) error {
	//TODO implement me
	panic("implement me")
}

func (store *fileLogStore) StoreLogs(logs []*Log) error {
	//TODO implement me
	panic("implement me")
}

func (store *fileLogStore) DeleteRange(min, max uint64) error {
	//TODO implement me
	panic("implement me")
}

type LogHandlers struct {
	handlers *rings.HashRing[*LogHandler]
}

func (handlers *LogHandlers) Dispatch(request *AppendEntriesRequest) (response *AppendEntriesResponse) {
	handler, got := handlers.handlers.Get(request.Key)
	if !got {
		// todo return failed
		return
	}
	response = handler.Handle(request)
	return
}

type LogHandler struct {
	// ch chan Future
}

func (handler *LogHandler) Key() (key string) {

	return
}

func (handler *LogHandler) Handle(request *AppendEntriesRequest) (response *AppendEntriesResponse) {

	return
}
