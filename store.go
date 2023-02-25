package raft

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/raft/encoding"
	"github.com/aacfactory/wal"
	"path/filepath"
	"strings"
	"sync"
)

type LogMeta interface {
	Term() (term uint64, err error)
	SetTerm(term uint64) (err error)
	Set(key []byte, val []byte) (err error)
	Get(key []byte) (val []byte, err error)
}

type Store interface {
	AcquireIndex() (term uint64, index uint64, err error)
	Meta() (meta LogMeta)
	FirstIndex() (index uint64, err error)
	LastIndex() (index uint64, err error)
	Read(index uint64) (log *Log, err error)
	Write(logs ...*Log) (err error)
	Commit(logs ...*Log) (err error)
	Remove(logs ...*Log) (err error)
	Close()
}

const (
	logFilename    = "logs.db"
	stableFilename = "stable.db"
)

var (
	ErrLogNotFound = fmt.Errorf("log not found")
	ErrNotFound    = fmt.Errorf("data not found")
)

func FileStore(dir string, caches int) (store Store, err error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		err = errors.ServiceError("create file store failed").WithCause(errors.ServiceError("dir is empty"))
		return
	}
	meta, metaErr := newFileMetaStore(dir, caches)
	if metaErr != nil {
		err = errors.ServiceError("create file store failed").WithCause(metaErr)
		return
	}
	aLog, createLogErr := wal.New(filepath.Join(dir, logFilename), wal.Unit64KeyEncoder(), wal.WithCacheSize(caches), wal.EnableTransaction(wal.ReadUncommitted), wal.UseSOT(wal.SOT1K))
	if createLogErr != nil {
		err = errors.ServiceError("create file store failed").WithCause(createLogErr)
		return
	}
	store = &fileStore{
		locker: sync.RWMutex{},
		writes: sync.WaitGroup{},
		meta:   meta,
		aLog:   aLog,
	}
	return
}

func newFileMetaStore(dir string, caches int) (meta *fileMetaStore, err error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		err = errors.ServiceError("create file meta store failed").WithCause(errors.ServiceError("dir is empty"))
		return
	}
	stable, createStableErr := wal.New(filepath.Join(dir, stableFilename), wal.StringKeyEncoder(), wal.WithCacheSize(caches), wal.UseSOT(wal.SOT128B))
	if createStableErr != nil {
		err = errors.ServiceError("create file meta store failed").WithCause(createStableErr)
		return
	}
	_, p, _, readErr := stable.Key("term")
	if readErr != nil {
		if errors.Map(readErr).Contains(wal.ErrNotFound) {
			p = make([]byte, 8)
		} else {
			err = errors.ServiceError("create file meta store failed").WithCause(readErr)
			return
		}
	}
	term := uint64(0)
	term = binary.BigEndian.Uint64(p)
	meta = &fileMetaStore{
		locker: sync.RWMutex{},
		writes: sync.WaitGroup{},
		term:   term,
		stable: stable,
	}
	return
}

type fileMetaStore struct {
	locker sync.RWMutex
	writes sync.WaitGroup
	term   uint64
	stable *wal.WAL[string]
}

func (meta *fileMetaStore) Term() (term uint64, err error) {
	meta.locker.RLock()
	defer meta.locker.RUnlock()
	term = meta.term
	return
}

func (meta *fileMetaStore) SetTerm(term uint64) (err error) {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, term)
	err = meta.Set([]byte("term"), p)
	if err != nil {
		err = errors.ServiceError("file meta store set term failed").WithCause(err)
		return
	}
	meta.term = term
	return
}

func (meta *fileMetaStore) Set(key []byte, val []byte) (err error) {
	meta.locker.Lock()
	defer meta.locker.Unlock()
	meta.writes.Add(1)
	defer meta.writes.Done()
	_, writeErr := meta.stable.Write(string(key), val)
	if writeErr != nil {
		err = errors.ServiceError("file meta store set value failed").WithCause(writeErr)
		return
	}
	return
}

func (meta *fileMetaStore) Get(key []byte) (val []byte, err error) {
	meta.locker.RLock()
	defer meta.locker.RUnlock()
	_, p, _, readErr := meta.stable.Key(string(key))
	if readErr != nil {
		if errors.Map(readErr).Contains(wal.ErrNotFound) {
			err = errors.ServiceError("file meta store get value failed").WithCause(ErrNotFound)
		} else {
			err = errors.ServiceError("file meta store get value failed").WithCause(readErr)
		}
		return
	}
	val = p
	return
}

func (meta *fileMetaStore) Close() {
	meta.locker.Lock()
	_ = meta.SetTerm(meta.term)
	meta.writes.Wait()
	meta.stable.Close()
	meta.locker.Unlock()
}

type fileStore struct {
	locker sync.RWMutex
	writes sync.WaitGroup
	meta   *fileMetaStore
	aLog   *wal.WAL[uint64]
}

func (store *fileStore) AcquireIndex() (term uint64, index uint64, err error) {
	store.locker.Lock()
	defer store.locker.Unlock()
	store.writes.Add(1)
	defer store.writes.Done()
	lastIndex, lastIndexErr := store.aLog.LastKey()
	if lastIndexErr != nil {
		if !errors.Map(lastIndexErr).Contains(wal.ErrNoLastIndex) {
			err = errors.ServiceError("file store acquire index failed").WithCause(lastIndexErr)
			return
		}
	}
	nextIndex := lastIndex + 1
	term, _ = store.Meta().Term()
	index = nextIndex
	return
}

func (store *fileStore) Meta() (meta LogMeta) {
	meta = store.meta
	return
}

func (store *fileStore) FirstIndex() (index uint64, err error) {
	index, err = store.aLog.FirstKey()
	if err != nil {
		if !errors.Map(err).Contains(wal.ErrNoFirstIndex) {
			err = errors.ServiceError("file store get first index failed").WithCause(err)
			return
		}
		return
	}
	return
}

func (store *fileStore) LastIndex() (index uint64, err error) {
	index, err = store.aLog.LastKey()
	if err != nil {
		if !errors.Map(err).Contains(wal.ErrNoLastIndex) {
			err = errors.ServiceError("file store get last index failed").WithCause(err)
			return
		}
		return
	}
	return
}

func (store *fileStore) Read(index uint64) (log *Log, err error) {
	_, p, state, readErr := store.aLog.Key(index)
	if readErr != nil {
		if errors.Map(readErr).Contains(wal.ErrNotFound) {
			err = errors.ServiceError("file store read log failed").WithCause(ErrLogNotFound)
			return
		}
		err = errors.ServiceError("file store read log failed").WithCause(readErr)
		return
	}
	log = &Log{}
	decodeErr := log.decodeFrom(encoding.NewDecoder(p))
	if decodeErr != nil {
		err = errors.ServiceError("file store read log failed").WithCause(decodeErr)
		return
	}
	log.Committed = state == wal.CommittedState
	return
}

func (store *fileStore) Write(logs ...*Log) (err error) {
	if logs == nil || len(logs) == 0 {
		return
	}
	store.writes.Add(1)
	defer store.writes.Done()
	size := uint64(len(logs))
	if size == 1 {
		log := logs[0]
		indexErr := store.validateWriteIndex(log.Index)
		if indexErr != nil {
			err = errors.ServiceError("file store write log failed").WithCause(indexErr)
			return
		}
		if log.Term == 0 {
			err = errors.ServiceError("file store write log failed").WithCause(errors.ServiceError("term of log is invalid"))
			return
		}
		p := log.encodeTo(encoding.NewEncoder())
		_, writeErr := store.aLog.Write(log.Index, p)
		if writeErr != nil {
			err = errors.ServiceError("file store write log failed").WithCause(writeErr)
			return
		}
		return
	}
	batch := store.aLog.Batch()
	defer batch.Close()

	for _, log := range logs {
		indexErr := store.validateWriteIndex(log.Index)
		if indexErr != nil {
			err = errors.ServiceError("file store write log failed").WithCause(indexErr)
			return
		}
		if log.Term == 0 {
			err = errors.ServiceError("file store write log failed").WithCause(errors.ServiceError("term of log is invalid"))
			return
		}
		p := log.encodeTo(encoding.NewEncoder())
		_, writeErr := batch.Write(log.Index, p)
		if writeErr != nil {
			err = errors.ServiceError("file store write log failed").WithCause(writeErr)
			return
		}
	}
	flushErr := batch.Flush()
	if flushErr != nil {
		err = errors.ServiceError("file store write log failed").WithCause(flushErr)
		return
	}
	return
}

func (store *fileStore) Commit(logs ...*Log) (err error) {
	if logs == nil || len(logs) == 0 {
		return
	}
	indexes := make([]uint64, 0, len(logs))
	for _, log := range logs {
		if log.Term == 0 {
			err = errors.ServiceError("file store commit log failed").WithCause(errors.ServiceError("term of log is invalid"))
			return
		}
		indexes = append(indexes, log.Index)
	}
	cmtErr := store.aLog.CommitKey(indexes...)
	if cmtErr != nil {
		if errors.Map(cmtErr).Contains(wal.ErrNotFound) {
			err = errors.ServiceError("file store commit log failed").WithCause(ErrLogNotFound)
		}
		err = errors.ServiceError("file store commit log failed").WithCause(cmtErr)
		return
	}
	return
}

func (store *fileStore) Remove(logs ...*Log) (err error) {
	if logs == nil || len(logs) == 0 {
		return
	}
	for _, log := range logs {
		if log.Term == 0 {
			err = errors.ServiceError("file store remove log failed").WithCause(errors.ServiceError("term of log is invalid"))
			return
		}
		rmErr := store.aLog.RemoveKey(log.Index)
		if rmErr != nil {
			err = errors.ServiceError("file store remove log failed").WithCause(rmErr)
			return
		}
	}
	return
}

func (store *fileStore) Close() {
	store.locker.Lock()
	store.writes.Wait()
	store.aLog.Close()
	store.meta.Close()
	store.locker.Unlock()
}

func (store *fileStore) validateWriteIndex(index uint64) (err error) {
	if index == 0 {
		err = errors.ServiceError("invalid index")
		return
	}
	lastIndex, lastIndexErr := store.aLog.LastKey()
	if lastIndexErr != nil {
		if errors.Map(lastIndexErr).Contains(wal.ErrNoLastIndex) {
			return
		}
		err = errors.ServiceError("invalid index").WithCause(lastIndexErr)
		return
	}
	if lastIndex < index {
		err = errors.ServiceError("invalid index")
		return
	}
	return
}
