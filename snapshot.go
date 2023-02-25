package raft

import (
	"bufio"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/raft/encoding"
	"github.com/aacfactory/raft/files"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SnapshotSink interface {
	io.WriteCloser
	Id() string
	Cancel() error
}

type SnapshotMetas []*SnapshotMeta

func (metas SnapshotMetas) Len() int {
	return len(metas)
}

func (metas SnapshotMetas) Less(i, j int) bool {
	return metas[i].Term <= metas[j].Term && metas[i].Index < metas[j].Index
}

func (metas SnapshotMetas) Swap(i, j int) {
	metas[i], metas[j] = metas[j], metas[i]
	return
}

type SnapshotMeta struct {
	Id                 string
	Index              uint64
	Term               uint64
	Configuration      Configuration
	ConfigurationIndex uint64
	Size               uint64
	CRC                []byte
	CreateAT           time.Time
}

func (meta *SnapshotMeta) WriteTo(writer io.Writer) (n int64, err error) {
	p := meta.Encode()
	nn, writeErr := writer.Write(p)
	if writeErr != nil {
		err = writeErr
		return
	}
	n = int64(nn)
	return
}

func (meta *SnapshotMeta) Encode() (p []byte) {
	encoder := encoding.NewEncoder()
	defer encoder.Close()
	encoder.WriteLengthFieldBasedFrame([]byte(meta.Id))
	encoder.WriteUint64(meta.Index)
	encoder.WriteUint64(meta.Term)
	if meta.Configuration.Servers == nil {
		meta.Configuration.Servers = make([]Server, 0, 1)
	}
	encoder.WriteUint64(uint64(len(meta.Configuration.Servers)))
	for _, server := range meta.Configuration.Servers {
		encoder.WriteUint64(uint64(server.Suffrage))
		encoder.WriteLengthFieldBasedFrame([]byte(server.Id))
		encoder.WriteLengthFieldBasedFrame([]byte(server.Address))
	}
	encoder.WriteUint64(meta.ConfigurationIndex)
	encoder.WriteUint64(meta.Size)
	if meta.CRC == nil || len(meta.CRC) == 0 {
		meta.CRC = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	}
	encoder.WriteLengthFieldBasedFrame(meta.CRC)
	p = encoder.Bytes()
	return
}

func (meta *SnapshotMeta) Decode(r io.Reader) (err error) {
	decoder := encoding.NewDecoderFromReader(r)
	var decodeErr error
	meta.Id, decodeErr = decoder.LengthFieldBasedStringFrame()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	meta.Index, decodeErr = decoder.Uint64()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	meta.Term, decodeErr = decoder.Uint64()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	meta.Configuration.Servers = make([]Server, 0, 1)
	serverLen := uint64(0)
	serverLen, decodeErr = decoder.Uint64()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	if serverLen > 0 {
		for i := uint64(0); i < serverLen; i++ {
			serverSuffrage := uint64(0)
			serverSuffrage, decodeErr = decoder.Uint64()
			if decodeErr != nil {
				err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
				return
			}
			serverId := ""
			serverId, decodeErr = decoder.LengthFieldBasedStringFrame()
			if decodeErr != nil {
				err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
				return
			}
			serverAddr := ""
			serverAddr, decodeErr = decoder.LengthFieldBasedStringFrame()
			if decodeErr != nil {
				err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
				return
			}
			meta.Configuration.Servers = append(meta.Configuration.Servers, Server{
				Suffrage: ServerSuffrage(serverSuffrage),
				Id:       serverId,
				Address:  serverAddr,
			})
		}
	}
	meta.ConfigurationIndex, decodeErr = decoder.Uint64()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	meta.Size, decodeErr = decoder.Uint64()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	meta.CRC, decodeErr = decoder.LengthFieldBasedFrame()
	if decodeErr != nil {
		err = errors.ServiceError("decode snapshot meta failed").WithCause(decodeErr)
		return
	}
	return
}

type SnapshotStore interface {
	Create(index uint64, term uint64, configuration Configuration, configurationIndex uint64) (sink SnapshotSink, err error)
	List() (metas []*SnapshotMeta, err error)
	Open(id string) (meta *SnapshotMeta, reader io.ReadCloser, err error)
}

//+--------------------------------------------------------------------------------------------------------------------+

const (
	tmpSnapshotFileSuffix = ".tmp"
	snapshotFileSuffix    = ".spt"
)

func FileSnapshotStore(dir string) (store SnapshotStore, err error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		err = errors.ServiceError("create file snapshot store failed").WithCause(errors.ServiceError("dir is empty"))
		return
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		err = errors.ServiceError("create file snapshot store failed").WithCause(err)
		return
	}
	exist := files.ExistFile(dir)
	if !exist {
		if mkdirErr := os.MkdirAll(dir, 0640); mkdirErr != nil {
			err = errors.ServiceError("create file snapshot store failed").WithCause(mkdirErr)
			return
		}
	}
	store = &fileSnapshotStore{
		path: dir,
	}
	return
}

type fileSnapshotStore struct {
	path string
}

func (store *fileSnapshotStore) Create(index uint64, term uint64, configuration Configuration, configurationIndex uint64) (sink SnapshotSink, err error) {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	id := fmt.Sprintf("%d-%d-%d", term, index, msec)

	path := filepath.Join(store.path, id+tmpSnapshotFileSuffix)
	exist := files.ExistFile(path)
	if exist {
		err = errors.ServiceError("create snapshot sink failed").WithCause(errors.ServiceError("file is exist"))
		return
	}
	file, createFileErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0640)
	if createFileErr != nil {
		err = errors.ServiceError("create snapshot sink failed").WithCause(createFileErr)
		return
	}

	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))
	multi := io.MultiWriter(file, stateHash)

	fileSink := &FileSnapshotSink{
		path: path,
		meta: &SnapshotMeta{
			Id:                 id,
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
			Size:               0,
		},
		file:      file,
		stateHash: stateHash,
		buffered:  bufio.NewWriter(multi),
		closed:    false,
	}

	// write meta
	if _, metaErr := fileSink.meta.WriteTo(fileSink); err != nil {
		err = errors.ServiceError("create snapshot sink failed").WithCause(metaErr)
		return
	}

	sink = fileSink
	return
}

func (store *fileSnapshotStore) List() (metas []*SnapshotMeta, err error) {
	entries, readErr := os.ReadDir(store.path)
	if readErr != nil {
		err = errors.ServiceError("list snapshot failed").WithCause(readErr)
		return
	}
	if entries == nil || len(entries) == 0 {
		return
	}
	metas = make([]*SnapshotMeta, 0, 1)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		if filepath.Ext(filename) != snapshotFileSuffix {
			continue
		}
		path := filepath.Join(store.path, filename)
		if !files.ExistFile(path) {
			continue
		}
		file, openFileErr := os.OpenFile(path, os.O_RDONLY, 0640)
		if openFileErr != nil {
			err = errors.ServiceError("list snapshot failed").WithCause(openFileErr)
			return
		}
		meta := &SnapshotMeta{}
		decodeErr := meta.Decode(file)
		if decodeErr != nil {
			_ = file.Close()
			err = errors.ServiceError("list snapshot failed").WithCause(decodeErr)
			return
		}
		msec, msecErr := strconv.ParseInt(filename[strings.LastIndexByte(filename, '-')+1:strings.LastIndexByte(filename, '.')], 10, 64)
		if msecErr == nil {
			meta.CreateAT = time.Unix(0, msec*int64(time.Millisecond))
		}
		metas = append(metas, meta)
	}
	if len(metas) == 0 {
		return
	}
	sort.Sort(SnapshotMetas(metas))
	return
}

func (store *fileSnapshotStore) Open(id string) (meta *SnapshotMeta, reader io.ReadCloser, err error) {
	path := filepath.Join(store.path, id+snapshotFileSuffix)
	exist := files.ExistFile(path)
	if !exist {
		err = errors.ServiceError("open snapshot failed").WithCause(errors.ServiceError("file is not exist"))
		return
	}
	file, openFileErr := os.OpenFile(path, os.O_RDONLY, 0640)
	if openFileErr != nil {
		err = errors.ServiceError("open snapshot failed").WithCause(openFileErr)
		return
	}
	meta = &SnapshotMeta{}
	readMetaErr := meta.Decode(file)
	if readMetaErr != nil {
		err = errors.ServiceError("open snapshot failed").WithCause(readMetaErr)
		return
	}
	msec, msecErr := strconv.ParseInt(path[strings.LastIndexByte(path, '-')+1:strings.LastIndexByte(path, '.')], 10, 64)
	if msecErr == nil {
		meta.CreateAT = time.Unix(0, msec*int64(time.Millisecond))
	}
	reader = file
	return
}

type FileSnapshotSink struct {
	path      string
	meta      *SnapshotMeta
	file      *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer
	closed    bool
}

func (sink *FileSnapshotSink) Write(p []byte) (n int, err error) {
	if sink.closed {
		err = errors.ServiceError("snapshot sink write failed").WithCause(errors.ServiceError("snapshot sink was closed"))
		return
	}
	n, err = sink.buffered.Write(p)
	if err != nil {
		err = errors.ServiceError("snapshot sink write failed").WithCause(err)
		return
	}
	sink.meta.Size = sink.meta.Size + uint64(n)
	return
}

func (sink *FileSnapshotSink) Close() (err error) {
	if sink.closed {
		err = errors.ServiceError("snapshot sink close failed").WithCause(errors.ServiceError("snapshot sink was closed"))
		return
	}
	sink.closed = true
	if flushErr := sink.buffered.Flush(); flushErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(flushErr)
		return
	}
	_, seekErr := sink.file.Seek(0, 0)
	if seekErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(seekErr)
		return
	}
	// update crc
	sink.meta.CRC = sink.stateHash.Sum(nil)
	// update size
	_, writeMetaErr := sink.meta.WriteTo(sink.file)
	if writeMetaErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(writeMetaErr)
		return
	}
	// sync
	syncErr := sink.file.Sync()
	if syncErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(syncErr)
		return
	}
	closeErr := sink.file.Close()
	if closeErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(closeErr)
		return
	}
	// rename
	statePath := sink.path[:strings.LastIndexByte(sink.path, '.')] + snapshotFileSuffix
	renameErr := os.Rename(sink.path, statePath)
	if renameErr != nil {
		sink.remove()
		err = errors.ServiceError("snapshot sink close failed").WithCause(renameErr)
		return
	}
	return
}

func (sink *FileSnapshotSink) Id() (id string) {
	id = sink.meta.Id
	return
}

func (sink *FileSnapshotSink) Cancel() (err error) {
	if sink.closed {
		return nil
	}
	sink.closed = true
	sink.remove()
	return
}

func (sink *FileSnapshotSink) remove() {
	_ = sink.file.Close()
	_ = os.Remove(sink.path)
}
