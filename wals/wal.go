package wals

import (
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

func New(path string) (wal *WAL, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		err = fmt.Errorf("create wal failed cause get absolute representation of path, %v", err)
		return
	}
	file, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	if openErr != nil {
		err = fmt.Errorf("create wal failed cause open file, %v", openErr)
		return
	}
	stat, statErr := file.Stat()
	if statErr != nil {
		err = fmt.Errorf("create wal failed cause open file, %v", statErr)
		return
	}
	// todo key as uint64
	// todo tree load from first uncommitted
	// todo batch pool
	wal = &WAL{
		locker:      sync.RWMutex{},
		tree:        NewEntryTree(),
		path:        path,
		file:        file,
		fileSize:    uint64(stat.Size()),
		uncommitted: 0,
		closed:      false,
	}
	return
}

type WAL struct {
	locker      sync.RWMutex
	tree        *EntryTree
	path        string
	file        *os.File
	fileSize    uint64
	uncommitted uint64
	closed      bool
}

func (wal *WAL) Read(key []byte) (p []byte, has bool) {
	wal.locker.RLocker()
	p, _, has = wal.tree.Get(Key(xxhash.Sum64(key)))
	// todo, if not has, read file
	wal.locker.RUnlock()
	return
}

func (wal *WAL) Write(key []byte, p []byte) (err error) {
	wal.locker.Lock()
	if wal.closed {
		err = errors.Join(errors.New("write write failed"), errors.New("wal was closed"))
		wal.locker.Unlock()
		return
	}
	k := Key(xxhash.Sum64(key))
	entry := NewEntry(k, p)
	pos, writeErr := wal.append(entry)
	if writeErr != nil {
		err = errors.Join(errors.New("wal write failed"), writeErr)
		wal.locker.Unlock()
		return
	}
	syncErr := wal.file.Sync()
	if syncErr != nil {
		wal.locker.Unlock()
		err = errors.Join(errors.New("wal write failed"), syncErr)
		return
	}
	wal.tree.Set(k, p, pos)
	wal.uncommitted++
	wal.locker.Unlock()
	return
}

func (wal *WAL) append(p []byte) (pos uint64, err error) {
	pLen := len(p)
	for {
		n, writeErr := wal.file.Write(p)
		if writeErr != nil {
			err = errors.Join(errors.New("write into file failed"), writeErr)
			return
		}
		if n == pLen {
			break
		}
		p = p[n:]
	}
	wal.fileSize = wal.fileSize + uint64(pLen)
	pos = wal.fileSize - uint64(pLen)
	return
}

func (wal *WAL) update(p []byte, offset int64) (err error) {
	pLen := len(p)
	for {
		n, writeErr := wal.file.WriteAt(p, offset)
		if writeErr != nil {
			err = errors.Join(errors.New("write into file failed"), writeErr)
			return
		}
		if n == pLen {
			break
		}
		p = p[n:]
	}
	return
}

func (wal *WAL) Commit(key []byte) (err error) {
	wal.locker.Lock()
	entry, pos, has := wal.tree.Get(Key(xxhash.Sum64(key)))
	if !has {
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%s was not found", string(key)))
		wal.locker.Unlock()
		return
	}
	entry.Commit()
	writeErr := wal.update(entry, int64(pos))
	if writeErr != nil {
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("commit %s failed", string(key)), writeErr)
		wal.locker.Unlock()
		return
	}
	syncErr := wal.file.Sync()
	if syncErr != nil {
		wal.locker.Unlock()
		err = errors.Join(errors.New("wal commit log failed"), syncErr)
		return
	}
	wal.uncommitted--
	wal.locker.Unlock()
	return
}

func (wal *WAL) Close() (err error) {
	wal.locker.Lock()
	wal.closed = true
	syncErr := wal.file.Sync()
	if syncErr != nil {
		err = syncErr
	}
	closeErr := wal.file.Close()
	if closeErr != nil {
		if err != nil {
			err = errors.Join(closeErr, err)
		} else {
			err = closeErr
		}
	}
	if err != nil {
		err = errors.Join(errors.New("wal close failed"), err)
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL) Batch() (batch *BatchWALAppender) {
	batch = &BatchWALAppender{
		wal:  wal,
		data: make([]byte, 0, blockSize),
	}
	return
}

func (wal *WAL) CreateSnapshot(threshold uint64, trailing uint64, sink SnapshotSink) (err error) {
	wal.locker.Lock()
	if wal.uncommitted < threshold && threshold != 0 {
		cancelErr := sink.Cancel()
		if cancelErr != nil {
			err = errors.Join(errors.New("wal create snapshot failed"), cancelErr)
		}
		wal.locker.Unlock()
		return
	}
	if uint64(wal.tree.Len()) < trailing && trailing != 0 {
		cancelErr := sink.Cancel()
		if cancelErr != nil {
			err = errors.Join(errors.New("wal create snapshot failed"), cancelErr)
		}
		wal.locker.Unlock()
		return
	}
	_, seekErr := wal.file.Seek(0, 0)
	if seekErr != nil {
		err = errors.Join(errors.New("wal create snapshot failed"), seekErr)
		wal.locker.Unlock()
		return
	}
	got := 0
	keys := make([]Key, 0, 1)
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for {
		block := NewBlock()
		_, readErr := io.ReadFull(wal.file, block)
		if readErr != nil {
			if readErr != io.EOF {
				err = errors.Join(errors.New("wal create snapshot failed"), readErr)
				wal.locker.Unlock()
				return
			} else {
				break
			}
		}
		_, span := block.Header()
		remains := span - 1
		if remains == 0 {
			_, _ = buf.Write(block)
		} else {
			remainBlocks := make([]byte, remains*blockSize)
			_, readRemainsErr := io.ReadFull(wal.file, remainBlocks)
			if readRemainsErr != nil {
				if readRemainsErr != io.EOF {
					err = errors.Join(errors.New("wal create snapshot failed"), readRemainsErr)
					wal.locker.Unlock()
					return
				} else {
					break
				}
			}
			_, _ = buf.Write(remainBlocks)
		}
		entry := Entry(buf.Bytes())
		if !entry.Committed() {
			prev := buf.Bytes()[0 : buf.Len()-len(entry)]
			buf.Reset()
			_, _ = buf.Write(prev)
			continue
		}
		keys = append(keys, entry.Key())
		got++
		if got < 10 {
			continue
		}
		got = 0
		p := buf.Bytes()
		for {
			n, writeErr := sink.Write(p)
			if writeErr != nil {
				err = errors.Join(errors.New("wal create snapshot failed"), writeErr)
				wal.locker.Unlock()
				return
			}
			if n == len(p) {
				break
			}
			p = p[n:]
		}
		buf.Reset()
	}
	_, _ = wal.file.Seek(0, 0)
	for _, key := range keys {
		wal.tree.Remove(key)
	}
	wal.locker.Unlock()
	return
}

type BatchWALAppender struct {
	wal         *WAL
	data        []byte
	uncommitted uint64
}

func (batch *BatchWALAppender) Write(key []byte, p []byte) (index uint64) {
	entry := NewEntry(Key(xxhash.Sum64(key)), p)
	batch.data = append(batch.data, entry...)
	batch.uncommitted++
	return
}

func (batch *BatchWALAppender) Commit(keys ...[]byte) (err error) {
	if keys == nil || len(keys) == 0 {
		return
	}
	batch.wal.locker.Lock()
	items := entryTreeItems(make([]*entryTreeItem, 0, len(keys)))
	for _, key := range keys {
		k := Key(xxhash.Sum64(key))
		entry, pos, has := batch.wal.tree.Get(k)
		if !has {
			err = errors.Join(errors.New("commit batch failed"), errors.New("some key was not found"))
			batch.wal.locker.Unlock()
			return
		}
		items = append(items, &entryTreeItem{
			key:   k,
			value: entry,
			pos:   pos,
		})
	}
	if len(items) == 1 {
		items[0].value.Commit()
		updateErr := batch.wal.update(items[0].value, int64(items[0].pos))
		if updateErr != nil {
			err = errors.Join(errors.New("commit batch failed"), updateErr)
			batch.wal.locker.Unlock()
			return
		}
		syncErr := batch.wal.file.Sync()
		if syncErr != nil {
			err = errors.Join(errors.New("commit batch failed"), syncErr)
			batch.wal.locker.Unlock()
			return
		}
		batch.wal.locker.Unlock()
		return
	}
	sort.Sort(items)
	pos := items[0].pos
	segment := make([]byte, 0, blockSize)
	segment = append(segment, items[0].value...)
	for i := 1; i <= len(items); i++ {
		low := items[i-1].pos
		high := items[i].pos
		lowLen := uint64(len(items[i-1].value))
		if high-low == lowLen {
			segment = append(segment, items[i].value...)
			continue
		}
		lostPos := low + lowLen
		lostLen := high - lostPos
		lost := make([]byte, lostLen)
		n := 0
		for {
			nn, readErr := batch.wal.file.ReadAt(lost[n:], int64(lostPos))
			if readErr != nil {
				if readErr != io.EOF {
					err = errors.Join(errors.New("commit batch failed"), readErr)
					batch.wal.locker.Unlock()
					return
				} else {
					break
				}
			}
			if uint64(nn) == lostLen {
				break
			}
			n = n + nn
		}
		segment = append(segment, lost...)
		segment = append(segment, items[i].value...)
	}
	for _, item := range items {
		item.value.Commit()
	}
	updateErr := batch.wal.update(segment, int64(pos))
	if updateErr != nil {
		err = errors.Join(errors.New("commit batch failed"), updateErr)
		batch.wal.locker.Unlock()
		return
	}
	syncErr := batch.wal.file.Sync()
	if syncErr != nil {
		err = errors.Join(errors.New("commit batch failed"), syncErr)
		batch.wal.locker.Unlock()
		return
	}
	batch.wal.uncommitted = batch.wal.uncommitted - uint64(len(items))
	batch.wal.locker.Unlock()
	return
}

func (batch *BatchWALAppender) Flush() (err error) {
	if len(batch.data) == 0 {
		return
	}
	batch.wal.locker.Lock()
	if batch.wal.closed {
		err = errors.Join(errors.New("flush batch wrote failed"), errors.New("wal was closed"))
		batch.wal.locker.Unlock()
		return
	}
	pos, writeErr := batch.wal.append(batch.data)
	if writeErr != nil {
		err = errors.Join(errors.New("flush batch wrote failed"), writeErr)
		batch.wal.locker.Unlock()
		return
	}
	syncErr := batch.wal.file.Sync()
	if syncErr != nil {
		err = errors.Join(errors.New("flush batch wrote failed"), syncErr)
		batch.wal.locker.Unlock()
		return
	}
	entries := DecodeEntries(batch.data)
	for _, entry := range entries {
		batch.wal.tree.Set(entry.Key(), entry.Data(), pos)
		pos = pos + uint64(len(entry))
	}
	batch.wal.uncommitted = batch.wal.uncommitted + batch.uncommitted
	batch.wal.locker.Unlock()
	return
}

type SnapshotSink interface {
	io.WriteCloser
	Id() string
	Cancel() error
}
