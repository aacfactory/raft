package raft

import (
	"io"
)

type FSM interface {
	Apply(key []byte, data []byte) (result []byte, err error)
	Snapshot() (FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
}

type SnapshotSink interface {
	io.WriteCloser
	Id() string
	Cancel() error
}

type FSMSnapshot interface {
	Persist(sink SnapshotSink) error
	Release()
}
