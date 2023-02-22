package raft

import (
	"io"
)

type FSM interface {
	Apply(data []byte) (result []byte, err error)
	Read(command []byte, argument []byte) (result []byte, err error)
	Snapshot() (FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
	Close() (err error)
}

type FSMSnapshot interface {
	Persist(sink SnapshotSink) error
	Release()
}
