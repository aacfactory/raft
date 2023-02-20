package raft

type MetaStore interface {
	Set(key []byte, val []byte) (err error)
	Get(key []byte) (val []byte, err error)
}
