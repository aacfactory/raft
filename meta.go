package raft

type MetaStore interface {
	Set(key []byte, val []byte) (err error)
	Get(key []byte) (val []byte, err error)
}

func FileMetaStore(path string) (meta MetaStore) {
	// todo
	meta = &fileMetaStore{}
	panic("todo")
	return
}

type fileMetaStore struct {
}

func (meta *fileMetaStore) Set(key []byte, val []byte) (err error) {
	//TODO implement me
	panic("implement me")
}

func (meta *fileMetaStore) Get(key []byte) (val []byte, err error) {
	//TODO implement me
	panic("implement me")
}
