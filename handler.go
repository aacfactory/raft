package raft

import "github.com/aacfactory/rings"

type Handlers struct {
	handlers *rings.HashRing[*Handler]
}

func (handlers *Handlers) Dispatch(request *AppendEntriesRequest) (response *AppendEntriesResponse) {
	handler, got := handlers.handlers.Get(request.Key)
	if !got {
		// todo return failed
		return
	}
	response = handler.Handle(request)
	return
}

type Handler struct {
	// ch chan Future
}

func (handler *Handler) Key() (key string) {

	return
}

func (handler *Handler) Handle(request *AppendEntriesRequest) (response *AppendEntriesResponse) {

	return
}
