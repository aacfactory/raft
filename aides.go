package raft

import (
	"sync"
	"sync/atomic"
)

type aides struct {
	currentTerm       uint64
	commitIndex       uint64
	lastApplied       uint64
	lastLock          sync.Mutex
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	lastLogIndex      uint64
	lastLogTerm       uint64
	routinesGroup     sync.WaitGroup
	state             State
}

func (ads *aides) getState() State {
	stateAddr := (*uint32)(&ads.state)
	return State(atomic.LoadUint32(stateAddr))
}

func (ads *aides) setState(s State) {
	stateAddr := (*uint32)(&ads.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (ads *aides) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&ads.currentTerm)
}

func (ads *aides) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&ads.currentTerm, term)
}

func (ads *aides) getLastLog() (index, term uint64) {
	ads.lastLock.Lock()
	index = ads.lastLogIndex
	term = ads.lastLogTerm
	ads.lastLock.Unlock()
	return
}

func (ads *aides) setLastLog(index, term uint64) {
	ads.lastLock.Lock()
	ads.lastLogIndex = index
	ads.lastLogTerm = term
	ads.lastLock.Unlock()
}

func (ads *aides) getLastSnapshot() (index, term uint64) {
	ads.lastLock.Lock()
	index = ads.lastSnapshotIndex
	term = ads.lastSnapshotTerm
	ads.lastLock.Unlock()
	return
}

func (ads *aides) setLastSnapshot(index, term uint64) {
	ads.lastLock.Lock()
	ads.lastSnapshotIndex = index
	ads.lastSnapshotTerm = term
	ads.lastLock.Unlock()
}

func (ads *aides) getCommitIndex() uint64 {
	return atomic.LoadUint64(&ads.commitIndex)
}

func (ads *aides) setCommitIndex(index uint64) {
	atomic.StoreUint64(&ads.commitIndex, index)
}

func (ads *aides) getLastApplied() uint64 {
	return atomic.LoadUint64(&ads.lastApplied)
}

func (ads *aides) setLastApplied(index uint64) {
	atomic.StoreUint64(&ads.lastApplied, index)
}

func (ads *aides) goFunc(f func()) {
	ads.routinesGroup.Add(1)
	go func() {
		defer ads.routinesGroup.Done()
		f()
	}()
}

func (ads *aides) waitShutdown() {
	ads.routinesGroup.Wait()
}

func (ads *aides) getLastIndex() uint64 {
	ads.lastLock.Lock()
	defer ads.lastLock.Unlock()
	return max(ads.lastLogIndex, ads.lastSnapshotIndex)
}

func (ads *aides) getLastEntry() (uint64, uint64) {
	ads.lastLock.Lock()
	defer ads.lastLock.Unlock()
	if ads.lastLogIndex >= ads.lastSnapshotIndex {
		return ads.lastLogIndex, ads.lastLogTerm
	}
	return ads.lastSnapshotIndex, ads.lastSnapshotTerm
}
