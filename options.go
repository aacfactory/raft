package raft

import (
	"fmt"
	"github.com/aacfactory/errors"
	"strings"
	"time"
)

func DefaultOptions(id string, fsm FSM, store Store, snapshot SnapshotStore, addresses Addresses, transport Transport) Options {
	return Options{
		Id:        id,
		Addresses: addresses,
		Nonvoter:  false,
		TLS:       nil,
		Leader:    DefaultLeaderOptions(),
		Snapshot:  DefaultSnapshotOptions(snapshot),
		Store:     store,
		FSM:       fsm,
		Transport: transport,
	}
}

type Options struct {
	Id        string
	Addresses Addresses
	Nonvoter  bool
	TLS       TLS
	Leader    LeaderOptions
	Snapshot  SnapshotOptions
	Store     Store
	FSM       FSM
	Transport Transport
}

func (options Options) Verify() (err error) {
	errs := errors.MakeErrors()
	options.Id = strings.TrimSpace(options.Id)
	if options.Id == "" {
		errs.Append(errors.ServiceError("id is required"))
	}
	if options.Addresses == nil {
		errs.Append(errors.ServiceError("members are required"))
	}
	leaderErr := options.Leader.Verify()
	if leaderErr != nil {
		errs.Append(leaderErr)
	}
	if options.Store == nil {
		errs.Append(errors.ServiceError("store is required"))
	}
	if options.FSM == nil {
		errs.Append(errors.ServiceError("fsm is required"))
	}
	if options.Transport == nil {
		errs.Append(errors.ServiceError("transport is required"))
	}
	if len(errs) > 0 {
		err = errors.ServiceError("verify options failed").WithCause(errs.Error())
	}
	return
}

func DefaultLeaderOptions() LeaderOptions {
	return LeaderOptions{
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}

type LeaderOptions struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	// LeaderLeaseTimeout
	// 用于控制“租约”的持续时间
	// 作为领导者而无法联系法定人数
	// 个节点。如果我们在没有联系的情况下达到这个间隔，我们将
	// 辞去领导职务。
	LeaderLeaseTimeout time.Duration
}

func (options LeaderOptions) Verify() (err error) {
	errs := errors.MakeErrors()
	if options.HeartbeatTimeout < 5*time.Millisecond {
		errs.Append(errors.ServiceError("heartbeat timeout is too low"))
	}
	if options.ElectionTimeout < 5*time.Millisecond {
		errs.Append(errors.ServiceError("election timeout is too low"))
	}
	if options.CommitTimeout < 1*time.Millisecond {
		errs.Append(errors.ServiceError("commit timeout is too low"))
	}
	if options.LeaderLeaseTimeout < 5*time.Millisecond {
		errs.Append(errors.ServiceError("leader lease timeout is too low"))
	}
	if options.LeaderLeaseTimeout > options.HeartbeatTimeout {
		errs.Append(errors.ServiceError(fmt.Sprintf("leader lease timeout (%s) cannot be larger than heartbeat timeout (%s)", options.LeaderLeaseTimeout, options.HeartbeatTimeout)))
	}
	if options.ElectionTimeout < options.HeartbeatTimeout {
		errs.Append(errors.ServiceError(fmt.Sprintf("election timeout (%s) must be equal or greater than heartbeat Timeout (%s)", options.ElectionTimeout, options.HeartbeatTimeout)))
	}
	if len(errs) > 0 {
		err = errors.ServiceError("verify leader options failed").WithCause(errs.Error())
	}
	return
}
