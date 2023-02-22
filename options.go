package raft

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func DefaultOptions(id string, localStorePath string, fsm FSM, local string, memberAddrs ...string) Options {
	localStorePath = strings.TrimSpace(localStorePath)
	return Options{
		Id:        id,
		Addresses: DesignatedAddresses(local, memberAddrs...),
		Nonvoter:  false,
		TLS:       nil,
		Leader:    DefaultLeaderOptions(),
		Snapshot:  DefaultSnapshotOptions(FileSnapshotStore(filepath.Join(localStorePath, "snapshots"))),
		MetaStore: FileMetaStore(filepath.Join(localStorePath, "meta")),
		LogStore:  FileLogStore(filepath.Join(localStorePath, "logs")),
		FSM:       fsm,
		Transport: TcpTransport(),
	}
}

type Options struct {
	Id        string
	Addresses Addresses
	Nonvoter  bool
	TLS       TLS
	Leader    LeaderOptions
	Snapshot  SnapshotOptions
	MetaStore MetaStore
	LogStore  LogStore
	FSM       FSM
	Transport Transport
}

func (options Options) Verify() (err error) {
	errs := make([]error, 0, 1)
	options.Id = strings.TrimSpace(options.Id)
	if options.Id == "" {
		errs = append(errs, errors.New("id is required"))
	}
	if options.Addresses == nil {
		errs = append(errs, errors.New("members are required"))
	}
	leaderErr := options.Leader.Verify()
	if leaderErr != nil {
		errs = append(errs, leaderErr)
	}
	if options.MetaStore == nil {
		errs = append(errs, errors.New("meta store is required"))
	}
	if options.LogStore == nil {
		errs = append(errs, errors.New("log store is required"))
	}
	if options.FSM == nil {
		errs = append(errs, errors.New("fsm is required"))
	}
	if options.Transport == nil {
		errs = append(errs, errors.New("transport is required"))
	}
	if len(errs) > 0 {
		err = errors.Join(errs...)
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
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	LeaderLeaseTimeout time.Duration
}

func (options LeaderOptions) Verify() (err error) {
	errs := make([]error, 0, 1)
	if options.HeartbeatTimeout < 5*time.Millisecond {
		errs = append(errs, errors.New("HeartbeatTimeout is too low"))
	}
	if options.ElectionTimeout < 5*time.Millisecond {
		errs = append(errs, errors.New("ElectionTimeout is too low"))
	}
	if options.CommitTimeout < 1*time.Millisecond {
		errs = append(errs, errors.New("CommitTimeout is too low"))
	}
	if options.LeaderLeaseTimeout < 5*time.Millisecond {
		errs = append(errs, errors.New("LeaderLeaseTimeout is too low"))
	}
	if options.LeaderLeaseTimeout > options.HeartbeatTimeout {
		errs = append(errs, fmt.Errorf("LeaderLeaseTimeout (%s) cannot be larger than heartbeat timeout (%s)", options.LeaderLeaseTimeout, options.HeartbeatTimeout))
	}
	if options.ElectionTimeout < options.HeartbeatTimeout {
		errs = append(errs, fmt.Errorf("ElectionTimeout (%s) must be equal or greater than Heartbeat Timeout (%s)", options.ElectionTimeout, options.HeartbeatTimeout))
	}
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}
	return
}
