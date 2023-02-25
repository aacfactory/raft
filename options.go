package raft

import (
	"fmt"
	"github.com/aacfactory/errors"
	"strings"
	"time"
)

func NewOptionsBuilder() (builder *OptionsBuilder) {
	builder = &OptionsBuilder{
		options: &Options{
			Id:        "",
			Addresses: nil,
			Nonvoter:  false,
			TLS:       nil,
			Leader: LeaderOptions{
				HeartbeatTimeout: 1000 * time.Millisecond,
				ElectionTimeout:  1000 * time.Millisecond,
				CommitTimeout:    50 * time.Millisecond,
				LeaseTimeout:     500 * time.Millisecond,
			},
			Snapshot: SnapshotOptions{
				Store:                    nil,
				TrailingLogs:             10240,
				Interval:                 120 * time.Second,
				Threshold:                8192,
				NoSnapshotRestoreOnStart: false,
			},
			Store:     nil,
			FSM:       nil,
			Transport: nil,
		},
	}
	return
}

type OptionsBuilder struct {
	options *Options
}

func (builder *OptionsBuilder) Id(id string) {
	builder.options.Id = id
}

func (builder *OptionsBuilder) Fsm(fsm FSM) {
	builder.options.FSM = fsm
}

func (builder *OptionsBuilder) Store(store Store) {
	builder.options.Store = store
}

func (builder *OptionsBuilder) SnapshotStore(store SnapshotStore) {
	builder.options.Snapshot.Store = store
}

func (builder *OptionsBuilder) SnapshotTrailingLogs(trailingLogs uint64) {
	builder.options.Snapshot.TrailingLogs = trailingLogs
}

func (builder *OptionsBuilder) SnapshotThreshold(threshold uint64) {
	builder.options.Snapshot.Threshold = threshold
}

func (builder *OptionsBuilder) SnapshotInterval(interval time.Duration) {
	builder.options.Snapshot.Interval = interval
}

func (builder *OptionsBuilder) SnapshotNotRestoreOnStart(yes bool) {
	builder.options.Snapshot.NoSnapshotRestoreOnStart = yes
}

func (builder *OptionsBuilder) Addresses(addresses Addresses) {
	builder.options.Addresses = addresses
}

func (builder *OptionsBuilder) Transport(transport Transport) {
	builder.options.Transport = transport
}

func (builder *OptionsBuilder) Nonvoter(yes bool) {
	builder.options.Nonvoter = yes
}

func (builder *OptionsBuilder) TLS(v TLS) {
	builder.options.TLS = v
}

func (builder *OptionsBuilder) LeaderHeartbeatTimeout(timeout time.Duration) {
	builder.options.Leader.HeartbeatTimeout = timeout
}

func (builder *OptionsBuilder) LeaderElectionTimeout(timeout time.Duration) {
	builder.options.Leader.ElectionTimeout = timeout
}

func (builder *OptionsBuilder) LeaderCommitTimeout(timeout time.Duration) {
	builder.options.Leader.CommitTimeout = timeout
}

func (builder *OptionsBuilder) LeaderLeaseTimeout(timeout time.Duration) {
	builder.options.Leader.LeaseTimeout = timeout
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

type LeaderOptions struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	// LeaseTimeout
	// 用于控制“租约”的持续时间
	// 作为领导者而无法联系法定人数
	// 个节点。如果我们在没有联系的情况下达到这个间隔，我们将
	// 辞去领导职务。
	LeaseTimeout time.Duration
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
	if options.LeaseTimeout < 5*time.Millisecond {
		errs.Append(errors.ServiceError("leader lease timeout is too low"))
	}
	if options.LeaseTimeout > options.HeartbeatTimeout {
		errs.Append(errors.ServiceError(fmt.Sprintf("leader lease timeout (%s) cannot be larger than heartbeat timeout (%s)", options.LeaseTimeout, options.HeartbeatTimeout)))
	}
	if options.ElectionTimeout < options.HeartbeatTimeout {
		errs.Append(errors.ServiceError(fmt.Sprintf("election timeout (%s) must be equal or greater than heartbeat Timeout (%s)", options.ElectionTimeout, options.HeartbeatTimeout)))
	}
	if len(errs) > 0 {
		err = errors.ServiceError("verify leader options failed").WithCause(errs.Error())
	}
	return
}

type SnapshotOptions struct {
	Store SnapshotStore
	// TrailingLogs控制快照后留下的日志数量。这是为了让我们可以快速回放跟踪者的日志，而不是被迫发送整个快照。此处传递的值是使用的初始设置。这可以在运行期间使用ReloadConfig进行调整。
	TrailingLogs uint64
	// SnapshotThreshold控制在执行快照之前必须有多少未完成的日志。这是为了通过重放一小组日志来防止过度快照。此处传递的值是使用的初始设置。这可以在运行期间使用ReloadConfig进行调整。
	Threshold                uint64
	Interval                 time.Duration
	NoSnapshotRestoreOnStart bool
}

func (options SnapshotOptions) Verify() (err error) {
	errs := errors.MakeErrors()
	if options.Store == nil {
		errs.Append(errors.ServiceError("snapshot store is too low"))
	}
	if options.Interval < 5*time.Millisecond {
		errs.Append(errors.ServiceError("snapshot interval is too low"))
	}
	err = errs.Error()
	if err != nil {
		err = errors.ServiceError("verify snapshot options failed").WithCause(err)
	}
	return
}
