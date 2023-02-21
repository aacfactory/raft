package raft

import (
	"fmt"
	"time"
)

type Config struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int

	// TrailingLogs controls how many logs we leave after a snapshot. This is used
	// so that we can quickly replay logs on a follower instead of being forced to
	// send an entire snapshot. The value passed here is the initial setting used.
	// This can be tuned during operation using ReloadConfig.
	TrailingLogs uint64

	// SnapshotInterval controls how often we check if we should perform a
	// snapshot. We randomly stagger between this value and 2x this value to avoid
	// the entire cluster from performing a snapshot at once. The value passed
	// here is the initial setting used. This can be tuned during operation using
	// ReloadConfig.
	SnapshotInterval time.Duration

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshotting by
	// replaying a small set of logs instead. The value passed here is the initial
	// setting used. This can be tuned during operation using ReloadConfig.
	SnapshotThreshold uint64

	// LeaderLeaseTimeout is used to control how long the "lease" lasts
	// for being the leader without being able to contact a quorum
	// of nodes. If we reach this interval without contact, we will
	// step down as leader.
	LeaderLeaseTimeout time.Duration

	LocalID string

	// NotifyCh is used to provide a channel that will be notified of leadership
	// changes. Raft will block writing to this channel, so it should either be
	// buffered or aggressively consumed.
	NotifyCh chan<- bool

	// NoSnapshotRestoreOnStart controls if raft will restore a snapshot to the
	// FSM on start. This is useful if your FSM recovers from other mechanisms
	// than raft snapshotting. Snapshot metadata will still be used to initialize
	// raft's configuration and index values.
	NoSnapshotRestoreOnStart bool

	// skipStartup allows NewRaft() to bypass all background work goroutines
	skipStartup bool
}

// ReloadableConfig is the subset of Config that may be reconfigured during
// runtime using raft.ReloadConfig. We choose to duplicate fields over embedding
// or accepting a Config but only using specific fields to keep the API clear.
// Reconfiguring some fields is potentially dangerous so we should only
// selectively enable it for fields where that is allowed.
type ReloadableConfig struct {
	// TrailingLogs controls how many logs we leave after a snapshot. This is used
	// so that we can quickly replay logs on a follower instead of being forced to
	// send an entire snapshot. The value passed here updates the setting at runtime
	// which will take effect as soon as the next snapshot completes and truncation
	// occurs.
	TrailingLogs uint64

	// SnapshotInterval controls how often we check if we should perform a snapshot.
	// We randomly stagger between this value and 2x this value to avoid the entire
	// cluster from performing a snapshot at once.
	SnapshotInterval time.Duration

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshots when we can
	// just replay a small set of logs.
	SnapshotThreshold uint64

	// HeartbeatTimeout specifies the time in follower state without
	// a leader before we attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without
	// a leader before we attempt an election.
	ElectionTimeout time.Duration
}

// apply sets the reloadable fields on the passed Config to the values in
// `ReloadableConfig`. It returns a copy of Config with the fields from this
// ReloadableConfig set.
func (rc *ReloadableConfig) apply(to Config) Config {
	to.TrailingLogs = rc.TrailingLogs
	to.SnapshotInterval = rc.SnapshotInterval
	to.SnapshotThreshold = rc.SnapshotThreshold
	to.HeartbeatTimeout = rc.HeartbeatTimeout
	to.ElectionTimeout = rc.ElectionTimeout
	return to
}

// fromConfig copies the reloadable fields from the passed Config.
func (rc *ReloadableConfig) fromConfig(from Config) {
	rc.TrailingLogs = from.TrailingLogs
	rc.SnapshotInterval = from.SnapshotInterval
	rc.SnapshotThreshold = from.SnapshotThreshold
	rc.HeartbeatTimeout = from.HeartbeatTimeout
	rc.ElectionTimeout = from.ElectionTimeout
}

// DefaultConfig returns a Config with usable defaults.
func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}

// ValidateConfig is used to validate a sane configuration
func ValidateConfig(config *Config) error {
	if len(config.LocalID) == 0 {
		return fmt.Errorf("LocalID cannot be empty")
	}
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("HeartbeatTimeout is too low")
	}
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("ElectionTimeout is too low")
	}
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("CommitTimeout is too low")
	}
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries is too large")
	}
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("SnapshotInterval is too low")
	}
	if config.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if config.LeaderLeaseTimeout > config.HeartbeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout (%s) cannot be larger than heartbeat timeout (%s)", config.LeaderLeaseTimeout, config.HeartbeatTimeout)
	}
	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout (%s) must be equal or greater than Heartbeat Timeout (%s)", config.ElectionTimeout, config.HeartbeatTimeout)
	}
	return nil
}
