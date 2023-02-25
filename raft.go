package raft

import (
	"context"
	"github.com/aacfactory/errors"
	"time"
)

func New(options Options) (r Raft, err error) {
	err = options.Verify()
	if err != nil {
		err = errors.ServiceError("new raft failed").WithCause(err)
		return
	}

	return
}

type Raft interface {
	// Run start node
	//
	// check cluster was serving, when serving, then call leader to add this node,
	// when not serving, then boot a cluster.
	Run(ctx context.Context) (err error)
	// Close shutdown node
	//
	// check leader, when this node is leader, then just shutdown,
	// when this nod is not leader, then call leader to remove this node and shutdown.
	Close(ctx context.Context) (err error)
	// Apply make fsm to apply a log
	Apply(ctx context.Context, key []byte, body []byte, timeout time.Duration) (result []byte, err error)
}
