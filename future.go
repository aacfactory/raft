package raft

import (
	"context"
	"fmt"
)

type Future[R any] interface {
	Wait(ctx context.Context) (result R, err error)
}

var (
	FutureWaitTimeoutErr = fmt.Errorf("future wait timeout")
)

type Promise[R any] struct {
	rch chan R
	ech chan error
}

func (p *Promise[R]) Wait(ctx context.Context) (result R, err error) {
	select {
	case <-ctx.Done():
		err = FutureWaitTimeoutErr
		break
	case result = <-p.rch:
		break
	case err = <-p.ech:
		break
	}
	close(p.rch)
	close(p.ech)
	return
}

func (p *Promise[R]) Succeed(result R) {
	p.rch <- result
}

func (p *Promise[R]) Failed(err error) {
	p.ech <- err
}
