package raft

import (
	"context"
	"time"
)

type Node struct {
	aides
}

func (node *Node) Run(ctx context.Context) (err error) {

	return
}

func (node *Node) Close(ctx context.Context) (err error) {

	return
}

func (node *Node) Apply(ctx context.Context, key []byte, body []byte, timeout time.Duration) (result []byte, err error) {

	return
}
