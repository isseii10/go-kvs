package raft

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/hashicorp/raft"
	"github.com/isseii10/go-kvs/store"
)

var ErrUnknownOp = errors.New("unknown op")

type Op int

const (
	Put Op = iota
	Del
)

type KVCmd struct {
	Op  Op     `json:"op"`
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

type StateMachine struct {
	store store.Store
}

func NewStateMachine(store store.Store) *StateMachine {
	return &StateMachine{store: store}
}

func (s *StateMachine) Apply(log *raft.Log) any {
	ctx := context.Background()

	cmd := KVCmd{}
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	return s.handleRequest(ctx, cmd)
}

func (s *StateMachine) Restore(rc io.ReadCloser) error {
	return s.store.Restore(rc)
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &KVSnapshot{rc}, nil
}

func (s *StateMachine) handleRequest(ctx context.Context, cmd KVCmd) error {
	switch cmd.Op {
	case Put:
		return s.store.Put(ctx, cmd.Key, cmd.Val)
	case Del:
		return s.store.Delete(ctx, cmd.Key)
	default:
		return ErrUnknownOp
	}
}
