package store

import (
	"context"
	"errors"
	"io"
)

var (
	ErrKeyNotFound  = errors.New("not found key")
	ErrUnknownOp    = errors.New("unknown op")
	ErrNotSupported = errors.New("not supported")
)

var Tombstone = []byte{0x00}

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	// Txn トランザクション用
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}
