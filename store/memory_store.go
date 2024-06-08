package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

type memoryStore struct {
	mtx sync.RWMutex
	m   map[uint64][]byte
	ttl map[uint64]uint64

	expire *time.Ticker
}

const defaultExpireInterval = 30 * time.Second

// TODO: ttl対応
func NewMemotyStore() Store {
	return &memoryStore{
		mtx:    sync.RWMutex{},
		m:      map[uint64][]byte{},
		ttl:    nil,
		expire: nil,
	}
}

func (s *memoryStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h, err := s.hash(key)
	if err != nil {
		return nil, err
	}

	v, ok := s.m[h]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (s *memoryStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return err
	}
	s.m[h] = value
	return nil
}

func (s *memoryStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return err
	}
	delete(s.m, h)
	return nil
}

func (s *memoryStore) Exists(ctx context.Context, key []byte) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h, err := s.hash(key)
	if err != nil {
		return false, err
	}
	_, ok := s.m[h]
	return ok, nil
}

func (s *memoryStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTxn()

	err := f(ctx, txn)
	if err != nil {
		return err
	}

	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.m[op.h] = op.v
		case OpTypeDelete:
			delete(s.m, op.h)
		default:
			return ErrUnknownOp
		}
	}
	return nil
}

// encodeを待つより先にUnlockしても良い
func (s *memoryStore) Snapshot() (io.ReadWriter, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(s.m); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *memoryStore) Restore(buf io.Reader) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := gob.NewDecoder(buf).Decode(&s.m); err != nil {
		return err
	}
	return nil
}

// Closeはやることない
func (s *memoryStore) Close() error {
	return nil
}

func (s *memoryStore) hash(key []byte) (uint64, error) {
	h := murmur3.New64()
	if _, err := h.Write(key); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func (s *memoryStore) NewTxn() *memoryStoreTxn {
	return &memoryStoreTxn{
		mtx: &sync.RWMutex{},
		m:   map[uint64][]byte{},
		ops: []memOp{},
		s:   s,
	}
}

type OpType uint8

const (
	OpTypePut OpType = iota
	OpTypeDelete
)

type memOp struct {
	opType OpType
	h      uint64
	v      []byte
	ttl    int64
}

type memoryStoreTxn struct {
	mtx *sync.RWMutex
	m   map[uint64][]byte
	ops []memOp
	s   *memoryStore
}

func (t *memoryStoreTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	h, err := t.s.hash(key)
	if err != nil {
		return nil, err
	}

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	v, ok := t.m[h]
	if ok && !bytes.Equal(v, Tombstone) {
		return v, nil
	}

	// Returns NotFound if deleted during transaction and then get
	if bytes.Equal(v, Tombstone) {
		return nil, ErrKeyNotFound
	}

	v, ok = t.s.m[h]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (t *memoryStoreTxn) Put(ctx context.Context, key []byte, value []byte) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return err
	}

	t.m[h] = value
	t.ops = append(t.ops, memOp{
		h:      h,
		opType: OpTypePut,
		v:      value,
	})
	return nil
}

func (t *memoryStoreTxn) Delete(ctx context.Context, key []byte) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return err
	}

	t.m[h] = Tombstone
	t.ops = append(t.ops, memOp{
		h:      h,
		opType: OpTypeDelete,
	})

	return nil
}

func (t *memoryStoreTxn) Exists(ctx context.Context, key []byte) (bool, error) {
	h, err := t.s.hash(key)
	if err != nil {
		return false, err
	}

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	_, ok := t.m[h]
	if ok {
		return true, nil
	}

	// Returns false if deleted during transaction
	for _, op := range t.ops {
		if op.h != h {
			continue
		}
		if op.opType == OpTypeDelete {
			return false, nil
		}
	}

	_, ok = t.s.m[h]
	return ok, nil
}
