package store

import (
	"github.com/deepfabric/thinkkv/pkg/engine"
)

type Iterator interface {
	Next() error
	Valid() bool
	Close() error
	Seek([]byte) error
	Key() []byte
	Value() ([]byte, error)
}

type Store interface {
	Sync() error
	Destroy() error

	// kv
	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
	GetOrSet([]byte, []byte) (bool, error)
	NewIterator([]byte) (Iterator, error)

	// list
	Lkey() ([]byte, error)
	Llen([]byte) (uint64, error)
	Lpush([]byte, []byte) error
	Lpop([]byte) ([]byte, error)
	Lhead([]byte) ([]byte, error)
	Lpops([]byte, int) ([][]byte, error)
}

type store struct {
	name string
	db   engine.DB
}

type iterator struct {
	itr engine.Iterator
}
