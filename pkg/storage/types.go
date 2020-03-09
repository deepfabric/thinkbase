package storage

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

const System = "_system"

// system -> table list
type Database interface {
	Close() error
	Tables() ([]string, error)
	Table(string) (Table, error)
}

// id -> count
// id.A -> attribute list
// row store: id.R.row number -> value
// column store: id.C.attr's name.row number -> value
// inverted index: id.S.attr's name.value.row number
type Table interface {
	Metadata() []string

	AddTuple(map[string]interface{}) error
	AddTuples([]map[string]interface{}) error

	GetTupleCount() (int, error)
	GetTuple(int, []string) (value.Tuple, error)
	GetTuples(int, int, []string) ([]value.Tuple, error)
	GetTuplesByIndex([]int, []string) ([]value.Tuple, error)

	GetAttributeByLimit(string, int, int) (value.Attribute, error)
}

type DB interface {
	Close() error
	NewBatch() (Batch, error)
	NewIterator([]byte) (Iterator, error)

	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
}

type Batch interface {
	Cancel() error
	Commit() error
	Del([]byte) error
	Set([]byte, []byte) error
}

type Iterator interface {
	Next() error
	Valid() bool
	Close() error
	Seek([]byte) error
	Key() []byte
	Value() ([]byte, error)
}

type database struct {
	sync.Mutex
	db     DB
	ids    []string
	tables map[string]*table
}

type table struct {
	sync.RWMutex
	*database
	cnt   int64
	id    string
	attrs []string
	mp    map[string]struct{}
}
