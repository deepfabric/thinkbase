package dictVector

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/container/store"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type DictVector interface {
	Destroy() error

	PopKey() (string, error)

	Delete(string) error

	Pops(string, int) (value.Array, error)

	PopsArray(string, int) (value.Array, error)

	Push(string, value.Array) error
}

const (
	Scale = 16
)

type dictVector struct {
	sync.RWMutex
	size  int
	limit int
	name  string
	db    store.Store
	mp    map[string]value.Array
}
