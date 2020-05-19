package dictionary

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/container/store"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type Dictionary interface {
	Destroy() error

	GetOrSet(value.Value) (bool, error) // 如果value是set的则返回false，如果是加载的则返回true
}

type dictionary struct {
	sync.RWMutex
	size  int
	limit int
	name  string
	db    store.Store
	mp    map[string]struct{}
}
