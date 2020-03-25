package mem

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type mem struct {
	sync.RWMutex
	mp map[string]value.Array
}
