package mem

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type mem struct {
	sync.RWMutex
	a value.Array
}
