package mem

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
)

type mem struct {
	sync.RWMutex
	vs []vector.Vector
}
