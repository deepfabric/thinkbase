package mem

import (
	"sync"
)

type mem struct {
	sync.Mutex
}
