package mem

import (
	"sync"
)

type mem struct {
	sync.Mutex
	mp map[string]int
}
