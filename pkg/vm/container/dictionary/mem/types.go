package mem

import "sync"

type mem struct {
	sync.RWMutex
	mp map[string]interface{}
}
