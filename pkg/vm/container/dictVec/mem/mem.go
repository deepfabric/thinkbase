package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *mem {
	return &mem{mp: make(map[string]value.Array)}
}

func (m *mem) Destroy() error {
	return nil
}

func (m *mem) PopKey() (string, error) {
	m.RLock()
	defer m.RUnlock()
	if len(m.mp) == 0 {
		return "", nil
	}
	for k, _ := range m.mp {
		return k, nil
	}
	return "", nil
}

func (m *mem) Pop(k string) (value.Value, error) {
	m.Lock()
	defer m.Unlock()
	a := m.mp[k]
	if len(a) == 0 {
		return nil, dictVec.NotExist
	}
	r := a[0]
	a[0] = nil
	if len(a[1:]) > 0 {
		m.mp[k] = a[1:]
	} else {
		delete(m.mp, k)
	}
	return r, nil
}

func (m *mem) Head(k string) (value.Value, error) {
	m.RLock()
	defer m.RUnlock()
	a := m.mp[k]
	if len(a) == 0 {
		return nil, dictVec.NotExist
	}
	return a[0], nil
}

func (m *mem) Push(k string, a value.Array) error {
	m.Lock()
	defer m.Unlock()
	if v, ok := m.mp[k]; ok {
		m.mp[k] = append(v, a...)
	} else {
		m.mp[k] = a
	}
	return nil
}

func (m *mem) Pops(k string, n, limit int) (value.Array, error) {
	var r value.Array

	m.Lock()
	defer m.Unlock()
	a, ok := m.mp[k]
	if !ok {
		return nil, nil
	}
	if n <= 0 {
		size := 0
		for size < limit && len(a) > 0 {
			size += a[0].Size()
			r = append(r, a[0])
			a[0] = nil
			a = a[1:]
		}
		if len(a) > 0 {
			m.mp[k] = a
		} else {
			delete(m.mp, k)
		}
		return r, nil
	}
	if len(a) < n {
		n = len(a)
	}
	for i := 0; i < n; i++ {
		r = append(r, a[i])
		a[i] = nil
	}
	if len(a[n:]) > 0 {
		m.mp[k] = a[n:]
	} else {
		delete(m.mp, k)
	}
	return r, nil
}
