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

func (m *mem) Len(k string) (int, error) {
	m.RLock()
	defer m.RUnlock()
	return len(m.mp[k]), nil
}

func (m *mem) Get(k string, idx int) (value.Value, error) {
	m.RLock()
	defer m.RUnlock()
	return m.mp[k][idx], nil
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

func (m *mem) PopsAll(n, limit int) (map[string]value.Array, error) {
	rq := make(map[string]value.Array)
	m.Lock()
	defer m.Unlock()
	if n <= 0 {
		if n = m.length(limit / len(m.mp)); n == 0 {
			return nil, nil
		}
	}
	for k, v := range m.mp {
		rq[k] = v[:n]
		m.mp[k] = v[n:]
	}
	return rq, nil
}

func (m *mem) length(limit int) int {
	n := 0
	for _, v := range m.mp {
		size := 0
		if n <= 0 {
			for i, t := range v {
				if size = size + t.Size(); size > limit {
					return n
				}
				n = i + 1
			}
		}
	}
	return n
}
