package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *mem {
	return &mem{}
}

func (m *mem) Destroy() error {
	return nil
}

func (m *mem) IsEmpty() (bool, error) {
	m.RLock()
	defer m.RUnlock()
	return len(m.a) == 0, nil
}

func (m *mem) Pop() (value.Value, error) {
	m.Lock()
	defer m.Unlock()
	if len(m.a) == 0 {
		return nil, nil
	}
	r := m.a[0]
	m.a[0] = nil
	m.a = m.a[1:]
	return r, nil
}

func (m *mem) Head() (value.Value, error) {
	m.RLock()
	defer m.RUnlock()
	if len(m.a) == 0 {
		return nil, nil
	}
	return m.a[0], nil
}

func (m *mem) Pops(n, limit int) (value.Array, error) {
	var r value.Array

	m.Lock()
	defer m.Unlock()
	if n <= 0 {
		size := 0
		for size < limit && len(m.a) > 0 {
			size += m.a[0].Size()
			r = append(r, m.a[0])
			m.a[0] = nil
			m.a = m.a[1:]
		}
		return r, nil
	}
	if len(m.a) < n {
		n = len(m.a)
	}
	for i := 0; i < n; i++ {
		r = append(r, m.a[i])
		m.a[i] = nil
	}
	m.a = m.a[n:]
	return r, nil
}

func (m *mem) Append(a value.Array) error {
	m.Lock()
	defer m.Unlock()
	m.a = append(m.a, a...)
	return nil
}
