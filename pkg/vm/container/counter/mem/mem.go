package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *mem {
	return &mem{mp: make(map[string]int)}
}

func (m *mem) Destroy() error {
	return nil
}

func (m *mem) Pops(limit int) (value.Array, error) {
	var a value.Array

	m.Lock()
	defer m.Unlock()
	size := 0
	for k, cnt := range m.mp {
		if size >= limit {
			return a, nil
		}
		v, _, err := encoding.DecodeValue([]byte(k))
		if err != nil {
			return nil, err
		}
		t := v.(value.Value)
		for cnt > 0 && size < limit {
			cnt--
			m.mp[k] = cnt
			a = append(a, t)
			size += t.Size()
		}
	}
	return a, nil
}

func (m *mem) Set(v value.Value) error {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}
	m.mp[string(k)] = 1
	return nil
}

func (m *mem) Del(v value.Value) error {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}
	delete(m.mp, string(k))
	return nil
}

func (m *mem) Get(v value.Value) (int, error) {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return -1, err
	}
	return m.mp[string(k)], nil
}

func (m *mem) Inc(v value.Value) error {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}
	if cnt, ok := m.mp[string(k)]; !ok {
		m.mp[string(k)] = 1
	} else {
		m.mp[string(k)] = cnt + 1
	}
	return nil
}

func (m *mem) Dec(v value.Value) (bool, error) {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return false, err
	}
	if cnt, ok := m.mp[string(k)]; !ok {
		return false, nil
	} else if cnt-1 == 0 {
		delete(m.mp, string(k))
	} else {
		m.mp[string(k)] = cnt - 1
	}
	return true, nil
}

func (m *mem) IncAndGet(v value.Value) (int, error) {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return -1, err
	}
	if cnt, ok := m.mp[string(k)]; !ok {
		m.mp[string(k)] = 1
		return 0, nil
	} else {
		m.mp[string(k)] = cnt + 1
		return cnt, nil
	}
}

func (m *mem) DecAndGet(v value.Value) (int, error) {
	m.Lock()
	defer m.Unlock()
	k, err := encoding.EncodeValue(v)
	if err != nil {
		return -1, err
	}
	if cnt, ok := m.mp[string(k)]; !ok {
		return 0, nil
	} else if cnt-1 == 0 {
		delete(m.mp, string(k))
		return cnt, nil
	} else {
		m.mp[string(k)] = cnt - 1
		return cnt, nil
	}
}
