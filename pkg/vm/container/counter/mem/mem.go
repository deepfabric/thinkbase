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
