package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *mem {
	return &mem{mp: make(map[string]interface{})}
}

func (m *mem) Destroy() error {
	return nil
}

func (m *mem) IsExit(k value.Value) error {
	m.RLock()
	defer m.RUnlock()
	key, err := encoding.EncodeValue(k)
	if err != nil {
		return err
	}
	if _, ok := m.mp[string(key)]; ok {
		return nil
	}
	return dictionary.NotExist
}

func (m *mem) Set(k value.Value, v interface{}) error {
	m.Lock()
	defer m.Unlock()
	key, err := encoding.EncodeValue(k)
	if err != nil {
		return err
	}
	m.mp[string(key)] = v
	return nil
}

func (m *mem) Get(k value.Value) (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	key, err := encoding.EncodeValue(k)
	if err != nil {
		return nil, err
	}
	if v, ok := m.mp[string(key)]; ok {
		return v, nil
	}
	return nil, dictionary.NotExist
}

func (m *mem) GetOrSet(k value.Value, v interface{}) (bool, interface{}, error) {
	m.Lock()
	defer m.Unlock()
	key, err := encoding.EncodeValue(k)
	if err != nil {
		return false, nil, err
	}
	if value, ok := m.mp[string(key)]; ok {
		return true, value, nil
	}
	m.mp[string(key)] = v
	return false, v, nil
}
