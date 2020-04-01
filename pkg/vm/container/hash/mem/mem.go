package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/hash"
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(n int, gen func() (vector.Vector, error)) *mem {
	var vs []vector.Vector

	for i := 0; i < n; i++ {
		v, err := gen()
		if err != nil {
			for _, v := range vs {
				v.Destroy()
			}
			return nil
		}
		vs = append(vs, v)
	}
	return &mem{vs: vs}
}

func (m *mem) Destroy() error {
	m.Lock()
	defer m.Unlock()
	for _, v := range m.vs {
		v.Destroy()
	}
	return nil
}

func (m *mem) Pop(idx int) (vector.Vector, error) {
	m.RLock()
	defer m.RUnlock()
	return m.vs[idx], nil
}

func (m *mem) Set(v value.Value) error {
	m.Lock()
	defer m.Unlock()
	key, err := encoding.EncodeValue(v)
	if err != nil {
		return err
	}
	return m.vs[hash.GenHash(key)%len(m.vs)].Append(value.Array{v})
}
