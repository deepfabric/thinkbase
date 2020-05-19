package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/textranging"

func New() *mem {
	return &mem{mp: make(map[string]*textranging.Ranging)}
}

func (m *mem) Set(id string, mp *textranging.Ranging) {
	//	m.mp[id] = mp
}

func (m *mem) Get(id string) (*textranging.Ranging, bool) {
	mp, ok := m.mp[id]
	return mp, ok
}
