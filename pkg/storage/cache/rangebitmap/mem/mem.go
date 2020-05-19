package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging"

func New() *mem {
	return &mem{mp: make(map[string]*ranging.Ranging)}
}

func (m *mem) Set(id string, mp *ranging.Ranging) {
	//	m.mp[id] = mp
}

func (m *mem) Get(id string) (*ranging.Ranging, bool) {
	mp, ok := m.mp[id]
	return mp, ok
}
