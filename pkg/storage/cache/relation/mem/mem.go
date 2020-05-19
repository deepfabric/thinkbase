package mem

import vrelation "github.com/deepfabric/thinkbase/pkg/vm/container/relation"

func New() *mem {
	return &mem{mp: make(map[string]vrelation.Relation)}
}

func (m *mem) Set(id string, r vrelation.Relation) {
	m.mp[id] = r
}

func (m *mem) Get(id string) (vrelation.Relation, bool) {
	r, ok := m.mp[id]
	return r, ok
}
