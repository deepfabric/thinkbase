package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

func New() *mem {
	return &mem{mp: make(map[string]*roaring.Bitmap)}
}

func (m *mem) Set(id string, mp *roaring.Bitmap) {
	//	m.mp[id] = mp
}

func (m *mem) Get(id string) (*roaring.Bitmap, bool) {
	mp, ok := m.mp[id]
	return mp, ok
}
