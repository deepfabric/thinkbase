package mdictionary

import (
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
)

func New() *mdictionary {
	return &mdictionary{mp: make(map[string]*roaring.Bitmap)}
}

func (m *mdictionary) Destroy() error {
	return nil
}

func (m *mdictionary) Set(k string, v *roaring.Bitmap) error {
	m.Lock()
	defer m.Unlock()
	m.mp[k] = v
	return nil
}

func (m *mdictionary) Range(f func(string, *roaring.Bitmap)) {
	m.RLock()
	defer m.RUnlock()
	for k, v := range m.mp {
		f(k, v)
	}
}
