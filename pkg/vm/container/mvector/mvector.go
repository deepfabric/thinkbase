package mvector

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

func New() *mvector {
	return &mvector{}
}

func (m *mvector) Destroy() error {
	return nil
}

func (m *mvector) Get(idx int) (*roaring.Bitmap, error) {
	m.RLock()
	defer m.RUnlock()
	return m.ms[idx], nil
}

func (m *mvector) Append(ms []*roaring.Bitmap) error {
	m.Lock()
	defer m.Unlock()
	m.ms = append(m.ms, ms...)
	return nil
}
