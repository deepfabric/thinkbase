package max

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/value"

func New() *max {
	return &max{}
}

func (m *max) Reset() {
	m.v = nil
}

func (m *max) Fill(a value.Attribute) error {
	if len(a) == 0 {
		return nil
	}
	if m.v == nil {
		m.v = a[0]
	}
	for _, v := range a {
		if value.Compare(v, m.v) > 0 {
			m.v = v
		}
	}
	return nil
}

func (m *max) Eval() (value.Value, error) {
	return m.v, nil
}
