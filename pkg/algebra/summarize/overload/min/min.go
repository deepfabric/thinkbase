package min

import "github.com/deepfabric/thinkbase/pkg/algebra/value"

func New() *min {
	return &min{}
}

func (m *min) Reset() {
	m.v = nil
}

func (m *min) Fill(a value.Attribute) error {
	if len(a) == 0 {
		return nil
	}
	if m.v == nil {
		m.v = a[0]
	}
	for _, v := range a {
		if value.Compare(v, m.v) < 0 {
			m.v = v
		}
	}
	return nil
}

func (m *min) Eval() (value.Value, error) {
	return m.v, nil
}
