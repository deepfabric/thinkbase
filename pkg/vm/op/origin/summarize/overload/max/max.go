package max

import (
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *max {
	return &max{}
}

func (m *max) Reset() {
	m.v = nil
}

func (m *max) Fill(a value.Array) error {
	if len(a) == 0 {
		return nil
	}
	for _, v := range a {
		if v.ResolvedType().Oid == types.T_null {
			continue
		}
		if m.v == nil || value.Compare(v, m.v) > 0 {
			m.v = v
		}
	}
	return nil
}

func (m *max) Eval() (value.Value, error) {
	return m.v, nil
}
