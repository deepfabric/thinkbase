package max

import (
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(typ int32) *max {
	return &max{typ: typ}
}

func (m *max) Reset() {
	m.v = nil
}

func (m *max) Fill(a value.Array) error {
	if len(a) == 0 {
		return nil
	}
	for _, v := range a {
		switch m.typ {
		case types.T_any:
			if v.ResolvedType().Oid == types.T_null {
				continue
			}
		default:
			if v.ResolvedType().Oid != m.typ {
				continue
			}
		}
		if m.v == nil || value.Compare(v, m.v) > 0 {
			m.v = v
		}
	}
	return nil
}

func (m *max) Eval() (value.Value, error) {
	if m.v == nil {
		return value.ConstNull, nil
	}
	return m.v, nil
}
