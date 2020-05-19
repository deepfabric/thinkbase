package count

import (
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(typ int32) *count {
	return &count{typ: typ}
}

func (c *count) Reset() {
	c.cnt = 0
}

func (c *count) Fill(a value.Array) error { // skip NULL
	for _, v := range a {
		switch c.typ {
		case types.T_any:
			if v.ResolvedType().Oid != types.T_null {
				c.cnt++
			}
		default:
			if v.ResolvedType().Oid == c.typ {
				c.cnt++
			}
		}
	}
	return nil
}

func (c *count) Eval() (value.Value, error) {
	return value.NewInt(c.cnt), nil
}
