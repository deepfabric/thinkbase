package count

import (
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *count {
	return &count{}
}

func (c *count) Reset() {
	c.cnt = 0
}

func (c *count) Fill(a value.Array) error { // skip NULL
	for _, v := range a {
		if v.ResolvedType().Oid != types.T_null {
			c.cnt++
		}
	}
	return nil
}

func (c *count) Eval() (value.Value, error) {
	return value.NewInt(c.cnt), nil
}
