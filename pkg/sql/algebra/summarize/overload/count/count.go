package count

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/value"

func New() *count {
	return &count{}
}

func (c *count) Reset() {
	c.cnt = 0
}

func (c *count) Fill(a value.Attribute) error {
	if len(a) == 0 {
		return nil
	}
	c.cnt += int64(len(a))
	return nil
}

func (c *count) Eval() (value.Value, error) {
	return value.NewInt(c.cnt), nil
}
