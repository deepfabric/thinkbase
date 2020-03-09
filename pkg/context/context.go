package context

import "github.com/deepfabric/thinkbase/pkg/algebra/relation"

func New() *context {
	return &context{placeholder: 0}
}

func (c *context) Placeholder() int {
	c.Lock()
	defer c.Unlock()
	c.placeholder++
	return c.placeholder
}

func (c *context) Relation(plh int) relation.Relation {
	if v, ok := c.mp.Load(plh); !ok {
		return nil
	} else {
		return v.(relation.Relation)
	}
}

func (c *context) AddRelation(r relation.Relation) {
	c.mp.Store(r.Placeholder(), r)
}
