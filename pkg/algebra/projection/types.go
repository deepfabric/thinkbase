package projection

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Attribute struct {
	Alias string
	E     extend.Extend
}

type Projection interface {
	Projection() (relation.Relation, error)
}

type projection struct {
	as []*Attribute
	c  context.Context
	r  relation.Relation
}
