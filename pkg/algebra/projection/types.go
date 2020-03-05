package projection

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
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
	r  relation.Relation
}
