package projection

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
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
